///
/// A connection has N requests of type ReqInfo.
///
///
#pragma once

#include "pool_fifo.hpp"
#include "throttle.hpp"
#include "backlog.hpp"
#include <map>

namespace arterial {

using namespace nifpp;

using BackLogValue = BaseReqID; // Type for backlog value
using ReqInfo      = RequestInfo<BaseReqID, ReqID>;

struct ThrottleInit {
  uint32_t rate;
  uint32_t window;
};

struct Connection {
  using Throttle    = basic_time_spacing_throttle<uint32_t>;
  using ThrottleVec = std::vector<time_spacing_throttle>;
  using ReqQueue    = BackLog<ReqInfo>;

  Connection(uint16_t a_id, BackLogValue a_backlog, std::vector<ThrottleInit>&& a_throttles)
  : m_id(a_id)
  , m_max_backlog(a_backlog)
  , m_requests(a_backlog, [this](auto i) {
    m_requests[i] = ReqInfo{.m_req_id = i};
  })
  {
    m_throttles.reserve(a_throttles.size());
    for (auto& t : a_throttles)
      m_throttles.emplace_back(Throttle(t.rate, t.window));
  }

  ReqInfo* Get
  (
    BaseReqID    req_id,
    uint32_t     ttl_us,
    ERL_NIF_TERM pid,
    time_val     now = now_utc()
  )
  {
    if (req_id >= m_requests.size()) [[unlikely]]
      return nullptr;

    auto& req = m_requests[req_id];
    req.Update(now.microseconds(), ttl_us, m_vsn);

    return &req;
  }

  uint32_t     ID()             const { return m_id;        }
  ThrottleVec& Throttles()            { return m_throttles; }
  ReqQueue&    Requests()             { return m_requests;  }
  size_t       ThrottlesCount() const { return m_throttles.size(); }
  TERM         Socket()         const { return m_socket;    }
  void         Socket(TERM socket)    { m_socket = socket;  }

private:
  alignas(64) std::atomic<uint32_t> m_vsn; // Version mask added to the req_count
  uint32_t                          m_id;
  BackLogValue                      m_max_backlog;
  ReqQueue                          m_requests;
  ThrottleVec                       m_throttles;
  TERM                              m_socket;
  std::string                       m_name;
};

} // namespace arterial