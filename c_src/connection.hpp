///
/// A connection has N requests of type ReqInfo.
///
///
#pragma once

#include "pool_fifo.hpp"
#include "throttle.hpp"
#include <erl_nif.h>
#include <map>

namespace arterial {

using BaseReqID = uint16_t;     // Internal request ID
using ReqID     = uint64_t;     // Request ID for sending over the wire

/// @brief Information about a request sent over the current connection
struct ReqInfo {
  BaseReqID m_req_id;
  uint64_t  m_last_req_id;
  uint64_t  m_ts_create;
  uint64_t  m_ts_expire;
  ErlNifPid m_pid;
};

struct ThrottleInit {
  uint32_t rate;
  uint32_t window;
};

struct Connection {
  using RequestVec  = std::vector<ReqInfo>;
  using Throttle    = time_spacing_throttle;
  using ThrottleVec = std::vector<time_spacing_throttle>;

  Connection(uint16_t id, uint16_t backlog_size, std::vector<ThrottleInit>&& throttles)
  : m_id(id)
  , m_max_backlog(backlog_size)
  , m_requests(backlog_size, [this](auto i) {
    m_requests[i] = ReqInfo{.m_req_id = i};
  })
  {
    m_throttles.reserve(throttles.size());
    for (auto& t : throttles)
      m_requests.emplace_back(Throttle(t.rate, t.window));
  }

  std::pair<ReqID, ReqInfo*> Get
  (
    BaseReqID    req_id,
    uint32_t     ttl_us,
    ERL_NIF_TERM pid,
    time_val     now = now_utc()
  )
  {
    if (req_id >= m_requests.size()) [[unlikely]]
      return std::make_pair(0, nullptr);

    auto  req_id = EncodeReqID(req_id);
    auto& req    = m_requests[req_id];

    req.m_last_req_id = req_id;
    req.m_ts_create   = now.microseconds();
    req.m_ts_expire   = req.m_ts_create + ttl_us;

    return std::make_pair(req_id, &req)
  }

  ReqID EncodeReqID(BaseReqID req_id)
  {
    assert(req_id <= m_max_backlog);

    return ReqID((++m_vsn) << 32) & 0x00FFFFFF00000000ull
         | ((m_id << 16) & 0xFFFF0000)
         | (req_id & 0xFFFF);
  }
private:
  alignas(64) std::atomic<uint32_t> m_vsn; // Version mask added to the req_count
  uint32_t                          m_id;
  uint32_t                          m_max_backlog;
  ObjectPoolLIFO<ReqInfo>           m_requests;
  ThrottleVec                       m_throttles;
};

} // namespace arterial