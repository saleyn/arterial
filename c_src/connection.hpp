#include "pool_fifo.hpp"
#include <erl_nif.h>
#include <map>

namespace arterial {

struct ReqInfo {
  uint32_t  m_req_id;
  uint64_t  m_ts_create;
  uint64_t  m_ts_expire;
  ErlNifPid m_pid;
};

struct Connection {
  using ReqMap = std::unordered_map<uint32_t, ReqInfo>;

  uint64_t EncodeReqID(uint32_t req_id)
  {
    assert(req_id <= m_max_backlog);

    return uint64_t((++m_vsn) << 32) & 0xFFFFFFFF00000000ull
         | (m_id << 16)
         | (req_id & 0xFFFF);
  }
private:
  alignas(64) std::atomic<uint32_t> m_vsn; // Version mask added to the req_count
  uint32_t                          m_id;
  uint32_t                          m_max_backlog;
  ReqMap                            m_requests;
};

} // namespace arterial