#pragma once

#include "connection.hpp"
#include "pool_fifo.hpp"
#include "nifpp.h"

namespace arterial {

struct ConnectionPool {
  using PooledConnection = PooledObject<Connection>;

  ConnectionPool(uint32_t size, uint16_t) : m_pool(size) {}

  /// This node must be one of the m_nodes owned by the pool.
  void CheckIn(const Connection& conn);

  /// @brief Remove the next available node from the head of the FIFO queue
  std::pair<Connection*, std::vector<BaseReqID>>
  CheckOut(size_t a_samples, time_val a_now = now_utc());
  
private:
  ObjectPoolFIFO<PooledConnection> m_pool;
};

//-----------------------------------------------------------------------------
// Implementation
//-----------------------------------------------------------------------------

std::pair<Connection*, std::vector<BaseReqID>>
ConnectionPool::CheckOut(size_t a_samples, time_val a_now)
{
  std::set<uint32_t> tried_connections;

  while (true) {
    auto p = m_pool.CheckOut();

    // No available connections
    if (!p)
      break;

    auto pass_throttle = 0;

    // Check throttles
    for (auto& t : p->Throttles) {
      auto ok = t.calc_available(a_samples, a_now) == a_samples;
      if (!ok) break;
      ++pass_throttle;
    }

    // If all throttles pass, check if we have enough free requests left
    // in the backlog of the selected connection. If so, we found the one
    // that can be used
    if (pass_throttle == p->ThrottlesCount()) {
      auto [success, ids] = p->Requests().UnsafeReserve(a_samples);

      if (success)
        return std::make_pair(p, ids);
    }

    // Return the connection to the end of the pool and try the next one
    m_pool.CheckIn(*p);

    // Did we make a full circle and unsuccessfully tried all connections?
    if (tried_connections.find(p->ID()) != tried_connections.end()) [[unlikely]]
      break;

    tried_connections.insert(p->ID());    
  }

  return std::make_pair(nullptr, std::vector<BaseReqID>());
}

} // namespace arterial