///
/// A connection has a fixed backlog capacity of in-flight requests, tracked
/// as a single atomic counter (the owning Erlang process -- one
/// `arterial_conn_owner` gen_server per connection -- tracks individual
/// request identity/ordering/timeouts itself; this layer only needs to know
/// whether the connection has room for one more), and at most one optional
/// rate throttle that must pass before the connection can be used to send
/// new requests.
///
#pragma once

#include "throttle.hpp"
#include <atomic>
#include <erl_nif.h>
#include <cstdint>
#include <memory>
#include <optional>

namespace arterial {

struct ThrottleInit {
  uint32_t rate;
  uint32_t window_msec;
};

//-----------------------------------------------------------------------------
/// @brief A pool connection slot: backlog capacity accounting (one atomic
/// counter) plus an optional rate throttle. The socket itself is owned
/// entirely on the Erlang side (by the connection's `arterial_conn_owner`
/// process) -- this layer never stores or hands back a socket term.
//-----------------------------------------------------------------------------
struct Connection {
  using Throttle = basic_time_spacing_throttle<uint32_t>;

  /// @param a_id       unique connection id (index in the owning pool)
  /// @param a_backlog  max number of in-flight requests on this connection
  /// @param a_throttle rate throttle that must pass before checkout, if any
  Connection(uint32_t a_id, uint32_t a_backlog,
             std::optional<ThrottleInit> a_thr = std::nullopt)
  : m_id(a_id)
  , m_backlog_capacity(a_backlog)
  {
    if (a_thr)
      m_throttle = std::make_unique<Throttle>(a_thr->rate, a_thr->window_msec);
  }

  Connection(Connection const&)            = delete;
  Connection& operator=(Connection const&) = delete;

  uint32_t ID() const { return m_id; }

  /// @brief True if this connection currently accepts new checkouts.
  /// Replaces pool_fifo.hpp's MakeAvailable()/MakeUnavailable() lazy-
  /// removal-from-ring scheme -- there is no ring anymore (selection is a
  /// plain round-robin index, see ConnectionPool::CheckOut() in
  /// arterial.hpp), so a connection is simply skipped by the selection
  /// loop while disabled, with no enqueue/dequeue bookkeeping needed.
  bool Enabled() const { return m_enabled.load(std::memory_order_acquire); }
  void SetEnabled(bool v) { m_enabled.store(v, std::memory_order_release); }

  /// @brief Attempt to reserve `a_samples` backlog slots, all-or-nothing,
  /// honoring both the optional throttle and the connection's fixed
  /// backlog capacity.
  /// @return true if `a_samples` slots were reserved; false if throttled
  /// or the backlog doesn't have room (no partial reservation left behind
  /// either way).
  bool TryReserve(size_t a_samples, uint64_t now_us)
  {
    if (m_throttle) {
      auto now = time_val(nsecs(int64_t(now_us) * 1000));
      if (!m_throttle->TryReserve(uint32_t(a_samples), now))
        return false;
    }

    // CAS loop (erlsem's sema_nif.cpp try_to_take() pattern), not a blind
    // fetch_add + unwind-on-overshoot: the latter briefly publishes an
    // over-capacity count to any concurrent reader/CheckIn racing this
    // call, and always pays a write even on the failing path. This never
    // stores anything unless the reservation actually fits.
    auto n = int32_t(a_samples);
    auto x = m_inflight.load(std::memory_order_relaxed);
    while (x + n <= int32_t(m_backlog_capacity)) {
      if (m_inflight.compare_exchange_weak(x, x + n,
            std::memory_order_acq_rel, std::memory_order_relaxed))
        return true;
      // CAS failed (lost the race to a concurrent reserve/checkin) -- x was
      // refreshed with the current value by compare_exchange_weak itself;
      // retry the capacity check against it.
    }
    return false;
  }

  /// @brief Release `n` previously reserved backlog slots (default 1).
  void CheckIn(uint32_t n = 1)
  {
    m_inflight.fetch_sub(int32_t(n), std::memory_order_acq_rel);
  }

  /// @brief True if this connection has zero in-flight requests right now.
  bool Drained() const { return m_inflight.load(std::memory_order_acquire) == 0; }

private:
  uint32_t                  m_id;
  uint32_t                  m_backlog_capacity;
  std::atomic<int32_t>      m_inflight{0};
  std::unique_ptr<Throttle> m_throttle; // nullptr if no throttle configured
  std::atomic<bool>         m_enabled{false};
};

} // namespace arterial
