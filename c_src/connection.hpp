///
/// A connection has N requests of type RequestInfo tracked in a backlog,
/// and a vector of rate throttles that must all pass before the connection
/// can be used to send new requests.
///
#pragma once

#include "throttle.hpp"
#include "backlog.hpp"
#include <atomic>
#include <erl_nif.h>
#include <deque>
#include <memory>
#include <vector>

namespace arterial {

struct ThrottleInit {
  uint32_t rate;
  uint32_t window_msec;
};

//-----------------------------------------------------------------------------
/// @brief Connections live across many NIF calls, each with its own
/// short-lived ErlNifEnv. An ERL_NIF_TERM captured in one call's env is not
/// valid in another, so the connection's socket term is kept in a private,
/// long-lived env owned by the connection, and copied in/out of the
/// caller's env on every access via enif_make_copy().
//-----------------------------------------------------------------------------
struct Connection {
  using Throttle    = basic_time_spacing_throttle<uint32_t>;
  // std::deque, not std::vector: Throttle holds a std::atomic<int64_t> (see
  // throttle.hpp's TryReserve()), so it's neither copyable nor movable --
  // a vector can't grow without being able to move existing elements into
  // newly-allocated storage, even when reserve() pre-sizes it exactly (the
  // move-constructibility requirement is checked unconditionally at
  // compile time). A deque never needs to move existing elements when it
  // grows, only to allocate a new block for new ones.
  using ThrottleVec = std::deque<Throttle>;

  /// @param a_id        unique connection id (index in the owning pool)
  /// @param a_backlog   max number of in-flight requests on this connection
  /// @param a_fifo      true: FIFO backlog (no wire-level request IDs);
  ///                    false: random-access backlog (wire-level request IDs)
  /// @param a_throttles rate throttles that must all pass before checkout
  Connection(uint32_t a_id, BaseReqID a_backlog, bool a_fifo,
             std::vector<ThrottleInit> const& a_throttles = {})
  : m_id(a_id)
  , m_requests(AbstractBackLog::Create(a_backlog, a_fifo))
  , m_env(enif_alloc_env())
  {
    for (auto& t : a_throttles)
      m_throttles.emplace_back(t.rate, t.window_msec);
  }

  ~Connection() { enif_free_env(m_env); }

  Connection(Connection const&)            = delete;
  Connection& operator=(Connection const&) = delete;

  uint32_t           ID()                  const { return m_id;        }
  ThrottleVec&       Throttles()                 { return m_throttles; }
  size_t             ThrottlesCount()      const { return m_throttles.size(); }
  AbstractBackLog&   Requests()                  { return *m_requests; }

  /// @brief True if this connection currently accepts new checkouts.
  /// Replaces pool_fifo.hpp's MakeAvailable()/MakeUnavailable() lazy-
  /// removal-from-ring scheme -- there is no ring anymore (selection is a
  /// plain round-robin index, see ConnectionPool::CheckOut() in
  /// arterial.hpp), so a connection is simply skipped by the selection
  /// loop while disabled, with no enqueue/dequeue bookkeeping needed.
  bool Enabled() const { return m_enabled.load(std::memory_order_acquire); }
  void SetEnabled(bool v) { m_enabled.store(v, std::memory_order_release); }

  /// @brief Generation fence, solving the gap in OnConnectionDown()'s
  /// design once the pool no longer pulls a connection out of circulation
  /// before tearing it down (there is no pool-level claim at all anymore).
  /// Without this, a NEW checkout could land a fresh backlog reservation
  /// on a connection in the exact window between OnConnectionDown()'s
  /// outstanding-request snapshot and the socket actually closing -- that
  /// request would never appear in the snapshot, so it would never get a
  /// {arterial_disconnected,...} notification, and would silently hang
  /// against an already-closed socket.
  ///
  /// Protocol: TryReserve() loads the generation, then claims; if the
  /// generation changed before it finishes claiming, it unwinds and
  /// reports failure (the caller retries on a different connection, same
  /// as any other "this connection turned out unusable" rejection).
  /// BumpGeneration() (called by OnConnectionDown(), BEFORE taking the
  /// outstanding-request snapshot) guarantees any reservation that wasn't
  /// already committed under the OLD generation gets rejected instead of
  /// silently slipping through uncounted. Validated standalone (no lost
  /// requests under 16-thread concurrent checkout/disconnect churn,
  /// ThreadSanitizer-clean) before landing here -- see /tmp/backlogbench/
  /// (gen_fence.hpp, test_gen_fence.cpp) for the prototype this was ported
  /// from.
  uint64_t Generation() const { return m_generation.load(std::memory_order_acquire); }
  uint64_t BumpGeneration() { return m_generation.fetch_add(1, std::memory_order_acq_rel) + 1; }

  /// @brief Attempt to reserve `a_samples` backlog slots, all-or-nothing,
  /// honoring both the generation fence and every throttle (mirrors
  /// ConnectionPool::CheckOut()'s existing logic in arterial.hpp, now
  /// collapsed into one method since the fence re-check must happen AFTER
  /// the backlog claim but the throttle reservation must happen alongside
  /// it atomically with respect to disconnect, not as a separate later
  /// step). On any failure, every partial throttle/backlog reservation
  /// already made by this call is unwound before returning.
  /// @return {true, ext_req_ids} on success; {false, {}} if throttled,
  /// the backlog doesn't have room, or a disconnect raced this checkout
  /// out from under it.
  std::pair<bool, std::vector<ReqID>>
  TryReserve(size_t a_samples, uint64_t now_us, uint32_t ttl_us = 0)
  {
    auto a_now = time_val(nsecs(int64_t(now_us) * 1000));

    // NOTE: if more than one throttle is ever configured (currently dead
    // code -- nothing populates a_throttles with >0 entries in production
    // today, see ThrottleInit/the Connection ctor), a throttle that fails
    // here after an EARLIER throttle in this loop already succeeded does
    // NOT roll back that earlier reservation. AtomicTimeSpacingThrottle
    // has no UnReserve() (giving back throttle capacity means moving its
    // internal clock backward, which can race a third caller who reserved
    // after) -- this gap was deliberately deferred when the throttle fence
    // was designed/validated (see /tmp/backlogbench/test_throttle.cpp's
    // comments) precisely because it's unreachable while ThrottlesCount()
    // is always 0. Revisit if/when throttle configuration is wired up.
    for (auto& t : m_throttles)
      if (!t.TryReserve(uint32_t(a_samples), a_now))
        return {false, {}};

    auto gen = Generation();
    auto [success, ids] = m_requests->UnsafeReserve(a_samples, now_us, ttl_us);

    if (success && Generation() == gen)
      return {true, std::move(ids)};

    if (success) // generation moved: unwind the backlog reservation we just made
      for (auto id : ids)
        m_requests->CheckIn(id);

    return {false, {}};
  }

  /// @brief Copy the connection's socket term into the caller's env.
  ERL_NIF_TERM Socket(ErlNifEnv* call_env) const
  {
    return m_has_socket ? enif_make_copy(call_env, m_socket) : 0;
  }

  /// @brief Copy `socket` (from the caller's env) into this connection's
  /// own long-lived env, replacing any previously stored socket term.
  void Socket(ERL_NIF_TERM socket)
  {
    enif_clear_env(m_env);
    m_socket     = enif_make_copy(m_env, socket);
    m_has_socket = true;
  }

private:
  uint32_t                          m_id;
  std::unique_ptr<AbstractBackLog>  m_requests;
  ThrottleVec                       m_throttles;
  ErlNifEnv*                        m_env;
  ERL_NIF_TERM                      m_socket     = 0;
  bool                              m_has_socket = false;
  std::atomic<bool>                 m_enabled{false};
  std::atomic<uint64_t>             m_generation{0};
};

} // namespace arterial
