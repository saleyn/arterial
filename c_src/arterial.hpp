#pragma once

#include "connection.hpp"
#include "pool_fifo.hpp"
#include "unordered_map_with_ttl.hpp"
#include "wait_list.hpp"
#include <erl_nif.h>
#include <atomic>
#include <set>
#include <vector>
#include <memory>

namespace arterial {

/// @brief Outcome of an asynchronous checkout attempt (CheckOutAsync()).
enum class AsyncCheckoutStatus { Ok, Queued, Rejected };

struct AsyncCheckoutResult {
  AsyncCheckoutStatus      status    = AsyncCheckoutStatus::Rejected;
  Connection*              conn      = nullptr;
  std::vector<ReqID>       ids;
  uint64_t                 waiter_id = 0; // valid only when status == Queued
};

//-----------------------------------------------------------------------------
/// @brief A pool of `Connection` objects checked out/in via a lock-free FIFO.
///
/// The Erlang side owns each connection's socket/protocol process lifecycle;
/// this pool only tracks which connections are currently available, their
/// per-connection backlog of in-flight requests, and their rate throttles.
///
/// The pool also owns a registry of in-flight asynchronous requests, keyed
/// by wire-level ext_req_id, used to notify the owning process with a
/// timeout message if a reply doesn't arrive before the request's TTL; and
/// (if `a_max_waiters` > 0) a bounded queue of asynchronous checkout
/// requests that arrive while every connection is busy, serviced as soon as
/// a connection frees up (see CheckOutAsync(), DrainWaitList()).
//-----------------------------------------------------------------------------
struct ConnectionPool {
  using PooledConnection = PooledObject<Connection>;
  using InflightEntry    = std::pair<ErlNifPid, Connection*>;
  using InflightMap      = unordered_map_with_ttl<ReqID, InflightEntry>;
  using WaiterMap        = unordered_map_with_ttl<uint64_t, ErlNifPid>;

  /// @brief Create a pool of `a_size` connections, each with a backlog of
  /// `a_backlog` in-flight requests, using a FIFO or random-access backlog
  /// depending on `a_fifo`. Connections have no socket and are unavailable
  /// for checkout until SetSocket()+MakeAvailable() are called on them.
  ///
  /// @param a_fixed_ttl_us if non-zero, every tracked in-flight request
  /// times out after this fixed duration (TrackInflight()'s ttl_us argument
  /// is then ignored). If zero, each request uses its own ttl_us, and the
  /// in-flight registry is kept sorted by each request's individual
  /// deadline instead of by a single shared one.
  ///
  /// @param a_max_waiters if non-zero, CheckOutAsync() queues callers that
  /// arrive while the pool is busy (up to this many waiters) instead of
  /// failing immediately; see CheckOutAsync(). Zero disables queuing
  /// entirely (CheckOutAsync() then behaves like CheckOut()).
  ConnectionPool(uint32_t a_size, BaseReqID a_backlog, bool a_fifo,
                 uint64_t a_fixed_ttl_us = 0, size_t a_max_waiters = 0)
  : m_scratch(MakeConnections(a_size, a_backlog, a_fifo))
  , m_pool(m_scratch)
  , m_inflight(a_fixed_ttl_us)
  , m_waiting(0)
  , m_fixed_ttl_us(a_fixed_ttl_us)
  , m_waiters(a_max_waiters)
  {}

  /// @brief Register `req_id` as in-flight, owned by `pid`, on behalf of
  /// `conn` (the connection whose backlog slot `req_id` occupies). Call
  /// once per request right after it's reserved via CheckOut(). `ttl_us`
  /// is the request's own timeout; it's ignored in fixed-TTL mode (see
  /// ctor). On timeout, `conn`'s backlog slot for `req_id` is released
  /// automatically (see SweepTimeouts()).
  void TrackInflight(ReqID req_id, ErlNifPid const& pid, Connection* conn,
                      uint64_t ttl_us, time_val a_now = now_utc())
  {
    auto now_us = uint64_t(a_now.microseconds());
    TrackInflightAt(req_id, pid, conn, now_us + ttl_us);
  }

  /// @brief Like TrackInflight(), but `expire_at` is an absolute deadline
  /// (microseconds) instead of a duration from now -- used to honor a
  /// queued waiter's original deadline (covering both the time spent
  /// queued and the time spent in-flight) once it's serviced. Still
  /// ignored in fixed-TTL mode (a fresh `m_fixed_ttl_us` window starts
  /// now, same as TrackInflight()).
  void TrackInflightAt(ReqID req_id, ErlNifPid const& pid, Connection* conn,
                        uint64_t expire_at)
  {
    auto entry = InflightEntry(pid, conn);
    if (m_fixed_ttl_us)
      m_inflight.try_add(req_id, std::move(entry), uint64_t(now_utc().microseconds()));
    else
      m_inflight.try_add_with_ttl(req_id, std::move(entry), expire_at);
  }

  /// @brief Stop tracking `req_id` (a reply was received, or the request's
  /// backlog slot was otherwise released).
  void UntrackInflight(ReqID req_id) { m_inflight.erase(req_id); }

  /// @brief Like CheckOut(), but if no connection currently qualifies and
  /// the pool was created with `a_max_waiters > 0`, queue the request
  /// instead of failing: as soon as a connection frees up (via CheckIn()
  /// or SweepTimeouts()), the queued caller is serviced and notified with
  /// `{arterial_ready, PoolName, ExtReqID, ConnID, Socket, ReqIDs}` (or, if
  /// it times out first while still queued, `{arterial_timeout, PoolName,
  /// WaiterID}` -- note this uses the internal waiter id, not a wire-level
  /// ext_req_id, since one was never assigned).
  ///
  /// On immediate success, the returned request(s) are also registered in
  /// the in-flight registry (as if TrackInflight() had been called), so
  /// `ttl_us` covers the whole call uniformly regardless of which path is
  /// taken.
  ///
  /// @return {Ok, conn, ids} if a connection was available immediately;
  /// {Queued, nullptr, {}} if no connection qualified but the request was
  /// queued; {Rejected, nullptr, {}} if no connection qualified AND the
  /// wait-list is disabled or full.
  AsyncCheckoutResult
  CheckOutAsync(ErlNifPid const& pid, uint64_t ttl_us, size_t a_samples,
                time_val a_now = now_utc())
  {
    auto [conn, ids] = CheckOut(a_samples, a_now);
    if (conn) {
      for (auto id : ids)
        TrackInflight(id, pid, conn, ttl_us, a_now);
      return {AsyncCheckoutStatus::Ok, conn, std::move(ids)};
    }

    auto now_us = uint64_t(a_now.microseconds());

    WaitEntry entry;
    entry.waiter_id = ++m_next_waiter_id;
    entry.pid       = pid;
    entry.samples   = uint32_t(a_samples);
    entry.expire_at = now_us + ttl_us;

    if (!m_waiters.TryEnqueue(entry))
      return {AsyncCheckoutStatus::Rejected, nullptr, {}, 0};

    m_waiting.try_add_with_ttl(entry.waiter_id, ErlNifPid(pid), entry.expire_at);
    return {AsyncCheckoutStatus::Queued, nullptr, {}, entry.waiter_id};
  }

  /// @brief Try to service queued waiters (see CheckOutAsync()) now that a
  /// connection may have freed up. Pops waiters in FIFO order; for each one
  /// that hasn't already timed out, retries a checkout and, on success,
  /// registers the assigned request(s) in the in-flight registry (under
  /// their real ext_req_id, honoring the waiter's original expire_at, as
  /// an immediately-successful CheckOutAsync() would) and sends
  /// `{arterial_ready, PoolName, ExtReqID, ConnID, Socket, ReqIDs}` to the
  /// waiter's owning process. Stops requeuing once it's cycled through the
  /// whole wait-list once, so a permanently-unserviceable waiter can't
  /// spin this loop forever.
  /// @return the number of waiters serviced.
  size_t DrainWaitList(ErlNifEnv* env, ERL_NIF_TERM pool_name, time_val a_now = now_utc())
  {
    if (!m_waiters.Enabled())
      return 0;

    size_t serviced  = 0;
    size_t requeued  = 0;
    size_t round_cap = m_waiters.Capacity();

    WaitEntry entry;
    while (requeued < round_cap && m_waiters.TryDequeue(entry)) {
      // Skip waiters that already timed out (evicted from m_waiting by a
      // concurrent/prior SweepTimeouts -- nothing to notify, just drop).
      if (m_waiting.find(entry.waiter_id) == m_waiting.end())
        continue;

      auto [conn, ids] = CheckOut(entry.samples, a_now);
      if (!conn) {
        m_waiters.TryEnqueue(entry); // still can't be served; try later
        ++requeued;
        continue;
      }

      m_waiting.erase(entry.waiter_id);
      for (auto id : ids)
        TrackInflightAt(id, entry.pid, conn, entry.expire_at);

      auto* msg_env = enif_alloc_env();
      std::vector<ERL_NIF_TERM> id_terms;
      id_terms.reserve(ids.size());
      for (auto id : ids)
        id_terms.push_back(enif_make_uint(msg_env, id));
      auto id_list = enif_make_list_from_array(msg_env, id_terms.data(), unsigned(id_terms.size()));

      auto msg = enif_make_tuple6(msg_env,
                   enif_make_atom(msg_env, "arterial_ready"),
                   enif_make_copy(msg_env, pool_name),
                   enif_make_uint(msg_env, ids.empty() ? 0 : ids[0]),
                   enif_make_uint(msg_env, conn->ID()),
                   conn->Socket(msg_env),
                   id_list);
      auto pid = entry.pid;
      enif_send(env, &pid, msg_env, msg);
      enif_free_env(msg_env);

      ++serviced;
    }

    return serviced;
  }

  /// @brief Sweep expired queued waiters (see CheckOutAsync()), sending
  /// `{arterial_timeout, PoolName, WaiterID}` to each one's owning process.
  /// The waiter's entry is left in the wait-list ring itself (it has no
  /// O(1) random-removal); DrainWaitList() silently discards it once
  /// popped, since by then it's no longer in m_waiting.
  /// @return the number of waiters that timed out.
  size_t SweepWaiters(ErlNifEnv* env, ERL_NIF_TERM pool_name, time_val a_now = now_utc())
  {
    auto now_us = uint64_t(a_now.microseconds());

    return m_waiting.refresh(now_us,
      [env, pool_name](uint64_t waiter_id, ErlNifPid& pid, uint64_t, uint64_t) {
        auto* msg_env = enif_alloc_env();
        auto  msg     = enif_make_tuple3(msg_env,
                          enif_make_atom(msg_env, "arterial_timeout"),
                          enif_make_copy(msg_env, pool_name),
                          enif_make_uint64(msg_env, waiter_id));
        enif_send(env, &pid, msg_env, msg);
        enif_free_env(msg_env);
        return true; // always evict on sweep
      });
  }

  /// @brief Sweep expired in-flight requests AND expired queued waiters
  /// (see SweepWaiters()), sending a `{arterial_timeout, PoolName, ReqID}`
  /// message (via enif_send) to each owning process and releasing the
  /// in-flight request's backlog slot on its connection (as if
  /// checkin_connection had been called for just that request).
  /// @return the number of in-flight requests that timed out (does not
  /// include expired waiters; see SweepWaiters()'s own return value).
  size_t SweepTimeouts(ErlNifEnv* env, ERL_NIF_TERM pool_name, time_val a_now = now_utc())
  {
    SweepWaiters(env, pool_name, a_now);

    auto now_us = uint64_t(a_now.microseconds());

    return m_inflight.refresh(now_us,
      [this, env, pool_name](ReqID req_id, InflightEntry& entry, uint64_t /*expire*/, uint64_t /*now*/) {
        auto& [pid, conn] = entry;

        auto* msg_env = enif_alloc_env();
        auto  msg     = enif_make_tuple3(msg_env,
                          enif_make_atom(msg_env, "arterial_timeout"),
                          enif_make_copy(msg_env, pool_name),
                          enif_make_uint(msg_env, req_id));
        enif_send(env, &pid, msg_env, msg);
        enif_free_env(msg_env);

        if (conn) {
          conn->Requests().CheckIn(req_id);
          CheckIn(conn->ID(), env, pool_name);
        }

        return true; // always evict on sweep
      });
  }

  /// @brief Return a previously checked-out connection (by id) back to the
  /// pool, making it available again for the next CheckOut(), then try to
  /// service queued waiters (see CheckOutAsync()) with the newly-freed
  /// capacity. `env`/`pool_name` are used only for the latter; pass `env =
  /// nullptr` to skip draining the wait-list (e.g. when called from a
  /// context with no live connection's worth of capacity to offer, or
  /// where waiter notification isn't wanted).
  void CheckIn(uint32_t id, ErlNifEnv* env = nullptr, ERL_NIF_TERM pool_name = 0)
  {
    auto* node = m_pool.Get(id);
    if (node) [[likely]]
      m_pool.CheckIn(*node);

    if (env)
      DrainWaitList(env, pool_name);
  }

  /// @brief Set the connection's current socket term.
  bool SetSocket(uint32_t id, ERL_NIF_TERM socket)
  {
    auto* conn = Get(id);
    if (!conn) [[unlikely]] return false;
    conn->Socket(socket);
    return true;
  }

  /// @brief Mark a connection (by id) as available/unavailable for checkout.
  bool MakeAvailable   (uint32_t id) { return SetAvailable(id, true);  }
  bool MakeUnavailable (uint32_t id) { return SetAvailable(id, false); }

  /// @brief Look up a connection by id (its index in the pool).
  Connection* Get(uint32_t id)
  {
    auto* node = m_pool.Get(id);
    return node ? node->Value() : nullptr;
  }

  /// @brief Remove the next available connection from the head of the FIFO
  /// queue whose throttles and backlog can accommodate `a_samples` new
  /// requests, reserving `a_samples` backlog slots on it.
  /// @return {connection, reserved request ids (wire-level ext_req_id)} or
  /// {nullptr, {}} if no connection currently qualifies.
  std::pair<Connection*, std::vector<ReqID>>
  CheckOut(size_t a_samples, time_val a_now = now_utc());

private:
  std::vector<std::unique_ptr<Connection>> m_scratch;
  ObjectPoolFIFO<Connection>               m_pool;
  InflightMap                              m_inflight;
  WaiterMap                                m_waiting;
  uint64_t                                 m_fixed_ttl_us;
  WaitList                                 m_waiters;
  std::atomic<uint64_t>                    m_next_waiter_id{0};

  bool SetAvailable(uint32_t id, bool available)
  {
    auto* node = m_pool.Get(id);
    return node && (available ? m_pool.MakeAvailable(*node)
                               : m_pool.MakeUnavailable(*node));
  }

  static std::vector<std::unique_ptr<Connection>>
  MakeConnections(uint32_t a_size, BaseReqID a_backlog, bool a_fifo)
  {
    std::vector<std::unique_ptr<Connection>> conns;
    conns.reserve(a_size);
    for (uint32_t i = 0; i < a_size; ++i)
      conns.emplace_back(std::make_unique<Connection>(i, a_backlog, a_fifo));
    return conns;
  }
};

//-----------------------------------------------------------------------------
// Implementation
//-----------------------------------------------------------------------------

inline std::pair<Connection*, std::vector<ReqID>>
ConnectionPool::CheckOut(size_t a_samples, time_val a_now)
{
  std::set<uint32_t> tried_connections;

  while (true) {
    auto* node = m_pool.CheckOut();

    // No available connections
    if (!node)
      break;

    auto* conn = node->Value();

    // Check throttles: all must currently have room for a_samples
    size_t pass_throttle = 0;
    for (auto& t : conn->Throttles()) {
      if (t.available(a_now) < a_samples) break;
      ++pass_throttle;
    }

    // If all throttles pass, check if we have enough free requests left
    // in the backlog of the selected connection. If so, we found the one
    // that can be used
    if (pass_throttle == conn->ThrottlesCount()) {
      auto [success, ids] = conn->Requests().UnsafeReserve(a_samples, a_now.microseconds());

      if (success) {
        for (auto& t : conn->Throttles())
          t.add(a_samples, a_now);
        return std::make_pair(conn, std::move(ids));
      }
    }

    // Did we make a full circle and unsuccessfully tried all connections?
    if (tried_connections.find(conn->ID()) != tried_connections.end()) [[unlikely]] {
      m_pool.CheckIn(*node);
      break;
    }

    tried_connections.insert(conn->ID());

    // Return the connection to the end of the pool and try the next one
    m_pool.CheckIn(*node);
  }

  return std::make_pair(nullptr, std::vector<ReqID>());
}

} // namespace arterial
