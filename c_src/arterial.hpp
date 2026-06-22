//-----------------------------------------------------------------------------
/// \file   arterial.hpp
/// \author Serge Aleynikov
//-----------------------------------------------------------------------------
/// \brief Lock-free connection pool engine backing the `arterial_nif` NIF.
///
/// `ConnectionPool` selects `Connection`s by plain round-robin index over a
/// fixed array -- no exclusive claim/release at all, shackle-style: many
/// callers can be inside a checkout attempt on the SAME connection at the
/// same time. Each `Connection` is independently safe under that via two
/// composed lock-free pieces: an atomic in-flight counter bounded by a fixed
/// backlog capacity (connection.hpp) and a CAS-loop rate throttle
/// (throttle.hpp).
///
/// Individual request identity, ordering, demuxing of replies, and
/// timeout/disconnect notification are NOT this layer's job anymore: each
/// connection has exactly one dedicated Erlang process (`arterial_conn_owner`)
/// that owns its socket and does all wire I/O, tracking its own pending
/// requests directly. This pool only ever needs to know "does this
/// connection have room for N more in-flight requests right now" -- a single
/// atomic counter per connection, not a per-slot backlog structure. This
/// replaced an earlier design where every checkout claimed an individually
/// tracked, wire-level request id from a per-connection backlog (a CAS-bitmap
/// or spinlocked ring, depending on whether the protocol carried its own
/// ids) and the calling Erlang process did raw socket I/O directly -- correct
/// at backlog=1 (only one process ever held a connection's only reservation),
/// but unsafe at backlog>1 once multiple different processes could legally
/// hold concurrent reservations on the same connection and then each
/// independently `recv` off the same shared socket with no coordination.
///
/// On top of selection, the pool also provides:
///   - an optional bounded queue-when-busy wait-list (wait_list.hpp) for
///     asynchronous checkouts that arrive while every connection is busy,
///     with its own TTL registry (sharded_ttl_map.hpp) so a queued waiter
///     that times out before being serviced is notified instead of waiting
///     forever;
///   - process-death monitoring: every checkout (synchronous or
///     asynchronous, immediate or served later from the wait-list) is
///     tracked against the calling Erlang process via `enif_monitor_process`,
///     so a process that dies while still holding a reservation has it
///     released automatically instead of stranding the connection forever
///     (see MonitorOwner(), ReleaseOwnerReservation(), OnProcessDown(),
///     owner_table.hpp) -- the same semaphore-with-process-monitoring
///     pattern as https://github.com/x0id/erlsem's sema_nif.cpp (atomic
///     count + monitor-on-first-acquire + release-on-proc_down), just
///     scoped per-connection instead of one global counter.
//-----------------------------------------------------------------------------
#pragma once

#include "connection.hpp"
#include "owner_table.hpp"
#include "sharded_ttl_map.hpp"
#include "wait_list.hpp"
#include <erl_nif.h>
#include <atomic>
#include <vector>
#include <memory>

namespace arterial {

/// @brief Outcome of an asynchronous checkout attempt (CheckOutAsync()).
enum class AsyncCheckoutStatus { Ok, Queued, Rejected };

struct AsyncCheckoutResult {
  AsyncCheckoutStatus      status    = AsyncCheckoutStatus::Rejected;
  Connection*              conn      = nullptr;
  uint64_t                 waiter_id = 0; // valid only when status == Queued
};

//-----------------------------------------------------------------------------
/// @brief A pool of `Connection` objects checked out/in via round-robin
/// selection.
///
/// The Erlang side owns each connection's socket/protocol process lifecycle
/// entirely (one `arterial_conn_owner` process per connection); this pool
/// only tracks which connections are currently available, their
/// per-connection in-flight count, and their rate throttles.
///
/// (if `a_max_waiters` > 0) a bounded queue of asynchronous checkout
/// requests that arrive while every connection is busy, serviced as soon as
/// a connection frees up (see CheckOutAsync(), DrainWaitList()).
//-----------------------------------------------------------------------------
struct ConnectionPool {
  using WaiterMap = ShardedTTLMap<uint64_t, ErlNifPid>;

  /// @brief Create a pool of `a_size` connections, each with a backlog
  /// capacity of `a_backlog` in-flight requests. Connections have no socket
  /// and are unavailable for checkout until MakeAvailable() is called on
  /// them (the socket itself is handed directly to the connection's
  /// `arterial_conn_owner` process, never through this pool).
  ///
  /// @param a_max_waiters if non-zero, CheckOutAsync() queues callers that
  /// arrive while the pool is busy (up to this many waiters) instead of
  /// failing immediately; see CheckOutAsync(). Zero disables queuing
  /// entirely (CheckOutAsync() then behaves like CheckOut()).
  ///
  /// @param a_ttl_shards shard count for the waiter registry
  /// (ShardedTTLMap, sharded_ttl_map.hpp). Zero (the default) picks
  /// ShardedTTLMap::DefaultShardCount(), scaled off
  /// std::thread::hardware_concurrency() -- pass a specific value to
  /// override that guess (e.g. if many pools share a small machine, or
  /// one pool alone should claim more shards on a large one).
  ConnectionPool(uint32_t a_size, uint32_t a_backlog,
                 size_t a_max_waiters = 0, size_t a_ttl_shards = 0)
  : m_conns(MakeConnections(a_size, a_backlog))
  , m_waiting(0, a_ttl_shards)
  , m_waiters(a_max_waiters)
  , m_owners(size_t(a_size) * size_t(a_backlog))
  {}

  /// @brief Like CheckOut(), but if no connection currently qualifies and
  /// the pool was created with `a_max_waiters > 0`, queue the request
  /// instead of failing: as soon as a connection frees up (via CheckIn()),
  /// the queued caller is serviced and notified with
  /// `{arterial_ready, PoolName, ConnID}` (or, if it times out first while
  /// still queued, `{arterial_timeout, PoolName, WaiterID}` -- the internal
  /// waiter id, since no wire-level id is ever assigned by this layer
  /// anymore).
  ///
  /// Opportunistically sweeps already-expired waiters (see SweepWaiters())
  /// before enqueuing this one -- there is no separate periodic sweep
  /// timer, so without this a waiter that times out while no connection
  /// is ever checked back in again would simply never be notified.
  ///
  /// @return {Ok, conn} if a connection was available immediately;
  /// {Queued, nullptr, WaiterID} if no connection qualified but the request
  /// was queued; {Rejected, nullptr, 0} if no connection qualified AND the
  /// wait-list is disabled or full.
  AsyncCheckoutResult
  CheckOutAsync(ErlNifEnv* env, ERL_NIF_TERM pool_name, ErlNifPid const& pid,
                size_t a_samples, time_val a_now = now_utc())
  {
    auto* conn = CheckOut(a_samples, a_now);
    if (conn) {
      if (!MonitorOwner(env, pid, conn->ID(), uint32_t(a_samples))) [[unlikely]] {
        conn->CheckIn(uint32_t(a_samples));
        return {AsyncCheckoutStatus::Rejected, nullptr, 0};
      }
      return {AsyncCheckoutStatus::Ok, conn, 0};
    }

    SweepWaiters(env, pool_name, a_now);

    auto now_us = uint64_t(a_now.microseconds());

    WaitEntry entry;
    entry.waiter_id = ++m_next_waiter_id;
    entry.pid       = pid;
    entry.samples   = uint32_t(a_samples);
    entry.expire_at = now_us + m_default_waiter_ttl_us;

    if (!m_waiters.TryEnqueue(entry))
      return {AsyncCheckoutStatus::Rejected, nullptr, 0};

    m_waiting.try_add_with_ttl(entry.waiter_id, ErlNifPid(pid), entry.expire_at);
    return {AsyncCheckoutStatus::Queued, nullptr, entry.waiter_id};
  }

  /// @brief Try to service queued waiters (see CheckOutAsync()) now that a
  /// connection may have freed up. Pops waiters in FIFO order; for each one
  /// that hasn't already timed out, retries a checkout and, on success,
  /// sends `{arterial_ready, PoolName, ConnID}` to the waiter's owning
  /// process. Stops requeuing once it's cycled through the whole wait-list
  /// once, so a permanently-unserviceable waiter can't spin this loop
  /// forever.
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
      // concurrent/prior SweepWaiters -- nothing to notify, just drop).
      if (!m_waiting.contains(entry.waiter_id))
        continue;

      auto* conn = CheckOut(entry.samples, a_now);
      if (!conn) {
        m_waiters.TryEnqueue(entry); // still can't be served; try later
        ++requeued;
        continue;
      }

      m_waiting.erase(entry.waiter_id);

      // The waiter now owns this connection's reserved backlog slot(s),
      // same as a direct CheckOut()/CheckOutAsync() caller; monitor it so
      // a dead waiter can't strand them either (see OnProcessDown()). If
      // the waiter is already gone (lost the race with its own death),
      // unwind the reservation instead of notifying a dead pid.
      if (!MonitorOwner(env, entry.pid, conn->ID(), entry.samples)) [[unlikely]] {
        conn->CheckIn(entry.samples);
        continue;
      }

      auto* msg_env = enif_alloc_env();
      auto  msg     = enif_make_tuple3(msg_env,
                        enif_make_atom(msg_env, "arterial_ready"),
                        enif_make_copy(msg_env, pool_name),
                        enif_make_uint(msg_env, conn->ID()));
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

  /// @brief Return a previously checked-out connection (by id) back to the
  /// pool, releasing `a_samples` in-flight slots, then try to service
  /// queued waiters (see CheckOutAsync()) with the newly-freed capacity.
  /// `env`/`pool_name` are used only for the latter; pass `env = nullptr`
  /// to skip draining the wait-list.
  void CheckIn(uint32_t id, uint32_t a_samples = 1,
               ErlNifEnv* env = nullptr, ERL_NIF_TERM pool_name = 0)
  {
    auto* conn = Get(id);
    if (conn)
      conn->CheckIn(a_samples);
    if (env)
      DrainWaitList(env, pool_name);
  }

  /// @brief Mark a connection (by id) as available/unavailable for checkout.
  bool MakeAvailable   (uint32_t id) { return SetEnabled(id, true);  }
  bool MakeUnavailable (uint32_t id) { return SetEnabled(id, false); }

  /// @brief Returns true if connection `id` currently has zero in-flight
  /// requests (i.e. safe to disconnect without abandoning any pending
  /// request). Used to implement a "bounce"/disconnect cycle: mark a
  /// connection unavailable (stops new checkouts from selecting it), poll
  /// this until it drains, then disconnect/reconnect it.
  /// @return false if `id` does not name a connection in this pool.
  bool IsDrained(uint32_t id)
  {
    auto* conn = Get(id);
    return conn && conn->Drained();
  }

  /// @brief Look up a connection by id (its index in the pool).
  Connection* Get(uint32_t id)
  {
    return id < m_conns.size() ? m_conns[id].get() : nullptr;
  }

  /// @brief Try every connection at most once, starting from a shared
  /// round-robin cursor, looking for one that's enabled and whose
  /// throttles+backlog can accommodate `a_samples` new requests; reserves
  /// `a_samples` in-flight slots on the first one that qualifies.
  /// @return the connection, or nullptr if none currently qualifies.
  Connection* CheckOut(size_t a_samples, time_val a_now = now_utc());

  /// @brief Record that `pid` now owns `count` reservation slots on
  /// connection `conn_id` (as just returned by CheckOut()/CheckOutAsync()),
  /// and start monitoring `pid` if this is its first reservation in this
  /// pool. Call once per checkout.
  /// @return false if `pid` is already dead (the monitor attempt failed);
  /// the caller should treat this like a checkout that must be unwound.
  bool MonitorOwner(ErlNifEnv* env, ErlNifPid const& pid, uint32_t conn_id, uint32_t count)
  {
    bool monitor_failed = false;

    bool claimed = m_owners.WithEntry(pid, [&](OwnerEntry& entry) {
      // Arm the monitor on a fresh entry, under the slot's lock, so two
      // threads racing to create `pid`'s first reservation can't both call
      // enif_monitor_process() on it.
      if (!entry.monitored) {
        if (enif_monitor_process(env, this, &pid, &entry.mon) != 0) {
          monitor_failed = true; // pid already gone
          return;
        }
        entry.monitored = true;
      }
      entry.by_conn[conn_id] += count;
    });

    if (claimed && monitor_failed) [[unlikely]]
      // The slot WithEntry() just claimed (or found already-empty) for
      // `pid` would otherwise sit there forever -- `pid` is already dead,
      // so nothing will ever come along to release it via
      // ReleaseOwnerReservation()/OnProcessDown(). Give it back now.
      m_owners.TakeEntryIf(pid,
        [](OwnerEntry const& entry) { return entry.by_conn.empty(); },
        [](OwnerEntry&&) {});

    return claimed && !monitor_failed;
  }

  /// @brief Drop `count` reservation slots (held on `conn_id`) from `pid`'s
  /// bookkeeping (the mirror image of MonitorOwner()). Call once per
  /// checkin, with the count just checked in. Demonitors `pid` once it no
  /// longer owns any reservation in this pool.
  void ReleaseOwnerReservation(ErlNifEnv* env, ErlNifPid const& pid,
                                uint32_t conn_id, uint32_t count)
  {
    bool found = m_owners.WithEntry(pid, [&](OwnerEntry& entry) {
      auto conn_it = entry.by_conn.find(conn_id);
      if (conn_it != entry.by_conn.end()) {
        if (conn_it->second <= count)
          entry.by_conn.erase(conn_it);
        else
          conn_it->second -= count;
      }
    });

    if (!found) [[unlikely]]
      return;

    // Only take (and demonitor) the entry if it's *still* empty under the
    // slot's lock -- another thread may have added a fresh reservation
    // (via MonitorOwner()) for the same pid between the update above and
    // here, in which case TakeEntryIf()'s predicate fails and nothing is
    // removed.
    m_owners.TakeEntryIf(pid,
      [](OwnerEntry const& entry) { return entry.by_conn.empty(); },
      [&](OwnerEntry&& entry) {
        if (entry.monitored)
          enif_demonitor_process(env, this, &entry.mon);
      });
  }

  /// @brief Called (via the resource's `down` event) when a process that
  /// owns one or more reservations in this pool dies. Releases every
  /// in-flight slot it still held -- across every connection -- exactly as
  /// if checkin_connection had been called for each one. The direct
  /// per-connection analogue of erlsem's `gc_pid` (atomic count
  /// fetch_sub on death), scoped per connection instead of one global
  /// counter.
  ///
  /// Does NOT drain the queue-when-busy wait-list: doing so means sending
  /// a `{arterial_ready, PoolName, ...}` message, which needs the pool's
  /// Erlang-side atom name -- not available here, since `down` fires
  /// asynchronously with no Erlang call (and thus no caller-supplied
  /// pool name) in progress. A waiter queued behind a connection released
  /// this way is instead serviced lazily, the next time anyone calls
  /// checkin_connection/2 on this pool (which already drains the
  /// wait-list itself).
  void OnProcessDown(ErlNifPid const& pid)
  {
    // The monitor already fired (that's why we're here) -- no need to
    // enif_demonitor_process() it.
    m_owners.TakeEntry(pid, [&](OwnerEntry&& entry) {
      for (auto& [conn_id, count] : entry.by_conn) {
        auto* conn = Get(conn_id);
        if (conn)
          conn->CheckIn(count);
      }
    });
  }

private:
  std::vector<std::unique_ptr<Connection>> m_conns;
  std::atomic<uint64_t>                    m_cursor{0};
  WaiterMap                                m_waiting;
  WaitList                                 m_waiters;
  std::atomic<uint64_t>                    m_next_waiter_id{0};
  OwnerTable                                m_owners;
  static constexpr uint64_t                m_default_waiter_ttl_us = 5'000'000;

  bool SetEnabled(uint32_t id, bool enabled)
  {
    auto* conn = Get(id);
    if (!conn) [[unlikely]] return false;
    conn->SetEnabled(enabled);
    return true;
  }

  static std::vector<std::unique_ptr<Connection>>
  MakeConnections(uint32_t a_size, uint32_t a_backlog)
  {
    std::vector<std::unique_ptr<Connection>> conns;
    conns.reserve(a_size);
    for (uint32_t i = 0; i < a_size; ++i)
      conns.emplace_back(std::make_unique<Connection>(i, a_backlog));
    return conns;
  }
};

//-----------------------------------------------------------------------------
// Implementation
//-----------------------------------------------------------------------------

inline Connection*
ConnectionPool::CheckOut(size_t a_samples, time_val a_now)
{
  if (m_conns.empty())
    return nullptr;

  auto now_us = uint64_t(a_now.microseconds());
  auto start  = m_cursor.fetch_add(1, std::memory_order_relaxed) % m_conns.size();

  for (size_t i = 0; i < m_conns.size(); ++i) {
    auto* conn = m_conns[(start + i) % m_conns.size()].get();

    if (!conn->Enabled())
      continue;

    if (conn->TryReserve(a_samples, now_us))
      return conn;
  }

  return nullptr;
}

} // namespace arterial
