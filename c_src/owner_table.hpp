//-----------------------------------------------------------------------------
/// \file   owner_table.hpp
/// \author Serge Aleynikov
//-----------------------------------------------------------------------------
/// \brief A fixed-capacity, open-addressing table mapping `ErlNifPid` to the
/// set of backlog reservations that process currently holds, used to
/// release a checked-out connection automatically if its owning process
/// dies (see ConnectionPool::MonitorOwner()/OnProcessDown() in arterial.hpp).
///
/// Sized at construction to `capacity` (rounded up to a power of 2), which
/// the owning ConnectionPool picks as `pool_size * backlog` -- the maximum
/// possible number of distinct simultaneous owners, since every reservation
/// consumes exactly one backlog slot and a slot can only be reserved by one
/// process at a time. No resizing: like wait_list.hpp's WaitList, the cost
/// is bounded upfront rather than handling unbounded growth.
///
/// Slots are claimed lock-free via CAS on a per-slot atomic state (Empty ->
/// Occupied), so two threads racing to insert different pids never contend.
/// Mutating a *found* slot's value (inserting/erasing a connection's worth
/// of reserved ids) is guarded by a per-slot spinlock instead of a single
/// pool-wide mutex: contention only happens between threads touching the
/// *same* pid concurrently (e.g. a racing checkin and death), which is rare
/// and brief, and never blocks threads working with other pids' slots.
//-----------------------------------------------------------------------------
#pragma once

#include "arterial_util.hpp"
#include <erl_nif.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <thread>
#include <vector>

namespace arterial {

//-----------------------------------------------------------------------------
/// @brief Everything a single owning process currently holds: a count of
/// reserved backlog slots per connection (individual request identity is
/// tracked entirely on the Erlang side, by the connection's
/// `arterial_conn_owner` process -- this table only needs "how many slots
/// does this pid hold on this connection" for crash cleanup), plus the
/// monitor watching that process (so it can be demonitored once its last
/// reservation is released).
//-----------------------------------------------------------------------------
struct OwnerEntry {
  ErlNifMonitor             mon{};
  bool                      monitored = false; // mon is armed
  std::map<uint32_t, uint32_t> by_conn; // conn_id -> reserved slot count
};

//-----------------------------------------------------------------------------
/// @brief Fixed-capacity table keyed by `ErlNifPid`, value `OwnerEntry`.
//-----------------------------------------------------------------------------
struct OwnerTable {
  explicit OwnerTable(size_t capacity)
  : m_mask(RoundUpPow2(capacity ? capacity : 1) - 1)
  {
    m_slots.reserve(m_mask + 1);
    for (size_t i = 0; i <= m_mask; ++i)
      m_slots.push_back(std::make_unique<Slot>());
  }

  /// @brief Look up `pid`'s entry and run `fn(OwnerEntry&)` on it while
  /// holding the slot's spinlock, inserting a fresh entry first if `pid`
  /// has none yet. `fn`'s return value (if non-void) is forwarded to the
  /// caller.
  /// @return {true, fn(entry)} on success; {false, {}} if the table is
  /// full and `pid` has no existing entry (capacity exhausted -- should
  /// not happen in practice since capacity tracks pool_size*backlog).
  template <typename Fn>
  auto WithEntry(ErlNifPid const& pid, Fn&& fn)
  {
    using Ret = decltype(fn(*(OwnerEntry*)nullptr));

    auto idx = FindOrClaim(pid);

    if constexpr (std::is_void_v<Ret>) {
      if (idx < 0) [[unlikely]]
        return false;

      SlotGuard guard(*m_slots[size_t(idx)]);
      fn(guard->entry);
      return true;
    } else {
      if (idx < 0) [[unlikely]]
        return std::pair<bool, Ret>{false, Ret{}};

      SlotGuard guard(*m_slots[size_t(idx)]);
      Ret ret = fn(guard->entry);
      return std::pair<bool, Ret>{true, std::move(ret)};
    }
  }

  /// @brief Remove `pid`'s entry entirely (if present) and pass it to
  /// `fn(OwnerEntry&&)`. Used by OnProcessDown() to take ownership of the
  /// entry's contents for release.
  /// @return true if `pid` had an entry (now removed).
  template <typename Fn>
  bool TakeEntry(ErlNifPid const& pid, Fn&& fn)
  {
    return TakeEntryIf(pid, [](OwnerEntry const&) { return true; }, std::forward<Fn>(fn));
  }

  /// @brief Like TakeEntry(), but only removes the entry if `pred(entry)`
  /// holds once the slot's lock is held -- used by
  /// ReleaseOwnerReservation() to avoid a race where another thread adds a
  /// fresh reservation (via MonitorOwner()/WithEntry()) for the same pid
  /// in between this entry being found empty and being taken, which would
  /// otherwise drop a live reservation on the floor.
  /// @return true if `pid` had an entry AND `pred` held (now removed).
  template <typename Pred, typename Fn>
  bool TakeEntryIf(ErlNifPid const& pid, Pred&& pred, Fn&& fn)
  {
    auto idx = Find(pid);
    if (idx < 0)
      return false;

    auto& slot = *m_slots[size_t(idx)];
    OwnerEntry taken;
    {
      SlotGuard guard(slot);
      // Re-check under the lock: another thread may have removed it, or
      // mutated it such that `pred` no longer holds, since Find() above.
      if (slot.state.load(std::memory_order_acquire) != State::Occupied ||
          !PidEquals(slot.pid, pid) || !pred(slot.entry))
        return false;

      taken = std::move(slot.entry);
      slot.entry = OwnerEntry{};
      slot.state.store(State::Tombstone, std::memory_order_release);
    }

    fn(std::move(taken));
    return true;
  }

private:
  enum class State : uint8_t {
    Empty,     ///< Never used; probing stops here -- pid is definitely absent.
    Occupied,  ///< Holds a live pid -> OwnerEntry mapping.
    Tombstone, ///< Vacated by TakeEntry(); reusable by a future FindOrClaim(),
               ///< but probing must continue past it (unlike Empty) since a
               ///< later-inserted pid may have been pushed beyond this slot.
  };

  struct Slot {
    std::atomic<State> state{State::Empty};
    std::atomic<bool>  busy{false}; // per-slot spinlock guarding `entry`/`pid`
    ErlNifPid          pid{};
    OwnerEntry         entry;
  };

  /// @brief RAII spinlock guard for a `Slot`'s `busy` flag: acquires on
  /// construction (spinning via `std::this_thread::yield()` until free),
  /// releases on destruction. `operator->()` gives access to the guarded
  /// `Slot` so callers read naturally as `guard->entry`, `guard->pid`, etc.
  struct SlotGuard {
    explicit SlotGuard(Slot& slot) : m_slot(slot)
    {
      while (m_slot.busy.exchange(true, std::memory_order_acquire))
        std::this_thread::yield();
    }

    ~SlotGuard() { m_slot.busy.store(false, std::memory_order_release); }

    SlotGuard(SlotGuard const&)            = delete;
    SlotGuard& operator=(SlotGuard const&) = delete;

    Slot* operator->() const { return &m_slot; }

  private:
    Slot& m_slot;
  };

  size_t                             m_mask;
  std::vector<std::unique_ptr<Slot>> m_slots;

  static bool PidEquals(ErlNifPid const& a, ErlNifPid const& b)
  {
    return enif_compare_pids(&a, &b) == 0;
  }

  static size_t Hash(ErlNifPid const& pid)
  {
    // ErlNifPid wraps a single opaque ERL_NIF_TERM handle (a tagged
    // integer internally); hashing that raw value directly is enough for
    // a table-internal, node-local hash -- no need to round-trip through
    // Erlang's own (term-portable) hash functions.
    return std::hash<ERL_NIF_TERM>{}(pid.pid);
  }

  /// @brief Linear-probe for `pid`'s existing slot.
  /// @return slot index, or -1 if not present.
  ptrdiff_t Find(ErlNifPid const& pid)
  {
    auto start = Hash(pid) & m_mask;
    for (size_t i = 0; i <= m_mask; ++i) {
      auto idx   = (start + i) & m_mask;
      auto& slot = *m_slots[idx];
      auto state = slot.state.load(std::memory_order_acquire);
      if (state == State::Empty)
        return -1; // probing stops at the first never-used slot
      if (state == State::Occupied && PidEquals(slot.pid, pid))
        return ptrdiff_t(idx);
    }
    return -1;
  }

  /// @brief Linear-probe for `pid`'s slot, claiming the first Empty (or
  /// reusable Tombstone) slot found along the way if `pid` isn't already
  /// present.
  /// @return slot index, or -1 if the table is full.
  ptrdiff_t FindOrClaim(ErlNifPid const& pid)
  {
    auto start = Hash(pid) & m_mask;
    ptrdiff_t first_tombstone = -1;

    for (size_t i = 0; i <= m_mask; ++i) {
      auto idx = (start + i) & m_mask;
      auto& slot = *m_slots[idx];
      auto state = slot.state.load(std::memory_order_acquire);

      if (state == State::Occupied) {
        if (PidEquals(slot.pid, pid))
          return ptrdiff_t(idx);
        continue;
      }

      if (state == State::Tombstone) {
        if (first_tombstone < 0)
          first_tombstone = ptrdiff_t(idx);
        continue;
      }

      // state == Empty: try to claim it (or fall back to an earlier
      // tombstone -- doesn't matter which, both are equally "free").
      auto claim_idx = size_t(first_tombstone >= 0 ? first_tombstone : ptrdiff_t(idx));
      auto& claim_slot = *m_slots[claim_idx];
      auto expected = claim_slot.state.load(std::memory_order_acquire);
      if (expected == State::Occupied) [[unlikely]] {
        // Lost a race for a tombstone slot; somebody else's insert beat
        // us to it. Re-check whether it became *our* pid (vanishingly
        // unlikely, but cheap to handle) before giving up on this slot.
        if (PidEquals(claim_slot.pid, pid))
          return ptrdiff_t(claim_idx);
      } else if (claim_slot.state.compare_exchange_strong(
                 expected, State::Occupied, std::memory_order_acq_rel)) {
        claim_slot.pid = pid;
        return ptrdiff_t(claim_idx);
      }

      // CAS lost the race (another thread claimed this exact slot first);
      // keep probing instead of retrying the same slot immediately.
    }

    // Wrapped all the way around without ever seeing an Empty slot (every
    // slot is Occupied or Tombstone -- only possible once the table has
    // been fully used at least once, e.g. a 1-slot table after its only
    // entry was taken). Fall back to claiming the earliest tombstone seen.
    if (first_tombstone >= 0) {
      auto claim_idx = size_t(first_tombstone);
      auto& claim_slot = *m_slots[claim_idx];
      auto expected = claim_slot.state.load(std::memory_order_acquire);
      if (expected == State::Occupied) {
        if (PidEquals(claim_slot.pid, pid))
          return ptrdiff_t(claim_idx);
      } else if (claim_slot.state.compare_exchange_strong(
                 expected, State::Occupied, std::memory_order_acq_rel)) {
        claim_slot.pid = pid;
        return ptrdiff_t(claim_idx);
      }
    }

    return -1; // table full
  }
};

} // namespace arterial
