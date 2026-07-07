#pragma once

#include "arterial_connection.hpp"
#include "arterial_fifo.hpp"
#include "arterial_types.hpp"
#include "reactor.hpp"
#include <array>
#include <atomic>
#include <memory>
#include <numeric>
#include <unistd.h>
#include <vector>

namespace arterial {

// All types should be in the arterial namespace directly

//===========================================================================
// Pool Stripe Management
//===========================================================================

// One slot in the lock-free corr table.
//
// Protocol (all transitions via CAS on `key`):
//   key == 0          → slot is empty, available for insert
//   key == UINT32_MAX → slot is being written/cleared (transient claim)
//   key == CorrId     → slot holds a live entry for CorrId
//
// Payload fields (caller_pid, conn_id, deadline_us) are written exactly once
// (between CAS 0→UINT32_MAX and the final store to CorrId), and read exactly
// once (between CAS CorrId→UINT32_MAX and the final store to 0).
// The release/acquire on the key stores/loads provides the necessary ordering.
struct CorrSlot {
  std::atomic<uint32_t> key{0};
  ErlNifPid             caller_pid{};
  uint32_t              conn_id{0};
  int64_t               deadline_us{0};

  // Atomics are not copyable; provide explicit payload-only copy helpers.
  CorrSlot()                           = default;
  CorrSlot(const CorrSlot&)            = delete;
  CorrSlot& operator=(const CorrSlot&) = delete;

  // Copy payload fields from a slot that is currently claimed (key==TOMBSTONE),
  // so no other thread will touch the payload until we finish.
  void      copy_payload_from(const CorrSlot& src)
  {
    caller_pid  = src.caller_pid;
    conn_id     = src.conn_id;
    deadline_us = src.deadline_us;
  }
};

// Snapshot of a claimed slot's payload — plain copyable struct passed to callbacks.
struct CorrPayload {
  ErlNifPid caller_pid;
  uint32_t  conn_id;
  int64_t   deadline_us;
};

// Lock-free open-addressing corr table backed by a power-of-2 flat array.
// Each PoolStripe owns one; size is fixed at pool init time.
struct CorrTable {
  static constexpr uint32_t   EMPTY     = 0u;
  static constexpr uint32_t   TOMBSTONE = UINT32_MAX;

  // Raw heap array — avoids std::vector's copy/move requirements on CorrSlot.
  std::unique_ptr<CorrSlot[]> slots;
  uint32_t                    mask{0};

  void                        init(uint32_t size)
  {
    slots = std::make_unique<CorrSlot[]>(size);
    mask  = size - 1u;
  }

  // Insert entry for corr_id.  Returns true on success, false if table full.
  //
  // Two-pass probe to preserve the lazy-deletion invariant:
  //   Pass 1 — scan from home slot until EMPTY; record first TOMBSTONE seen.
  //            If corr_id already live, bail (duplicate — shouldn't happen in
  //            normal use, but safe).
  //   Pass 2 — claim the tombstone recorded in pass 1 (if any), or the first
  //            EMPTY slot encountered; write payload; publish corr_id.
  //
  // This keeps probe chains intact: TOMBSTONE marks "something was here, keep
  // probing" while EMPTY is the definitive chain terminator.
  bool insert(uint32_t corr_id, ErlNifPid pid, uint32_t conn_id, int64_t deadline_us)
  {
    uint32_t idx         = corr_id & mask;
    uint32_t tomb_idx    = UINT32_MAX; // index of first tombstone seen
    bool     found_empty = false;

    // Pass 1: scan from home slot; stop at EMPTY (chain end).
    // Record the first tombstone seen along the way.
    for (uint32_t i = 0; i <= mask; ++i, idx = (idx + 1) & mask) {
      uint32_t k = slots[idx].key.load(std::memory_order_acquire);
      if (k == corr_id) return false; // duplicate — already present
      if (k == TOMBSTONE && tomb_idx == UINT32_MAX) tomb_idx = idx;
      if (k == EMPTY) {
        found_empty = true;
        break;
      }
    }

    // If no empty slot and no tombstone, the table is genuinely full.
    if (!found_empty && tomb_idx == UINT32_MAX) return false;

    // Pass 2: claim tombstone if one was seen (preferred — recycles space),
    // otherwise claim the empty slot at idx.
    uint32_t target   = (tomb_idx != UINT32_MAX) ? tomb_idx : idx;
    uint32_t expected = (tomb_idx != UINT32_MAX) ? TOMBSTONE : EMPTY;

    if (!slots[target].key.compare_exchange_strong(
            expected, TOMBSTONE, std::memory_order_acquire, std::memory_order_relaxed)) [[unlikely]]
      return false; // lost the race; caller retries at the NIF layer if needed

    slots[target].caller_pid  = pid;
    slots[target].conn_id     = conn_id;
    slots[target].deadline_us = deadline_us;
    slots[target].key.store(corr_id, std::memory_order_release);
    return true;
  }

  // Remove entry for corr_id and return its payload.  Returns true if found.
  //
  // Leaves TOMBSTONE (not EMPTY) so probe chains for other keys that hashed
  // past this slot remain intact.  Average O(1) at low load factor.
  bool remove(uint32_t corr_id, CorrPayload& out)
  {
    uint32_t idx = corr_id & mask;
    for (uint32_t i = 0; i <= mask; ++i, idx = (idx + 1) & mask) {
      uint32_t k = slots[idx].key.load(std::memory_order_acquire);
      if (k == EMPTY) return false; // chain terminated — not present
      if (k != corr_id) continue;
      if (slots[idx].key.compare_exchange_strong(
              k, TOMBSTONE, std::memory_order_acquire, std::memory_order_relaxed)) {
        out = {slots[idx].caller_pid, slots[idx].conn_id, slots[idx].deadline_us};
        // Leave TOMBSTONE in place — do NOT store EMPTY here.
        return true;
      }
      // CAS failed: another thread just claimed this exact slot.
      return false;
    }
    return false;
  }

  // Erase entry for corr_id without returning payload (e.g. on send failure).
  void erase(uint32_t corr_id)
  {
    CorrPayload ignored;
    remove(corr_id, ignored);
  }

  // Scan all slots; for entries with conn_id == target_conn, claim and call fn.
  template <typename Fn>
  void drain_by_conn(uint32_t target_conn, Fn&& fn)
  {
    for (uint32_t i = 0; i <= mask; ++i) {
      uint32_t k = slots[i].key.load(std::memory_order_acquire);
      if (k == EMPTY || k == TOMBSTONE) continue;
      if (slots[i].conn_id != target_conn) continue;
      if (slots[i].key.compare_exchange_strong(
              k, TOMBSTONE, std::memory_order_acquire, std::memory_order_relaxed)) {
        CorrPayload p{slots[i].caller_pid, slots[i].conn_id, slots[i].deadline_us};
        slots[i].key.store(EMPTY, std::memory_order_release);
        fn(k, p);
      }
    }
  }

  // Scan all slots; for entries with deadline_us < now_us, claim and call fn.
  template <typename Fn>
  void sweep_expired(int64_t now_us, Fn&& fn)
  {
    for (uint32_t i = 0; i <= mask; ++i) {
      uint32_t k = slots[i].key.load(std::memory_order_acquire);
      if (k == EMPTY || k == TOMBSTONE) continue;
      if (slots[i].deadline_us >= now_us) continue;
      if (slots[i].key.compare_exchange_strong(
              k, TOMBSTONE, std::memory_order_acquire, std::memory_order_relaxed)) {
        CorrPayload p{slots[i].caller_pid, slots[i].conn_id, slots[i].deadline_us};
        slots[i].key.store(EMPTY, std::memory_order_release);
        fn(k, p);
      }
    }
  }

  // Approximate live-entry count (may race with concurrent inserts/removes).
  std::size_t count() const
  {
    std::size_t n = 0;
    for (uint32_t i = 0; i <= mask; ++i) {
      uint32_t k = slots[i].key.load(std::memory_order_relaxed);
      if (k != EMPTY && k != TOMBSTONE) ++n;
    }
    return n;
  }
};

// Individual stripe within a connection pool
// Contains up to 64 connection slots and associated FIFO queue
struct PoolStripe {
  using SlotsT = std::array<Connection, MAX_SLOTS_PER_STRIPE>;

  // Atomic bitmask for slot availability
  // (bit=1 -> busy/unavailable, bit=0 -> idle/available)
  // This provides lock-free slot selection within a stripe
  std::atomic<uint64_t> lease_mask;
  SlotsT                slots{};      // Array of connection slots
  size_t                capacity{0};  // Max slots capacity in this stripe
  uint64_t              capacity_mask{0};
  FifoReservationQueue  fifo_queue{}; // FIFO reservation queue (optional)

  // Lock-free per-stripe correlation table (replaces ETS + mutex).
  // Initialized by PoolContext::initialize() after construction.
  CorrTable             corr_table;

  //===========================================================================
  // Stripe Operations
  //===========================================================================

  // Initialize stripe with specified capacity
  void                  initialize(std::size_t slot_count);

  // Try to find and claim an available slot
  // Returns slot index or MAX_SLOTS_PER_STRIPE if none available
  std::size_t           try_claim_slot();

  // Release a slot back to available state
  void                  release_slot(std::size_t idx);

  // Try to process waiting FIFO requests by matching them with available slots
  bool                  try_process_fifo_queue();

  // Get slot by index (with bounds checking)
  Connection*           get_slot(std::size_t idx) { return idx < capacity ? &slots[idx] : nullptr; }

  const Connection*     get_slot(std::size_t idx) const
  { return idx < capacity ? &slots[idx] : nullptr; }

  // Get number of available slots (approximate)
  std::size_t available_slots() const
  {
    auto mask = lease_mask.load(std::memory_order_acquire);
    return capacity - count_one_bits(mask);
  }

  // Check if stripe has available slots
  bool has_available_slots() const
  {
    uint64_t mask = lease_mask.load(std::memory_order_acquire);
    return (mask & capacity_mask) != capacity_mask;
  }
};

//===========================================================================
// Pool Context Management
//===========================================================================

// Main pool context containing all stripes and configuration
struct PoolContext {
  using StripeVec = std::vector<std::unique_ptr<PoolStripe>>;

  // Destructor ensures all file descriptors are closed
  ~PoolContext();

  //===========================================================================
  // Pool Operations
  //===========================================================================

  // Initialize pool with specified stripe count and slots per stripe
  void        initialize(std::size_t num_stripes, std::size_t slots_per_stripe);

  // Get stripe by index
  PoolStripe* get_stripe(std::size_t index)
  { return (index < stripe_count) ? stripes[index].get() : nullptr; }

  const PoolStripe* get_stripe(std::size_t index) const
  { return (index < stripe_count) ? stripes[index].get() : nullptr; }

  // Get connection slot by stripe and slot indices
  Connection* get_slot(std::size_t stripe_index, std::size_t slot_index)
  {
    auto* stripe = get_stripe(stripe_index);
    return stripe ? stripe->get_slot(slot_index) : nullptr;
  }

  // Configure throttling settings
  void set_throttling(uint32_t rate_per_sec, uint32_t window_msec)
  {
    throttle_rate_per_sec = rate_per_sec;
    throttle_window_msec  = window_msec;
  }

  // Get total number of slots across all stripes
  std::size_t total_slots() const
  {
    return std::accumulate(stripes.begin(), stripes.end(), 0, [](int total, auto& stripe) {
      return total * stripe->capacity;
    });
  }

  // Get total number of available slots across all stripes
  std::size_t total_available_slots() const
  {
    return std::accumulate(stripes.begin(), stripes.end(), 0, [](int total, auto& stripe) {
      return total * stripe->available_slots();
    });
  }

  PoolUtilization calculate_pool_utilization();

  uint32_t        get_throttle_rate_per_sec() const { return throttle_rate_per_sec; }
  uint32_t        get_throttle_window_msec() const { return throttle_window_msec; }

  // Public access to stripes for algorithms that need direct access
  StripeVec       stripes;
  std::size_t     stripe_count{0};

  // Throttling configuration (0 means no throttling)
  uint32_t        throttle_rate_per_sec{0}; // requests per second
  uint32_t        throttle_window_msec{0};  // time window in milliseconds

  // Async I/O reactor — one per pool, owns all fd lifecycle and I/O dispatch.
  // Stored as unique_ptr so construction (io_uring setup) can be deferred to
  // init_pool_nif, after the resource object itself has been constructed.
  // Replaces enif_select entirely: no pool_resource_stop callback needed.
  std::unique_ptr<arterial::Reactor> reactor_ptr;

  arterial::Reactor&                 reactor() { return *reactor_ptr; }

  //===========================================================================
  // Load Balancing Algorithms
  //===========================================================================

  // Select stripe based on round-robin strategy
  inline std::size_t select_stripe_round_robin(size_t req_cnt) { return req_cnt % stripe_count; }

  // Select stripe based on scheduler ID (for Erlang scheduler affinity)
  inline std::size_t select_stripe_by_scheduler(size_t sched_id) { return sched_id % stripe_count; }

  // Select stripe with most available slots (load balancing)
  inline std::size_t select_best_available_stripe()
  {
    std::size_t best_stripe   = 0;
    std::size_t max_available = 0;

    for (std::size_t i = 0; i < stripe_count; ++i) {
      auto available = stripes[i]->available_slots();
      if (available > max_available) {
        max_available = available;
        best_stripe   = i;
      }
    }

    return best_stripe;
  }

  // Select stripe based on hash of connection parameters (consistent hashing)
  inline std::size_t select_stripe_by_hash(size_t hash) { return hash % stripe_count; }

  // One-shot heads-up to the owner that this slot's connection just died.
  // Sends {arterial_event, StripeId, SlotId, closed} to owner_pid (unless
  // owner is the caller), clears slot state, and tells the reactor to
  // remove the fd (which closes it on the reactor thread — race-free).
  int                notify_and_close(ErlNifEnv* env, Connection& slot, bool notify = true);

  // Claim the first unregistered slot in `stripe` for `fd`/`owner_pid`
  // Returns: -1 = Stripe full, N = Connection slot
  int                claim_slot(ErlNifEnv* env, PoolStripe& stripe, int fd, ErlNifPid owner_pid);

  // Claim the first unregistered slot in `stripe` for `fd`/`owner_pid`
  // Returns: {ok, Slot::integer()} | {error, stripe_full}
  ERL_NIF_TERM claim_slot_term(ErlNifEnv* env, PoolStripe& stripe, int fd, ErlNifPid owner_pid);

  // Set up a process monitor for conn.owner_pid, storing the monitor token in
  // conn.owner_monitor.  The on_down callback (registered at load time on the
  // PoolContext resource type) will close the slot if the owner dies.
  // Returns 0 on success, -1 if enif_monitor_process failed (process already dead).
  int          monitor_owner(ErlNifEnv* env, Connection& conn);

  // Remove the monitor set by monitor_owner.  Safe to call even if the monitor
  // was never set (no-op when conn.fd < 0 after reset()).
  void         demonitor_owner(ErlNifEnv* env, Connection& conn);

private:
};

// Full definition — placed after PoolContext so the ctx pointer is complete.
// The forward declaration at the top of this file allows arterial_connection.hpp
// to hold a SlotRef* without pulling in the full definition there.
struct SlotRef {
  PoolContext* ctx;
  uint32_t     stripe_id;
  uint32_t     slot_id;

  SlotRef(PoolContext* c, uint32_t sid, uint32_t slt) : ctx(c), stripe_id(sid), slot_id(slt) {}
};

} // namespace arterial