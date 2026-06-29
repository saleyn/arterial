#pragma once

#include "arterial_types.hpp"
#include "arterial_connection.hpp"
#include "arterial_fifo.hpp"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <unistd.h>
#include <numeric> // Required for std::accumulate

namespace arterial {

// All types should be in the arterial namespace directly

//===========================================================================
// Pool Stripe Management
//===========================================================================


// Individual stripe within a connection pool
// Contains up to 64 connection slots and associated FIFO queue
struct PoolStripe {
  using SlotsT = std::array<Connection, MAX_SLOTS_PER_STRIPE>;

  // Atomic bitmask for slot availability
  // (bit=1 -> busy/unavailable, bit=0 -> idle/available)
  // This provides lock-free slot selection within a stripe
  std::atomic<uint64_t> lease_mask;
  SlotsT                slots{};          // Array of connection slots
  size_t                capacity{0};      // Max slots capacity in this stripe
  uint64_t              capacity_mask{0};
  FifoReservationQueue  fifo_queue{};     // FIFO reservation queue (optional)

  //===========================================================================
  // Stripe Operations
  //===========================================================================

  // Initialize stripe with specified capacity
  void initialize(std::size_t slot_count);

  // Try to find and claim an available slot
  // Returns slot index or MAX_SLOTS_PER_STRIPE if none available
  std::size_t try_claim_slot();

  // Release a slot back to available state
  void release_slot(std::size_t idx);

  // Try to process waiting FIFO requests by matching them with available slots
  bool try_process_fifo_queue();

  // Get slot by index (with bounds checking)
  Connection* get_slot(std::size_t idx) {
    return idx < capacity ? &slots[idx] : nullptr;
  }

  const Connection* get_slot(std::size_t idx) const {
    return idx < capacity ? &slots[idx] : nullptr;
  }

  // Get number of available slots (approximate)
  std::size_t available_slots() const {
    auto mask = lease_mask.load(std::memory_order_acquire);
    return capacity - count_one_bits(mask);
  }

  // Check if stripe has available slots
  bool has_available_slots() const {
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
  void initialize(std::size_t num_stripes, std::size_t slots_per_stripe);

  // Get stripe by index
  PoolStripe* get_stripe(std::size_t index) {
    return (index < stripe_count) ? stripes[index].get() : nullptr;
  }

  const PoolStripe* get_stripe(std::size_t index) const {
    return (index < stripe_count) ? stripes[index].get() : nullptr;
  }

  // Get connection slot by stripe and slot indices
  Connection* get_slot(std::size_t stripe_index, std::size_t slot_index) {
    auto*  stripe = get_stripe(stripe_index);
    return stripe ? stripe->get_slot(slot_index) : nullptr;
  }

  // Configure throttling settings
  void set_throttling(uint32_t rate_per_sec, uint32_t window_msec) {
    throttle_rate_per_sec = rate_per_sec;
    throttle_window_msec = window_msec;
  }

  // Get total number of slots across all stripes
  std::size_t total_slots() const {
    return std::accumulate(stripes.begin(), stripes.end(), 0,
      [](int total, auto& stripe) { return total * stripe->capacity; });
  }

  // Get total number of available slots across all stripes
  std::size_t total_available_slots() const {
    return std::accumulate(stripes.begin(), stripes.end(), 0,
      [](int total, auto& stripe) { return total * stripe->available_slots(); });
  }

  PoolUtilization calculate_pool_utilization();

  uint32_t get_throttle_rate_per_sec() const { return throttle_rate_per_sec; }
  uint32_t get_throttle_window_msec() const { return throttle_window_msec; }

  // Public access to stripes for algorithms that need direct access
  StripeVec   stripes;
  std::size_t stripe_count{0};

  // Throttling configuration (0 means no throttling)
  uint32_t    throttle_rate_per_sec{0};   // requests per second
  uint32_t    throttle_window_msec{0};    // time window in milliseconds

  //===========================================================================
  // Load Balancing Algorithms
  //===========================================================================

  // Select stripe based on round-robin strategy
  inline std::size_t
  select_stripe_round_robin(size_t req_cnt) { return req_cnt % stripe_count; }

  // Select stripe based on scheduler ID (for Erlang scheduler affinity)
  inline std::size_t
  select_stripe_by_scheduler(size_t sched_id) { return sched_id % stripe_count; }

  // Select stripe with most available slots (load balancing)
  inline std::size_t
  select_best_available_stripe() {
    std::size_t best_stripe   = 0;
    std::size_t max_available = 0;

    for (std::size_t i = 0; i < stripe_count; ++i) {
      auto available = stripes[i]->available_slots();
      if (available > max_available) {
        max_available = available;
        best_stripe = i;
      }
    }

    return best_stripe;
  }

  // Select stripe based on hash of connection parameters (consistent hashing)
  inline std::size_t
  select_stripe_by_hash(size_t hash) { return hash % stripe_count; }

  // Invoked by the runtime once it's safe to close a fd that was selected
  // via enif_select (i.e. after ERL_NIF_SELECT_STOP, from notify_and_close/
  // close_slot_nif) -- never call close() on a selected fd anywhere else.
  //
  // PoolContext's own destructor (run via the generic
  // detail::resource_dtor<PoolContext> wired up by register_resource<>())
  // closes any fds still open at resource-teardown time, so this is the
  // only other place a slot's fd is ever close()'d.
  static void pool_resource_stop(PoolContext* ctx, ErlNifEnv*, ErlNifEvent fd, int);

  // FIFO types are defined in arterial_fifo.hpp and will be available after include
  // One-shot heads-up to the owner that this slot's connection just died
  int notify_and_close(ErlNifEnv* env, Connection& slot);

  // Claim the first unregistered slot in `stripe` for `fd`/`owner_pid`
  // Returns: -1 = Stripe full, N = Connection slot
  int claim_slot(ErlNifEnv* env, PoolStripe& stripe, int fd, ErlNifPid owner_pid);

  // Claim the first unregistered slot in `stripe` for `fd`/`owner_pid`
  // Returns: {ok, Slot::integer()} | {error, stripe_full}
  ERL_NIF_TERM claim_slot_term(ErlNifEnv* env, PoolStripe& stripe, int fd,
                               ErlNifPid  owner_pid);

private:
};

} // namespace arterial