#include "arterial_pool.hpp"

namespace arterial {

//=============================================================================
// Stripe Operations
//=============================================================================

// Initialize stripe with specified capacity
void PoolStripe::initialize(std::size_t slot_count) {
  capacity      = std::min(slot_count, MAX_SLOTS_PER_STRIPE);
  capacity_mask = (1ULL << capacity) - 1;

  // Initialize lease mask - all slots start as available (bit=0)
  lease_mask.store(0, std::memory_order_release);

  // Initialize slots
  for (uint32_t i = 0; i < capacity; ++i) {
    slots[i].stripe_id = 0;  // Will be set by PoolContext
    slots[i].slot_id   = i;
    slots[i].reset();
  }
}

// Try to find and claim an available slot
// Returns slot index or MAX_SLOTS_PER_STRIPE if none available
std::size_t PoolStripe::try_claim_slot() {
  uint64_t mask = lease_mask.load(std::memory_order_acquire);

  for (std::size_t i = 0; i < capacity; ++i) {
    uint64_t bit = 1ULL << i;

    // Skip if slot is already busy
    if (mask & bit) continue;

    // Try to claim this slot
    uint64_t expected = mask;
    if (lease_mask.compare_exchange_weak(expected, mask|bit, std::memory_order_acq_rel))
      return i;  // Successfully claimed slot i

    // CAS failed, reload mask and continue
    mask = expected;
  }

  return MAX_SLOTS_PER_STRIPE;  // No available slots
}

// Release a slot back to available state
inline void PoolStripe::release_slot(std::size_t slot_index) {
  if (slot_index >= capacity) return;

  uint64_t bit = 1ULL << slot_index;
  lease_mask.fetch_and(~bit, std::memory_order_release);
  slots[slot_index].release();
}

// Try to process waiting FIFO requests by matching them with available slots
inline bool PoolStripe::try_process_fifo_queue() {
  if (!fifo_queue.is_initialized() || fifo_queue.empty()) {
    return false;  // No queued requests
  }

  // Try to dequeue a waiting request
  auto entry_opt = fifo_queue.dequeue();
  if (!entry_opt.has_value()) {
    return false;  // No valid entries or queue became empty
  }

  auto entry = *entry_opt;
  uint64_t current_mask = lease_mask.load(std::memory_order_relaxed);

  // Look for an available slot
  for (std::size_t i = 0; i < capacity; ++i) {
    uint64_t target_bit = 1ULL << i;

    // Skip if slot is already busy
    if (current_mask & target_bit) continue;

    // Try to claim this slot
    if (lease_mask.compare_exchange_weak(current_mask, current_mask | target_bit,
                                              std::memory_order_acq_rel)) {
      auto& slot = slots[i];

      // Verify slot is actually available
      if (slot.status.load(std::memory_order_acquire) != SLOT_AVAILABLE) {
        // Slot not ready - release and try next
        lease_mask.fetch_and(~target_bit, std::memory_order_release);
        current_mask = lease_mask.load(std::memory_order_relaxed);
        continue;
      }

      // Success - assign this slot to the queued request
      slot.enable_fifo_mode();
      slot.status.store(SLOT_FIFO_RESERVED, std::memory_order_release);
      slot.set_fifo_request(entry.m_requester_pid, entry.m_reservation_id);

      return true;  // Successfully processed one queued request
    }

    // CAS failed, reload mask and continue
    current_mask = lease_mask.load(std::memory_order_relaxed);
  }

  // No available slots - request will need to be requeued or time out
  return false;
}

//===========================================================================
// Pool Context Management
//===========================================================================

// Destructor ensures all file descriptors are closed
PoolContext::~PoolContext() {
  for (auto& stripe_ptr : stripes)
    for (auto& slot : stripe_ptr->slots) {
      if (slot.fd != -1) {
        close(slot.fd);
        slot.fd = -1;
      }

      #ifdef HAVE_OPENSSL
      if (slot.ssl) {
        SSL_free(slot.ssl);
        slot.ssl = nullptr;
      }
      #endif
    }
}

// Initialize pool with specified stripe count and slots per stripe
void PoolContext::initialize(std::size_t num_stripes, std::size_t slots_per_stripe) {
  stripe_count = num_stripes;
  stripes.clear();
  stripes.reserve(num_stripes);

  for (std::size_t i = 0; i < num_stripes; ++i) {
    auto stripe = std::make_unique<PoolStripe>();
    stripe->initialize(slots_per_stripe);

    // Set stripe_id for all slots in this stripe
    for (uint32_t j = 0; j < stripe->capacity; ++j)
      stripe->slots[j].stripe_id = i;

    stripes.emplace_back(std::move(stripe));
  }
}

inline PoolUtilization PoolContext::calculate_pool_utilization() {
  auto total     = total_slots();
  auto available = total_available_slots();
  auto busy      = total - available;
  auto util      = (total > 0) ? (static_cast<double>(busy) / total) * 100.0 : 0.0;

  return PoolUtilization{
    .total_slots         = total,
    .available_slots     = available,
    .busy_slots          = busy,
    .utilization_percent = util
  };
}

// Invoked by the runtime once it's safe to close a fd that was selected
void PoolContext::pool_resource_stop(PoolContext* ctx, ErlNifEnv*, ErlNifEvent fd, int) {
  for (auto& stripe_ptr : ctx->stripes) {
    auto& stripe = *stripe_ptr;
    for (auto& slot : stripe.slots) {
      if (slot.fd == fd) {
        close(slot.fd);
        slot.fd = -1;
        slot.pending_buffer.clear();
        slot.bytes_written = 0;
        slot.status.store(SLOT_EMPTY, std::memory_order_release);
        stripe.lease_mask.fetch_and(~(1ULL << slot.slot_id), std::memory_order_release);
        return;
      }
    }
  }
}

// FIFO types are defined in arterial_fifo.hpp and will be available after include
// One-shot heads-up to the owner that this slot's connection just died
int PoolContext::notify_and_close(ErlNifEnv* env, Connection& slot) {

  ErlNifPid self_pid;
  enif_self(env, &self_pid);
  if (enif_compare_pids(&self_pid, &slot.owner_pid) != 0) {
    nifpp::msg_env msg_env;  // Auto-frees at the end of the scope
    auto msg = slot.make_event_msg(msg_env, am_closed);
    enif_send(env, &slot.owner_pid, msg_env, msg);
  }

  // Clear status and lease bit immediately so claim_slot can reuse this
  // slot. Leave fd so pool_resource_stop can find and close it.
  slot.status.store(SLOT_EMPTY, std::memory_order_release);
  auto& stripe = *this->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << slot.slot_id), std::memory_order_release);

  return enif_select(env, slot.fd, ERL_NIF_SELECT_STOP, this, nullptr, am_stop);
}

// Claim the first unregistered slot in `stripe` for `fd`/`owner_pid`
int PoolContext::claim_slot(
  ErlNifEnv* env, PoolStripe& stripe, int fd, ErlNifPid owner_pid)
{
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  while (true) {
    int slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return -1; // Stripe full

    auto& slot = stripe.slots[slot_id];
    if (slot.status.load(std::memory_order_acquire) != SLOT_EMPTY) {
      // This slot is occupied, mark it as busy in our local mask
      current_mask |= (1ULL << slot_id);
      continue;
    }

    slot.fd = fd;
    slot.owner_pid = owner_pid;
    slot.status.store(SLOT_AVAILABLE, std::memory_order_relaxed);

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_release,
          std::memory_order_relaxed)) {
      slot.arm_read(env, this); // TODO: handle error
      return slot_id;
    }

    // CAS failed, current_mask was updated by compare_exchange_weak
    slot.fd = -1;
    slot.status.store(SLOT_EMPTY, std::memory_order_relaxed);
    // Continue with the updated current_mask from the failed CAS
  }
}

inline ERL_NIF_TERM PoolContext::claim_slot_term(
  ErlNifEnv* env, PoolStripe& stripe, int fd, ErlNifPid owner_pid)
{
  auto   res = claim_slot(env, stripe, fd, owner_pid);
  return res >= 0 ? make_tuple(env, am_ok,    res)
                  : make_tuple(env, am_error, am_stripe_full);
}


} // namespace arterial