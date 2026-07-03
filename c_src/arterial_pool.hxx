#include "arterial_pool.hpp"
#include "arterial_connection_timer.hxx"  // cancel_connection_timeout

namespace arterial {

//=============================================================================
// arm_read / arm_write / arm_connect — Reactor-based implementations
//
// These replace the old enif_select_read / enif_select_write calls.
// The Reactor thread handles all I/O events and dispatches into the C++
// handle_readable / handle_writable methods directly, then sends the
// resulting high-level messages to owner_pid via enif_send.
//=============================================================================

// Helper: build a callback that runs handle_readable, then sends:
//   {arterial_event, StripeId, SlotId, data, Binary}  — when data arrives
//   {arterial_event, StripeId, SlotId, closed}         — when connection closes
// to owner_pid so the Erlang arterial_connection can decode and dispatch.
// Returns 0 to keep registered, -1 to tell the reactor to RemoveFd+close.
static inline ReadHandler make_read_handler(PoolContext* ctx,
                                             unsigned stripe_id,
                                             unsigned slot_id)
{
  auto& conn    = ctx->stripes[stripe_id]->slots[slot_id];
  uint32_t gen  = conn.generation.load(std::memory_order_acquire);
  return [ctx, stripe_id, slot_id, gen](int /*fd*/, void*) -> int {
    auto& c = ctx->stripes[stripe_id]->slots[slot_id];
    // Guard against slot reuse: if generation changed the slot was closed.
    if (c.generation.load(std::memory_order_acquire) != gen) return -1;
    nifpp::msg_env me;
    auto res = c.handle_readable(me, ctx);

    using ReadResult = Connection::ReadResult;

    if (res.result == ReadResult::DATA) {
      TERM bin_term(enif_make_binary(me, &res.data));
      auto msg = nifpp::make(me,
        std::make_tuple(am_arterial_event, stripe_id, slot_id,
                        am_read, bin_term));
      enif_send(nullptr, &c.owner_pid, me, msg);
      return 0;
    }
    if (res.result == ReadResult::CLOSED || res.result == ReadResult::ERROR)
      return -1;
    return 0;
  };
}

static inline WriteHandler make_write_handler(PoolContext* ctx,
                                               unsigned stripe_id,
                                               unsigned slot_id)
{
  auto& conn    = ctx->stripes[stripe_id]->slots[slot_id];
  uint32_t gen  = conn.generation.load(std::memory_order_acquire);
  return [ctx, stripe_id, slot_id, gen](int /*fd*/, void*) -> int {
    auto& c = ctx->stripes[stripe_id]->slots[slot_id];
    if (c.generation.load(std::memory_order_acquire) != gen) return -1;
    auto res = c.handle_writable(nullptr, ctx);

    using WriteResult = Connection::WriteResult;
    if (res.result == WriteResult::CONNECT_OK ||
        res.result == WriteResult::CONNECT_FAILED) {
      if (res.send_connect_msg) {
        nifpp::msg_env me;
        auto msg = c.make_connect_result_msg(me, res.connect_result);
        enif_send(nullptr, &c.owner_pid, me, msg);
      }
      return (res.result == WriteResult::CONNECT_FAILED) ? -1 : 0;
    }
    if (res.result == WriteResult::CLOSED || res.result == WriteResult::ERROR)
      return -1;
    return 0;
  };
}

static inline ErrorHandler make_error_handler(PoolContext* ctx,
                                               unsigned stripe_id,
                                               unsigned slot_id)
{
  auto& conn    = ctx->stripes[stripe_id]->slots[slot_id];
  uint32_t gen  = conn.generation.load(std::memory_order_acquire);
  return [ctx, stripe_id, slot_id, gen](int /*fd*/, void*) {
    auto& c = ctx->stripes[stripe_id]->slots[slot_id];
    if (c.generation.load(std::memory_order_acquire) != gen) return;
    ErlNifEnv* env = enif_alloc_env();
    if (env) {
      ctx->notify_and_close(env, c);
      enif_free_env(env);
    }
  };
}

inline int Connection::arm_read(ErlNifEnv* /*env*/, const PoolContext* ctx)
{
  // Register the fd with the reactor for persistent read notifications.
  // With the reactor's edge-triggered multishot mode, this is idempotent:
  // calling AddFd again on an already-registered fd is harmless (the reactor
  // treats it as an update). In practice arm_read is called:
  //   - Once at claim_slot_term (initial registration)
  //   - From handle_readable/writable to re-arm after an event (no-op here
  //     since the reactor is persistent — events keep firing automatically)
  auto* mctx = const_cast<PoolContext*>(ctx);
  mctx->reactor().AddFd(
    fd,
    make_read_handler(mctx, stripe_id, slot_id),
    make_error_handler(mctx, stripe_id, slot_id),
    make_write_handler(mctx, stripe_id, slot_id));
  return 0;
}

inline int Connection::arm_write(ErlNifEnv* /*env*/, const PoolContext* ctx)
{
  // One-shot write-ready notification (for pending sends after partial write).
  auto* mctx = const_cast<PoolContext*>(ctx);
  mctx->reactor().ArmWrite(fd);
  return 0;
}

inline int Connection::arm_connect(ErlNifEnv* /*env*/, const PoolContext* ctx)
{
  // Store handlers without triggering a kernel poll registration (avoids
  // EPOLLIN on an EINPROGRESS socket).  ArmWrite then registers EPOLLOUT
  // (via do_arm_write → reactor_mod) to detect connect completion.
  auto* mctx = const_cast<PoolContext*>(ctx);
  mctx->reactor().RegisterHandlers(
    fd,
    {},                                               // no on_read during connect
    make_error_handler(mctx, stripe_id, slot_id),
    make_write_handler(mctx, stripe_id, slot_id));
  mctx->reactor().ArmWrite(fd);
  return 0;
}

// Install a connect timeout for the given fd via the reactor.
// Called from connect_with_opts / connect_proto_with_opts after arm_connect.
// Replaces nifpp::select_read(timeout_fd, ...) entirely.
inline void Connection::set_connect_timeout(const PoolContext* ctx, uint64_t timeout_ms)
{
  if (timeout_ms == 0 || fd < 0) return;
  auto* mctx   = const_cast<PoolContext*>(ctx);
  unsigned sid = stripe_id;
  unsigned slt = slot_id;
  // On timeout: send {arterial_event, StripeId, SlotId, timeout} to owner_pid
  // and close the connecting socket (the reactor calls RemoveFd for us after
  // on_timeout returns via the normal timeout dispatch path).
  ErlNifPid pid = owner_pid;
  mctx->reactor().AddFd(
    fd,
    {},                                           // keep existing handlers
    make_error_handler(mctx, sid, slt),
    make_write_handler(mctx, sid, slt),
    // on_timeout: close the connecting fd, free the slot, notify owner.
    [mctx, sid, slt, pid](int cfd, void*) {
      auto& conn = mctx->stripes[sid]->slots[slt];
      cancel_connection_timeout(conn);
      // Queue RemoveFd so the reactor deregisters from epoll and closes cfd
      // on the next iteration.  We clear conn.fd now so the PoolContext
      // destructor does not double-close it.
      conn.fd = -1;
      mctx->reactor().RemoveFd(cfd);
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      mctx->stripes[sid]->lease_mask.fetch_and(
        ~(1ULL << slt), std::memory_order_release);
      // Send timeout message to owner
      nifpp::msg_env me;
      auto msg = conn.make_event_msg(me, am_timeout);
      enif_send(nullptr, &pid, me, msg);
    });
  mctx->reactor().SetTimeout(fd, timeout_ms);
}

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

// Invoked by the runtime once it's safe to close a fd that was selected.
// Scans slots for the fd; if already cleared by handle_connection_timeout_nif
// (conn.fd=-1 set before SELECT_STOP), closes via the event parameter.
// One-shot heads-up to the owner that this slot's connection just died.
// The reactor closes the fd on its own thread — no enif_select(STOP) needed.
int PoolContext::notify_and_close(ErlNifEnv* env, Connection& slot) {
  // Remove the owner monitor first to prevent a concurrent on_down from
  // re-entering notify_and_close for the same slot.
  demonitor_owner(env, slot);

  ErlNifPid self_pid;
  enif_self(env, &self_pid);
  if (enif_compare_pids(&self_pid, &slot.owner_pid) != 0) {
    nifpp::msg_env msg_env;
    auto msg = slot.make_event_msg(msg_env, am_closed);
    enif_send(env, &slot.owner_pid, msg_env, msg);
  }

  // Clear status and lease bit immediately so claim_slot can reuse this slot.
  slot.status.store(SLOT_EMPTY, std::memory_order_release);
  auto& stripe = *this->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << slot.slot_id), std::memory_order_release);

  // Tell the reactor to remove and close the fd on its thread.
  // This is race-free: the reactor is the sole owner of the fd lifecycle.
  if (slot.fd >= 0) {
    reactor().RemoveFd(slot.fd);
    slot.fd = -1;
  }
  return 0;
}

// Claim the first unregistered slot in `stripe` for `fd`/`owner_pid`
int PoolContext::claim_slot(
  ErlNifEnv* /*env*/, PoolStripe& stripe, int fd, ErlNifPid owner_pid)
{
  // Two masks:
  //   actual_mask  — the live lease_mask from the atomic, refreshed on CAS failure
  //   scan_mask    — actual_mask | bits we locally skipped due to non-EMPTY status
  // We keep scan_mask separate because compare_exchange_weak may write the fresh
  // actual value back into our variable, erasing the locally-accumulated skips.
  uint64_t actual_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  uint64_t skip_bits   = 0;    // bits set purely from non-EMPTY status checks

  while (true) {
    uint64_t scan_mask = actual_mask | skip_bits;
    int slot_id = std::countr_zero(~scan_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return -1; // Stripe full

    auto& slot = stripe.slots[slot_id];
    if (slot.status.load(std::memory_order_acquire) != SLOT_EMPTY) {
      // Slot belongs to an active connection (SLOT_AVAILABLE/SLOT_CONNECTING/…).
      // Remember this skip locally so we don't re-examine it on CAS retry.
      skip_bits |= (1ULL << slot_id);
      continue;
    }

    // CAS the lease bit first.  Only the winner initialises fd/owner_pid/status
    // — writing them before the CAS creates a data race when multiple callers
    // concurrently compete for the same slot: the CAS loser's stores overwrite
    // the winner's fd with -1 and leave the slot corrupted.
    uint64_t target_bit  = (1ULL << slot_id);
    uint64_t expected    = actual_mask;          // actual value without skip_bits
    uint64_t desired     = expected | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          expected, desired,
          std::memory_order_acquire,
          std::memory_order_relaxed)) {
      // We own this slot. Initialise under the lease bit so any reader
      // that checks status after seeing bit=1 gets consistent data.
      slot.fd         = fd;
      slot.owner_pid  = owner_pid;
      slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
      return slot_id;
    }
    // CAS failed: `expected` was updated to the current actual mask.
    // Preserve skip_bits — the non-EMPTY slots we already checked haven't changed.
    actual_mask = expected;
    // A slot that appeared non-EMPTY may have since been freed (status → SLOT_EMPTY
    // after close_slot).  Clear skip_bits for any slot whose lease bit is now 0.
    skip_bits &= actual_mask;
  }
}

inline ERL_NIF_TERM PoolContext::claim_slot_term(
  ErlNifEnv* env, PoolStripe& stripe, int fd, ErlNifPid owner_pid)
{
  auto res = claim_slot(env, stripe, fd, owner_pid);
  if (res < 0)
    return make_tuple(env, am_error, am_stripe_full);
  stripe.slots[res].arm_read(env, this);
  return make_tuple(env, am_ok, res);
}

int PoolContext::monitor_owner(ErlNifEnv* env, Connection& conn)
{
  // Build the on_down callback that ERTS calls directly — O(1), no scan.
  nifpp::resource_events<SlotRef> events{
    [](SlotRef* ref, ErlNifEnv* denv, ErlNifPid* /*pid*/, ErlNifMonitor* /*mon*/) {
      auto& c = ref->ctx->stripes[ref->stripe_id]->slots[ref->slot_id];
      // Guard against slot reuse: demonitor_owner nulls slot_ref before reset().
      if (c.slot_ref == ref)
        ref->ctx->notify_and_close(denv, c);
    }
  };

  auto ref = construct_resource_with_events<SlotRef>(
    events, SlotRef{this, conn.stripe_id, conn.slot_id});

  // Hold an extra reference so the SlotRef outlives the resource_ptr destructor.
  conn.slot_ref = ref.get();
  enif_keep_resource(conn.slot_ref);

  int rc = enif_monitor_process(env, conn.slot_ref, &conn.owner_pid, &conn.owner_monitor);
  if (rc != 0) {
    enif_release_resource(conn.slot_ref);
    conn.slot_ref = nullptr;
  }
  return rc;
}

void PoolContext::demonitor_owner(ErlNifEnv* env, Connection& conn)
{
  if (!conn.slot_ref) [[unlikely]] return;
  enif_demonitor_process(env, conn.slot_ref, &conn.owner_monitor);
  enif_release_resource(conn.slot_ref);
  conn.slot_ref = nullptr;
}

} // namespace arterial