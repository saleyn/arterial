#pragma once

#include "enif.hpp"
#include "arterial_types.hpp"
#include "throttle.hpp"
#include <atomic>
#include <vector>
#include <chrono>

// Forward declare FIFO types - definitions in arterial_fifo.hpp (included later)
// NIF type handling - use real type when headers are available, placeholder otherwise
#ifdef __ERL_NIF_H__
using NifPid = ErlNifPid;  // Use real type when NIF headers are available
#else
using NifPid = void*;      // Placeholder for header-only usage
#endif

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#endif

namespace arterial {

struct PoolContext;
struct FifoQueueEntry;
class  FifoReservationQueue;

using namespace nifpp;

// Lock-free-ish raw-socket connection pool: each "stripe" is a single
// atomic uint64 lease mask (bit=1 -> slot unregistered or currently
// leased/busy, bit=0 -> registered and idle) covering up to 64 "slots"
// (physical sockets). A caller picks a stripe itself (e.g. by scheduler
// id, see arterial_connection/arterial_client) and this NIF auto-selects
// any idle slot within it via CAS on that one atomic -- no per-slot lock.
//
// Reads and writes happen as plain non-blocking syscalls invoked directly
// inside whichever Erlang process calls send_and_release/3 (writes) or
// handle_readable/3 (reads) -- there is no in-NIF callback invoked by the
// runtime on fd readiness (no such thing exists in erl_nif.h); enif_select
// only ever delivers a *message* to a process, which must then call back
// into the NIF to actually do the I/O. That message is a caller-supplied
// "custom message" (ERL_NIF_SELECT_CUSTOM_MSG), so the NIF itself decides
// its shape: `{arterial_event, StripeId, SlotId, read | write | closed}`.
//
// register_socket/4 and connect/7 both arm the read side once and target
// every future read-ready/write-ready/closed message at the registering
// "owner" pid (expected to be the long-lived arterial_connection worker
// for that slot, not whichever transient process happens to call
// send_and_release/3 for a given request).
//
// connect/7 opens and connects the fd itself (a dirty, IO-bound NIF, see
// its ErlNifFunc entry, since connect(2) can block) -- prefer it over
// register_socket/4, which hands off an *already-open* fd (e.g. extracted
// from an OTP `socket()` via `socket:getopt(Sock, otp, fd)`) and is kept
// only for callers that genuinely need to register a pre-existing fd.
// That fd still has another resource (the `socket()` term's own esock
// resource) believing it owns it, and erts logs a "stealing control of
// fd=N" warning both when register_socket/4 takes it over and again,
// potentially against a since-reused fd number, when that `socket()`
// term is eventually garbage collected -- connect/7 has no such
// competing owner at any point, since the fd is born inside this NIF.

//===========================================================================
// Connection Slot Management
//===========================================================================

// Cache-line aligned connection slot structure
// Contains both regular connection state and FIFO mode extensions
struct alignas(64) Connection {
  using StatusT      = std::atomic<uint32_t>;
  using AtomicUInt64 = std::atomic<uint64_t>;
  using Throttle     = arterial::time_spacing_throttle;

  // Core connection state
  StatusT             status{SLOT_AVAILABLE};
  int                 fd{-1};
  uint32_t            stripe_id{0};
  uint32_t            slot_id{0};

  // Long-lived process that owns this slot's read/write-ready
  // notifications (set once, at register_socket/4 time).
  NifPid              owner_pid{};

  // Buffer management for pending writes
  std::vector<char>   pending_buffer;
  std::size_t         bytes_written{0};

  // Throttling state: time spacing throttle for this slot
  Throttle            throttle{0, 1000};

#ifdef HAVE_OPENSSL
  SSL*                ssl{nullptr};
  ProtocolType        protocol{PROTO_TCP};
#endif

  //---------------------------------------------------------------------------
  // FIFO Mode 3 Extensions (integrated - zero overhead when unused)
  //---------------------------------------------------------------------------
  std::atomic<bool>   fifo_mode_enabled{false};
  NifPid              fifo_requester_pid{};
  AtomicUInt64        fifo_total_requests{0};
  AtomicUInt64        fifo_total_timeouts{0};
  std::atomic<bool>   fifo_request_active{false};
  uint64_t            fifo_reservation_id{0};

  //===========================================================================
  // Connection Management Methods
  //===========================================================================

  // Reset slot to available state
  void reset() {
    status.store(SLOT_AVAILABLE, std::memory_order_release);
    fd = -1;
    owner_pid = {};
    pending_buffer.clear();
    bytes_written = 0;

    // Reset FIFO state
    fifo_mode_enabled.store(false, std::memory_order_release);
    fifo_request_active.store(false, std::memory_order_release);
    fifo_requester_pid = {};
    fifo_reservation_id = 0;

#ifdef HAVE_OPENSSL
    if (ssl) {
      SSL_free(ssl);
      ssl = nullptr;
    }
#endif
  }

  // Check if slot is currently available for use
  bool is_available() const {
    return status.load(std::memory_order_acquire) == SLOT_AVAILABLE && fd >= 0;
  }

  // Mark slot as busy (atomic operation)
  bool try_claim() {
    uint32_t expected = SLOT_AVAILABLE;
    return status.compare_exchange_weak(expected, SLOT_BUSY,
                                       std::memory_order_acq_rel);
  }

  // Release slot back to available state
  void release() {
    pending_buffer.clear();
    bytes_written = 0;
    status.store(SLOT_AVAILABLE, std::memory_order_release);
  }

  //===========================================================================
  // FIFO Mode Methods
  //===========================================================================

  // Enable FIFO mode for this connection slot
  void enable_fifo_mode() {
    fifo_mode_enabled.store(true, std::memory_order_release);
  }

  // Check if FIFO mode is enabled for this slot
  bool is_fifo_enabled() const {
    return fifo_mode_enabled.load(std::memory_order_acquire);
  }

  // Set active FIFO request (returns false if already active)
  bool set_fifo_request(NifPid pid, uint64_t res_id) {
    // Use compare_exchange to atomically check and set
    bool expected = false;
    if (!fifo_request_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
      return false;

    fifo_requester_pid = pid;
    fifo_reservation_id = res_id;
    fifo_total_requests.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  // Clear active FIFO request
  void clear_fifo_request() {
    fifo_request_active.store(false, std::memory_order_release);
    // Use memory barrier to ensure proper ordering
    std::atomic_thread_fence(std::memory_order_seq_cst);
    fifo_requester_pid = {};
    // fifo_reservation_id is preserved for verification in release
  }

  // Check if FIFO request is currently active
  bool has_active_fifo_request() const {
    return fifo_request_active.load(std::memory_order_acquire);
  }

  // Increment timeout counter for FIFO requests
  void increment_fifo_timeouts() {
    fifo_total_timeouts.fetch_add(1, std::memory_order_relaxed);
  }

  // Message generation for connection results
  inline TERM make_connect_result_msg(nifpp::msg_env& msg_env, TERM result)
  {
    return make(msg_env, std::make_tuple(am_arterial_event, stripe_id, slot_id,
                                         am_connect_result, result));
  }

  inline TERM make_event_msg(nifpp::msg_env& msg_env, const atom& kind) {
    return make(msg_env, std::make_tuple(am_arterial_event, stripe_id, slot_id, kind));
  }

  //===========================================================================
  // Socket Operations
  //===========================================================================

  // Re-arm (one-shot) read/write readiness notification, targeted at the
  // slot's owner pid, using a freshly allocated env each time -- enif_select
  // permanently adopts msg/msg_env, so it can never be reused across calls.
  inline int arm_read(ErlNifEnv* env, const PoolContext* ctx) {
    nifpp::msg_env msg_env;
    auto msg = make_event_msg(msg_env, am_read);
    return nifpp::select_read(env, fd, ctx, &owner_pid, msg, msg_env);
  }

  inline int arm_write(ErlNifEnv* env, const PoolContext* ctx) {
    nifpp::msg_env msg_env;
    auto msg = make_event_msg(msg_env, am_write);
    return nifpp::select_write(env, fd, ctx, &owner_pid, msg, msg_env);
  }

  inline int arm_connect(ErlNifEnv* env, const PoolContext* ctx) {
    nifpp::msg_env msg_env;
    auto msg = make_event_msg(msg_env, am_write);
    return nifpp::select_write(env, fd, ctx, &owner_pid, msg, msg_env);
  }
};

} // namespace arterial