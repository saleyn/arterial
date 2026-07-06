#pragma once

#include "enif.hpp"
#include "arterial_types.hpp"
#include "throttle.hpp"
#include <atomic>
#include <vector>
#include <chrono>
#include <memory>

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
class  ConnectionTimeout;

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
  StatusT             status{SLOT_EMPTY};
  int                 fd{-1};  // TODO: Replace with FileDescriptor for RAII safety
  uint32_t            stripe_id{0};
  uint32_t            slot_id{0};
  // Monotonically increasing generation counter.  Incremented on every
  // close/reset so that reactor callbacks captured at an earlier generation
  // can detect slot reuse and bail out without touching stale fields.
  std::atomic<uint32_t> generation{0};

  // Long-lived process that owns this slot's read/write-ready
  // notifications (set once, at register_socket/4 time).
  NifPid              owner_pid{};
  ErlNifMonitor       owner_monitor{};  // valid when slot_ref != nullptr
  // Per-connection NIF resource that backs the process monitor.
  // Allocated at monitor_owner() time, released at demonitor_owner() time.
  // ERTS dispatches on_down directly to this pointer — O(1), no scan.
  arterial::SlotRef*  slot_ref{nullptr};

  // Buffer management for pending writes
  std::vector<char>   pending_buffer;
  std::size_t         bytes_written{0};

  // Throttling state: time spacing throttle for this slot
  Throttle            throttle{0, 1000};

#ifdef HAVE_OPENSSL
  SSL*                ssl{nullptr};
  ProtocolType        protocol{PROTO_TCP};
#endif

  // Connection timeout management
  std::unique_ptr<ConnectionTimeout> timer;

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
    status.store(SLOT_EMPTY, std::memory_order_release);
    fd = -1;
    owner_pid = {};
    owner_monitor = {};
    slot_ref = nullptr;
    pending_buffer.clear();
    bytes_written = 0;

    // Cancel any active timeout - RAII cleanup
    timer.reset();  // Destructor handles cancellation automatically

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

  // Register/re-arm fd with the pool's Reactor for async I/O notification.
  // All events (read, write, connect-complete, timeout) are dispatched by the
  // reactor thread, which calls handle_readable/handle_writable/notify_and_close
  // directly in C++ and sends high-level messages to owner_pid via enif_send.
  // No enif_select is used — the Reactor owns the fd lifecycle entirely.
  inline int arm_read(ErlNifEnv* env, const PoolContext* ctx);
  inline int arm_write(ErlNifEnv* env, const PoolContext* ctx);
  inline int arm_connect(ErlNifEnv* env, const PoolContext* ctx);
  // Install a connect-phase timeout via the Reactor.
  // Replaces the old nifpp::select_read(timerfd, ...) approach entirely.
  inline void set_connect_timeout(const PoolContext* ctx, uint64_t timeout_ms);

  //===========================================================================
  // High-level Connection Event Handlers
  //===========================================================================

  // Result types for connection event handling
  enum class ReadResult {
    DATA,           // Data read successfully, binary attached
    CLOSED,         // Connection closed
    HANDSHAKE_READ, // SSL handshake needs more reads
    HANDSHAKE_WRITE,// SSL handshake needs writes
    CONNECT_OK,     // Connection established successfully
    CONNECT_FAILED, // Connection failed
    ERROR           // Other error occurred
  };

  enum class WriteResult {
    OK,             // Write completed successfully
    CLOSED,         // Connection closed
    HANDSHAKE_READ, // SSL handshake needs reads
    HANDSHAKE_WRITE,// SSL handshake needs more writes
    CONNECT_OK,     // Connection established successfully
    CONNECT_FAILED, // Connection failed
    ERROR           // Other error occurred
  };

  struct ReadResultData {
    ReadResult result;
    nifpp::binary data;        // For DATA result
    bool send_connect_msg;     // Whether to send connection result message
    TERM connect_result;       // am_ok or am_connect_failed

    ReadResultData(ReadResult r) : result(r), data(0), send_connect_msg(false) {}
    ReadResultData(ReadResult r, nifpp::binary&& d) : result(r), data(std::move(d)), send_connect_msg(false) {}
  };

  struct WriteResultData {
    WriteResult result;
    bool send_connect_msg;     // Whether to send connection result message
    TERM connect_result;       // am_ok or am_connect_failed

    WriteResultData(WriteResult r, bool send_msg = false, TERM result = am_unknown)
      : result(r), send_connect_msg(send_msg), connect_result(result) {}
  };

  // Handle readable events - contains all the business logic
  ReadResultData handle_readable(ErlNifEnv* env, PoolContext* ctx);

  // Handle writable events - contains all the business logic
  WriteResultData handle_writable(ErlNifEnv* env, PoolContext* ctx);

  //===========================================================================
  // Connection Establishment Methods
  //===========================================================================

  // Result types for connection operations
  enum class ConnectResult {
    OK,                 // Connection established successfully, slot ready
    CONNECTING,         // Connection in progress, slot reserved
    FAILED,             // Connection failed
    STRIPE_FULL,        // No available slots in stripe
    SOCKET_FAILED,      // Socket creation failed
    CONFIG_FAILED,      // Socket configuration failed
    SELECT_FAILED,      // Event registration failed
    SSL_FAILED          // SSL setup/handshake failed
  };

  struct ConnectResultData {
    ConnectResult result;
    int           slot_id; // Valid slot ID for successful connections
    TERM     error_reason; // Specific error atom for failures

    ConnectResultData(ConnectResult r, int slot = -1)
      : result(r), slot_id(slot), error_reason(am_unknown) {}
    ConnectResultData(ConnectResult r, int slot, TERM reason)
      : result(r), slot_id(slot), error_reason(reason) {}
  };

  // Protocol-aware connection establishment
  static ConnectResultData connect_proto(ErlNifEnv* env, PoolContext* ctx,
                                       unsigned int stripe_id,
                                       const IP4Tuple& octets,
                                       int port, unsigned int timeout_ms,
                                       ProtocolType protocol, bool nodelay,
                                       const ErlNifPid& owner_pid);

  // Protocol-aware async connection establishment
  static ConnectResultData connect_async_proto(ErlNifEnv* env, PoolContext* ctx,
                                             unsigned int stripe_id,
                                             const IP4Tuple& octets,
                                             int port, ProtocolType protocol, bool nodelay,
                                             const ErlNifPid& owner_pid);

  //===========================================================================
  // Send and Release Method
  //===========================================================================

  // Result types for send and release operations
  enum class SendResult {
    OK,                 // Data sent successfully, slot released
    PARTIAL,            // Partial write, slot still leased for completion
    POOL_BUSY,          // No available slots in stripe
    WRITE_FAILED,       // Write operation failed, connection closed
    CLOSED              // Connection closed during operation
  };

  struct SendResultData {
    SendResult result;
    int        slot_id; // Slot ID used for the operation
    TERM  error_reason; // Specific error atom for failures

    SendResultData(SendResult r, int slot = -1)
      : result(r), slot_id(slot), error_reason(am_unknown) {}
    SendResultData(SendResult r, int slot, TERM reason)
      : result(r), slot_id(slot), error_reason(reason) {}
  };

  // Send data and release slot (with automatic retry and slot selection)
  static SendResultData send_and_release(ErlNifEnv* env, PoolContext* ctx,
                                        unsigned int stripe_id,
                                        ERL_NIF_TERM data_list);

  // Connection establishment with socket options
  static ConnectResultData connect_with_opts(ErlNifEnv* env, PoolContext* ctx,
                                            unsigned int stripe_id,
                                            const IP4Tuple& octets,
                                            int port, unsigned int timeout_ms,
                                            bool nodelay, const ErlNifPid& owner_pid,
                                            ERL_NIF_TERM socket_opts);

  // Protocol-aware connection establishment with socket options
  static ConnectResultData connect_proto_with_opts(ErlNifEnv* env, PoolContext* ctx,
                                                  unsigned int stripe_id,
                                                  const IP4Tuple& octets,
                                                  int port, unsigned int timeout_ms,
                                                  ProtocolType protocol, bool nodelay,
                                                  const ErlNifPid& owner_pid,
                                                  ERL_NIF_TERM socket_opts);

  //===========================================================================
  // FIFO Operations Methods
  //===========================================================================

  // Result types for FIFO operations
  enum class FifoResult {
    OK,                    // Operation completed successfully
    REQUEST_SENT,          // FIFO request sent (partial or complete)
    POOL_BUSY,            // No available slots in stripe
    SLOT_BUSY,            // FIFO slot is already busy
    PARTIAL,              // Partial operation, needs retry
    WRITE_FAILED,         // Write operation failed
    TIMEOUT,              // Operation timed out
    INVALID_RESERVATION,  // Invalid reservation ID
    NOT_ENABLED           // FIFO mode not enabled
  };

  struct FifoResultData {
    FifoResult result;
    int        slot_id;         // Valid slot ID for successful operations
    uint64_t   reservation_id;  // Reservation ID for FIFO operations
    TERM       error_reason;    // Specific error atom for failures

    FifoResultData(FifoResult r, int slot = -1, uint64_t res_id = 0)
      : result(r), slot_id(slot), reservation_id(res_id), error_reason(am_unknown) {}
    FifoResultData(FifoResult r, int slot, uint64_t res_id, TERM reason)
      : result(r), slot_id(slot), reservation_id(res_id), error_reason(reason) {}
  };

  // Reserve slot and send FIFO request atomically (performance optimization)
  static FifoResultData reserve_send_fifo_request(ErlNifEnv* env, PoolContext* ctx,
                                                 unsigned int stripe_id,
                                                 ERL_NIF_TERM data_list,
                                                 unsigned int reserv_timeout,
                                                 unsigned int req_timeout);
};

} // namespace arterial