#include "enif.hpp"
#include "throttle.hpp"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <bit>
#include <errno.h>
#include <fcntl.h>
#include <cstring>
#include <unistd.h>
#include <ctime>
#include <chrono>

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#endif

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

enum SlotStatus : uint32_t {
  SLOT_EMPTY         = 0,
  SLOT_AVAILABLE     = 1,
  SLOT_LEASED        = 2,
  SLOT_WRITE_POLLING = 3,
  SLOT_CONNECTING    = 4,
  SLOT_SSL_HANDSHAKE = 5
};

enum ProtocolType : uint32_t {
  PROTO_TCP = 0,
  PROTO_UDP = 1,
  PROTO_SSL = 2
};

struct alignas(64) ConnSlot {
  std::atomic<uint32_t> status{SLOT_EMPTY};
  int fd{-1};
  unsigned int stripe_id{0};
  unsigned int slot_id{0};

  // Long-lived process that owns this slot's read/write-ready
  // notifications (set once, at register_socket/4 time).
  ErlNifPid owner_pid{};

  std::vector<char> pending_buffer;
  size_t bytes_written{0};

  // Throttling state: time spacing throttle for this slot
  arterial::time_spacing_throttle throttle{0, 1000};

#ifdef HAVE_OPENSSL
  SSL* ssl{nullptr};
  ProtocolType protocol{PROTO_TCP};
#endif
};

// Fixed-size: ConnSlot/PoolStripe hold std::atomic members, so they're
// neither movable nor copyable -- a std::vector<ConnSlot> could never
// grow/resize (every growth path needs to relocate existing elements).
// 64 is already the hard cap (lease_mask is one uint64), so a plain
// array costs nothing extra.
struct PoolStripe {
  std::atomic<uint64_t> lease_mask;  // Initialized explicitly in init_pool_nif
  std::array<ConnSlot, 64> slots{};
  size_t capacity{0};
};

struct PoolContext {
  // unique_ptr<PoolStripe>, not PoolStripe, for the same reason: the
  // vector itself must be able to grow (move elements) at init_pool
  // time, which a non-movable PoolStripe can't do directly.
  std::vector<std::unique_ptr<PoolStripe>> stripes;
  size_t stripe_count{0};

  // Throttling configuration (0 means no throttling)
  uint32_t throttle_rate_per_sec{0};   // tokens per second
  uint32_t throttle_burst{0};          // maximum burst tokens

  // Raw fds aren't RAII-managed by any member here, so closing them on
  // teardown needs an explicit destructor (unlike ConnectionPool in
  // arterial.hpp, which owns no raw resources and needs none).
  ~PoolContext() {
    for (auto& stripe_ptr : stripes)
      for (auto& slot : stripe_ptr->slots)
        if (slot.fd != -1) close(slot.fd);
  }
};

//===========================================================================
// Custom atoms (initialized once in load(); see nifpp::initialize_known_atoms
// for am_ok/am_error, shared with arterial.cpp)
//===========================================================================
static atom am_arterial_event;
static atom am_read;
static atom am_write;
static atom am_closed;
static atom am_stop;
static atom am_connect_result;
static atom am_connecting;
static atom am_stripe_full;
static atom am_max_slots_exceeded_64;
static atom am_failed_to_set_nonblocking;
static atom am_socket_failed;
static atom am_connect_failed;
static atom am_timeout;
static atom am_no_connections_available;
static atom am_write_failed;
static atom am_alloc_failed;
static atom am_unsupported_protocol;

#ifdef HAVE_OPENSSL
// Global SSL context - initialized once
static SSL_CTX* g_ssl_ctx = nullptr;
#endif

//===========================================================================
// SSL Helpers
//===========================================================================

#ifdef HAVE_OPENSSL
static bool init_ssl_context() {
  if (g_ssl_ctx) return true;

  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();

  const SSL_METHOD* method = TLS_client_method();
  g_ssl_ctx = SSL_CTX_new(method);
  if (!g_ssl_ctx) {
    ERR_print_errors_fp(stderr);
    return false;
  }

  // Set some secure defaults
  SSL_CTX_set_verify(g_ssl_ctx, SSL_VERIFY_PEER, nullptr);
  SSL_CTX_set_verify_depth(g_ssl_ctx, 4);
  SSL_CTX_set_options(g_ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

  return true;
}

static void cleanup_ssl() {
  if (g_ssl_ctx) {
    SSL_CTX_free(g_ssl_ctx);
    g_ssl_ctx = nullptr;
  }
  EVP_cleanup();
  ERR_free_strings();
}

static void cleanup_slot_ssl(ConnSlot& slot) {
  if (slot.ssl) {
    SSL_shutdown(slot.ssl);
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
  }
}

// Forward declarations for SSL functions used in connection handling
static bool setup_ssl_on_socket(ConnSlot& slot, int fd);
static int ssl_handshake_step(ConnSlot& slot);
#endif


//===========================================================================
// Helpers
//===========================================================================

static TERM make_event_msg(ErlNifEnv* env, unsigned int stripe_id,
                            unsigned int slot_id, const atom& kind) {
  return make(env, std::make_tuple(am_arterial_event, stripe_id, slot_id, kind));
}

static TERM make_connect_result_msg(ErlNifEnv* env, unsigned int stripe_id,
                                   unsigned int slot_id, const atom& result) {
  return make(env, std::make_tuple(am_arterial_event, stripe_id, slot_id, am_connect_result, result));
}

// Re-arm (one-shot) read/write readiness notification, targeted at the
// slot's owner pid, using a freshly allocated env each time -- enif_select
// permanently adopts msg/msg_env, so it can never be reused across calls.
static int arm_read(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  nifpp::msg_env msg_env;
  auto msg = make_event_msg(msg_env, slot.stripe_id, slot.slot_id, am_read);
  return nifpp::select_read(env, slot.fd, ctx, &slot.owner_pid, msg, msg_env);
}

static int arm_write(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  nifpp::msg_env msg_env;
  auto msg = make_event_msg(msg_env, slot.stripe_id, slot.slot_id, am_write);
  return nifpp::select_write(env, slot.fd, ctx, &slot.owner_pid, msg, msg_env);
}

static int arm_connect(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  nifpp::msg_env msg_env;
  auto msg = make_event_msg(msg_env, slot.stripe_id, slot.slot_id, am_write);
  return nifpp::select_write(env, slot.fd, ctx, &slot.owner_pid, msg, msg_env);
}

// One-shot heads-up to the owner that this slot's connection just died,
// followed by ERL_NIF_SELECT_STOP, which deselects both read and write and
// asynchronously invokes pool_resource_stop (the only place fd is actually
// closed -- never directly here, in case a select is still in flight).
//
// Skips the message when the *caller* is already the owner (e.g.
// handle_readable_nif discovering the close itself): that caller's own
// "closed" return value is enough, and additionally enif_send-ing it the
// same news would double-fire arterial_connection2's disconnect handling
// (a second, redundant reconnect-timer schedule -- mostly harmless, but
// pointless). Only a transient, non-owner caller (e.g.
// send_and_release_nif's caller) has no other way to learn the
// connection died and genuinely needs the proactive message.
static int notify_and_close(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  ErlNifPid self_pid;
  enif_self(env, &self_pid);
  if (enif_compare_pids(&self_pid, &slot.owner_pid) != 0) {
    nifpp::msg_env msg_env;  // Auto-frees at the end of the scope
    auto msg = make_event_msg(msg_env, slot.stripe_id, slot.slot_id, am_closed);
    enif_send(env, &slot.owner_pid, msg_env, msg);
  }
  return enif_select(env, slot.fd, ERL_NIF_SELECT_STOP, ctx, nullptr, am_stop);
}

// Claim the first unregistered slot in `stripe` for `fd`/`owner_pid` via
// CAS on its lease mask, arm its first read-readiness notification, and
// return the claimed slot id -- shared by register_socket_nif (handed an
// already-open fd) and connect_nif (which opens the fd itself). `fd` is
// expected already non-blocking.
static ERL_NIF_TERM claim_slot(ErlNifEnv* env, PoolContext* ctx, PoolStripe& stripe,
                                int fd, ErlNifPid owner_pid) {
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  while (true) {
    int slot_id = std::countr_zero(current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return make(env, std::make_tuple(am_error, am_stripe_full));

    auto& slot = stripe.slots[slot_id];
    if (slot.fd > -1) {
      current_mask &= ~(1ULL << slot_id);
      continue;
    }

    slot.fd = fd;
    slot.owner_pid = owner_pid;
    slot.status.store(SLOT_AVAILABLE, std::memory_order_relaxed);

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask & ~target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_release,
          std::memory_order_relaxed)) {
      arm_read(env, ctx, slot); // TODO: handle error
      return make(env, std::make_tuple(am_ok, static_cast<unsigned int>(slot_id)));
    }

    slot.fd = -1;
    slot.status.store(SLOT_EMPTY, std::memory_order_relaxed);
  }
}

// Resolve {PoolRef, StripeId, SlotId} (the shape shared by
// handle_readable/3, handle_writable/3, close_slot/3) to a ConnSlot&, or
// nullptr if any index is out of range.
static ConnSlot* resolve_slot(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[],
                               PoolContext** out_ctx) {
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;
  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !get(env, argv[2], slot_id) ||
      stripe_id >= ctx->stripe_count) {
    return nullptr;
  }
  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) return nullptr;
  *out_ctx = ctx;
  return &stripe.slots[slot_id];
}

//===========================================================================
// Resource callbacks
//===========================================================================

// Invoked by the runtime once it's safe to close a fd that was selected
// via enif_select (i.e. after ERL_NIF_SELECT_STOP, from notify_and_close/
// close_slot_nif) -- never call close() on a selected fd anywhere else.
//
// PoolContext's own destructor (run via the generic
// detail::resource_dtor<PoolContext> wired up by register_resource<>())
// closes any fds still open at resource-teardown time, so this is the
// only other place a slot's fd is ever close()'d.
static void pool_resource_stop(PoolContext* ctx, ErlNifEnv*, ErlNifEvent fd, int /*is_direct_call*/) {
  for (auto& stripe_ptr : ctx->stripes) {
    auto& stripe = *stripe_ptr;
    for (auto& slot : stripe.slots) {
      if (slot.fd == fd) {
        close(slot.fd);
        slot.fd = -1;
        slot.pending_buffer.clear();
        slot.bytes_written = 0;
        slot.status.store(SLOT_EMPTY, std::memory_order_release);
        stripe.lease_mask.fetch_or(1ULL << slot.slot_id, std::memory_order_release);
        return;
      }
    }
  }
}

//===========================================================================
// NIFs
//===========================================================================

static ERL_NIF_TERM init_pool_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  unsigned int num_stripes;
  unsigned int slots_per_stripe;

  if (argc != 2 ||
      !get(env, argv[0], num_stripes) ||
      !get(env, argv[1], slots_per_stripe)) {
    return enif_make_badarg(env);
  }

  if (slots_per_stripe > 64) {
    return make(env, std::make_tuple(am_error, am_max_slots_exceeded_64));
  }

  auto ctx = construct_resource_with_events<PoolContext>(
    resource_events<PoolContext>(nullptr, pool_resource_stop));

  ctx->stripe_count = num_stripes;
  ctx->stripes.resize(num_stripes);

  for (unsigned int i = 0; i < num_stripes; ++i) {
    ctx->stripes[i] = std::make_unique<PoolStripe>();
    auto& stripe = *ctx->stripes[i];
    stripe.capacity = slots_per_stripe;

    // Initialize lease mask: 0 = available, 1 = leased
    // Set all slots beyond capacity as permanently leased (unavailable)
    uint64_t initial_mask;
    if (slots_per_stripe < 64) {
      initial_mask = ~((1ULL << slots_per_stripe) - 1);
    } else {
      initial_mask = 0ULL;
    }
    stripe.lease_mask.store(initial_mask, std::memory_order_relaxed);

    for (unsigned int j = 0; j < 64; ++j) {
      stripe.slots[j].fd = -1;
      stripe.slots[j].stripe_id = i;
      stripe.slots[j].slot_id = j;
    }
  }

  return make(env, std::make_tuple(am_ok, ctx));
}

// Configure throttling for a pool
static ERL_NIF_TERM configure_throttle_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int rate_per_sec;
  unsigned int burst;

  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], rate_per_sec) ||
      !get(env, argv[2], burst)) {
    return enif_make_badarg(env);
  }

  ctx->throttle_rate_per_sec = rate_per_sec;
  ctx->throttle_burst = burst;

  // Initialize throttle for each slot in each stripe
  auto now = arterial::now_utc();
  for (auto& stripe_ptr : ctx->stripes) {
    for (auto& slot : stripe_ptr->slots) {
      // Initialize time spacing throttle with the configured rate
      // Using 1000ms window (1 second) as default
      slot.throttle.init(rate_per_sec, 1000, now);
    }
  }

  return am_ok;
}

static ERL_NIF_TERM register_socket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;
  int raw_fd;
  ErlNifPid owner_pid;

  if (argc != 4 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !get(env, argv[2], raw_fd) || raw_fd < 0 ||
      !get(env, argv[3], owner_pid)) {
    return enif_make_badarg(env);
  }

  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);
  auto& stripe = *ctx->stripes[stripe_id];

  int flags = fcntl(raw_fd, F_GETFL, 0);
  if (flags == -1 || fcntl(raw_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }

  return claim_slot(env, ctx, stripe, raw_fd, owner_pid);
}

// Open and connect a brand-new IPv4 TCP socket entirely inside this NIF
// (a dirty, IO-bound job -- see its ErlNifFunc entry -- since connect(2)
// can block for the full timeout), then claim a slot for it exactly like
// register_socket_nif. Unlike register_socket/4, the fd never has any
// other owner (no Erlang `socket()` term, no `prim_socket` resource
// fighting over it) -- the safer alternative to handing off an
// already-open fd, see arterial_connection2's moduledoc.
static ERL_NIF_TERM connect_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;
  int port;
  unsigned int timeout_ms;
  bool nodelay;
  ErlNifPid owner_pid;

  if (argc != 7 ||
      !get(env, argv[0], ctx)        ||
      !get(env, argv[1], stripe_id)  ||
      !get(env, argv[2], octets)     ||
      !get(env, argv[3], port)       || port < 0 || port > 65535 ||
      !get(env, argv[4], timeout_ms) ||
      !get(env, argv[5], nodelay)    ||
      !get(env, argv[6], owner_pid)) {
    return enif_make_badarg(env);
  }
  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);
  auto& stripe = *ctx->stripes[stripe_id];

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return make(env, std::make_tuple(am_error, am_socket_failed));
  }

  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }
  if (nodelay) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }

  struct sockaddr_in addr{};
  auto [o0, o1, o2, o3] = octets;
  addr.sin_family       = AF_INET;
  addr.sin_port         = htons(static_cast<uint16_t>(port));
  uint32_t ip_host      = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  addr.sin_addr.s_addr  = htonl(ip_host);

  int rc = connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  if (rc < 0 && errno != EINPROGRESS) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_connect_failed));
  }
  // For EINPROGRESS, connection is in progress - proceed with slot claiming
  // The slot will be marked as SLOT_CONNECTING and completion will be
  // handled via enif_select write-ready notifications

  return claim_slot(env, ctx, stripe, fd, owner_pid);
}

// Non-blocking version of connect_nif: starts connection and returns immediately,
// then sends completion notification via message when connection completes or fails
static ERL_NIF_TERM connect_async_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;
  int port;
  bool nodelay;
  ErlNifPid owner_pid;

  if (argc != 6 ||
      !get(env, argv[0], ctx)        ||
      !get(env, argv[1], stripe_id)  ||
      !get(env, argv[2], octets)     ||
      !get(env, argv[3], port)       || port < 0 || port > 65535 ||
      !get(env, argv[4], nodelay)    ||
      !get(env, argv[5], owner_pid)) {
    return enif_make_badarg(env);
  }
  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);
  auto& stripe = *ctx->stripes[stripe_id];

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return make(env, std::make_tuple(am_error, am_socket_failed));
  }

  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }
  if (nodelay) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(static_cast<uint16_t>(port));
  uint32_t ip_host     = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  addr.sin_addr.s_addr = htonl(ip_host);

  int rc = connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  if (rc < 0 && errno != EINPROGRESS) [[unlikely]] {
    close(fd);
    return make(env, std::make_tuple(am_error, am_connect_failed));
  }

  // Find and claim a slot for this connecting socket
  auto current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  int  slot_id      = -1;

  do {
    slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]] {
      close(fd);
      return make(env, std::make_tuple(am_error, am_stripe_full));
    }

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) [[likely]]
      break;

  } while (true);

  ConnSlot& slot = stripe.slots[slot_id];
  slot.fd = fd;
  slot.stripe_id = stripe_id;
  slot.slot_id = slot_id;
  slot.owner_pid = owner_pid;
  slot.pending_buffer.clear();
  slot.bytes_written = 0;

  if (rc == 0) {
    // Connection completed immediately
    slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
    arm_read(env, ctx, slot);
    return make(env, std::make_tuple(am_ok, static_cast<unsigned int>(slot_id)));
  } else {
    // Connection in progress - arm write notification for completion
    slot.status.store(SLOT_CONNECTING, std::memory_order_release);
    arm_connect(env, ctx, slot);
    return make(env, std::make_tuple(am_ok, am_connecting, static_cast<unsigned int>(slot_id)));
  }
}

// Time spacing throttling check - returns true if the request was allowed
static inline bool throttle_allow(PoolContext* ctx, ConnSlot& slot) {
  return ctx->throttle_rate_per_sec == 0                // No throttling configured
      || slot.throttle.add(1, arterial::now_utc()) > 0; // check if we can add one request
}

static ERL_NIF_TERM send_and_release_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;

  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !enif_is_list(env, argv[2])) [[unlikely]]
    return enif_make_badarg(env);

  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);

  auto  list         = argv[2];
  auto& stripe       = *ctx->stripes[stripe_id];
  auto  current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  auto  slot_id      = -1;

  // Original CAS loop structure - throttle check after successful CAS
  do {
    slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return make(env, std::make_tuple(am_error, am_no_connections_available));

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) [[likely]] {

      // CAS succeeded - now check if slot is available and passes throttling
      auto& candidate_slot = stripe.slots[slot_id];
      if (candidate_slot.status.load(std::memory_order_relaxed) == SLOT_AVAILABLE &&
          throttle_allow(ctx, candidate_slot)) {
        // Success - slot is leased and passes throttling
        break;
      } else {
        // Slot doesn't pass throttling or isn't available - release it and try next
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
        continue;
      }
    }

  } while (true);

  auto& slot = stripe.slots[slot_id];
  slot.status.store(SLOT_LEASED, std::memory_order_relaxed);
  TERM slot_term = make(env, static_cast<unsigned int>(slot_id));

  unsigned int list_len = 0;
  enif_get_list_length(env, list, &list_len);

  // Inline storage for the common case (arterial_client2 always calls
  // this with a single-element list) -- avoids a heap allocation on
  // every write; only lists longer than this fall back to the heap.
  constexpr size_t s_inline_iov_size = 8;
  std::array<struct iovec, s_inline_iov_size> inline_iov;
  std::vector<struct iovec>                   heap_iov;
  struct iovec* iov;
  if (list_len <= s_inline_iov_size) {
    iov = inline_iov.data();
  } else {
    heap_iov.resize(list_len);
    iov = heap_iov.data();
  }

  ERL_NIF_TERM head, tail = list;
  unsigned int i = 0;
  size_t total_bytes = 0;

  while (enif_get_list_cell(env, tail, &head, &tail)) {
    ErlNifBinary bin;
    if (enif_inspect_binary(env, head, &bin)) {
      iov[i].iov_base = bin.data;
      iov[i].iov_len  = bin.size;
      total_bytes    += bin.size;
      i++;
    }
  }

  ssize_t written = 0;
  uint64_t target_bit = (1ULL << slot_id);

#ifdef HAVE_OPENSSL
  if (slot.ssl) {
    // SSL doesn't support writev, so we need to write sequentially
    for (unsigned int j = 0; j < i && written >= 0; ++j) {
      ssize_t n = SSL_write(slot.ssl, iov[j].iov_base, static_cast<int>(iov[j].iov_len));
      if (n > 0) {
        written += n;
      } else {
        int ssl_error =  SSL_get_error(slot.ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
          // Would block, we'll handle partial write below
          break;
        } else {
          // SSL error
          cleanup_slot_ssl(slot);
          notify_and_close(env, ctx, slot);
          return make(env, std::make_tuple(am_error, am_write_failed));
        }
      }

      // Check if we wrote the complete iovec entry
      if (n < static_cast<ssize_t>(iov[j].iov_len)) {
        // Partial write, we need to handle this in the buffer logic below
        break;
      }
    }
  } else
#endif
  {
    written = (i > 0) ? writev(slot.fd, iov, i) : 0;

    if (written < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        written = 0;
      else {
        notify_and_close(env, ctx, slot);
        return make(env, std::make_tuple(am_error, am_write_failed));
      }
    }
  }

  if (static_cast<size_t>(written) < total_bytes) {
    slot.pending_buffer.resize(total_bytes);
    size_t offset = 0;
    for (unsigned int j = 0; j < i; ++j) {
      std::memcpy(slot.pending_buffer.data() + offset, iov[j].iov_base, iov[j].iov_len);
      offset += iov[j].iov_len;
    }
    slot.bytes_written = static_cast<size_t>(written);
    slot.status.store(SLOT_WRITE_POLLING, std::memory_order_release);

    arm_write(env, ctx, slot); // TODO: handle errors
    return make(env, std::make_tuple(am_ok, slot_term));
  }

  slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
  stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
  return make(env, std::make_tuple(am_ok, slot_term));
}

static ERL_NIF_TERM handle_readable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  if (slot.fd == -1) return am_closed;

  // FIONREAD is only a sizing hint, never proof of anything: it can
  // legitimately report 0 on a perfectly healthy connection (e.g. under
  // heavy concurrent load) without that meaning EOF -- only read(2)'s
  // own return value (0 = EOF, -1/EAGAIN = nothing available right now,
  // not closed) is authoritative. Treating a 0 FIONREAD as "closed"
  // outright (as this used to) spuriously killed live connections under
  // load.
  int bytes_available = 0;
  ioctl(slot.fd, FIONREAD, &bytes_available);
  size_t read_size = bytes_available > 0 ? static_cast<size_t>(bytes_available) : 8192;

  ErlNifBinary bin;
  if (!enif_alloc_binary(read_size, &bin)) [[unlikely]] {
    return make(env, std::make_tuple(am_error, am_alloc_failed));
  }

  ssize_t n;

#ifdef HAVE_OPENSSL
  if (slot.ssl) {
    n = SSL_read(slot.ssl, bin.data, static_cast<int>(read_size));
    if (n <= 0) {
      int ssl_error = SSL_get_error(slot.ssl, static_cast<int>(n));
      enif_release_binary(&bin);

      if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
        arm_read(env, ctx, slot);
        ErlNifBinary empty; enif_alloc_binary(0, &empty);
        return make(env, std::make_tuple(am_ok, TERM(enif_make_binary(env, &empty))));
      }

      // SSL connection closed or error
      cleanup_slot_ssl(slot);
      notify_and_close(env, ctx, slot);
      return am_closed;
    }
  } else
#endif
  {
    n = read(slot.fd, bin.data, read_size);

    if (n <= 0) {
      enif_release_binary(&bin);
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        arm_read(env, ctx, slot);
        ErlNifBinary empty; enif_alloc_binary(0, &empty);
        return make(env, std::make_tuple(am_ok, TERM(enif_make_binary(env, &empty))));
      }
      notify_and_close(env, ctx, slot);
      return am_closed;
    }
  }

  if (static_cast<size_t>(n) < bin.size) enif_realloc_binary(&bin, static_cast<size_t>(n));
  auto bin_term = TERM(enif_make_binary(env, &bin));

  arm_read(env, ctx, slot);
  return make(env, std::make_tuple(am_ok, bin_term));
}

static ERL_NIF_TERM handle_writable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  if (slot.fd == -1) return am_closed;

  uint32_t current_status = slot.status.load(std::memory_order_acquire);

  // Handle connection completion for async connect
  if (current_status == SLOT_CONNECTING) {
    int so_err = 0;
    socklen_t len = sizeof(so_err);
    if (getsockopt(slot.fd, SOL_SOCKET, SO_ERROR, &so_err, &len) == -1 || so_err != 0) {
      // Connection failed
      nifpp::msg_env msg_env;
      auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_connect_failed);
      enif_send(env, &slot.owner_pid, msg_env, msg);
      notify_and_close(env, ctx, slot);
      return am_closed;
    } else {
      // Connection succeeded, check if we need SSL handshake
#ifdef HAVE_OPENSSL
      if (slot.protocol == PROTO_SSL) {
        if (!setup_ssl_on_socket(slot, slot.fd)) {
          nifpp::msg_env msg_env;
          auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_connect_failed);
          enif_send(env, &slot.owner_pid, msg_env, msg);
          notify_and_close(env, ctx, slot);
          return am_closed;
        }

        // Start SSL handshake
        int handshake_result = ssl_handshake_step(slot);
        if (handshake_result == 1) {
          // Handshake completed
          slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
          nifpp::msg_env msg_env;
          auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_ok);
          enif_send(env, &slot.owner_pid, msg_env, msg);
          arm_read(env, ctx, slot);
          return am_ok;
        } else if (handshake_result == 0) {
          // Handshake needs more time
          slot.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          arm_connect(env, ctx, slot); // Continue with SSL handshake
          return am_ok;
        } else {
          // Handshake failed
          cleanup_slot_ssl(slot);
          nifpp::msg_env msg_env;
          auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_connect_failed);
          enif_send(env, &slot.owner_pid, msg_env, msg);
          notify_and_close(env, ctx, slot);
          return am_closed;
        }
      } else
#endif
      {
        // Plain TCP connection succeeded
        slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
        nifpp::msg_env msg_env;
        auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_ok);
        enif_send(env, &slot.owner_pid, msg_env, msg);
        arm_read(env, ctx, slot);
        return am_ok;
      }
    }
  }

#ifdef HAVE_OPENSSL
  // Handle SSL handshake continuation
  if (current_status == SLOT_SSL_HANDSHAKE) {
    int handshake_result = ssl_handshake_step(slot);
    if (handshake_result == 1) {
      // Handshake completed
      slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
      nifpp::msg_env msg_env;
      auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_ok);
      enif_send(env, &slot.owner_pid, msg_env, msg);
      arm_read(env, ctx, slot);
      return am_ok;
    } else if (handshake_result == 0) {
      // Handshake still needs more time
      arm_connect(env, ctx, slot);
      return am_ok;
    } else {
      // Handshake failed
      cleanup_slot_ssl(slot);
      nifpp::msg_env msg_env;
      auto msg = make_connect_result_msg(msg_env, slot.stripe_id, slot.slot_id, am_connect_failed);
      enif_send(env, &slot.owner_pid, msg_env, msg);
      notify_and_close(env, ctx, slot);
      return am_closed;
    }
  }
#endif

  size_t remaining = slot.pending_buffer.size() - slot.bytes_written;
  while (remaining > 0) {
    ssize_t n;

#ifdef HAVE_OPENSSL
    if (slot.ssl) {
      n = SSL_write(slot.ssl, slot.pending_buffer.data() + slot.bytes_written, static_cast<int>(remaining));
      if (n <= 0) {
        int ssl_error = SSL_get_error(slot.ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
          arm_write(env, ctx, slot);
          return am_ok;
        }
        cleanup_slot_ssl(slot);
        notify_and_close(env, ctx, slot);
        return am_closed;
      }
    } else
#endif
    {
      n = write(slot.fd, slot.pending_buffer.data() + slot.bytes_written, remaining);
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          arm_write(env, ctx, slot);
          return am_ok;
        }
        notify_and_close(env, ctx, slot);
        return am_closed;
      }
    }

    slot.bytes_written += static_cast<size_t>(n);
    remaining -= static_cast<size_t>(n);
  }

  slot.pending_buffer.clear();
  slot.bytes_written = 0;
  slot.status.store(SLOT_AVAILABLE, std::memory_order_release);

  auto& stripe = *ctx->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << slot.slot_id), std::memory_order_release);
  return am_ok;
}

// Helper function to create socket for a specific protocol
static int create_socket_for_protocol(ProtocolType protocol) {
  switch (protocol) {
    case PROTO_TCP:
      return socket(AF_INET, SOCK_STREAM, 0);
    case PROTO_UDP:
      return socket(AF_INET, SOCK_DGRAM, 0);
    case PROTO_SSL:
      // SSL uses TCP socket - SSL handshake happens after connection
      return socket(AF_INET, SOCK_STREAM, 0);
    default:
      return -1;
  }
}

// Helper function to set socket options based on protocol
static bool configure_socket_for_protocol(int fd, ProtocolType protocol, bool nodelay) {
  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) return false;

  switch (protocol) {
    case PROTO_TCP:
    case PROTO_SSL:  // SSL uses TCP socket underneath
      if (nodelay) {
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      }
      break;
    case PROTO_UDP:
      // UDP doesn't need nodelay, already connectionless
      break;
  }
  return true;
}

// Socket options support
struct SocketOption {
  int level;
  int optname;
  int value;
  bool is_boolean;
};

// Parse Erlang socket options list and apply to socket
static bool apply_socket_options(int fd, ErlNifEnv* env, ERL_NIF_TERM options_list) {
  if (enif_is_empty_list(env, options_list)) {
    return true; // No options to apply
  }

  ERL_NIF_TERM head, tail = options_list;

  while (enif_get_list_cell(env, tail, &head, &tail)) {
    // Parse each option - can be atom or {Level, OptName, Value} tuple
    if (enif_is_atom(env, head)) {
      // Handle common atom-based options
      char opt_name[64];
      if (enif_get_atom(env, head, opt_name, sizeof(opt_name), ERL_NIF_LATIN1)) {
        if (strcmp(opt_name, "keepalive") == 0) {
          int one = 1;
          if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) != 0) {
            return false;
          }
        } else if (strcmp(opt_name, "nodelay") == 0) {
          int one = 1;
          if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
            return false;
          }
        } else if (strcmp(opt_name, "reuseaddr") == 0) {
          int one = 1;
          if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) {
            return false;
          }
        }
      }
    } else {
      // Handle tuple-based options {Option, Value}
      const ERL_NIF_TERM* tuple_elements;
      int tuple_arity;

      if (enif_get_tuple(env, head, &tuple_arity, &tuple_elements) && tuple_arity == 2) {
        char opt_name[64];
        int opt_value;

        if (enif_get_atom(env, tuple_elements[0], opt_name, sizeof(opt_name), ERL_NIF_LATIN1) &&
            enif_get_int(env, tuple_elements[1], &opt_value)) {

          if (strcmp(opt_name, "keepalive") == 0) {
            int value = opt_value ? 1 : 0;
            if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &value, sizeof(value)) != 0)
              return false;
          } else if (strcmp(opt_name, "nodelay") == 0) {
            int value = opt_value ? 1 : 0;
            if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &value, sizeof(value)) != 0)
              return false;
          } else if (strcmp(opt_name, "reuseaddr") == 0) {
            int value = opt_value ? 1 : 0;
            if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &value, sizeof(value)) != 0)
              return false;
          } else if (strcmp(opt_name, "sndbuf") == 0) {
            if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "rcvbuf") == 0) {
            if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "priority") == 0) {
            if (setsockopt(fd, SOL_SOCKET, SO_PRIORITY, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "tos") == 0) {
            if (setsockopt(fd, IPPROTO_IP, IP_TOS, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "user_timeout") == 0) {
            // TCP_USER_TIMEOUT: modern way to control TCP retransmission timeout (milliseconds)
            if (setsockopt(fd, IPPROTO_TCP, TCP_USER_TIMEOUT, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "cork") == 0) {
            // TCP_CORK: batch small writes for efficiency
            int value = opt_value ? 1 : 0;
            if (setsockopt(fd, IPPROTO_TCP, TCP_CORK, &value, sizeof(value)) != 0)
              return false;
          } else if (strcmp(opt_name, "quickack") == 0) {
            // TCP_QUICKACK: disable delayed ACK algorithm
            int value = opt_value ? 1 : 0;
            if (setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &value, sizeof(value)) != 0)
              return false;
          } else if (strcmp(opt_name, "rcvlowat") == 0) {
            // SO_RCVLOWAT: minimum bytes for read readiness
            if (setsockopt(fd, SOL_SOCKET, SO_RCVLOWAT, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "sndlowat") == 0) {
            // SO_SNDLOWAT: minimum bytes for write readiness
            if (setsockopt(fd, SOL_SOCKET, SO_SNDLOWAT, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "keepidle") == 0) {
            // TCP_KEEPIDLE: seconds before starting keepalive probes
            if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "keepintvl") == 0) {
            // TCP_KEEPINTVL: interval between keepalive probes (seconds)
            if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &opt_value, sizeof(opt_value)) != 0)
              return false;
          } else if (strcmp(opt_name, "keepcnt") == 0) {
            // TCP_KEEPCNT: number of keepalive probes before giving up
            if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &opt_value, sizeof(opt_value)) != 0)
              return false;
          }
        } else if (enif_get_atom(env, tuple_elements[0], opt_name, sizeof(opt_name), ERL_NIF_LATIN1) &&
                   strcmp(opt_name, "linger") == 0) {
          // Handle {linger, {OnOff, LingerTime}} format
          const ERL_NIF_TERM* linger_tuple;
          int linger_arity;
          if (enif_get_tuple(env, tuple_elements[1], &linger_arity, &linger_tuple) && linger_arity == 2) {
            int on_off, linger_time;
            if (enif_get_int(env, linger_tuple[0], &on_off) &&
                enif_get_int(env, linger_tuple[1], &linger_time)) {
              struct linger l;
              l.l_onoff = on_off ? 1 : 0;
              l.l_linger = linger_time; // seconds
              if (setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l)) != 0)
                return false;
            }
          }
        }
      }
    }
  }

  return true;
}

#ifdef HAVE_OPENSSL
// Helper to setup SSL on a connected socket
static bool setup_ssl_on_socket(ConnSlot& slot, int fd) {
  if (!init_ssl_context()) return false;

  slot.ssl = SSL_new(g_ssl_ctx);
  if (!slot.ssl) return false;

  if (SSL_set_fd(slot.ssl, fd) != 1) {
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
    return false;
  }

  slot.protocol = PROTO_SSL;
  return true;
}

// Perform SSL handshake (non-blocking)
static int ssl_handshake_step(ConnSlot& slot) {
  if (!slot.ssl) return -1;

  int result = SSL_connect(slot.ssl);
  if (result == 1) {
    // Handshake completed successfully
    return 1;
  }

  int ssl_error = SSL_get_error(slot.ssl, result);
  if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
    // Handshake needs more data, continue later
    return 0;
  }

  // Handshake failed
  return -1;
}
#endif

// Helper to parse protocol atom
static ProtocolType parse_protocol(ErlNifEnv* env, ERL_NIF_TERM term) {
  atom proto_atom;
  if (!get(env, term, proto_atom)) return static_cast<ProtocolType>(-1);

  atom tcp_atom = atom(env, "tcp");
  atom udp_atom = atom(env, "udp");
  atom ssl_atom = atom(env, "ssl");

  if (proto_atom == tcp_atom) return PROTO_TCP;
  if (proto_atom == udp_atom) return PROTO_UDP;
  if (proto_atom == ssl_atom) return PROTO_SSL;

  return static_cast<ProtocolType>(-1);
}

// Protocol-aware version of connect_nif
static ERL_NIF_TERM connect_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;
  int port;
  unsigned int timeout_ms;
  bool nodelay;
  ErlNifPid owner_pid;

  if (argc != 8                     ||
      !get(env, argv[0], ctx)       ||
      !get(env, argv[1], stripe_id) ||
      !get(env, argv[2], octets)    ||
      !get(env, argv[3], port)      || port < 0 || port > 65535 ||
      !get(env, argv[4], timeout_ms)||
      !get(env, argv[6], nodelay)   ||
      !get(env, argv[7], owner_pid)) {
    return enif_make_badarg(env);
  }

  if (stripe_id >= ctx->stripe_count) {
    return enif_make_badarg(env);
  }

  ProtocolType protocol = parse_protocol(env, argv[5]);
  if (protocol == static_cast<ProtocolType>(-1)) {
    return enif_make_badarg(env);
  }

#ifndef HAVE_OPENSSL
  // SSL requires OpenSSL at compile time
  if (protocol == PROTO_SSL)
    return make(env, std::make_tuple(am_error, am_unsupported_protocol));
#endif

  int fd = create_socket_for_protocol(protocol);
  if (fd == -1) {
    return make(env, std::make_tuple(am_error, am_socket_failed));
  }

  if (!configure_socket_for_protocol(fd, protocol, nodelay)) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(static_cast<uint16_t>(port));
  uint32_t ip_host = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  server_addr.sin_addr.s_addr = htonl(ip_host);

  // For UDP, "connecting" just sets the default destination
  // For TCP/SSL, this is a real connection
  int rc = connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr));

  if (protocol == PROTO_UDP) {
    // UDP connect() just sets default peer, always succeeds immediately
    rc = 0;
  } else if (rc != 0 && errno != EINPROGRESS) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_connect_failed));
  }

  // For blocking connect on TCP/SSL, wait for completion
  if ((protocol == PROTO_TCP || protocol == PROTO_SSL) && rc != 0) {
    fd_set write_fds, error_fds;
    FD_ZERO(&write_fds);
    FD_ZERO(&error_fds);
    FD_SET(fd, &write_fds);
    FD_SET(fd, &error_fds);

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    int select_rc = select(fd + 1, nullptr, &write_fds, &error_fds, &tv);
    if (select_rc == 0) {
      close(fd);
      return make(env, std::make_tuple(am_error, am_timeout));
    } else if (select_rc < 0 || FD_ISSET(fd, &error_fds)) {
      close(fd);
      return make(env, std::make_tuple(am_error, am_connect_failed));
    }

    // Check if connection actually succeeded
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) != 0 || error != 0) {
      close(fd);
      return make(env, std::make_tuple(am_error, am_connect_failed));
    }

#ifdef HAVE_OPENSSL
    // For SSL, now perform the handshake
    if (protocol == PROTO_SSL) {
      auto& stripe = *ctx->stripes[stripe_id];

      // We need a temporary slot to perform handshake
      ConnSlot temp_slot{};
      temp_slot.fd = fd;

      if (!setup_ssl_on_socket(temp_slot, fd)) {
        close(fd);
        return make(env, std::make_tuple(am_error, am_connect_failed));
      }

      // Perform blocking SSL handshake with timeout
      time_t start_time = time(nullptr);
      time_t deadline = start_time + (timeout_ms / 1000);

      while (time(nullptr) < deadline) {
        int handshake_result = ssl_handshake_step(temp_slot);
        if (handshake_result == 1) {
          // Handshake completed, now claim slot and transfer SSL context
          ERL_NIF_TERM result = claim_slot(env, ctx, stripe, fd, owner_pid);

          // Find the claimed slot and transfer SSL context
          if (enif_is_tuple(env, result)) {
            const ERL_NIF_TERM* tuple_elements;
            int tuple_arity;
            if (enif_get_tuple(env, result, &tuple_arity, &tuple_elements) &&
                tuple_arity == 2 &&
                enif_is_identical(tuple_elements[0], am_ok)) {

              unsigned int slot_id;
              if (enif_get_uint(env, tuple_elements[1], &slot_id) && slot_id < stripe.capacity) {
                auto& claimed_slot = stripe.slots[slot_id];
                claimed_slot.ssl = temp_slot.ssl;
                claimed_slot.protocol = PROTO_SSL;
                temp_slot.ssl = nullptr; // Transfer ownership
              }
            }
          }

          cleanup_slot_ssl(temp_slot); // Clean up temp (should be nullptr now)
          return result;
        } else if (handshake_result == -1) {
          cleanup_slot_ssl(temp_slot);
          close(fd);
          return make(env, std::make_tuple(am_error, am_connect_failed));
        }

        // Handshake needs more time, wait a bit
        usleep(10000); // 10ms
      }

      // Timeout during SSL handshake
      cleanup_slot_ssl(temp_slot);
      close(fd);
      return make(env, std::make_tuple(am_error, am_timeout));
    }
#endif
  }

  auto& stripe = *ctx->stripes[stripe_id];
  return claim_slot(env, ctx, stripe, fd, owner_pid);
}

// Protocol-aware async version
static ERL_NIF_TERM connect_async_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;
  int port;
  bool nodelay;
  ErlNifPid owner_pid;

  if (argc != 7 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !get(env, argv[2], octets) ||
      !get(env, argv[3], port) || port < 0 || port > 65535 ||
      !get(env, argv[5], nodelay) ||
      !get(env, argv[6], owner_pid)) {
    return enif_make_badarg(env);
  }

  if (stripe_id >= ctx->stripe_count) {
    return enif_make_badarg(env);
  }

  ProtocolType protocol = parse_protocol(env, argv[4]);
  if (protocol == static_cast<ProtocolType>(-1)) {
    return enif_make_badarg(env);
  }

#ifndef HAVE_OPENSSL
  // SSL requires OpenSSL at compile time
  if (protocol == PROTO_SSL) {
    return make(env, std::make_tuple(am_error, am_unsupported_protocol));
  }
#endif

  int fd = create_socket_for_protocol(protocol);
  if (fd == -1) {
    return make(env, std::make_tuple(am_error, am_socket_failed));
  }

  if (!configure_socket_for_protocol(fd, protocol, nodelay)) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(static_cast<uint16_t>(port));
  uint32_t ip_host = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  server_addr.sin_addr.s_addr = htonl(ip_host);

  int rc = connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr));

  if (protocol == PROTO_UDP) {
    // UDP "connect" always succeeds immediately
    auto& stripe = *ctx->stripes[stripe_id];
    return claim_slot(env, ctx, stripe, fd, owner_pid);
  }

  // For TCP/SSL, handle async connection like connect_async_nif
  if (rc == 0) {
    // Connection completed immediately
#ifdef HAVE_OPENSSL
    if (protocol == PROTO_SSL) {
      // Need to start SSL handshake
      auto& stripe = *ctx->stripes[stripe_id];
      // Allocate slot first, then setup SSL
      auto current_mask = stripe.lease_mask.load(std::memory_order_acquire);

      for (;;) {
        if (current_mask == UINT64_MAX) {
          close(fd);
          return make(env, std::make_tuple(am_error, am_stripe_full));
        }

        int slot_id = std::countr_zero(current_mask);
        if (static_cast<size_t>(slot_id) >= stripe.capacity) {
          close(fd);
          return make(env, std::make_tuple(am_error, am_stripe_full));
        }

        auto& slot = stripe.slots[slot_id];
        if (slot.fd > -1) {
          current_mask &= ~(1ULL << slot_id);
          continue;
        }

        slot.fd = fd;
        slot.owner_pid = owner_pid;
        slot.stripe_id = stripe_id;
        slot.slot_id = slot_id;
        slot.pending_buffer.clear();
        slot.bytes_written = 0;
#ifdef HAVE_OPENSSL
        slot.protocol = protocol;
#endif

        uint64_t target_bit = (1ULL << slot_id);
        uint64_t new_mask   = current_mask & ~target_bit;

        if (stripe.lease_mask.compare_exchange_weak(
              current_mask, new_mask,
              std::memory_order_release,
              std::memory_order_relaxed)) {

          if (!setup_ssl_on_socket(slot, fd)) {
            close(fd);
            stripe.lease_mask.fetch_or(target_bit, std::memory_order_release);
            return make(env, std::make_tuple(am_error, am_connect_failed));
          }

          // Start SSL handshake
          int handshake_result = ssl_handshake_step(slot);
          if (handshake_result == 1) {
            // Handshake completed immediately
            slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
            arm_read(env, ctx, slot);
            return make(env, std::make_tuple(am_ok, static_cast<unsigned int>(slot_id)));
          } else if (handshake_result == 0) {
            // Handshake in progress
            slot.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
            arm_connect(env, ctx, slot); // Reuse connect notification for handshake
            return make(env, std::make_tuple(am_ok, am_connecting, static_cast<unsigned int>(slot_id)));
          } else {
            // Handshake failed
            cleanup_slot_ssl(slot);
            close(fd);
            stripe.lease_mask.fetch_or(target_bit, std::memory_order_release);
            return make(env, std::make_tuple(am_error, am_connect_failed));
          }
        }

        slot.fd = -1;
        slot.status.store(SLOT_EMPTY, std::memory_order_relaxed);
      }
    } else
#endif
    {
      auto& stripe = *ctx->stripes[stripe_id];
      return claim_slot(env, ctx, stripe, fd, owner_pid);
    }
  } else if (errno == EINPROGRESS) {
    // Connection in progress, register and set up for async notification
    auto& stripe = *ctx->stripes[stripe_id];
    uint64_t current_mask = stripe.lease_mask.load(std::memory_order_acquire);

    for (;;) {
      if (current_mask == UINT64_MAX) {
        close(fd);
        return make(env, std::make_tuple(am_error, am_stripe_full));
      }

      int slot_id = std::countr_zero(current_mask);
      if (static_cast<size_t>(slot_id) >= stripe.capacity) {
        close(fd);
        return make(env, std::make_tuple(am_error, am_stripe_full));
      }

      auto& slot = stripe.slots[slot_id];
      if (slot.fd > -1) {
        current_mask &= ~(1ULL << slot_id);
        continue;
      }

      slot.fd = fd;
      slot.owner_pid = owner_pid;
      slot.stripe_id = stripe_id;
      slot.slot_id = slot_id;
      slot.pending_buffer.clear();
      slot.bytes_written = 0;
#ifdef HAVE_OPENSSL
      slot.protocol = protocol;
#endif
      slot.status.store(SLOT_CONNECTING, std::memory_order_relaxed);

      uint64_t target_bit = (1ULL << slot_id);
      uint64_t new_mask   = current_mask & ~target_bit;

      if (stripe.lease_mask.compare_exchange_weak(
            current_mask, new_mask,
            std::memory_order_release,
            std::memory_order_relaxed)) {
        arm_connect(env, ctx, slot);
        return make(env, std::make_tuple(am_ok, am_connecting, static_cast<unsigned int>(slot_id)));
      }

      slot.fd = -1;
      slot.status.store(SLOT_EMPTY, std::memory_order_relaxed);
    }
  } else {
    // Connection failed immediately
    close(fd);
    return make(env, std::make_tuple(am_error, am_connect_failed));
  }
}

// Force-close a slot (bouncer recycle, or teardown of an idle connection)
// -- unlike notify_and_close, this is caller-initiated, so no "closed"
// heads-up is sent (the caller already knows).
static ERL_NIF_TERM close_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  if (slot.fd == -1) return am_ok;

#ifdef HAVE_OPENSSL
  cleanup_slot_ssl(slot);
#endif

  enif_select(env, slot.fd, ERL_NIF_SELECT_STOP, ctx, nullptr, am_stop);
  return am_ok;
}

static int load(ErlNifEnv* env,
  [[maybe_unused]] void** priv_data, [[maybe_unused]] ERL_NIF_TERM load_info)
{
  nifpp::initialize_known_atoms(env);

  #ifdef HAVE_OPENSSL
  // Initialize OpenSSL
  if (!init_ssl_context())
    return 1;
  #endif

  am_alloc_failed              = atom(env, "alloc_failed");
  am_arterial_event                = atom(env, "arterial_event");
  am_closed                    = atom(env, "closed");
  am_connect_failed            = atom(env, "connect_failed");
  am_connect_result            = atom(env, "connect_result");
  am_connecting                = atom(env, "connecting");
  am_failed_to_set_nonblocking = atom(env, "failed_to_set_nonblocking");
  am_max_slots_exceeded_64     = atom(env, "max_slots_exceeded_64");
  am_no_connections_available  = atom(env, "no_connections_available");
  am_read                      = atom(env, "read");
  am_socket_failed             = atom(env, "socket_failed");
  am_stop                      = atom(env, "stop");
  am_stripe_full               = atom(env, "stripe_full");
  am_timeout                   = atom(env, "timeout");
  am_unsupported_protocol      = atom(env, "unsupported_protocol");
  am_write                     = atom(env, "write");
  am_write_failed              = atom(env, "write_failed");

  return register_resource<PoolContext>(env, "arterial_pool_context") ? 0 : 1;
}

static void unload(ErlNifEnv* env, void* priv_data) {
  (void)env; (void)priv_data;
  #ifdef HAVE_OPENSSL
  cleanup_ssl();
  #endif
}

// Socket options enhanced functions (stubs for now)
static ERL_NIF_TERM connect_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  // For now, just call the regular connect function ignoring socket options
  if (argc < 7) return enif_make_badarg(env);
  ERL_NIF_TERM args[7];
  for (int i = 0; i < 7; i++) args[i] = argv[i];
  return connect_nif(env, 7, args);
}

static ERL_NIF_TERM connect_proto_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  // For now, just call the regular connect_proto function ignoring socket options
  if (argc < 8) return enif_make_badarg(env);
  ERL_NIF_TERM args[8];
  for (int i = 0; i < 8; i++) args[i] = argv[i];
  return connect_proto_nif(env, 8, args);
}

static ErlNifFunc nif_funcs[] = {
  {"init_pool",               2, init_pool_nif,               0},
  {"configure_throttle",      3, configure_throttle_nif,      0},
  {"register_socket",         4, register_socket_nif,         0},
  {"connect",                 7, connect_nif,                 ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connect_async",           6, connect_async_nif,           0},
  {"connect_proto",           8, connect_proto_nif,           ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connect_async_proto",     7, connect_async_proto_nif,     0},
  {"send_and_release",        3, send_and_release_nif,        0},
  {"handle_readable",         3, handle_readable_nif,         0},
  {"handle_writable",         3, handle_writable_nif,         0},
  {"connect_with_opts",       8, connect_with_opts_nif,       ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connect_proto_with_opts", 9, connect_proto_with_opts_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"close_slot",              3, close_slot_nif,              0}
};

ERL_NIF_INIT(arterial_nif, nif_funcs, load, nullptr, nullptr, unload)
