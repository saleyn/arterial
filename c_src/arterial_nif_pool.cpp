#include "enif.hpp"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <bit>
#include <errno.h>
#include <fcntl.h>
#include <cstring>

using namespace nifpp;

// Lock-free-ish raw-socket connection pool: each "stripe" is a single
// atomic uint64 lease mask (bit=1 -> slot unregistered or currently
// leased/busy, bit=0 -> registered and idle) covering up to 64 "slots"
// (physical sockets). A caller picks a stripe itself (e.g. by scheduler
// id, see arterial_connection2/arterial_client2) and this NIF auto-selects
// any idle slot within it via CAS on that one atomic -- no per-slot lock.
//
// Reads and writes happen as plain non-blocking syscalls invoked directly
// inside whichever Erlang process calls send_and_release/3 (writes) or
// handle_readable/3 (reads) -- there is no in-NIF callback invoked by the
// runtime on fd readiness (no such thing exists in erl_nif.h); enif_select
// only ever delivers a *message* to a process, which must then call back
// into the NIF to actually do the I/O. That message is a caller-supplied
// "custom message" (ERL_NIF_SELECT_CUSTOM_MSG), so the NIF itself decides
// its shape: `{arterial_pool_event, StripeId, SlotId, read | write | closed}`.
//
// register_socket/4 and connect/7 both arm the read side once and target
// every future read-ready/write-ready/closed message at the registering
// "owner" pid (expected to be the long-lived arterial_connection2 worker
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
  SLOT_WRITE_POLLING = 3
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
};

// Fixed-size: ConnSlot/PoolStripe hold std::atomic members, so they're
// neither movable nor copyable -- a std::vector<ConnSlot> could never
// grow/resize (every growth path needs to relocate existing elements).
// 64 is already the hard cap (lease_mask is one uint64), so a plain
// array costs nothing extra.
struct PoolStripe {
  std::atomic<uint64_t> lease_mask{~0ULL};
  std::array<ConnSlot, 64> slots{};
  size_t capacity{0};
};

struct PoolContext {
  // unique_ptr<PoolStripe>, not PoolStripe, for the same reason: the
  // vector itself must be able to grow (move elements) at init_pool
  // time, which a non-movable PoolStripe can't do directly.
  std::vector<std::unique_ptr<PoolStripe>> stripes;
  size_t stripe_count{0};

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
static atom am_arterial_pool_event;
static atom am_read;
static atom am_write;
static atom am_closed;
static atom am_stop;
static atom am_stripe_full;
static atom am_max_slots_exceeded_64;
static atom am_failed_to_set_nonblocking;
static atom am_socket_failed;
static atom am_connect_failed;
static atom am_timeout;
static atom am_no_connections_available;
static atom am_write_failed;
static atom am_alloc_failed;

//===========================================================================
// Helpers
//===========================================================================

static TERM make_event_msg(ErlNifEnv* env, unsigned int stripe_id,
                            unsigned int slot_id, const atom& kind) {
  return make(env, std::make_tuple(am_arterial_pool_event, stripe_id, slot_id, kind));
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
    stripe.lease_mask.store(~0ULL, std::memory_order_relaxed);
    for (unsigned int j = 0; j < 64; ++j) {
      stripe.slots[j].fd = -1;
      stripe.slots[j].stripe_id = i;
      stripe.slots[j].slot_id = j;
    }
  }

  return make(env, std::make_tuple(am_ok, ctx));
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

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(static_cast<uint16_t>(port));
  uint32_t ip_host = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  addr.sin_addr.s_addr = htonl(ip_host);

  int rc = connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  if (rc < 0 && errno != EINPROGRESS) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_connect_failed));
  }
  if (rc < 0) {
    struct pollfd pfd{fd, POLLOUT, 0};
    int pr = poll(&pfd, 1, static_cast<int>(timeout_ms));
    if (pr <= 0) {
      close(fd);
      return make(env, std::make_tuple(am_error, am_timeout));
    }
    int so_err = 0;
    socklen_t len = sizeof(so_err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len) == -1 || so_err != 0) {
      close(fd);
      return make(env, std::make_tuple(am_error, am_connect_failed));
    }
  }

  return claim_slot(env, ctx, stripe, fd, owner_pid);
}

static ERL_NIF_TERM send_and_release_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;

  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !enif_is_list(env, argv[2])) [[unlikely]] {
    return enif_make_badarg(env);
  }
  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);

  ERL_NIF_TERM list = argv[2];
  auto& stripe = *ctx->stripes[stripe_id];
  auto  current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  int slot_id = -1;

  do {
    slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return make(env, std::make_tuple(am_error, am_no_connections_available));

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) [[likely]] 
      break;

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

  ssize_t  written    = (i > 0) ? writev(slot.fd, iov, i) : 0;
  uint64_t target_bit = (1ULL << slot_id);

  if (written < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      written = 0;
    } else {
      notify_and_close(env, ctx, slot);
      return make(env, std::make_tuple(am_error, am_write_failed));
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

  ssize_t n = read(slot.fd, bin.data, read_size);

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

  size_t remaining = slot.pending_buffer.size() - slot.bytes_written;
  while (remaining > 0) {
    ssize_t n = write(slot.fd, slot.pending_buffer.data() + slot.bytes_written, remaining);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        arm_write(env, ctx, slot);
        return am_ok;
      }
      notify_and_close(env, ctx, slot);
      return am_closed;
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

// Force-close a slot (bouncer recycle, or teardown of an idle connection)
// -- unlike notify_and_close, this is caller-initiated, so no "closed"
// heads-up is sent (the caller already knows).
static ERL_NIF_TERM close_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  if (slot.fd == -1) return am_ok;
  enif_select(env, slot.fd, ERL_NIF_SELECT_STOP, ctx, nullptr, am_stop);
  return am_ok;
}

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  (void)priv_data; (void)load_info;
  nifpp::initialize_known_atoms(env);

  am_arterial_pool_event       = atom(env, "arterial_pool_event");
  am_read                      = atom(env, "read");
  am_write                     = atom(env, "write");
  am_closed                    = atom(env, "closed");
  am_stop                      = atom(env, "stop");
  am_stripe_full               = atom(env, "stripe_full");
  am_max_slots_exceeded_64     = atom(env, "max_slots_exceeded_64");
  am_failed_to_set_nonblocking = atom(env, "failed_to_set_nonblocking");
  am_socket_failed             = atom(env, "socket_failed");
  am_connect_failed            = atom(env, "connect_failed");
  am_timeout                   = atom(env, "timeout");
  am_no_connections_available  = atom(env, "no_connections_available");
  am_write_failed              = atom(env, "write_failed");
  am_alloc_failed              = atom(env, "alloc_failed");

  return register_resource<PoolContext>(env, "arterial_pool_context") ? 0 : 1;
}

static ErlNifFunc nif_funcs[] = {
  {"init_pool",        2, init_pool_nif,        0},
  {"register_socket",  4, register_socket_nif,  0},
  {"connect",          7, connect_nif,          ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"send_and_release", 3, send_and_release_nif, 0},
  {"handle_readable",  3, handle_readable_nif,  0},
  {"handle_writable",  3, handle_writable_nif,  0},
  {"close_slot",       3, close_slot_nif,       0}
};

ERL_NIF_INIT(arterial_nif_pool, nif_funcs, load, nullptr, nullptr, nullptr)
