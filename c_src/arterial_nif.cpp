#include "enif.hpp"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
#include <iterator>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
//#include <sys/poll.h>
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
#include "arterial_atoms.hpp"
#include "arterial_types.hpp"
#include "arterial_protocol.hpp"
#include "arterial_socket.hpp"
#include "arterial_ssl.hpp"
#include "arterial_connection.hpp"
#include "arterial_fifo.hxx"
#include "arterial_core.hpp"
#include "arterial_pool.hxx"
#include "arterial_connection.hxx"
#include "arterial_connection_timer.hxx"
#include "arterial_nif_functions.hpp"

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#endif

using namespace nifpp;
using namespace arterial;

//===========================================================================
// NIFs
//===========================================================================

// Round up to the next power of two (returns 1 for n=0).
static inline uint32_t next_pow2(uint32_t n) {
  if (n == 0) return 1;
  --n;
  n |= n >>  1; n |= n >>  2; n |= n >>  4;
  n |= n >>  8; n |= n >> 16;
  return n + 1;
}

// init_pool(NumStripes, SlotsPerStripe)
// init_pool(NumStripes, SlotsPerStripe, CorrTableSize)
//
// CorrTableSize (optional): number of slots in each stripe's lock-free corr
// table.  Must be a power of 2; if not, it is rounded up.  When omitted the
// default is max(256, next_pow2(SlotsPerStripe * 16)).
static ERL_NIF_TERM init_pool_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  unsigned int num_stripes;
  unsigned int slots_per_stripe;

  if (argc < 2 || argc > 3) [[unlikely]]
    return enif_make_badarg(env);

  if (!get(env, argv[0], num_stripes) ||
      !get(env, argv[1], slots_per_stripe)) [[unlikely]]
    return enif_make_badarg(env);

  if (slots_per_stripe > 64)
    return make_tuple(env, am_error, am_max_slots_exceeded_64);

  // Determine corr table size per stripe.
  uint32_t corr_size;
  if (argc == 3) {
    unsigned int requested;
    if (!get(env, argv[2], requested) || requested == 0) [[unlikely]]
      return enif_make_badarg(env);
    corr_size = next_pow2(requested);
  } else {
    corr_size = std::max(256u, next_pow2(slots_per_stripe * 16));
  }

  auto ctx = construct_resource<PoolContext>();

  ctx->stripe_count = num_stripes;
  ctx->stripes.resize(num_stripes);

  for (auto i = 0u; i < num_stripes; ++i) {
    ctx->stripes[i] = std::make_unique<PoolStripe>();
    auto& stripe = *ctx->stripes[i];
    stripe.capacity = slots_per_stripe;
    stripe.corr_table.init(corr_size);

    // Initialize lease mask: 0 = available, 1 = leased
    // Set all slots beyond capacity as permanently leased (unavailable)
    uint64_t initial_mask =
      (slots_per_stripe < 64) ? ~((1ULL << slots_per_stripe) - 1) : 0ULL;

    stripe.lease_mask.store(initial_mask, std::memory_order_relaxed);

    for (auto j = 0u; j < 64; ++j) {
      stripe.slots[j].fd        = -1;
      stripe.slots[j].stripe_id =  i;
      stripe.slots[j].slot_id   =  j;
      stripe.slots[j].status.store(SLOT_EMPTY, std::memory_order_relaxed);
    }
  }

  // Start the reactor — it runs on its own thread for this pool's lifetime.
  // No owner_pid: the reactor exit message would go to the supervisor (the
  // init_pool caller), which has no handle_info and would log a spurious
  // "unexpected message" warning on every normal pool stop.
  ctx->reactor_ptr = std::make_unique<arterial::Reactor>("arterial_pool");
  if (!ctx->reactor_ptr->valid())
    return make_tuple(env, am_error, am_reactor_init_failed);
  ctx->reactor_ptr->start();

  return make_tuple(env, am_ok, ctx);
}

//-----------------------------------------------------------------------------
// Configure throttling for a pool
//-----------------------------------------------------------------------------
static ERL_NIF_TERM configure_throttle_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int rate_per_sec;
  unsigned int window_msec;

  assert(argc == 3);

  if (!get(env, argv[0], ctx)          ||
      !get(env, argv[1], rate_per_sec) ||
      !get(env, argv[2], window_msec)) [[unlikely]]
    return enif_make_badarg(env);

  ctx->throttle_rate_per_sec = rate_per_sec;
  ctx->throttle_window_msec = window_msec;

  // Initialize throttle for each conn in each stripe
  auto now = arterial::now_utc();
  for (auto& stripe_ptr : ctx->stripes)
    for (auto& conn : stripe_ptr->slots)
      // Initialize time spacing throttle with the configured rate and window
      conn.throttle.init(rate_per_sec, window_msec, now);

  return am_ok;
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM register_socket_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  int          raw_fd;
  ErlNifPid    owner_pid;

  assert(argc == 4);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], raw_fd)
    || (raw_fd < 0)
    || !get(env, argv[3], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  int   flags  = fcntl(raw_fd, F_GETFL, 0);

  if (UNLIKELY(flags == -1 || fcntl(raw_fd, F_SETFL, flags | O_NONBLOCK) == -1))
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);

  int slot_id = ctx->claim_slot(env, stripe, raw_fd, owner_pid);
  if (slot_id < 0)
    return make_tuple(env, am_error, am_stripe_full);

  Connection& conn = stripe.slots[slot_id];
  conn.arm_read(env, ctx);

  if (ctx->monitor_owner(env, conn) != 0) {
    ctx->reactor().remove_fd(raw_fd);
    stripe.release_slot(slot_id);
    return make_tuple(env, am_error, am_connect_failed);
  }

  return make_tuple(env, am_ok, slot_id);
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM send_and_release_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !enif_is_list(env, argv[2])) [[unlikely]]
    return enif_make_badarg(env);

  // Delegate to Connection method
  auto result = Connection::send_and_release(env, ctx, stripe_id, argv[2]);

  // Handle the result and convert to appropriate Erlang terms
  switch (result.result) {
    case Connection::SendResult::OK:
    case Connection::SendResult::PARTIAL:
      return make_tuple(env, am_ok, result.slot_id);

    case Connection::SendResult::POOL_BUSY:
    case Connection::SendResult::WRITE_FAILED:
    case Connection::SendResult::CLOSED:
      return make_tuple(env, am_error, result.error_reason);

    default:
      return make_tuple(env, am_error, am_unknown);
  }
}

//-----------------------------------------------------------------------------
// send_on_slot(PoolRef, StripeId, SlotId, DataList) → ok | {error, Reason}
//
// Like send_and_release but writes on a *specific* slot instead of scanning
// for any free one.  Required when multiple workers share a stripe and each
// must send on its own connection (otherwise send_and_release may pick a
// neighbour's slot, routing the reply to the wrong process).
//-----------------------------------------------------------------------------
static ERL_NIF_TERM send_on_slot_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext*  ctx;
  unsigned int  stripe_id, slot_id;

  assert(argc == 4);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1))
    || !get(env, argv[2], slot_id)
    || !enif_is_list(env, argv[3])) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return enif_make_badarg(env);

  auto& conn = stripe.slots[slot_id];

  // Atomically claim this specific slot by setting its lease bit.
  uint64_t target_bit = 1ULL << slot_id;
  uint64_t expected   = stripe.lease_mask.load(std::memory_order_relaxed) & ~target_bit;
  uint64_t desired    = expected | target_bit;

  for (int retries = 0; retries < 32; ++retries) {
    if (stripe.lease_mask.compare_exchange_weak(
          expected, desired,
          std::memory_order_acquire,
          std::memory_order_relaxed))
      goto claimed;
    expected &= ~target_bit;  // clear our bit in the refreshed expected value
    desired   = expected | target_bit;
  }
  return make_tuple(env, am_error, am_pool_busy);

claimed:
  if (conn.status.load(std::memory_order_acquire) != SLOT_AVAILABLE || conn.fd < 0) {
    stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
    return make_tuple(env, am_error, am_pool_busy);
  }
  conn.status.store(SLOT_LEASED, std::memory_order_relaxed);

  // Build iovec from the data list and writev directly to conn.fd.
  unsigned int list_len = 0;
  enif_get_list_length(env, argv[3], &list_len);

  constexpr size_t s_inline_iov_size = 8;
  std::array<struct iovec, s_inline_iov_size> inline_iov;
  std::vector<struct iovec> heap_iov;
  struct iovec* iov = (list_len <= s_inline_iov_size)
    ? inline_iov.data()
    : (heap_iov.resize(list_len), heap_iov.data());

  ERL_NIF_TERM head, tail = argv[3];
  unsigned int i = 0;
  size_t total_bytes = 0;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    ErlNifBinary bin;
    if (enif_inspect_binary(env, head, &bin)) {
      iov[i].iov_base = bin.data;
      iov[i].iov_len  = bin.size;
      total_bytes    += bin.size;
      ++i;
    }
  }

  ssize_t written = 0;
  if (i > 0) {
  RETRY:
    written = writev(conn.fd, iov, static_cast<int>(i));
    if (written < 0) {
      if (errno == EINTR) [[unlikely]] goto RETRY;
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        ctx->notify_and_close(env, conn);
        return make_tuple(env, am_error, am_write_failed);
      }
      written = 0;
    }
  }

  if (static_cast<size_t>(written) < total_bytes) {
    // Partial write: buffer remaining bytes and arm writable notification.
    size_t done = static_cast<size_t>(written);
    conn.pending_buffer.resize(total_bytes);
    size_t off = 0;
    for (unsigned int j = 0; j < i; ++j) {
      std::memcpy(conn.pending_buffer.data() + off, iov[j].iov_base, iov[j].iov_len);
      off += iov[j].iov_len;
    }
    conn.bytes_written = done;
    conn.status.store(SLOT_WRITE_POLLING, std::memory_order_release);
    conn.arm_write(env, ctx);
    return am_ok;
  }

  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
  stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
  return am_ok;
}

//-----------------------------------------------------------------------------
// Force-close a slot (bouncer recycle, or teardown of an idle connection)
// unlike notify_and_close, this is caller-initiated, so no "closed"
// heads-up is sent (the caller already knows).
//-----------------------------------------------------------------------------
static ERL_NIF_TERM close_slot_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Bump generation BEFORE remove_fd so any in-flight reactor callback that
  // fires after this point sees a mismatched generation and skips the stale slot.
  conn.generation.fetch_add(1, std::memory_order_release);

  // Remove the owner monitor before clearing the slot — prevents a race where
  // on_down fires after the slot is released and tries to close an already-dead fd.
  ctx->demonitor_owner(env, conn);

  // Always clear the lease bit and status, even if fd==-1 (pool_resource_stop
  // may have already closed it). This is the authoritative cleanup point called
  // by arterial_connection after set_slot_unavailable sets the bit.
  conn.status.store(SLOT_EMPTY, std::memory_order_release);
  auto& stripe = *ctx->stripes[conn.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);

  if (conn.fd == -1) return am_ok;

#ifdef HAVE_OPENSSL
  cleanup_slot_ssl(conn);
#endif

  // Reactor closes the fd on its thread — no enif_select(STOP) needed.
  ctx->reactor().remove_fd(conn.fd);
  conn.fd = -1;
  return am_ok;
}

//=============================================================================
// Reactor server NIFs — test-infrastructure only (echo servers, reactor_bench)
//=============================================================================

#ifdef TEST

//-----------------------------------------------------------------------------
// reactor_listen(PoolRef, Port) → {ok, Fd} | {error, Reason}
// Creates a non-blocking, REUSEADDR TCP listen socket.
// Port=0 → ephemeral; the actual port can be read with inet:port/1 or by
// calling getsockname on the returned fd.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM reactor_listen_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  int port;
  if (!get(env, argv[0], ctx) || !get(env, argv[1], port, 0, 65535))
    return badarg(env);

  int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
  if (fd < 0) return make_tuple(env, am_error, am_socket_failed);

  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port        = htons(static_cast<uint16_t>(port));

  if (::bind(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    ::close(fd);
    return make_tuple(env, am_error, am_connect_failed);  // re-use existing error atom
  }
  if (::listen(fd, 256) < 0) {
    ::close(fd);
    return make_tuple(env, am_error, am_connect_failed);
  }

  // Retrieve actual port (useful when port=0 was requested).
  socklen_t len = sizeof(addr);
  ::getsockname(fd, reinterpret_cast<struct sockaddr*>(&addr), &len);
  int actual_port = ntohs(addr.sin_port);

  return make_tuple(env, am_ok,
    make_tuple(env, static_cast<unsigned int>(fd),
                    static_cast<unsigned int>(actual_port)));
}

//-----------------------------------------------------------------------------
// reactor_accept(PoolRef, ListenFd, OwnerPid) → ok | {error, Reason}
// Registers ListenFd with the reactor.  When a client connects, the reactor
// fires the accept handler which calls accept4() and sends:
//   {arterial_accept, ListenFd, ClientFd, {O1,O2,O3,O4}, Port}
// to OwnerPid (persistent — keeps firing for every new connection).
//-----------------------------------------------------------------------------
static ERL_NIF_TERM reactor_accept_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  int listen_fd;
  ErlNifPid owner_pid;
  if (!get(env, argv[0], ctx) || !get(env, argv[1], listen_fd) ||
      !get(env, argv[2], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  // Register the listen fd for persistent readable events.
  // The handler accepts and notifies owner_pid for each new connection.
  ctx->reactor().add_fd(
    listen_fd,
    // on_readable: drain all pending connections and notify owner for each.
    // Loop until accept4 returns EAGAIN so no connections are missed when
    // multiple clients arrive in the same edge-triggered EPOLLIN event.
    // NOTE: env must NOT be captured — it is freed when the NIF returns.
    //       Each accepted connection gets its own msg_env for term allocation.
    [lfd = listen_fd, owner_pid](int /*fd*/, void*) -> int {
      for (;;) {
        struct sockaddr_in caddr{};
        socklen_t clen = sizeof(caddr);
        int cfd = ::accept4(lfd, reinterpret_cast<struct sockaddr*>(&caddr),
                            &clen, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (cfd < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) break;
          // Fatal accept error: notify owner then let the reactor remove the fd.
          nifpp::msg_env me;
          ERL_NIF_TERM msg = make_tuple(me, am_arterial_accept, lfd, am_closed, errno);
          enif_send(nullptr, const_cast<ErlNifPid*>(&owner_pid), me, msg);
          return -1;
        }

        uint32_t ip  = ntohl(caddr.sin_addr.s_addr);
        uint16_t prt = ntohs(caddr.sin_port);
        nifpp::msg_env me;
        ERL_NIF_TERM msg = make_tuple(me, am_arterial_accept, lfd, cfd, ip, (uint32_t)prt);
        if (!enif_send(nullptr, const_cast<ErlNifPid*>(&owner_pid), me, msg))
          close(cfd);
      }
      return 0;  // keep listen fd registered
    },
    // on_error
    [](int, void*) {}
  );
  return am_ok;
}

//-----------------------------------------------------------------------------
// reactor_close_fd(PoolRef, Fd) → ok
// Removes Fd from the reactor (closes it on the reactor thread).
//-----------------------------------------------------------------------------
static ERL_NIF_TERM reactor_close_fd_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  int fd;
  if (!get(env, argv[0], ctx) || !get(env, argv[1], fd)) [[unlikely]]
    return enif_make_badarg(env);
  ctx->reactor().remove_fd(fd);
  return am_ok;
}

//-----------------------------------------------------------------------------
// reactor_register_client(PoolRef, StripeId, ClientFd, OwnerPid) →
//   {ok, SlotId} | {error, stripe_full | failed_to_set_nonblocking}
//
// Registers a client fd (from reactor_accept) with the NIF pool so the
// reactor delivers:
//   {arterial_event, StripeId, SlotId, read, Binary}
// to OwnerPid on each read event.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM reactor_register_client_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext*  ctx;
  unsigned int  stripe_id;
  int           client_fd;
  ErlNifPid     owner_pid;

  if (!get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1)) ||
      !get(env, argv[2], client_fd) ||
      !get(env, argv[3], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  // Ensure non-blocking (accept4 with SOCK_NONBLOCK handles this, but belt+braces).
  int flags = ::fcntl(client_fd, F_GETFL, 0);
  if (flags == -1 || ::fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) == -1)
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);

  auto& stripe  = *ctx->stripes[stripe_id];
  int   slot_id = ctx->claim_slot(env, stripe, client_fd, owner_pid);
  if (slot_id < 0) [[unlikely]]
    return make_tuple(env, am_error, am_stripe_full);

  auto& conn = stripe.slots[slot_id];
  conn.arm_read(env, ctx);

  if (ctx->monitor_owner(env, conn) != 0) {
    ctx->reactor().remove_fd(client_fd);
    stripe.release_slot(slot_id);
    return make_tuple(env, am_error, am_connect_failed);
  }

  // Clear the lease bit so send_and_release can claim the slot.
  stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);

  return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
}

#endif // TEST

//-----------------------------------------------------------------------------
static ERL_NIF_TERM connect_proto_with_opts_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  unsigned int timeout_ms;
  ProtocolType protocol;
  bool         nodelay;
  ErlNifPid    owner_pid;
  IP4Tuple     octets;

  assert(argc == 9);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !get(env, argv[4], timeout_ms)
    || !parse_protocol(env, argv[5], protocol)
    || !get(env, argv[6], nodelay)
    || !get(env, argv[7], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  // Delegate to Connection method
  auto result = Connection::connect_proto_with_opts(env, ctx, stripe_id, octets, port, timeout_ms, protocol, nodelay, owner_pid, argv[8]);

  // Handle the result and convert to appropriate Erlang terms
  switch (result.result) {
    case Connection::ConnectResult::OK:
      return make_tuple(env, am_ok, static_cast<unsigned int>(result.slot_id));

    case Connection::ConnectResult::CONNECTING:
      return make_tuple(env, am_ok, am_connecting, static_cast<unsigned int>(result.slot_id));

    case Connection::ConnectResult::FAILED:
    case Connection::ConnectResult::STRIPE_FULL:
    case Connection::ConnectResult::SOCKET_FAILED:
    case Connection::ConnectResult::CONFIG_FAILED:
    case Connection::ConnectResult::SELECT_FAILED:
    case Connection::ConnectResult::SSL_FAILED:
      return make_tuple(env, am_error, result.error_reason);

    default:
      return make_tuple(env, am_error, am_unknown);
  }
}

//-----------------------------------------------------------------------------
// Check if a connection slot is available (authoritative availability check)
//-----------------------------------------------------------------------------
static ERL_NIF_TERM is_slot_available_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Check slot status and lease mask for authoritative availability
  // This must match the logic in Connection::is_available() and send_and_release_nif
  bool  ready     = (conn.status.load(std::memory_order_acquire) == SLOT_AVAILABLE);
  auto& stripe    = *ctx->stripes[conn.stripe_id];
  auto  curr_mask = stripe.lease_mask.load(std::memory_order_acquire);
  bool  unleased  = !(curr_mask & (1ULL << conn.slot_id));

  // A slot is available if it's ready and unleased
  // For manually set availability (via set_slot_available), we don't require a valid fd
  // For real connections, the fd is checked during connection establishment
  return (ready && unleased) ? am_true : am_false;
}

//-----------------------------------------------------------------------------
// Mark a connection conn as available for new sends
//-----------------------------------------------------------------------------
static ERL_NIF_TERM set_slot_available_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Set slot status to available
  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);

  // Clear lease mask bit to make slot available for send_and_release
  auto& stripe = *ctx->stripes[conn.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);

  return am_ok;
}

//-----------------------------------------------------------------------------
// Mark a connection slot as unavailable for new sends
//-----------------------------------------------------------------------------
static ERL_NIF_TERM set_slot_unavailable_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Set lease mask bit to prevent new sends
  auto& stripe = *ctx->stripes[conn.stripe_id];
  stripe.lease_mask.fetch_or(1ULL << conn.slot_id, std::memory_order_release);

  // Set slot status to indicate unavailability (but preserve specific states like CONNECTING)
  uint32_t current_status = conn.status.load(std::memory_order_acquire);
  if (current_status == SLOT_AVAILABLE)
    conn.status.store(SLOT_EMPTY, std::memory_order_release);

  // If conn is CONNECTING, SSL_HANDSHAKE, etc., leave those states intact
  return am_ok;
}

//===========================================================================
// FIFO Mode 3 structures and implementations
//===========================================================================

//-----------------------------------------------------------------------------
// Simple FIFO extension structure
//-----------------------------------------------------------------------------
static ERL_NIF_TERM reserve_fifo_connection_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, timeout_ms;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], timeout_ms)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);

  // Try immediate reservation first (fast path)
  int attempt = 0;
  while (attempt < 3) {  // Limit immediate retries to avoid busy-waiting
    int slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity)
      break;  // No slots available, try queueing

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) {

      auto& conn = stripe.slots[slot_id];

      // Check if conn is available and has valid fd
      uint32_t conn_status = conn.status.load(std::memory_order_acquire);
      if (conn_status != SLOT_AVAILABLE || conn.fd < 0) {
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
        attempt++;
        continue;
      }

      // Successfully reserved conn immediately
      conn.enable_fifo_mode();
      conn.status.store(SLOT_FIFO_RESERVED, std::memory_order_release);

      // Generate reservation ID
      static std::atomic<uint64_t> reservation_counter{1};
      uint64_t reservation_id = reservation_counter.fetch_add(1, std::memory_order_relaxed);

      // Set up the FIFO request
      ErlNifPid caller_pid;
      enif_self(env, &caller_pid);
      if (!conn.set_fifo_request(caller_pid, reservation_id)) {
        // Failed to set fifo request - slot might be in inconsistent state
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
        attempt++;
        continue;
      }

      return make_tuple(env, am_ok, am_fifo_reserved,
        stripe_id, static_cast<unsigned int>(slot_id), reservation_id
      );
    }
    attempt++;
  }

  // Fast path failed - check if any slots exist and what their status is
  bool has_connecting_slots = false;
  bool has_failed_slots = false;

  for (size_t i = 0; i < stripe.capacity; i++) {
    auto slot_status = stripe.slots[i].status.load(std::memory_order_acquire);
    if (slot_status == SLOT_CONNECTING)
      has_connecting_slots = true;
    else if (slot_status == SLOT_EMPTY && stripe.slots[i].fd == -1)
      has_failed_slots = true;
  }

  // If no slots are connecting and we have failed connections, return error immediately
  // If we have connecting slots, we could wait, but for now return timeout
  // to avoid hanging tests. A real implementation would use async notification.
  return !has_connecting_slots && has_failed_slots
       ? make_tuple(env, am_error, am_pool_busy)
       : make_tuple(env, am_error, am_timeout);
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM send_fifo_request_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, slot_id, timeout_ms;
  uint64_t reservation_id;

  assert(argc == 6);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], slot_id)
    || !get(env, argv[3], reservation_id)
    || !enif_is_list(env, argv[4])
    || !get(env, argv[5], timeout_ms)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return enif_make_badarg(env);

  auto& conn = stripe.slots[slot_id];

  // Verify the reservation
  if (conn.status.load(std::memory_order_acquire) != SLOT_FIFO_RESERVED)
    return make_tuple(env, am_error, am_invalid_reservation);

  if (!conn.is_fifo_enabled())
    return make_tuple(env, am_error, am_fifo_not_enabled);

  // Verify reservation ID matches
  if (conn.fifo_reservation_id != reservation_id)
    return make_tuple(env, am_error, am_invalid_reservation);

  // Send the data using the same mechanism as send_and_release
  auto request_data = argv[4];
  unsigned int list_len = 0;
  enif_get_list_length(env, request_data, &list_len);

  constexpr size_t s_inline_iov_size = 8;
  std::array<struct iovec, s_inline_iov_size> inline_iov;
  std::vector<struct iovec> heap_iov;
  struct iovec* iov;

  if (list_len <= s_inline_iov_size)
    iov = inline_iov.data();
  else {
    heap_iov.resize(list_len);
    iov = heap_iov.data();
  }

  ERL_NIF_TERM head, tail = request_data;
  for (unsigned int i = 0; i < list_len && enif_get_list_cell(env, tail, &head, &tail); ++i) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, head, &bin)) {
      conn.clear_fifo_request();
      return enif_make_badarg(env);
    }
    iov[i].iov_base = bin.data;
    iov[i].iov_len = bin.size;
  }

  // Capture the calling process as the reply destination
  ErlNifPid caller_pid;
  enif_self(env, &caller_pid);

REPEAT1:
  // Write data to socket
  ssize_t bytes_written = writev(conn.fd, iov, list_len);

  if (bytes_written == -1) {
    if (errno == EINTR) [[unlikely]]
      goto REPEAT1;
    else if (errno != EAGAIN && errno != EWOULDBLOCK) {
      conn.clear_fifo_request();
      return make_tuple(env, am_error, am_write_failed);
    }
  }

  // Record who to reply to when bytes arrive, then mark sent
  conn.fifo_requester_pid    = caller_pid;
  conn.fifo_request_active.store(true, std::memory_order_release);
  conn.status.store(SLOT_FIFO_REQUEST_SENT, std::memory_order_release);

  return make_tuple(env, am_ok, am_fifo_request_sent);
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM release_fifo_connection_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;
  uint64_t reservation_id;

  assert(argc == 4);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], slot_id)
    || !get(env, argv[3], reservation_id)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return enif_make_badarg(env);

  auto& conn = stripe.slots[slot_id];

  // Verify reservation ID
  if (conn.fifo_reservation_id != reservation_id)
    return make_tuple(env, am_error, am_invalid_reservation);

  // Clean up FIFO state
  conn.clear_fifo_request();

  // Return conn to available state
  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
  stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);

  // Process any queued FIFO requests now that a connection is available
  stripe.try_process_fifo_queue();

  return am_ok;
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM fifo_connection_status_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], slot_id)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return enif_make_badarg(env);

  auto&    conn   = stripe.slots[slot_id];
  uint32_t status = conn.status.load(std::memory_order_acquire);

  // Check if this is a FIFO slot
  if (!conn.is_fifo_enabled())
    return make_tuple(env, am_ok, am_fifo_disabled, 0U, 0U);

  // Return detailed FIFO status
  ERL_NIF_TERM status_atom;
  switch (status) {
    case SLOT_FIFO_RESERVED:
      status_atom = am_fifo_reserved;
      break;
    case SLOT_FIFO_REQUEST_SENT:
      status_atom = am_fifo_request_sent;
      break;
    case SLOT_FIFO_DRAINING:
      status_atom = am_fifo_draining;
      break;
    default:
      status_atom = am_unknown;
  }

  uint64_t total_requests = conn.fifo_total_requests.load(std::memory_order_relaxed);
  uint64_t total_timeouts = conn.fifo_total_timeouts.load(std::memory_order_relaxed);

  return make_tuple(env, am_ok, status_atom, total_requests, total_timeouts);
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM handle_fifo_reply_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], slot_id)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return enif_make_badarg(env);

  auto& conn = stripe.slots[slot_id];

  if (!conn.is_fifo_enabled())
    return make_tuple(env, am_error, am_fifo_not_enabled);

  // Send reply message to the original requester
  nifpp::msg_env msg_env;
  auto reply_msg = make(msg_env, std::make_tuple(
    am_arterial_fifo_reply,
    stripe_id, slot_id, argv[3]
  ));

  enif_send(env, &conn.fifo_requester_pid, msg_env, reply_msg);

  // Clean up the FIFO request
  conn.clear_fifo_request();

  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);

  // Release the conn lease
  stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);

  return am_ok;
}

//===========================================================================
// Combined reserve + send FIFO request (performance optimization)
//===========================================================================
static ERL_NIF_TERM reserve_send_fifo_request_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, reserv_timeout, req_timeout;

  assert(argc == 5);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, ctx->stripe_count-1)
    || !enif_is_list(env, argv[2])
    || !get(env, argv[3], reserv_timeout)
    || !get(env, argv[4], req_timeout)) [[unlikely]]
    return enif_make_badarg(env);

  // Delegate to Connection method
  auto result = Connection::reserve_send_fifo_request(env, ctx, stripe_id, argv[2], reserv_timeout, req_timeout);

  // Handle the result and convert to appropriate Erlang terms
  switch (result.result) {
    case Connection::FifoResult::OK:
    case Connection::FifoResult::REQUEST_SENT:
      return make_tuple(env,
        am_ok, am_fifo_request_sent,
        stripe_id,
        static_cast<unsigned int>(result.slot_id),
        result.reservation_id
      );

    case Connection::FifoResult::POOL_BUSY:
    case Connection::FifoResult::SLOT_BUSY:
    case Connection::FifoResult::WRITE_FAILED:
    case Connection::FifoResult::PARTIAL:
    case Connection::FifoResult::TIMEOUT:
    case Connection::FifoResult::INVALID_RESERVATION:
    case Connection::FifoResult::NOT_ENABLED:
      return make_tuple(env, am_error, result.error_reason);

    default:
      return make_tuple(env, am_error, am_unknown);
  }
}

//-----------------------------------------------------------------------------
// NIF: info
//-----------------------------------------------------------------------------

#ifndef ARTERIAL_VERSION
#define ARTERIAL_VERSION "unknown"
#endif
#ifndef ARTERIAL_APP_VERSION
#define ARTERIAL_APP_VERSION "unknown"
#endif
#ifndef ARTERIAL_OPT_LEVEL
#define ARTERIAL_OPT_LEVEL "none"
#endif
#ifndef ARTERIAL_PGO
#define ARTERIAL_PGO 0
#endif
#ifndef ARTERIAL_BACKEND
#  if defined(REACTOR_BACKEND_URING)
#    define ARTERIAL_BACKEND "io_uring"
#  elif defined(REACTOR_BACKEND_EPOLL)
#    define ARTERIAL_BACKEND "epoll"
#  elif defined(REACTOR_BACKEND_KQUEUE)
#    define ARTERIAL_BACKEND "kqueue"
#  else
#    define ARTERIAL_BACKEND "unknown"
#  endif
#endif
#ifdef TEST
#  define ARTERIAL_PROFILE "test"
#else
#  define ARTERIAL_PROFILE "prod"
#endif

//-----------------------------------------------------------------------------
// register_and_send(PoolRef, StripeId, CorrId, CallerPid, DeadlineUs, DataList)
//   → {ok, SlotId} | {error, Reason}
//
// Atomically inserts the correlation entry and then sends the data in one NIF
// call, replacing the two-call sequence register_corr + send_and_release on
// the hot path.  On send failure the correlation entry is removed before
// returning the error so the caller does not need to call unregister_corr.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM register_and_send_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, corr_id;
  ErlNifPid    caller_pid;
  ErlNifSInt64 deadline_us;

  assert(argc == 6);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1))
    || !get(env, argv[2], corr_id)
    || !enif_get_local_pid(env, argv[3], &caller_pid)
    || !enif_get_int64(env, argv[4], &deadline_us)
    || !enif_is_list(env, argv[5])) [[unlikely]]
    return enif_make_badarg(env);

  auto& ct = ctx->stripes[stripe_id]->corr_table;
  if (!ct.insert(corr_id, caller_pid,
                 static_cast<uint32_t>(stripe_id),
                 static_cast<int64_t>(deadline_us))) [[unlikely]]
    return make_tuple(env, am_error, enif_make_atom(env, "table_full"));

  auto result = Connection::send_and_release(env, ctx, stripe_id, argv[5]);

  switch (result.result) {
    case Connection::SendResult::OK:
    case Connection::SendResult::PARTIAL:
      return make_tuple(env, am_ok, result.slot_id);

    default:
      ct.erase(corr_id);
      return make_tuple(env, am_error, result.error_reason);
  }
}

//-----------------------------------------------------------------------------
// unregister_corr(PoolRef, StripeId, CorrId) → ok
//-----------------------------------------------------------------------------
static ERL_NIF_TERM unregister_corr_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, corr_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1))
    || !get(env, argv[2], corr_id)) [[unlikely]]
    return enif_make_badarg(env);

  ctx->stripes[stripe_id]->corr_table.erase(corr_id);
  return am_ok;
}

//-----------------------------------------------------------------------------
// lookup_and_remove_corr(PoolRef, StripeId, CorrId)
//   → {CallerPid, ConnId} | not_found
//-----------------------------------------------------------------------------
static ERL_NIF_TERM lookup_and_remove_corr_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, corr_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1))
    || !get(env, argv[2], corr_id)) [[unlikely]]
    return enif_make_badarg(env);

  CorrPayload out;
  if (!ctx->stripes[stripe_id]->corr_table.remove(corr_id, out))
    return enif_make_atom(env, "not_found");

  return make_tuple(env, out.caller_pid, out.conn_id);
}

//-----------------------------------------------------------------------------
// corr_count(PoolRef, StripeId) → Count::non_neg_integer()
//-----------------------------------------------------------------------------
static ERL_NIF_TERM corr_count_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;

  assert(argc == 2);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1))) [[unlikely]]
    return enif_make_badarg(env);

  return enif_make_uint(env,
    static_cast<unsigned int>(ctx->stripes[stripe_id]->corr_table.count()));
}

//-----------------------------------------------------------------------------
// drain_corr_map(PoolRef, StripeId, ConnId) → ok
// Removes all entries for ConnId and sends {arterial_disconnected, CorrId}
// to each waiting caller.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM drain_corr_map_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id, conn_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1))
    || !get(env, argv[2], conn_id)) [[unlikely]]
    return enif_make_badarg(env);

  ctx->stripes[stripe_id]->corr_table.drain_by_conn(conn_id,
    [](uint32_t cid, CorrPayload p) {
      nifpp::msg_env me;
      auto msg = nifpp::make(me,
        std::make_tuple(am_arterial_disconnected, static_cast<unsigned int>(cid)));
      enif_send(nullptr, &p.caller_pid, me, msg);
    });

  return am_ok;
}

//-----------------------------------------------------------------------------
// sweep_corr_map(PoolRef, NowUs) → ExpiredCount::integer()
// Scans all stripes for entries whose deadline has passed and sends
// {arterial_timeout, CorrId} to each expired caller.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM sweep_corr_map_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  ErlNifSInt64 now_us;

  assert(argc == 2);

  if  (!get(env, argv[0], ctx)
    || !enif_get_int64(env, argv[1], &now_us)) [[unlikely]]
    return enif_make_badarg(env);

  unsigned int expired_count = 0;

  for (std::size_t s = 0; s < ctx->stripe_count; ++s) {
    ctx->stripes[s]->corr_table.sweep_expired(static_cast<int64_t>(now_us),
      [&](uint32_t cid, CorrPayload p) {
        nifpp::msg_env me;
        auto msg = nifpp::make(me,
          std::make_tuple(am_arterial_timeout, static_cast<unsigned int>(cid)));
        enif_send(nullptr, &p.caller_pid, me, msg);
        ++expired_count;
      });
  }

  return enif_make_uint(env, expired_count);
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM info_nif(ErlNifEnv* env, int argc, [[maybe_unused]] const ERL_NIF_TERM argv[])
{
  if (argc != 0) [[unlikely]]
    return enif_make_badarg(env);

  auto opt = [=]() -> ERL_NIF_TERM {
    if (!strcmp(ARTERIAL_OPT_LEVEL, "none")) return enif_make_atom(env, "none");
    if (!strcmp(ARTERIAL_OPT_LEVEL, "O1"))   return enif_make_int(env, 1);
    if (!strcmp(ARTERIAL_OPT_LEVEL, "O2"))   return enif_make_int(env, 2);
    if (!strcmp(ARTERIAL_OPT_LEVEL, "O3"))   return enif_make_int(env, 3);
    return make_binary(env, std::string_view(ARTERIAL_OPT_LEVEL));
  };

  auto backend = [=]() -> ERL_NIF_TERM {
    if (!strcmp(ARTERIAL_BACKEND, "io_uring")) return am_io_uring;
    if (!strcmp(ARTERIAL_BACKEND, "epoll"))    return am_epoll;
    if (!strcmp(ARTERIAL_BACKEND, "kqueue"))   return am_kqueue;
    return am_unknown;
  };

  auto profile = [=]() -> ERL_NIF_TERM {
    return strcmp(ARTERIAL_PROFILE, "test") == 0 ? am_test : am_prod;
  };

  ERL_NIF_TERM keys[] = { am_version, am_app_version, am_pgo, am_optimization, am_backend, am_profile };
  ERL_NIF_TERM vals[] = {
    make_binary(env, std::string_view(ARTERIAL_VERSION)),
    make_binary(env, std::string_view(ARTERIAL_APP_VERSION)),
    ARTERIAL_PGO ? am_true : am_false,
    opt(),
    backend(),
    profile()
  };

  ERL_NIF_TERM map;
  enif_make_map_from_arrays(env, keys, vals, std::size(keys), &map);
  return map;
}

//=============================================================================
// NIF Initialization/Finalization
//=============================================================================

static int load(ErlNifEnv* env,
  [[maybe_unused]] void** priv_data, [[maybe_unused]] ERL_NIF_TERM load_info)
{
  nifpp::initialize_known_atoms(env);

  #ifdef HAVE_OPENSSL
  // Initialize OpenSSL
  if (!init_ssl_context())
    return 1;
  #endif

  // RAII timer system requires no global initialization

  if (!register_resource<PoolContext>(env, "arterial_pool_context"))
    return 1;

  // SlotRef: one per live connection, used as the enif_monitor_process object.
  // on_down is dispatched by ERTS directly to the SlotRef — O(1), no scan.
  // The per-instance on_down callback is set in monitor_owner() via
  // construct_resource_with_events; the type just needs to be registered here.
  return register_resource<SlotRef>(env, "arterial_slot_ref") ? 0 : 1;
}

static void unload([[maybe_unused]] ErlNifEnv* env, [[maybe_unused]] void* priv_data)
{
  // RAII timer system requires no global cleanup

  #ifdef HAVE_OPENSSL
  cleanup_ssl();
  #endif
}

// Function table is defined in arterial_nif_functions.hpp

ERL_NIF_INIT(arterial_nif, nif_funcs, load, nullptr, nullptr, unload)
