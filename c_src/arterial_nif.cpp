#include "enif.hpp"
#include <atomic>
#include <array>
#include <vector>
#include <memory>
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

static ERL_NIF_TERM init_pool_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  // Debug: Test if debug logging works at all
  unsigned int num_stripes;
  unsigned int slots_per_stripe;

  assert(argc == 2);

  if (!get(env, argv[0], num_stripes) ||
      !get(env, argv[1], slots_per_stripe)) [[unlikely]]
    return enif_make_badarg(env);

  if (slots_per_stripe > 64)
    return make_tuple(env, am_error, am_max_slots_exceeded_64);

  auto ctx = construct_resource_with_events<PoolContext>(
    resource_events<PoolContext>(nullptr, PoolContext::pool_resource_stop));

  ctx->stripe_count = num_stripes;
  ctx->stripes.resize(num_stripes);

  for (auto i = 0u; i < num_stripes; ++i) {
    ctx->stripes[i] = std::make_unique<PoolStripe>();
    auto& stripe = *ctx->stripes[i];
    stripe.capacity = slots_per_stripe;

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

  return UNLIKELY(flags == -1 || fcntl(raw_fd, F_SETFL, flags | O_NONBLOCK) == -1)
       ? make_tuple(env, am_error, am_failed_to_set_nonblocking)
       : ctx->claim_slot_term(env, stripe, raw_fd, owner_pid);
}

//-----------------------------------------------------------------------------
// Open and connect a brand-new IPv4 TCP socket entirely inside this NIF
// in non-blocking mode, then claim a conn for it exactly like
// register_socket_nif. Unlike register_socket/4, the fd never has any
// other owner (no Erlang `socket()` term, no `prim_socket` resource
// fighting over it) -- the safer alternative to handing off an
// already-open fd, see arterial_connection2's moduledoc.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM connect_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  IP4Tuple     octets;
  int          port;
  unsigned int timeout_ms;
  bool         nodelay;
  ErlNifPid    owner_pid;

  assert(argc == 7);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !get(env, argv[4], timeout_ms)
    || !get(env, argv[5], nodelay)
    || !get(env, argv[6], owner_pid))
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return make_tuple(env, am_error, am_socket_failed);

  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
    close(fd);
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);
  }
  if (nodelay) {
    static constexpr int one = 1;
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
    return make_tuple(env, am_error, am_connect_failed);
  }
  // For EINPROGRESS, connection is in progress - proceed with slot claiming
  // The slot will be marked as SLOT_CONNECTING and completion will be
  // handled via enif_select write-ready notifications

  // Claim slot and setup timeout if needed
  auto slot_result = ctx->claim_slot(env, stripe, fd, owner_pid);
  if (slot_result < 0) {
    return make_tuple(env, am_error, am_stripe_full);
  }

  // Set up connection timeout using RAII if timeout_ms > 0
  if (timeout_ms > 0) {
    Connection& conn = stripe.slots[slot_result];
    int timeout_fd = setup_connection_timeout_fd(conn, timeout_ms);
    if (timeout_fd < 0) {
      // Failed to set up RAII timeout - clean up and return error
      stripe.release_slot(slot_result);
      close(fd);
      return make_tuple(env, am_error, am_timeout_setup_failed);
    }
    // Note: timeout_fd should be registered with enif_select by the connection code
  }

  return make_tuple(env, am_ok, slot_result);
}

//-----------------------------------------------------------------------------
// Non-blocking version of connect_nif: starts connection and returns
// immediately, then sends completion notification via message when connection
// completes or fails.
//-----------------------------------------------------------------------------
static ERL_NIF_TERM connect_async_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  IP4Tuple     octets;
  int          port;
  bool         nodelay;
  ErlNifPid    owner_pid;

  assert(argc == 6);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !get(env, argv[4], nodelay)
    || !get(env, argv[5], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  auto& stripe = *ctx->stripes[stripe_id];

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0)
    return make_tuple(env, am_error, am_socket_failed);

  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
    close(fd);
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);
  }
  if (nodelay) {
    static constexpr int one = 1;
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
    return make_tuple(env, am_error, am_connect_failed);
  }

  // Use the centralized claim_slot function for consistent slot allocation
  int slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);

  // Check if slot claiming failed
  if (slot_id < 0) {
    close(fd);
    return make_tuple(env, am_error, am_stripe_full);
  }

  auto& conn = stripe.slots[slot_id];

  if (rc == 0) {
    // Connection completed immediately
    conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
    conn.arm_read(env, ctx); // TODO: error handling?
    return make_tuple(env, am_ok, slot_id);
  }

  // Connection in progress - arm write notification for completion
  conn.status.store(SLOT_CONNECTING, std::memory_order_release);

  // Check if select registration succeeds
  if (conn.arm_connect(env, ctx) < 0) {
    // Revert status, clear lease bit, and return error
    conn.status.store(SLOT_EMPTY, std::memory_order_release);
    stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
    close(conn.fd);
    conn.fd = -1;
    return make_tuple(env, am_error, am_select_failed);
  }

  return make_tuple(env, am_ok, am_connecting, slot_id);
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
static ERL_NIF_TERM handle_readable_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  using ReadResult = Connection::ReadResult;

  PoolContext* ctx;
  auto pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Delegate to Connection method
  auto res = conn.handle_readable(env, ctx);

  // Handle the result and convert to appropriate Erlang terms
  switch (res.result) {
    case ReadResult::DATA:
      return make_tuple(env, am_ok, std::move(res.data));

    case ReadResult::CLOSED:
      ctx->notify_and_close(env, conn);
      return am_closed;

    case ReadResult::HANDSHAKE_READ:
    case ReadResult::HANDSHAKE_WRITE:
      return make_tuple(env, am_ok, std::move(res.data));

    case ReadResult::CONNECT_OK:
    case ReadResult::CONNECT_FAILED:
      if (res.send_connect_msg) {
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, res.connect_result);
        enif_send(env, &conn.owner_pid, msg_env, msg);
      }
      if (res.result == ReadResult::CONNECT_FAILED) {
        ctx->notify_and_close(env, conn);
        return am_closed;
      }
      return make_tuple(env, am_ok, std::move(res.data));

    case ReadResult::ERROR:
      return make_tuple(env, am_error, am_alloc_failed);

    default:
      return make_tuple(env, am_error, am_unknown);
  }
}

//-----------------------------------------------------------------------------
static ERL_NIF_TERM handle_writable_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  using WriteResult = Connection::WriteResult;

  PoolContext* ctx;
  Connection*  pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Delegate to Connection method
  auto res = conn.handle_writable(env, ctx);

  // Handle the result and convert to appropriate Erlang terms
  switch (res.result) {
    case WriteResult::OK:
      return am_ok;

    case WriteResult::CLOSED:
      ctx->notify_and_close(env, conn);
      return am_closed;

    case WriteResult::HANDSHAKE_READ:
    case WriteResult::HANDSHAKE_WRITE:
      return am_ok;

    case WriteResult::CONNECT_OK:
    case WriteResult::CONNECT_FAILED:
      if (res.send_connect_msg) {
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, res.connect_result);
        enif_send(env, &conn.owner_pid, msg_env, msg);
      }
      if (res.result == WriteResult::CONNECT_FAILED) {
        ctx->notify_and_close(env, conn);
        return am_closed;
      }
      return am_ok;

    case WriteResult::ERROR:
      return make_tuple(env, am_error, am_unknown);

    default:
      return make_tuple(env, am_error, am_unknown);
  }
}

//-----------------------------------------------------------------------------
// Protocol-aware version of connect_nif
//-----------------------------------------------------------------------------
static ERL_NIF_TERM connect_proto_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  unsigned int timeout_ms;
  bool         nodelay;
  ErlNifPid    owner_pid;
  ProtocolType protocol;
  IP4Tuple     octets;

  assert(argc == 8);

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
  auto result = Connection::connect_proto(
                  env, ctx, stripe_id, octets,
                  port, timeout_ms, protocol, nodelay, owner_pid);

  // Handle the result and convert to appropriate Erlang terms
  switch (result.result) {
    case Connection::ConnectResult::OK:
      return make_tuple(env, am_ok, result.slot_id);

    case Connection::ConnectResult::CONNECTING:
      return make_tuple(env, am_ok, am_connecting, result.slot_id);

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
// Protocol-aware async version
//-----------------------------------------------------------------------------
static ERL_NIF_TERM connect_async_proto_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  bool         nodelay;
  ErlNifPid    owner_pid;
  ProtocolType protocol;
  IP4Tuple     octets;

  assert(argc == 7);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !parse_protocol(env, argv[4], protocol)
    || !get(env, argv[5], nodelay)
    || !get(env, argv[6], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  // Delegate to Connection method
  auto result = Connection::connect_async_proto(env, ctx, stripe_id, octets, port, protocol, nodelay, owner_pid);

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

  enif_select(env, conn.fd, ERL_NIF_SELECT_STOP, ctx, nullptr, am_stop);
  return am_ok;
}

//-----------------------------------------------------------------------------
// Handle connection timeout cleanup - free slot and release resources
//-----------------------------------------------------------------------------
static ERL_NIF_TERM handle_connection_timeout_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = arterial::resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  // Free the slot before touching any fds
  conn.status.store(SLOT_EMPTY, std::memory_order_release);
  auto& stripe = *ctx->stripes[conn.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);

  // enif_select_read is one-shot: the registration was already consumed when
  // the timeout message was delivered, so we can close the timer fd directly.
  conn.timer.reset();  // RAII closes the timer fd

  // Clear conn.fd before SELECT_STOP so that if pool_resource_stop fires
  // after this slot is reused, it won't find the old fd and zero the new one.
  int fd = conn.fd;
  conn.fd = -1;

  if (fd != -1) {
#ifdef HAVE_OPENSSL
    cleanup_slot_ssl(conn);
#endif
    // Deregister socket from enif_select (arm_connect registered it write-ready).
    // pool_resource_stop will close(fd) once SELECT_STOP is acknowledged.
    enif_select(env, fd, ERL_NIF_SELECT_STOP, ctx, nullptr, am_stop);
  }

  return am_ok;
}

//-----------------------------------------------------------------------------
// Socket options enhanced functions (stubs for now)
//-----------------------------------------------------------------------------
static ERL_NIF_TERM connect_with_opts_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  unsigned int timeout_ms;
  bool         nodelay;
  ErlNifPid    owner_pid;
  IP4Tuple     octets;

  assert(argc == 8);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !get(env, argv[4], timeout_ms)
    || !get(env, argv[5], nodelay)
    || !get(env, argv[6], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  // Delegate to Connection method
  auto result = Connection::connect_with_opts(env, ctx, stripe_id, octets, port, timeout_ms, nodelay, owner_pid, argv[7]);

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
  if (current_status == SLOT_AVAILABLE) {
    conn.status.store(SLOT_EMPTY, std::memory_order_release);
  }
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
    uint64_t new_mask = current_mask | target_bit;

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

      return make(env, std::make_tuple(
        am_ok, am_fifo_reserved,
        stripe_id, static_cast<unsigned int>(slot_id), reservation_id
      ));
    }
    attempt++;
  }

  // Fast path failed - check if any slots exist and what their status is
  bool has_connecting_slots = false;
  bool has_failed_slots = false;

  for (size_t i = 0; i < stripe.capacity; i++) {
    auto slot_status = stripe.slots[i].status.load(std::memory_order_acquire);
    if (slot_status == SLOT_CONNECTING) {
      has_connecting_slots = true;
    } else if (slot_status == SLOT_EMPTY && stripe.slots[i].fd == -1) {
      has_failed_slots = true;
    }
  }

  // If no slots are connecting and we have failed connections, return error immediately
  if (!has_connecting_slots && has_failed_slots) {
    return make_tuple(env, am_error, am_pool_busy);
  }

  // If we have connecting slots, we could wait, but for now return timeout
  // to avoid hanging tests. A real implementation would use async notification.
  return make_tuple(env, am_error, am_timeout);
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

  // Set conn to draining state, then back to available
  conn.status.store(SLOT_FIFO_DRAINING, std::memory_order_release);
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
      return make(env, std::make_tuple(
        am_ok, am_fifo_request_sent,
        stripe_id,                             // Use the stripe_id parameter
        static_cast<unsigned int>(result.slot_id),
        result.reservation_id
      ));

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

  return register_resource<PoolContext>(env, "arterial_pool_context") ? 0 : 1;
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
