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

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/opensslv.h>
#endif

using namespace nifpp;
using namespace arterial;

//=============================================================================
// NIF Function Declarations
//=============================================================================

static ERL_NIF_TERM init_pool_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM configure_throttle_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM register_socket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_async_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM send_and_release_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_readable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_writable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_async_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM close_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_proto_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM is_slot_available_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM set_slot_available_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM set_slot_unavailable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// FIFO Mode 3 NIF functions
static ERL_NIF_TERM reserve_fifo_connection_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM send_fifo_request_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM release_fifo_connection_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM fifo_connection_status_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_fifo_reply_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM reserve_send_fifo_request_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//===========================================================================
// NIFs
//===========================================================================

// Resolve {PoolRef, StripeId, SlotId} (the shape shared by
// handle_readable/3, handle_writable/3, close_slot/3) to a Connection&, or
// nullptr if any index is out of range.
inline Connection* resolve_slot(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[], PoolContext** out_ctx)
{
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;

  assert(argc == 3);

  if  (!get(env, argv[0], ctx)
    || !ctx
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !ctx->stripes[stripe_id]
    || !get(env, argv[2], slot_id)) [[unlikely]]
    return nullptr;

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return nullptr;

  *out_ctx = ctx;
  return &stripe.slots[slot_id];
}

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

// Configure throttling for a pool
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

// Open and connect a brand-new IPv4 TCP socket entirely inside this NIF
// in non-blocking mode, then claim a conn for it exactly like
// register_socket_nif. Unlike register_socket/4, the fd never has any
// other owner (no Erlang `socket()` term, no `prim_socket` resource
// fighting over it) -- the safer alternative to handing off an
// already-open fd, see arterial_connection2's moduledoc.
static ERL_NIF_TERM connect_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;
  int port;
  unsigned int timeout_ms;
  bool nodelay;
  ErlNifPid owner_pid;

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

  return ctx->claim_slot_term(env, stripe, fd, owner_pid);
}

// Non-blocking version of connect_nif: starts connection and returns immediately,
// then sends completion notification via message when connection completes or fails
static ERL_NIF_TERM connect_async_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;
  int port;
  bool nodelay;
  ErlNifPid owner_pid;

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

  auto  list         = argv[2];
  auto& stripe       = *ctx->stripes[stripe_id];
  auto  current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  auto  slot_id      = -1;

  // Loop with retry limit to prevent infinite loops
  int retry_count = 0;
  const int max_retries = stripe.capacity * 2; // Allow reasonable number of retries

  do {
    slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return make_tuple(env, am_error, am_no_connections_available);

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask | target_bit;

    if (!stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) [[unlikely]] {
      retry_count++;
      if (retry_count >= max_retries) [[unlikely]]
        return make_tuple(env, am_error, am_no_connections_available);
      continue;
    }

    // CAS succeeded - now check if slot is available and passes throttling
    auto& candidate_slot = stripe.slots[slot_id];
    uint32_t slot_status = candidate_slot.status.load(std::memory_order_acquire);

    if (slot_status == SLOT_AVAILABLE && candidate_slot.fd >= 0 &&
        throttle_allow(ctx, candidate_slot))
      break; // Success - slot is leased and passes throttling

    // Slot doesn't pass throttling or isn't available - release it and try next
    stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
    current_mask = stripe.lease_mask.load(std::memory_order_relaxed);

    retry_count++;
    if (retry_count >= max_retries) [[unlikely]]
      return make_tuple(env, am_error, am_no_connections_available);
  } while (true);

  auto& conn = stripe.slots[slot_id];
  conn.status.store(SLOT_LEASED, std::memory_order_relaxed);

  unsigned int list_len = 0;
  enif_get_list_length(env, list, &list_len);

  // Inline storage for the common case (arterial_client2 always calls
  // this with a single-element list) -- avoids a heap allocation on
  // every write; only lists longer than this fall back to the heap.
  constexpr size_t s_inline_iov_size = 8;
  std::array<struct iovec, s_inline_iov_size> inline_iov;
  std::vector<struct iovec>                   heap_iov;
  struct iovec* iov;
  if (list_len <= s_inline_iov_size)
    iov = inline_iov.data();
  else {
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

  ssize_t  written    = 0;
  uint64_t target_bit = (1ULL << slot_id);

#ifdef HAVE_OPENSSL
  if (conn.ssl) {
    // SSL doesn't support writev, so we need to write sequentially
    for (unsigned int j = 0; j < i && written >= 0; ++j) {
    RETRY1:
      ssize_t n = SSL_write(conn.ssl, iov[j].iov_base, static_cast<int>(iov[j].iov_len));
      if (n > 0)
        written += n;
      else {
        int ssl_error =  SSL_get_error(conn.ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE)
          // Would block, we'll handle partial write below
          break;

        // Handle retryable SSL errors
        if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
          // Temporary X.509 error - will retry
          break;
        }

        if (ssl_error == SSL_ERROR_SYSCALL) {
          // Check if it's a temporary system error
          if (errno == EINTR) [[unlikely]]
            goto RETRY1;
          if (errno == EAGAIN || errno == EWOULDBLOCK)
            // Temporary system error - retry
            break;
        }

        if (ssl_error == SSL_ERROR_ZERO_RETURN)
          // Clean SSL shutdown from peer - treat as partial write completion
          break;

        // Unrecoverable SSL error
        cleanup_slot_ssl(conn);
        ctx->notify_and_close(env, conn);
        return make_tuple(env, am_error, am_write_failed);
      }

      // Check if we wrote the complete iovec entry
      if (n < static_cast<ssize_t>(iov[j].iov_len))
        // Partial write, we need to handle this in the buffer logic below
        break;
    }
  }
  else
#endif
  {
  RETRY2:
    written = (i > 0) ? writev(conn.fd, iov, i) : 0;

    if (written < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        written = 0;
      else if (errno == EINTR) [[unlikely]]
        goto RETRY2;
      else {
        ctx->notify_and_close(env, conn);
        return make_tuple(env, am_error, am_write_failed);
      }
    }
  }

  if (static_cast<size_t>(written) < total_bytes) {
    conn.pending_buffer.resize(total_bytes);
    size_t offset = 0;
    for (unsigned int j = 0; j < i; ++j) {
      std::memcpy(conn.pending_buffer.data() + offset, iov[j].iov_base, iov[j].iov_len);
      offset += iov[j].iov_len;
    }
    conn.bytes_written = static_cast<size_t>(written);
    conn.status.store(SLOT_WRITE_POLLING, std::memory_order_release);

    conn.arm_write(env, ctx); // TODO: handle errors

    // CRITICAL FIX: Also arm read for eventual response even with partial writes
    conn.arm_read(env, ctx); // TODO: error handling?

    return make_tuple(env, am_ok, slot_id);
  }

  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
  stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);

  // CRITICAL FIX: Arm socket for reading response after successful send
  conn.arm_read(env, ctx); // TODO: error handling?

  return make_tuple(env, am_ok, slot_id);
}

static ERL_NIF_TERM handle_readable_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  auto pconn = resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  if (conn.fd == -1) return am_closed;

  // Stale read event from a previous connection on this slot: ignore.
  // This can happen when the slot is reused before pool_resource_stop
  // deregisters the old fd's enif_select.
  uint32_t current_status = conn.status.load(std::memory_order_acquire);
  if (current_status == SLOT_CONNECTING || current_status == SLOT_EMPTY)
    return make_tuple(env, am_ok, binary{0});

#ifdef HAVE_OPENSSL
  // Handle ongoing SSL handshake
  if (current_status == SLOT_SSL_HANDSHAKE) {
    if (conn.protocol == PROTO_SSL && conn.ssl) {
      int handshake_result = ssl_handshake_blocking(conn, 5000);

      if (handshake_result == 1) {
        // Handshake completed successfully
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        auto& stripe = *ctx->stripes[conn.stripe_id];
        stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, am_ok);
        enif_send(env, &conn.owner_pid, msg_env, msg);
        conn.arm_read(env, ctx); // TODO: error handling?
        // Return empty data to indicate handshake completion
        return make_tuple(env, am_ok, binary{0});
      } else if (handshake_result == 0) {
        // Still needs READ - arm read event and return
        conn.arm_read(env, ctx); // TODO: error handling?
        return make_tuple(env, am_ok, binary{0});
      } else if (handshake_result == -2) {
        // Still needs WRITE - arm write event
        conn.arm_write(env, ctx); // TODO: handle errors
        return make_tuple(env, am_ok, binary{0});
      } else {
        // Handshake failed
        cleanup_slot_ssl(conn);
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, am_connect_failed);
        enif_send(env, &conn.owner_pid, msg_env, msg);
        ctx->notify_and_close(env, conn);
        return am_closed;
      }
    }
  }
#endif

  // FIONREAD is only a sizing hint, never proof of anything: it can
  // legitimately report 0 on a perfectly healthy connection (e.g. under
  // heavy concurrent load) without that meaning EOF -- only read(2)'s
  // own return value (0 = EOF, -1/EAGAIN = nothing available right now,
  // not closed) is authoritative. Treating a 0 FIONREAD as "closed"
  // outright (as this used to) spuriously killed live connections under
  // load.
  int bytes_available = 0;
  ioctl(conn.fd, FIONREAD, &bytes_available);
  size_t read_size = bytes_available > 0 ? static_cast<size_t>(bytes_available) : 8192;

  binary bin(read_size);
  if (!bin) [[unlikely]]
    return make_tuple(env, am_error, am_alloc_failed);

  ssize_t n;

#ifdef HAVE_OPENSSL
  if (conn.ssl) {
  RETRY0:
    n = SSL_read(conn.ssl, bin.data, static_cast<int>(read_size));
    if (n <= 0) {
      int ssl_error = SSL_get_error(conn.ssl, static_cast<int>(n));

      if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
        conn.arm_read(env, ctx); // TODO: error handling?
        return make_tuple(env, am_ok, binary{0});
      }

      // Handle retryable SSL errors more gracefully
      if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
        // Retry after X.509 operations - arm read and try again
        conn.arm_read(env, ctx);
        return make_tuple(env, am_ok, binary{0});
      }

      if (ssl_error == SSL_ERROR_SYSCALL) {
        // Check system error - only close if it's a real error
        if (errno == EINTR)
          goto RETRY0;
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          cleanup_slot_ssl(conn);
          ctx->notify_and_close(env, conn);
          return am_closed;
        }
        // Temporary system error - retry
        conn.arm_read(env, ctx);
        return make_tuple(env, am_ok, binary{0});
      }

      if (ssl_error == SSL_ERROR_ZERO_RETURN) {
        // Clean SSL shutdown from peer - this is normal end-of-data, not an error
        // Don't force disconnect, just return empty data to signal end
        return make_tuple(env, am_ok, binary{0});
      }

      // SSL connection closed or unrecoverable error
      cleanup_slot_ssl(conn);
      ctx->notify_and_close(env, conn);
      return am_closed;
    }
  }
  else
#endif
  {
  RETRY1:
    n = read(conn.fd, bin.data, read_size);

    if (n <= 0) {
      if (errno == EINTR) [[unlikely]]
        goto RETRY1;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        conn.arm_read(env, ctx); // TODO: error handling?
        return make_tuple(env, am_ok, binary{0});
      }
      ctx->notify_and_close(env, conn);
      return am_closed;
    }
  }

  if (static_cast<size_t>(n) < bin.size && !bin.realloc(n)) [[unlikely]] {
    cleanup_slot_ssl(conn);
    ctx->notify_and_close(env, conn);
    return am_closed;
  }

  conn.arm_read(env, ctx); // TODO: error handling?

  // FIFO Mode 3: if this slot is reserved, deliver reply directly
  // to the waiting caller instead of returning bytes for codec decoding.
  if (conn.fifo_request_active.load(std::memory_order_acquire)) {
    // Safety check: ensure fifo mode is actually enabled before sending message
    if (conn.is_fifo_enabled()) {
      nifpp::msg_env msg_env;
      auto reply_msg = make_tuple(msg_env,
        am_arterial_fifo_reply,
        conn.stripe_id,
        conn.slot_id,
        std::move(bin)
      );
      enif_send(env, &conn.fifo_requester_pid, msg_env, reply_msg);
      conn.clear_fifo_request();
      // Return empty binary so arterial_connection has nothing to decode.
      return make_tuple(env, am_ok, binary{0});
    } else {
      // FIFO request was active but mode is not enabled - clear it
      conn.clear_fifo_request();
    }
  }

  return make_tuple(env, am_ok, std::move(bin));
}

static ERL_NIF_TERM handle_writable_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection*  pconn = resolve_slot(env, argc, argv, &ctx);
  if (!pconn) [[unlikely]]
    return enif_make_badarg(env);
  Connection& conn = *pconn;

  if (conn.fd == -1) [[unlikely]]
    return am_closed;

  uint32_t current_status = conn.status.load(std::memory_order_acquire);

  // Handle connection completion for async connect
  if (current_status == SLOT_CONNECTING) {
    int so_err = 0;
    socklen_t len = sizeof(so_err);
    if (getsockopt(conn.fd, SOL_SOCKET, SO_ERROR, &so_err, &len) == -1 || so_err != 0) {
      // Connection failed
      nifpp::msg_env msg_env;
      auto msg = conn.make_connect_result_msg(msg_env, am_connect_failed);
      enif_send(env, &conn.owner_pid, msg_env, msg);
      ctx->notify_and_close(env, conn);
      return am_closed;
    } else {
      // Connection succeeded, check if we need SSL handshake
#ifdef HAVE_OPENSSL
      if (conn.protocol == PROTO_SSL) {
        if (!setup_ssl_on_socket(conn, conn.fd)) {
          nifpp::msg_env msg_env;
          auto msg = conn.make_connect_result_msg(msg_env, am_connect_failed);
          enif_send(env, &conn.owner_pid, msg_env, msg);
          ctx->notify_and_close(env, conn);
          return am_closed;
        }

        // Start non-blocking SSL handshake
        int handshake_result = ssl_handshake_blocking(conn, 5000);

        if (handshake_result == 1) {
          // Handshake completed successfully
          conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
          auto& stripe = *ctx->stripes[conn.stripe_id];
          stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);
          nifpp::msg_env msg_env;
          auto msg = conn.make_connect_result_msg(msg_env, am_ok);
          enif_send(env, &conn.owner_pid, msg_env, msg);
          conn.arm_read(env, ctx); // TODO: error handling?
          return am_ok;
        } else if (handshake_result == 0) {
          // Handshake needs READ - set status and arm read event
          conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          conn.arm_read(env, ctx); // TODO: error handling?
          return am_ok;
        } else if (handshake_result == -2) {
          // Handshake needs WRITE - set status and arm write event
          conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          conn.arm_write(env, ctx); // TODO: error handling?
          return am_ok;
        } else {
          // Handshake failed
          cleanup_slot_ssl(conn);
          nifpp::msg_env msg_env;
          auto msg = conn.make_connect_result_msg(msg_env, am_connect_failed);
          enif_send(env, &conn.owner_pid, msg_env, msg);
          ctx->notify_and_close(env, conn);
          return am_closed;
        }
      } else
#endif
      {
        // Plain TCP connection succeeded
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        auto& stripe = *ctx->stripes[conn.stripe_id];
        stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, am_ok);
        enif_send(env, &conn.owner_pid, msg_env, msg);
        conn.arm_read(env, ctx); // TODO: error handling?
        return am_ok;
      }
    }
  }

#ifdef HAVE_OPENSSL
  // Handle ongoing SSL handshake
  if (current_status == SLOT_SSL_HANDSHAKE && conn.protocol == PROTO_SSL && conn.ssl) {
    switch (ssl_handshake_blocking(conn, 5000)) {
      case 1:
      {
        // Handshake completed successfully
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        auto& stripe = *ctx->stripes[conn.stripe_id];
        stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, am_ok);
        enif_send(env, &conn.owner_pid, msg_env, msg);
        conn.arm_read(env, ctx); // TODO: error handling?
        return am_ok;
      }
      case 0:
        // Still needs READ - arm read event
        conn.arm_read(env, ctx); // TODO: error handling?
        return am_ok;
      case -2:
        // Still needs WRITE - arm write event
        conn.arm_write(env, ctx); // TODO: error handling?
        return am_ok;
      default:
      {
        // Handshake failed
        cleanup_slot_ssl(conn);
        nifpp::msg_env msg_env;
        auto msg = conn.make_connect_result_msg(msg_env, am_connect_failed);
        enif_send(env, &conn.owner_pid, msg_env, msg);
        ctx->notify_and_close(env, conn);
        return am_closed;
      }
    }
  }
#endif


  // Handle pending write operations
  size_t remaining = conn.pending_buffer.size() - conn.bytes_written;
  while (remaining > 0) {
    ssize_t n;

#ifdef HAVE_OPENSSL
    if (conn.ssl) {
      n = SSL_write(conn.ssl, conn.pending_buffer.data() + conn.bytes_written, static_cast<int>(remaining));
      if (n <= 0) {
        int ssl_error = SSL_get_error(conn.ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
          conn.arm_write(env, ctx); // TODO: handle errors
          return am_ok;
        }

        // Handle retryable SSL errors more gracefully
        if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
          // Retry after X.509 operations
          conn.arm_write(env, ctx);
          return am_ok;
        }

        if (ssl_error == SSL_ERROR_SYSCALL) {
          // Check system error - only close if it's a real error
          if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            cleanup_slot_ssl(conn);
            ctx->notify_and_close(env, conn);
            return am_closed;
          }
          // Temporary system error - retry
          conn.arm_write(env, ctx);
          return am_ok;
        }

        if (ssl_error == SSL_ERROR_ZERO_RETURN) {
          // Clean SSL shutdown from peer - don't treat as error
          conn.arm_write(env, ctx);
          return am_ok;
        }

        // Unrecoverable SSL error
        cleanup_slot_ssl(conn);
        ctx->notify_and_close(env, conn);
        return am_closed;
      }
    } else
#endif
    {
      while ((n = write(conn.fd, conn.pending_buffer.data() + conn.bytes_written, remaining)) < 0 && errno == EINTR);
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          conn.arm_write(env, ctx); // TODO: handle errors
          return am_ok;
        }
        ctx->notify_and_close(env, conn);
        return am_closed;
      }
    }

    conn.bytes_written += static_cast<size_t>(n);
    remaining -= static_cast<size_t>(n);
  }

  conn.pending_buffer.clear();
  conn.bytes_written = 0;
  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);

  auto& stripe = *ctx->stripes[conn.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << conn.slot_id), std::memory_order_release);
  return am_ok;
}

// Protocol-aware version of connect_nif
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
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;

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

#ifndef HAVE_OPENSSL
  // SSL requires OpenSSL at compile time
  if (protocol == PROTO_SSL)
    return make_tuple(env, am_error, am_unsupported_protocol);
#endif

  int fd = create_socket_for_protocol(protocol);
  if (fd == -1)
    return make_tuple(env, am_error, am_socket_failed);

  if (!configure_socket_for_protocol(fd, protocol, nodelay)) {
    close(fd);
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);
  }

  struct sockaddr_in server_addr{};
  auto [o0, o1, o2, o3]  = octets;
  server_addr.sin_family = AF_INET;
  server_addr.sin_port   = htons(static_cast<uint16_t>(port));
  uint32_t ip_host       = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  server_addr.sin_addr.s_addr = htonl(ip_host);

  // For UDP, "connecting" just sets the default destination
  // For TCP/SSL, this is a real connection
  int rc = connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr));

  if (protocol == PROTO_UDP)
    // UDP connect() just sets default peer, always succeeds immediately
    rc = 0;
  else if (rc != 0 && errno != EINPROGRESS) {
    close(fd);
    return make_tuple(env, am_error, am_connect_failed);
  }

  // Handle non-blocking connect like connect_async_proto_nif
  if (rc == 0) {
    // Connection completed immediately
#ifdef HAVE_OPENSSL
    if (protocol == PROTO_SSL) {
      auto& stripe = *ctx->stripes[stripe_id];
      int  slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);

      if (slot_id < 0) {
        close(fd);
        return make_tuple(env, am_error, am_stripe_full);
      }

      auto& conn = stripe.slots[slot_id];
      conn.protocol = protocol;

      if (!setup_ssl_on_socket(conn, fd)) {
        close(fd);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return make_tuple(env, am_error, am_connect_failed);
      }

      // Perform non-blocking SSL handshake
      int handshake_result = ssl_handshake_blocking(conn, 5000);
      if (handshake_result == 1) {
        // Handshake completed successfully
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        conn.arm_read(env, ctx);
        return make_tuple(env, am_ok, slot_id);
      } else if (handshake_result == 0) {
        // Handshake needs READ - set status and arm read event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_read(env, ctx);
        return make_tuple(env, am_ok, am_connecting, slot_id);
      } else if (handshake_result == -2) {
        // Handshake needs WRITE - set status and arm write event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_write(env, ctx);
        return make_tuple(env, am_ok, am_connecting, slot_id);
      } else {
        // Handshake failed - clean up the claimed slot
        cleanup_slot_ssl(conn);
        close(fd);
        conn.status.store(SLOT_EMPTY, std::memory_order_release);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return make_tuple(env, am_error, am_connect_failed);
      }
    } else
#endif
    {
      // For TCP/UDP, proceed with immediate slot claiming
      auto& stripe = *ctx->stripes[stripe_id];
      return ctx->claim_slot_term(env, stripe, fd, owner_pid);
    }
  } else if (errno == EINPROGRESS) {
    // Connection in progress, register and set up for async notification
    auto& stripe = *ctx->stripes[stripe_id];
    int slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);

    if (slot_id < 0) {
      close(fd);
      return make_tuple(env, am_error, am_stripe_full);
    }

    auto& conn = stripe.slots[slot_id];
#ifdef HAVE_OPENSSL
    conn.protocol = protocol;
#endif
    conn.status.store(SLOT_CONNECTING, std::memory_order_release);

    // Check if select registration succeeds
    if (conn.arm_connect(env, ctx) < 0) {
      // Revert status and lease bit, then return error
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      close(fd);
      return make_tuple(env, am_error, am_select_failed);
    }

    return make_tuple(env, am_ok, am_connecting, slot_id);
  } else {
    // Connection failed immediately
    close(fd);
    return make_tuple(env, am_error, am_connect_failed);
  }

  auto&  stripe = *ctx->stripes[stripe_id];
  return ctx->claim_slot_term(env, stripe, fd, owner_pid);
}

// Protocol-aware async version
static ERL_NIF_TERM connect_async_proto_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  bool         nodelay;
  ErlNifPid    owner_pid;
  ProtocolType protocol;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;

  assert(argc == 7);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !parse_protocol(env, argv[4], protocol)
    || !get(env, argv[5], nodelay)
    || !get(env, argv[6], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

#ifndef HAVE_OPENSSL
  // SSL requires OpenSSL at compile time
  if (protocol == PROTO_SSL)
    return make_tuple(env, am_error, am_unsupported_protocol);
#endif

  int fd = create_socket_for_protocol(protocol);
  if (fd == -1)
    return make_tuple(env, am_error, am_socket_failed);

  if (!configure_socket_for_protocol(fd, protocol, nodelay)) {
    close(fd);
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(static_cast<uint16_t>(port));
  uint32_t ip_host = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  server_addr.sin_addr.s_addr = htonl(ip_host);

  int rc = connect(fd, (struct sockaddr*)&server_addr, sizeof(server_addr));

  if (protocol == PROTO_UDP) {
    // UDP "connect" sets default destination, but can still fail
    if (rc != 0) {
      close(fd);
      return make_tuple(env, am_error, am_connect_failed);
    }

    // UDP connect succeeded, set up the connection immediately
    auto& stripe = *ctx->stripes[stripe_id];
    auto slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);

    // CRITICAL FIX: For UDP, we need to make the conn available for send_and_release
    // immediately after claiming it, since UDP has no connection handshake phase.
    // Extract slot_id from the result and clear the lease mask bit.
    if (slot_id >= 0 && slot_id < int(stripe.capacity)) {
      // Clear the lease mask bit to make the conn available for send_and_release
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
    }

    return slot_id < 0 ? make_tuple(env, am_error, am_stripe_full)
                       : make_tuple(env, am_ok,    slot_id);
  }

  // For TCP/SSL, handle async connection like connect_async_nif
  if (rc == 0) {
    // Connection completed immediately
#ifdef HAVE_OPENSSL
    if (protocol == PROTO_SSL) {
      // Use the centralized claim_slot function instead of duplicating logic
      auto& stripe  = *ctx->stripes[stripe_id];
      auto  slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);

      // Check if slot claiming failed
      if (slot_id < 0) {
        // claim_slot failed, close fd and return error
        close(fd);
        return make_tuple(env, am_error, am_connect_failed);
      }

      auto& conn = stripe.slots[slot_id];
      conn.protocol = protocol;

      if (!setup_ssl_on_socket(conn, fd)) {
        close(fd);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return make_tuple(env, am_error, am_connect_failed);
      }

      // Perform SSL handshake
      int handshake_result = ssl_handshake_blocking(conn, 5000);
      if (handshake_result == 1) {
        // Handshake completed successfully
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        conn.arm_read(env, ctx); // TODO: error handling?
        return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
      } else if (handshake_result == 0) {
        // Handshake needs READ - set status and arm read event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_read(env, ctx); // TODO: error handling?
        return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
      } else if (handshake_result == -2) {
        // Handshake needs WRITE - set status and arm write event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_write(env, ctx); // TODO: error handling?
        return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
      } else {
        // Handshake failed - clean up the claimed slot
        cleanup_slot_ssl(conn);
        close(fd);
        conn.status.store(SLOT_EMPTY, std::memory_order_release);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return make_tuple(env, am_error, am_connect_failed);
      }
    } else
#endif
    {
      auto&  stripe = *ctx->stripes[stripe_id];
      return ctx->claim_slot_term(env, stripe, fd, owner_pid);
    }
  } else if (errno == EINPROGRESS) {
    // Connection in progress, register and set up for async notification
    // Use the centralized claim_slot function for consistency
    auto& stripe  = *ctx->stripes[stripe_id];
    auto  slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);

    // Check if slot claiming failed
    if (slot_id < 0) {
      // claim_slot failed, close fd and return error
      close(fd);
      return make_tuple(env, am_error, am_stripe_full);
    }

    auto& conn = stripe.slots[slot_id];
#ifdef HAVE_OPENSSL
    conn.protocol = protocol;
#endif
    conn.status.store(SLOT_CONNECTING, std::memory_order_relaxed);

    // Check if select registration succeeds
    if (conn.arm_connect(env, ctx) < 0) {
      // Revert status and lease bit, then return error
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      close(fd);
      return make_tuple(env, am_error, am_select_failed);
    }

    return make_tuple(env, am_ok, am_connecting, static_cast<unsigned int>(slot_id));
  } else {
    // Connection failed immediately
    close(fd);
    return make_tuple(env, am_error, am_connect_failed);
  }
}

// Force-close a slot (bouncer recycle, or teardown of an idle connection)
// -- unlike notify_and_close, this is caller-initiated, so no "closed"
// heads-up is sent (the caller already knows).
static ERL_NIF_TERM close_slot_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = resolve_slot(env, argc, argv, &ctx);
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

// Socket options enhanced functions (stubs for now)
static ERL_NIF_TERM connect_with_opts_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  fflush(stderr);
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  unsigned int timeout_ms;
  bool         nodelay;
  ErlNifPid    owner_pid;
  ERL_NIF_TERM socket_opts;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;

  assert(argc == 8);

  if  (!get(env, argv[0], ctx)
    || !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count-1))
    || !get(env, argv[2], octets)
    || !get(env, argv[3], port, 0, 65535)
    || !get(env, argv[4], timeout_ms)
    || !get(env, argv[5], nodelay)
    || !get(env, argv[6], owner_pid)) [[unlikely]]
    return enif_make_badarg(env);

  socket_opts = argv[7];

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == -1)
    return make_tuple(env, am_error, am_socket_failed);

  // Set non-blocking before applying custom options
  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
    close(fd);
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);
  }

  ERL_NIF_TERM err;

  // Apply custom socket options
  if (!arterial::apply_sock_opts(fd, env, socket_opts, err)) {
    close(fd);
    return make(env, std::make_tuple(am_error,
      err == 0 ? am_socket_option_failed : err));
  }

  // Apply nodelay if requested
  if (nodelay) {
    static constexpr int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }

  struct sockaddr_in addr{};
  auto [o0, o1, o2, o3] = octets;
  addr.sin_family       = AF_INET;
  addr.sin_port         = htons(port);
  addr.sin_addr.s_addr  = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);

  int result = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
  if (result == -1) {
    if (errno != EINPROGRESS) {
      close(fd);
      return make_tuple(env, am_error, am_connect_failed);
    }
    // For EINPROGRESS, connection is in progress - proceed with slot claiming
    // The slot will be marked as SLOT_CONNECTING and completion will be
    // handled via enif_select write-ready notifications
  }

  // Get the stripe for claiming
  if (stripe_id >= ctx->stripe_count) {
    close(fd);
    return enif_make_badarg(env);
  }

  PoolStripe& stripe = *ctx->stripes[stripe_id];
  return ctx->claim_slot_term(env, stripe, fd, owner_pid);
}

static ERL_NIF_TERM connect_proto_with_opts_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  // Debug: verify this function is called
  fflush(stderr);
  PoolContext* ctx;
  unsigned int stripe_id;
  int          port;
  unsigned int timeout_ms;
  ProtocolType protocol;
  bool         nodelay;
  ErlNifPid    owner_pid;
  ERL_NIF_TERM socket_opts;
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int> octets;

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

  socket_opts = argv[8];

#ifndef HAVE_OPENSSL
  if (protocol == PROTO_SSL)
    return make_tuple(env, am_error, am_ssl_not_supported);
#endif

  int fd = create_socket_for_protocol(protocol);
  if (fd == -1)
    return make_tuple(env, am_error, am_socket_failed);

  // Configure socket for protocol (sets non-blocking)
  if (!configure_socket_for_protocol(fd, protocol, false)) {  // nodelay handled separately
    close(fd);
    return make_tuple(env, am_error, am_failed_to_set_nonblocking);
  }

  ERL_NIF_TERM err;

  // Apply custom socket options
  if (!arterial::apply_sock_opts(fd, env, socket_opts, err)) {
    close(fd);
    return make(env, std::make_tuple(am_error,
      err == 0 ? am_socket_option_failed : err));
  }

  // Apply nodelay if requested
  if (nodelay && (protocol == PROTO_TCP || protocol == PROTO_SSL)) {
    static constexpr int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(port);
  addr.sin_addr.s_addr = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);

  int result = -1;
  if (protocol == PROTO_UDP) {
    // For UDP, we "connect" to set default destination (client mode)
    // This allows send/recv to work with the default peer
    result = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (result != 0) {
      close(fd);
      return make_tuple(env, am_error, am_connect_failed);
    }
  } else {
    // TCP and SSL use connect
    result = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (result < 0 && errno != EINPROGRESS) {
      close(fd);
      return make_tuple(env, am_error, am_connect_failed);
    }
    // For EINPROGRESS, connection is in progress - proceed with slot claiming
  }

  // Get the stripe for claiming
  PoolStripe& stripe = *ctx->stripes[stripe_id];

#ifdef HAVE_OPENSSL
  // For SSL connections, need to claim slot manually and set up SSL
  if (protocol == PROTO_SSL) {
    auto slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);
    if (slot_id < 0) {
      close(fd);
      return make_tuple(env, am_error, am_stripe_full);
    }

    auto& conn = stripe.slots[slot_id];
    conn.protocol = protocol;

    if (!setup_ssl_on_socket(conn, fd)) {
      close(fd);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return make_tuple(env, am_error, am_connect_failed);
    }

    // Perform SSL handshake
    int handshake_result = ssl_handshake_blocking(conn, 5000);
    if (handshake_result == 1) {
      // Handshake completed successfully
      conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
      conn.arm_read(env, ctx);
      return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
    } else if (handshake_result == 0) {
      // Handshake needs READ - set status and arm read event
      conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
      conn.arm_read(env, ctx);
      return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
    } else if (handshake_result == -2) {
      // Handshake needs WRITE - set status and arm write event
      conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
      conn.arm_write(env, ctx);
      return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
    } else {
      // Handshake failed - clean up the claimed slot
      cleanup_slot_ssl(conn);
      close(fd);
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return make_tuple(env, am_error, am_connect_failed);
    }
  } else
#endif
  {
    // For TCP/UDP, claim slot and set protocol
    auto slot_id = ctx->claim_slot(env, stripe, fd, owner_pid);
    if (slot_id < 0) {
      close(fd);
      return make_tuple(env, am_error, am_stripe_full);
    }

    auto& conn = stripe.slots[slot_id];
    conn.protocol = protocol;

    return make_tuple(env, am_ok, static_cast<unsigned int>(slot_id));
  }
}

// Check if a connection slot is available (authoritative availability check)
static ERL_NIF_TERM is_slot_available_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = resolve_slot(env, argc, argv, &ctx);
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

// Mark a connection conn as available for new sends
static ERL_NIF_TERM set_slot_available_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = resolve_slot(env, argc, argv, &ctx);
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

// Mark a connection slot as unavailable for new sends
static ERL_NIF_TERM set_slot_unavailable_nif(
  ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  PoolContext* ctx;
  Connection* pconn = resolve_slot(env, argc, argv, &ctx);
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

// Simple FIFO extension structure

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
    return make_tuple(env, am_error, am_no_connections_available);
  }

  // If we have connecting slots, we could wait, but for now return timeout
  // to avoid hanging tests. A real implementation would use async notification.
  return make_tuple(env, am_error, am_timeout);
}

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

  auto& stripe = *ctx->stripes[stripe_id];

  // Get caller PID for queuing if needed
  ErlNifPid caller_pid;
  enif_self(env, &caller_pid);

  // Try immediate reservation first (fast path)
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);

  // Immediate reservation attempt (limited retries for performance)
  int retry_count = 0;
  const int max_retries = 3;  // Reduce retries for better performance

  while (retry_count < max_retries) {
    int slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) {
      // No immediate slots - try queuing
      break;
    }

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_relaxed, std::memory_order_relaxed)) {

      auto& conn = stripe.slots[slot_id];
      // Use relaxed ordering for performance - status check still provides safety
      if (conn.status.load(std::memory_order_relaxed) != SLOT_AVAILABLE) {
        // Slot not ready - release and try next
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_relaxed);
        current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
        retry_count++;
        continue;
      }

      // Success - we have a slot, now send the request immediately
      conn.enable_fifo_mode();

      // Generate reservation ID
      static std::atomic<uint64_t> reservation_counter{1000000};
      uint64_t reservation_id = reservation_counter.fetch_add(1,
                                                              std::memory_order_relaxed);
      if (!conn.set_fifo_request(caller_pid, reservation_id)) {
        // Failed to set request - release conn
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        return make_tuple(env, am_error, am_fifo_slot_busy);
      }

      conn.status.store(SLOT_FIFO_RESERVED, std::memory_order_release);

      // Send the request data immediately (combined operation)
      // Use stack-allocated array for better performance (most requests have few segments)
      static constexpr size_t MAX_IOVECS = 32;
      iovec iovecs[MAX_IOVECS];
      size_t iovec_count = 0;
      size_t total_bytes = 0;

      // Process the request data and send it
      ERL_NIF_TERM head, tail = argv[2];
      while (enif_get_list_cell(env, tail, &head, &tail) && iovec_count < MAX_IOVECS) {
        ErlNifBinary bin;
        if (!enif_inspect_binary(env, head, &bin)) {
          // Cleanup on error
          stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
          return enif_make_badarg(env);
        }
        iovecs[iovec_count].iov_base = const_cast<void*>(reinterpret_cast<const void*>(bin.data));
        iovecs[iovec_count].iov_len = bin.size;
        total_bytes += bin.size;
        iovec_count++;
      }

      if (iovec_count == 0) {
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        return enif_make_badarg(env);
      }

    REPEAT2:
      // Perform the write
      ssize_t bytes_written = writev(conn.fd, iovecs, static_cast<int>(iovec_count));

      if (bytes_written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // Socket would block - atomic operation can't complete, caller should retry
          conn.clear_fifo_request();
          stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
          return make_tuple(env, am_error, am_partial);
        }
        else if (errno == EINTR) [[unlikely]]
          goto REPEAT2;
        else {
          conn.clear_fifo_request();
          stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
          return make_tuple(env, am_error, am_write_failed);
        }
      }

      // Check if we wrote all the data - this is meant to be an atomic operation
      if (static_cast<size_t>(bytes_written) < total_bytes) {
        // Atomic operation failed - either nothing written (EAGAIN) or partial write
        // In both cases, clear the reservation and let caller use regular two-step process
        conn.clear_fifo_request();
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        return make_tuple(env, am_error, am_partial);
      }

      conn.status.store(SLOT_FIFO_REQUEST_SENT, std::memory_order_release);

      // Return success with reservation info for later release
      return make(env, std::make_tuple(
        am_ok, am_fifo_request_sent,
        static_cast<unsigned int>(conn.stripe_id),
        static_cast<unsigned int>(slot_id),
        reservation_id
      ));
    }
    retry_count++;
  }

  // Fast path failed - return error immediately
  // TODO: Implement proper queuing and async notification
  return make_tuple(env, am_error, am_no_connections_available);
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

  return register_resource<PoolContext>(env, "arterial_pool_context") ? 0 : 1;
}

static void unload([[maybe_unused]] ErlNifEnv* env, [[maybe_unused]] void* priv_data)
{
  #ifdef HAVE_OPENSSL
  cleanup_ssl();
  #endif
}

static ErlNifFunc nif_funcs[] = {
  {"init_pool",                   2, init_pool_nif,               0},
  {"configure_throttle",          3, configure_throttle_nif,      0},
  {"register_socket",             4, register_socket_nif,         0},
  {"connect",                     7, connect_nif,                 0},
  {"connect_async",               6, connect_async_nif,           0},
  {"connect_proto",               8, connect_proto_nif,           0},
  {"connect_async_proto",         7, connect_async_proto_nif,     0},
  {"send_and_release",            3, send_and_release_nif,        0},
  {"handle_readable",             3, handle_readable_nif,         0},
  {"handle_writable",             3, handle_writable_nif,         0},
  {"connect_with_opts",           8, connect_with_opts_nif,       0},
  {"connect_proto_with_opts",     9, connect_proto_with_opts_nif, 0},
  {"close_slot",                  3, close_slot_nif,              0},
  {"is_slot_available",           3, is_slot_available_nif,       0},
  {"set_slot_available",          3, set_slot_available_nif,      0},
  {"set_slot_unavailable",        3, set_slot_unavailable_nif,    0},

  // FIFO Mode 3 functions
  {"reserve_fifo_connection",     3, reserve_fifo_connection_nif,   0},
  {"send_fifo_request",           6, send_fifo_request_nif,         0},
  {"release_fifo_connection",     4, release_fifo_connection_nif,   0},
  {"fifo_connection_status",      3, fifo_connection_status_nif,    0},
  {"handle_fifo_reply",           4, handle_fifo_reply_nif,         0},
  {"reserve_send_fifo_request",   5, reserve_send_fifo_request_nif, 0}
};

ERL_NIF_INIT(arterial_nif, nif_funcs, load, nullptr, nullptr, unload)
