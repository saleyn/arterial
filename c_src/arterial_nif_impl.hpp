#pragma once

#include "arterial_core.hpp"
#include "arterial_protocol.hpp"
#include "arterial_socket.hpp"
#include "arterial_ssl.hpp"

namespace arterial {

//=============================================================================
// NIF Function Implementations
//=============================================================================

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

  auto ctx = std::make_unique<PoolContext>();
  ctx->stripe_count = num_stripes;
  ctx->stripes.reserve(num_stripes);

  for (auto i = 0u; i < num_stripes; ++i) {
    auto stripe = std::make_unique<PoolStripe>();
    stripe->capacity = slots_per_stripe;

    // Initialize lease mask: set unused bits to 1 (unavailable)
    uint64_t initial_mask;
    if (slots_per_stripe < 64) {
      initial_mask = ~((1ULL << slots_per_stripe) - 1);
    } else {
      initial_mask = 0ULL;
    }
    stripe->lease_mask.store(initial_mask, std::memory_order_relaxed);

    for (auto j = 0u; j < 64; ++j) {
      stripe->slots[j].fd        = -1;
      stripe->slots[j].stripe_id =  i;
      stripe->slots[j].slot_id   =  j;
    }

    ctx->stripes.push_back(std::move(stripe));
  }

  return make(env, std::make_tuple(am_ok, ctx.release()));
}

static ERL_NIF_TERM configure_throttle_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int rate_per_sec;
  unsigned int window_msec;

  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], rate_per_sec) ||
      !get(env, argv[2], window_msec)) {
    return enif_make_badarg(env);
  }

  ctx->throttle_rate_per_sec = rate_per_sec;
  ctx->throttle_window_msec  = window_msec;

  // Update throttling for all existing slots
  for (auto& stripe_ptr : ctx->stripes)
    for (auto& slot : stripe_ptr->slots)
      if (slot.fd != -1)
        slot.throttle = arterial::time_spacing_throttle(rate_per_sec, window_msec);

  return am_ok;
}

static ERL_NIF_TERM register_socket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;
  int raw_fd;
  ErlNifPid owner_pid;

  if (argc != 4                     ||
      !get(env, argv[0], ctx)       ||
      !get(env, argv[1], stripe_id) ||
      !get(env, argv[2], raw_fd)    ||
      !get(env, argv[3], owner_pid)) {
    return enif_make_badarg(env);
  }

  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);

  if (fcntl(raw_fd, F_SETFL, O_NONBLOCK) == -1) {
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }

  auto& stripe = *ctx->stripes[stripe_id];
  return claim_slot(env, ctx, stripe, raw_fd, owner_pid);
}

// Generic connection function that uses protocol handlers
template<ProtocolType Protocol>
static ERL_NIF_TERM connect_generic_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[],
                                        bool async = false, bool with_opts = false) {
  PoolContext* ctx;
  unsigned int stripe_id;
  std::tuple<int, int, int, int> octets;
  int port;
  int timeout_ms;
  bool nodelay;
  ErlNifPid owner_pid;
  ERL_NIF_TERM socket_opts = 0;

  int expected_argc = with_opts ? (async ? 8 : 9) : (async ? 6 : 7);
  if (argc != expected_argc) return enif_make_badarg(env);

  int arg_idx = 0;
  if (!get(env, argv[arg_idx++], ctx)       ||
      !get(env, argv[arg_idx++], stripe_id) ||
      !get(env, argv[arg_idx++], octets)    ||
      !get(env, argv[arg_idx++], port, 0, 65535) {
    return enif_make_badarg(env);
  }

  if (!async) {
    if (!get(env, argv[arg_idx++], timeout_ms)) return enif_make_badarg(env);
  }

  if (!get(env, argv[arg_idx++], nodelay) ||
      !get(env, argv[arg_idx++], owner_pid)) {
    return enif_make_badarg(env);
  }

  if (with_opts)
    socket_opts = argv[arg_idx++];

  if (stripe_id >= ctx->stripe_count)
    return enif_make_badarg(env);

  using Handler = ProtocolHandler<Protocol>;

  int fd = Handler::create_socket();
  if (fd == -1)
    return make(env, std::make_tuple(am_error, am_socket_failed));

  if (!Handler::configure_socket(fd, nodelay)) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_failed_to_set_nonblocking));
  }

  if (with_opts && !apply_sock_opts(fd, env, socket_opts)) {
    close(fd);
    return make(env, std::make_tuple(am_error, am_socket_option_failed));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(port);
  addr.sin_addr.s_addr = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);

  int connect_result = Handler::connect_socket(fd, addr);

  if (connect_result == 0 || (connect_result == -1 && errno == EINPROGRESS)) {
    auto& stripe = *ctx->stripes[stripe_id];
    auto claimed_result = claim_slot(env, ctx, stripe, fd, owner_pid);

    // Check if claim_slot succeeded
    const ERL_NIF_TERM* tuple_elements;
    int tuple_arity;
    if (enif_get_tuple(env, claimed_result, &tuple_arity, &tuple_elements) &&
        tuple_arity == 2 && enif_is_identical(tuple_elements[0], am_ok)) {

      unsigned int slot_id;
      if (enif_get_uint(env, tuple_elements[1], &slot_id) && slot_id < stripe.capacity) {
        auto& slot = stripe.slots[slot_id];

        if (!Handler::setup_connection(slot, fd)) {
          close(fd);
          stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
          return make(env, std::make_tuple(am_error, am_connect_failed));
        }

        if (async) {
          if (connect_result == 0) // Connection completed immediately
            return Handler::handle_connect_result(slot, env, ctx) == 1
                  ? make(env, std::make_tuple(am_ok, slot_id))
                  : make(env, std::make_tuple(am_error, am_connect_failed));
          else {
            // Connection in progress
            slot.status.store(SLOT_CONNECTING, std::memory_order_release);
            arm_connect(env, ctx, slot);
            return make(env, std::make_tuple(am_ok, am_connecting, slot_id));
          }
        } else {
          // Synchronous connection - wait for completion
          if (connect_result == -1 && errno == EINPROGRESS) {
            // Use select to wait for connection completion
            fd_set write_fds;
            struct timeval tv;

            FD_ZERO(&write_fds);
            FD_SET(fd, &write_fds);
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;

            int select_result = select(fd + 1, nullptr, &write_fds, nullptr, &tv);
            if (select_result <= 0) {
              close(fd);
              stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
              return make(env, std::make_tuple(am_error, am_timeout));
            }
          }

          int result = Handler::handle_connect_result(slot, env, ctx);
          if (result == 1) {
            stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
            return make(env, std::make_tuple(am_ok, slot_id));
          } else {
            close(fd);
            stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
            return make(env, std::make_tuple(am_error, am_connect_failed));
          }
        }
      }
    }
  }

  close(fd);
  return make(env, std::make_tuple(am_error, am_connect_failed));
}

static ERL_NIF_TERM connect_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return connect_generic_nif<PROTO_TCP>(env, argc, argv, false, false);
}

static ERL_NIF_TERM connect_async_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return connect_generic_nif<PROTO_TCP>(env, argc, argv, true, false);
}

static ERL_NIF_TERM connect_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  return connect_generic_nif<PROTO_TCP>(env, argc, argv, false, true);
}

static ERL_NIF_TERM connect_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  // Extract protocol from arguments
  char protocol_str[8];
  if (!enif_get_atom(env, argv[5], protocol_str, sizeof(protocol_str), ERL_NIF_LATIN1)) {
    return enif_make_badarg(env);
  }

  if (strcmp(protocol_str, "tcp") == 0) {
    return connect_generic_nif<PROTO_TCP>(env, argc, argv, false, false);
  } else if (strcmp(protocol_str, "udp") == 0) {
    return connect_generic_nif<PROTO_UDP>(env, argc, argv, false, false);
#ifdef HAVE_OPENSSL
  } else if (strcmp(protocol_str, "ssl") == 0) {
    return connect_generic_nif<PROTO_SSL>(env, argc, argv, false, false);
#endif
  } else {
    return make(env, std::make_tuple(am_error, am_unsupported_protocol));
  }
}

static ERL_NIF_TERM connect_async_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  // Extract protocol from arguments (position 4 for async)
  char protocol_str[8];
  if (!enif_get_atom(env, argv[4], protocol_str, sizeof(protocol_str), ERL_NIF_LATIN1)) {
    return enif_make_badarg(env);
  }

  if (strcmp(protocol_str, "tcp") == 0) {
    return connect_generic_nif<PROTO_TCP>(env, argc, argv, true, false);
  } else if (strcmp(protocol_str, "udp") == 0) {
    return connect_generic_nif<PROTO_UDP>(env, argc, argv, true, false);
#ifdef HAVE_OPENSSL
  } else if (strcmp(protocol_str, "ssl") == 0) {
    return connect_generic_nif<PROTO_SSL>(env, argc, argv, true, false);
#endif
  } else {
    return make(env, std::make_tuple(am_error, am_unsupported_protocol));
  }
}

static ERL_NIF_TERM connect_proto_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  // Extract protocol from arguments (position 5 for with_opts)
  char protocol_str[8];
  if (!enif_get_atom(env, argv[5], protocol_str, sizeof(protocol_str), ERL_NIF_LATIN1)) {
    return enif_make_badarg(env);
  }

  if (strcmp(protocol_str, "tcp") == 0) {
    return connect_generic_nif<PROTO_TCP>(env, argc, argv, false, true);
  } else if (strcmp(protocol_str, "udp") == 0) {
    return connect_generic_nif<PROTO_UDP>(env, argc, argv, false, true);
#ifdef HAVE_OPENSSL
  } else if (strcmp(protocol_str, "ssl") == 0) {
    return connect_generic_nif<PROTO_SSL>(env, argc, argv, false, true);
#endif
  } else {
    return make(env, std::make_tuple(am_error, am_unsupported_protocol));
  }
}

static ERL_NIF_TERM send_and_release_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  unsigned int stripe_id;

  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !enif_is_list(env, argv[2])) {
    return enif_make_badarg(env);
  }

  if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);

  auto list = argv[2];
  auto& stripe = *ctx->stripes[stripe_id];
  auto current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  int slot_id = -1;

  // CAS loop to find and lease a slot
  do {
    slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) {
      return make(env, std::make_tuple(am_error, am_no_connections_available));
    }

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) {

      // CAS succeeded - check if slot is available and passes throttling
      auto& candidate_slot = stripe.slots[slot_id];
      if (candidate_slot.status.load(std::memory_order_relaxed) == SLOT_AVAILABLE &&
          throttle_allow(ctx, candidate_slot))
        break; // Success - slot is leased and passes throttling
      else {
        // Slot doesn't pass throttling or isn't available - release it
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
        continue;
      }
    }
  } while (true);

  auto& slot = stripe.slots[slot_id];
  slot.status.store(SLOT_LEASED, std::memory_order_relaxed);
  TERM slot_term = make(env, static_cast<unsigned int>(slot_id));

  // Convert Erlang list to iovec array
  unsigned int list_len = 0;
  enif_get_list_length(env, list, &list_len);

  constexpr size_t s_inline_iov_size = 8;
  std::array<struct iovec, s_inline_iov_size> inline_iov;
  std::vector<struct iovec> heap_iov;
  struct iovec* iov;

  if (list_len <= s_inline_iov_size) {
    iov = inline_iov.data();
  } else {
    heap_iov.resize(list_len);
    iov = heap_iov.data();
  }

  ERL_NIF_TERM head, tail = list;
  for (unsigned int i = 0; i < list_len && enif_get_list_cell(env, tail, &head, &tail); ++i) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, head, &bin)) {
      slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return enif_make_badarg(env);
    }
    iov[i].iov_base = bin.data;
    iov[i].iov_len = bin.size;
  }

  // Use protocol-specific write
  ssize_t bytes_written;
  if (slot.protocol == PROTO_SSL) {
    bytes_written = ProtocolHandler<PROTO_SSL>::write_data(slot, iov, list_len);
  } else if (slot.protocol == PROTO_UDP) {
    bytes_written = ProtocolHandler<PROTO_UDP>::write_data(slot, iov, list_len);
  } else {
    bytes_written = ProtocolHandler<PROTO_TCP>::write_data(slot, iov, list_len);
  }

  if (bytes_written == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
    notify_and_close(env, ctx, slot);
    return make(env, std::make_tuple(am_error, am_write_failed));
  }

  size_t total_bytes = 0;
  for (unsigned int i = 0; i < list_len; ++i) {
    total_bytes += iov[i].iov_len;
  }

  if (static_cast<size_t>(bytes_written) < total_bytes) {
    // Partial write - buffer the remainder
    slot.pending_buffer.resize(total_bytes - bytes_written);
    size_t offset = 0;
    size_t remaining = bytes_written;

    for (unsigned int i = 0; i < list_len; ++i) {
      if (remaining >= iov[i].iov_len) {
        remaining -= iov[i].iov_len;
      } else {
        size_t copy_start = remaining;
        size_t copy_len = iov[i].iov_len - remaining;
        memcpy(&slot.pending_buffer[offset],
               static_cast<char*>(iov[i].iov_base) + copy_start,
               copy_len);
        offset += copy_len;
        remaining = 0;
      }
    }

    slot.bytes_written = 0;
    arm_write(env, ctx, slot);
    return make(env, std::make_tuple(am_ok, slot_term));
  }

  slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
  stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
  return make(env, std::make_tuple(am_ok, slot_term));
}

// Additional NIF functions (handle_readable, handle_writable, etc.) would follow
// similar patterns but are omitted for brevity. They would use the protocol
// handlers for read/write operations.

static ERL_NIF_TERM close_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  // Use protocol-specific cleanup
  if (slot.protocol == PROTO_SSL) {
    ProtocolHandler<PROTO_SSL>::cleanup_connection(slot);
  } else if (slot.protocol == PROTO_UDP) {
    ProtocolHandler<PROTO_UDP>::cleanup_connection(slot);
  } else {
    ProtocolHandler<PROTO_TCP>::cleanup_connection(slot);
  }

  slot.pending_buffer.clear();
  slot.bytes_written = 0;
  slot.status.store(SLOT_EMPTY, std::memory_order_release);

  auto& stripe = *ctx->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_or(1ULL << slot.slot_id, std::memory_order_release);

  return am_ok;
}

// New unified state management functions
static ERL_NIF_TERM is_slot_available_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  // Check both slot status and lease mask for authoritative availability
  bool slot_ready = (slot.status.load(std::memory_order_acquire) == SLOT_AVAILABLE);

  auto& stripe = *ctx->stripes[slot.stripe_id];
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_acquire);
  bool slot_unleased = !(current_mask & (1ULL << slot.slot_id));

  return (slot_ready && slot_unleased) ? am_true : am_false;
}

static ERL_NIF_TERM set_slot_available_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  // Set slot status to available
  slot.status.store(SLOT_AVAILABLE, std::memory_order_release);

  // Clear lease mask bit to make slot available for send_and_release
  auto& stripe = *ctx->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << slot.slot_id), std::memory_order_release);

  return am_ok;
}

static ERL_NIF_TERM set_slot_unavailable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
  PoolContext* ctx;
  ConnSlot* slot_ptr = resolve_slot(env, argc, argv, &ctx);
  if (!slot_ptr) return enif_make_badarg(env);
  ConnSlot& slot = *slot_ptr;

  // Set lease mask bit to prevent new sends
  auto& stripe = *ctx->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_or(1ULL << slot.slot_id, std::memory_order_release);

  // Set slot status to indicate unavailability (but preserve specific states like CONNECTING)
  uint32_t current_status = slot.status.load(std::memory_order_acquire);
  if (current_status == SLOT_AVAILABLE) {
    slot.status.store(SLOT_EMPTY, std::memory_order_release);
  }

  return am_ok;
}

//=============================================================================
// NIF Function Table
//=============================================================================

static ErlNifFunc nif_funcs[] = {
  {"init_pool",               2, init_pool_nif,               0},
  {"configure_throttle",      3, configure_throttle_nif,      0},
  {"register_socket",         4, register_socket_nif,         0},
  {"connect",                 7, connect_nif,                 ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connect_async",           6, connect_async_nif,           0},
  {"connect_proto",           8, connect_proto_nif,           ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connect_async_proto",     7, connect_async_proto_nif,     0},
  {"send_and_release",        3, send_and_release_nif,        0},
  {"connect_with_opts",       8, connect_with_opts_nif,       ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"connect_proto_with_opts", 9, connect_proto_with_opts_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
  {"close_slot",              3, close_slot_nif,              0},
  {"is_slot_available",       3, is_slot_available_nif,       0},
  {"set_slot_available",      3, set_slot_available_nif,      0},
  {"set_slot_unavailable",    3, set_slot_unavailable_nif,    0}
};

//=============================================================================
// NIF Lifecycle Functions
//=============================================================================

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
#ifdef HAVE_OPENSSL
  init_ssl();
#endif
  return 0;
}

static void unload(ErlNifEnv* env, void* priv_data) {
#ifdef HAVE_OPENSSL
  cleanup_ssl();
#endif
}

} // namespace arterial