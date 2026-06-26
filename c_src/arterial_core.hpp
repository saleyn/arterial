#pragma once

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

//=============================================================================
// Custom Atoms (declared in global nifpp namespace)
//=============================================================================

NIFPP_ADD_KNOWN_ATOM(am_arterial_event);
NIFPP_ADD_KNOWN_ATOM(am_read);
NIFPP_ADD_KNOWN_ATOM(am_write);
NIFPP_ADD_KNOWN_ATOM(am_closed);
NIFPP_ADD_KNOWN_ATOM(am_stop);
NIFPP_ADD_KNOWN_ATOM(am_connect_result);
NIFPP_ADD_KNOWN_ATOM(am_connecting);
NIFPP_ADD_KNOWN_ATOM(am_stripe_full);
NIFPP_ADD_KNOWN_ATOM(am_max_slots_exceeded_64);
NIFPP_ADD_KNOWN_ATOM(am_failed_to_set_nonblocking);
NIFPP_ADD_KNOWN_ATOM(am_socket_failed);
NIFPP_ADD_KNOWN_ATOM(am_connect_failed);
NIFPP_ADD_KNOWN_ATOM(am_timeout);
NIFPP_ADD_KNOWN_ATOM(am_write_failed);
NIFPP_ADD_KNOWN_ATOM(am_no_connections_available);
NIFPP_ADD_KNOWN_ATOM(am_unsupported_protocol);
NIFPP_ADD_KNOWN_ATOM(am_socket_option_failed);

NIFPP_ADD_KNOWN_ATOM(am_tcp);
NIFPP_ADD_KNOWN_ATOM(am_udp);
NIFPP_ADD_KNOWN_ATOM(am_ssl);

namespace arterial {

//=============================================================================
// Core Enumerations
//=============================================================================

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

//=============================================================================
// Core Data Structures
//=============================================================================

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
  std::atomic<uint64_t> lease_mask;  // Initialized explicitly in
                                     // init_pool_nif
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
  uint32_t throttle_rate_per_sec{0};   // requests per second
  uint32_t throttle_window_msec{0};    // time window in milliseconds

  // Raw fds aren't RAII-managed by any member here, so closing them on
  // teardown needs an explicit destructor (unlike ConnectionPool in
  // arterial.hpp, which owns no raw resources and needs none).
  ~PoolContext() {
    for (auto& stripe_ptr : stripes)
      for (auto& slot : stripe_ptr->slots)
        if (slot.fd != -1) close(slot.fd);
  }
};

//=============================================================================
// Utility Functions
//=============================================================================

// Resolve {PoolRef, StripeId, SlotId} (the shape shared by
// handle_readable/3, handle_writable/3, close_slot/3) to a ConnSlot&, or
// nullptr if any index is out of range.
inline ConnSlot* resolve_slot(ErlNifEnv* env, int argc,
                             const ERL_NIF_TERM argv[],
                             PoolContext** out_ctx) {
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;

  if (argc != 3 ||
      !get(env, argv[0], ctx) ||
      !get(env, argv[1], stripe_id) ||
      !get(env, argv[2], slot_id)) {
    return nullptr;
  }

  if (stripe_id >= ctx->stripe_count) return nullptr;
  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) return nullptr;

  *out_ctx = ctx;
  return &stripe.slots[slot_id];
}

// Time spacing throttling check - returns true if the request was allowed
inline bool throttle_allow(PoolContext* ctx, ConnSlot& slot) {
  return ctx->throttle_rate_per_sec == 0                // No throttling configured
      || slot.throttle.add(1, arterial::now_utc()) > 0; // check if we can add one request
}

} // namespace arterial