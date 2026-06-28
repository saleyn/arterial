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
NIFPP_ADD_KNOWN_ATOM(am_closed);
NIFPP_ADD_KNOWN_ATOM(am_connect_failed);
NIFPP_ADD_KNOWN_ATOM(am_connect_result);
NIFPP_ADD_KNOWN_ATOM(am_connecting);
NIFPP_ADD_KNOWN_ATOM(am_failed_to_set_nonblocking);
NIFPP_ADD_KNOWN_ATOM(am_max_slots_exceeded_64);
NIFPP_ADD_KNOWN_ATOM(am_no_connections_available);
NIFPP_ADD_KNOWN_ATOM(am_read);
NIFPP_ADD_KNOWN_ATOM(am_socket_failed);
NIFPP_ADD_KNOWN_ATOM(am_socket_option_failed);
NIFPP_ADD_KNOWN_ATOM(am_stop);
NIFPP_ADD_KNOWN_ATOM(am_stripe_full);
NIFPP_ADD_KNOWN_ATOM(am_timeout);
NIFPP_ADD_KNOWN_ATOM(am_unsupported_protocol);
NIFPP_ADD_KNOWN_ATOM(am_write_failed);
NIFPP_ADD_KNOWN_ATOM(am_write);

NIFPP_ADD_KNOWN_ATOM(am_ssl);
NIFPP_ADD_KNOWN_ATOM(am_tcp);
NIFPP_ADD_KNOWN_ATOM(am_udp);

// Socket options
NIFPP_ADD_KNOWN_ATOM(am_add_membership);
NIFPP_ADD_KNOWN_ATOM(am_cork);
NIFPP_ADD_KNOWN_ATOM(am_drop_membership);
NIFPP_ADD_KNOWN_ATOM(am_keepalive);
NIFPP_ADD_KNOWN_ATOM(am_keepcnt);
NIFPP_ADD_KNOWN_ATOM(am_keepidle);
NIFPP_ADD_KNOWN_ATOM(am_keepintvl);
NIFPP_ADD_KNOWN_ATOM(am_linger);
NIFPP_ADD_KNOWN_ATOM(am_multicast_if);
NIFPP_ADD_KNOWN_ATOM(am_multicast_loop);
NIFPP_ADD_KNOWN_ATOM(am_multicast_ttl);
NIFPP_ADD_KNOWN_ATOM(am_nodelay);
NIFPP_ADD_KNOWN_ATOM(am_priority);
NIFPP_ADD_KNOWN_ATOM(am_quickack);
NIFPP_ADD_KNOWN_ATOM(am_rcvbuf);
NIFPP_ADD_KNOWN_ATOM(am_rcvlowat);
NIFPP_ADD_KNOWN_ATOM(am_reuseaddr);
NIFPP_ADD_KNOWN_ATOM(am_sndbuf);
NIFPP_ADD_KNOWN_ATOM(am_sndlowat);
NIFPP_ADD_KNOWN_ATOM(am_tos);
NIFPP_ADD_KNOWN_ATOM(am_user_timeout);

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

// Return true, and parse an integer or boolean term into `value`
inline bool get_int_or_bool_option(ErlNifEnv* env, ERL_NIF_TERM opt, int& value) {
  if (get(env, opt, value)) return true;
  if (enif_is_identical(opt, am_true))  { value = 1; return true; }
  if (enif_is_identical(opt, am_false)) { value = 0; return true; }
  return false;
}

// If the {OptName, OptValue} option matches given name, set it by calling Fun(fd, value)
template <typename Fun>
inline bool set_int_or_bool_sockopt(ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name, Fun f) {
  if (!enif_is_identical(opts[0], name)) return false;
  int value;
  if (get(env, opts[1], value)) goto SET;
  if (enif_is_identical(opts[1], am_true))  { value = 1; goto SET; }
  if (enif_is_identical(opts[1], am_false)) { value = 0; goto SET; }
  return false;

SET:
  return f(value) == 0;  // setsockopt returns 0 on success
}

// If the {OptName, OptValue} option matches given name, set it
inline bool set_int_or_bool_sockopt(ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name, int fd, int opt_class, int opt_type) {
  auto f = [=](int v) {
    return setsockopt(fd, opt_class, opt_type, &v, sizeof(v));
  };
  return set_int_or_bool_sockopt(env, opts, name, f);
}

// If the {OptName, OptValue} option matches given name, set it
inline bool set_atom_sockopt(const ERL_NIF_TERM opt, ERL_NIF_TERM name, int fd, int opt_class, int opt_type) {
  if (!enif_is_identical(opt, name)) return false;
  static constexpr int v = 1;
  return setsockopt(fd, opt_class, opt_type, &v, sizeof(v)) == 0;
}

template <typename Tuple, typename Fun>
inline bool set_tuple_sockopt(ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name, Fun f) {
  if (!enif_is_identical(opts[0], name)) return false;
  Tuple tup;
  if (!get(env, opts[1], tup)) return false;
  return f(tup) == 0;
}

// If the {OptName, OptValue :: {{A,B,C,D}, {E,F,G,H}}} option matches given name, set it
inline bool set_addr_sockopt(ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name, int fd, int opt_class, int opt_type) {
  std::tuple<ERL_NIF_TERM, ERL_NIF_TERM> arg_tup;

  return set_tuple_sockopt<decltype(arg_tup)>(env, opts, name,
    [=](auto& tup) {
      auto& maddr = std::get<0>(tup);
      auto& iaddr = std::get<1>(tup);
      using FourOctetsT = std::tuple<unsigned int, unsigned int, unsigned int, unsigned int>;
      FourOctetsT mcast_addr, if_addr;
      if (!get(env, maddr, mcast_addr) || !get(env, iaddr, if_addr))
        return false;
      struct ip_mreq mreq;
      auto m0 = std::get<0>(mcast_addr);
      auto m1 = std::get<1>(mcast_addr);
      auto m2 = std::get<2>(mcast_addr);
      auto m3 = std::get<3>(mcast_addr);
      auto i0 = std::get<0>(if_addr);
      auto i1 = std::get<1>(if_addr);
      auto i2 = std::get<2>(if_addr);
      auto i3 = std::get<3>(if_addr);
      mreq.imr_multiaddr.s_addr = htonl((m0 << 24) | (m1 << 16) | (m2 << 8) | m3);
      mreq.imr_interface.s_addr = htonl((i0 << 24) | (i1 << 16) | (i2 << 8) | i3);
      return setsockopt(fd, opt_class, opt_type, &mreq, sizeof(mreq)) == 0;
    });
}

static bool apply_sock_opts(int fd, ErlNifEnv* env, ERL_NIF_TERM options_list) {
  if (enif_is_empty_list(env, options_list))
    return true; // No options to apply

  ERL_NIF_TERM head, tail = options_list;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    // Parse each option - can be atom or tuple-based options {Option, Value}
    const ERL_NIF_TERM* tuple_elements;
    int                 tuple_arity;

    if (enif_is_atom(env, head)) {
      // Handle common atom-based options
      if  (!arterial::set_atom_sockopt(head, am_keepalive, fd, SOL_SOCKET,  SO_KEEPALIVE)
        && !arterial::set_atom_sockopt(head, am_nodelay,   fd, IPPROTO_TCP, TCP_NODELAY)
        && !arterial::set_atom_sockopt(head, am_reuseaddr, fd, SOL_SOCKET,  SO_REUSEADDR)
      )
        return false;
    } else if (enif_get_tuple(env, head, &tuple_arity, &tuple_elements) && tuple_arity == 2 &&
               enif_is_atom(env, tuple_elements[0])) {
      // Handle tuple-based options {Option, Value}
      if  (!arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepalive, fd, SOL_SOCKET,  SO_KEEPALIVE)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_nodelay,   fd, IPPROTO_TCP, TCP_NODELAY)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_linger,
            [fd](int v) {
              struct linger l = {.l_onoff = 1, .l_linger = v /* seconds */};
              return setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
            })
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_reuseaddr,     fd, SOL_SOCKET,  SO_REUSEADDR)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_sndbuf,        fd, SOL_SOCKET,  SO_SNDBUF)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_rcvbuf,        fd, SOL_SOCKET,  SO_RCVBUF)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_priority,      fd, SOL_SOCKET,  SO_PRIORITY)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_tos,           fd, IPPROTO_IP,  IP_TOS)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_user_timeout,  fd, IPPROTO_TCP, TCP_USER_TIMEOUT)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_cork,          fd, IPPROTO_TCP, TCP_CORK)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_quickack,      fd, IPPROTO_TCP, TCP_QUICKACK)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_rcvlowat,      fd, SOL_SOCKET,  SO_RCVLOWAT)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_sndlowat,      fd, SOL_SOCKET,  SO_SNDLOWAT)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepidle,      fd, IPPROTO_TCP, TCP_KEEPIDLE)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepintvl,     fd, IPPROTO_TCP, TCP_KEEPINTVL)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepcnt,       fd, IPPROTO_TCP, TCP_KEEPCNT)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_multicast_ttl,
            [fd](int v) {
              unsigned char ttl = (unsigned char)v;
              return setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
            })
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_multicast_loop, fd, IPPROTO_IP, IP_MULTICAST_LOOP)
        && !arterial::set_tuple_sockopt<std::tuple<bool, int>>(env, tuple_elements, am_linger,
            [fd](const std::tuple<bool, int>& tup) {
              struct linger l = {.l_onoff = std::get<0>(tup) ? 1 : 0, .l_linger = std::get<1>(tup) /* seconds */};
              return setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
            })
        // Handle {multicast_if, {A, B, C, D}} format for interface address
        && !set_tuple_sockopt<std::tuple<unsigned int, unsigned int, unsigned int, unsigned int>>
            (env, tuple_elements, am_multicast_if,
            [fd](const auto& arg) {
              auto& [o0, o1, o2, o3] = arg;
              struct in_addr interface_addr;
              interface_addr.s_addr = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);
              return setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &interface_addr, sizeof(interface_addr));
            })
        // Handle {add_membership, {{A,B,C,D}, {E,F,G,H}}} format
        && !set_addr_sockopt(env, tuple_elements, am_multicast_if, fd, IPPROTO_IP, IP_ADD_MEMBERSHIP)
        // Handle {drop_membership, {{A,B,C,D}, {E,F,G,H}}} format
        && !set_addr_sockopt(env, tuple_elements, am_multicast_if, fd, IPPROTO_IP, IP_DROP_MEMBERSHIP)
      )
        return false;
    } else {
      return false;
    }
  }

  return true;
}

} // namespace arterial