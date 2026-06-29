#pragma once

#include "enif.hpp"

namespace arterial {

// Forward declarations to avoid circular includes
struct PoolContext;
struct Connection;

using namespace nifpp;

//=============================================================================
// Socket Options Management
//=============================================================================

// If the {OptName, OptValue} option matches given name, set it by calling Fun(fd, value)
template <typename Fun>
inline bool set_int_or_bool_sockopt(
  ErlNifEnv* env, const ERL_NIF_TERM* opts,
  ERL_NIF_TERM name, Fun f, ERL_NIF_TERM& err)
{
  if (!enif_is_identical(opts[0], name)) return false;
  int value;
  if (get(env, opts[1], value)) goto SET;
  if (enif_is_identical(opts[1], am_true))  { value = 1; goto SET; }
  if (enif_is_identical(opts[1], am_false)) { value = 0; goto SET; }

  err = name;
  return false;

SET:
  auto res = f(value) == 0;  // setsockopt returns 0 on success
  if (!res) [[unlikely]] err = name;
  return res;
}

// If the {OptName, OptValue} option matches given name, set it
inline bool set_int_or_bool_sockopt(
  ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name, int fd,
  int opt_class, int opt_type, ERL_NIF_TERM& err)
{
  auto f = [=](int v) {
    return setsockopt(fd, opt_class, opt_type, &v, sizeof(v));
  };
  return set_int_or_bool_sockopt(env, opts, name, f, err);
}

// If the {OptName, OptValue} option matches given name, set it
inline bool set_atom_sockopt(
  const ERL_NIF_TERM opt, ERL_NIF_TERM name,
  int fd, int opt_class, int opt_type, ERL_NIF_TERM& err)
{
  if (!enif_is_identical(opt, name)) return false;
  static constexpr int v = 1;
  auto res = setsockopt(fd, opt_class, opt_type, &v, sizeof(v)) == 0;
  if (!res) [[unlikely]] err = name;
  return res;
}

template <typename Tuple, typename Fun>
inline bool set_tuple_sockopt(
  ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name,
  Fun f, ERL_NIF_TERM& err)
{
  if (!enif_is_identical(opts[0], name)) return false;
  Tuple tup;
  if (!get(env, opts[1], tup)) [[unlikely]] { err = name; return false; }
  auto res = f(tup) == 0;
  if (!res) [[unlikely]] err = name;
  return res;
}

// If the {OptName, OptValue :: {{A,B,C,D}, {E,F,G,H}}} option matches given name, set it
inline bool set_addr_sockopt(
  ErlNifEnv* env, const ERL_NIF_TERM* opts, ERL_NIF_TERM name,
  int fd, int opt_class, int opt_type, ERL_NIF_TERM& err)
{
  return set_tuple_sockopt<std::tuple<ERL_NIF_TERM, ERL_NIF_TERM>>(
    env, opts, name,
    [=](auto& tup) {
      auto& maddr = std::get<0>(tup);
      auto& iaddr = std::get<1>(tup);
      using FourOctetsT = std::tuple<unsigned int, unsigned int, unsigned int, unsigned int>;
      FourOctetsT mcast_addr, if_addr;
      if (!get(env, maddr, mcast_addr) || !get(env, iaddr, if_addr)) {
        return false;
      }
      struct ip_mreq mreq;
      auto [m0, m1, m2, m3] = mcast_addr;
      auto [i0, i1, i2, i3] = if_addr;
      mreq.imr_multiaddr.s_addr = htonl((m0 << 24) | (m1 << 16) | (m2 << 8) | m3);
      mreq.imr_interface.s_addr = htonl((i0 << 24) | (i1 << 16) | (i2 << 8) | i3);
      return setsockopt(fd, opt_class, opt_type, &mreq, sizeof(mreq)) == 0;
    }, err);
}

//===========================================================================
// Socket Options Application
//===========================================================================

/**
 * @brief Applies a list of socket options to a file descriptor.
 *
 * This function processes an Erlang list of socket options and applies them to the
 * specified file descriptor. It supports both atom-based options (e.g., 'keepalive')
 * and tuple-based options (e.g., {sndbuf, 8192}) with comprehensive error handling.
 *
 * @param fd The file descriptor to configure
 * @param env The NIF environment for Erlang term operations
 * @param options_list Erlang list of socket options to apply
 *
 * @return true if all options were applied successfully, false on any failure
 *
 * ## Supported Option Formats
 *
 * ### Atom-based Options (boolean flags):
 * - `keepalive` - Enable TCP keepalive (SO_KEEPALIVE)
 * - `nodelay` - Disable Nagle algorithm (TCP_NODELAY)
 * - `reuseaddr` - Allow address reuse (SO_REUSEADDR)
 *
 * ### Tuple-based Options {Option, Value}:
 *
 * #### Buffer Management:
 * - `{sndbuf, Size}` - Set send buffer size (SO_SNDBUF)
 * - `{rcvbuf, Size}` - Set receive buffer size (SO_RCVBUF)
 * - `{rcvlowat, Size}` - Set receive low-water mark (SO_RCVLOWAT)
 * - `{sndlowat, Size}` - Set send low-water mark (SO_SNDLOWAT)
 *
 * #### TCP-specific:
 * - `{keepidle, Seconds}` - Time before keepalive probes (TCP_KEEPIDLE)
 * - `{keepintvl, Seconds}` - Interval between keepalive probes (TCP_KEEPINTVL)
 * - `{keepcnt, Count}` - Number of keepalive probes (TCP_KEEPCNT)
 * - `{user_timeout, Ms}` - TCP user timeout (TCP_USER_TIMEOUT)
 * - `{cork, Boolean}` - TCP cork option (TCP_CORK)
 * - `{quickack, Boolean}` - TCP quick ACK (TCP_QUICKACK)
 *
 * #### Quality of Service:
 * - `{priority, Level}` - Socket priority (SO_PRIORITY)
 * - `{tos, Value}` - Type of Service field (IP_TOS)
 *
 * #### Connection Lifecycle:
 * - `{linger, Seconds}` - Linger timeout on close (SO_LINGER)
 * - `{linger, {Boolean, Seconds}}` - Linger with enable flag
 *
 * #### Multicast Options:
 * - `{multicast_ttl, TTL}` - Multicast time-to-live (IP_MULTICAST_TTL)
 * - `{multicast_loop, Boolean}` - Multicast loopback (IP_MULTICAST_LOOP)
 * - `{multicast_if, {A,B,C,D}}` - Multicast interface address (IP_MULTICAST_IF)
 * - `{add_membership, {{A,B,C,D}, {E,F,G,H}}}` - Join multicast group
 * - `{drop_membership, {{A,B,C,D}, {E,F,G,H}}}` - Leave multicast group
 *
 * ## Examples
 *
 * ```erlang
 * % Atom-based options
 * Options1 = [keepalive, nodelay, reuseaddr],
 *
 * % Mixed atom and tuple options
 * Options2 = [
 *   nodelay,
 *   {sndbuf, 65536},
 *   {rcvbuf, 65536},
 *   {keepidle, 7200}
 * ],
 *
 * % Advanced multicast configuration
 * Options3 = [
 *   {multicast_ttl, 1},
 *   {multicast_if, {192, 168, 1, 100}},
 *   {add_membership, {{224, 0, 0, 1}, {192, 168, 1, 100}}}
 * ].
 * ```
 *
 * ## Error Handling
 *
 * The function uses short-circuit evaluation with chained boolean operators.
 * If any option fails to apply (setsockopt returns error), the entire function
 * returns false immediately. This ensures atomic application - either all
 * options succeed or none are applied.
 *
 * ## Implementation Notes
 *
 * - Uses helper functions like `set_atom_sockopt()` and `set_int_or_bool_sockopt()`
 *   for type-safe option parsing and application
 * - Supports lambda functions for complex options like linger and multicast TTL
 * - Handles both IPv4 addresses as 4-tuples and structured data types
 * - Empty option list is treated as success (no-op)
 *
 * ## Platform Compatibility
 *
 * Some socket options may not be available on all platforms:
 * - TCP_USER_TIMEOUT: Linux-specific
 * - TCP_CORK: Linux-specific
 * - TCP_QUICKACK: Linux-specific
 * - Multicast options: May vary by OS and network stack
 *
 * @see arterial::set_atom_sockopt() for atom option handling
 * @see arterial::set_int_or_bool_sockopt() for tuple option handling
 * @see arterial::set_tuple_sockopt() for complex tuple options
 * @see setsockopt(2) for underlying socket option semantics
 */
bool apply_sock_opts(int fd, ErlNifEnv* env, ERL_NIF_TERM options_list, ERL_NIF_TERM& err) {
  err = 0;

  if (enif_is_empty_list(env, options_list))
    return true; // No options to apply

  ERL_NIF_TERM head, tail = options_list;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    // Parse each option - can be atom or tuple-based options {Option, Value}
    const ERL_NIF_TERM* tuple_elements;
    int                 tuple_arity;

    if (enif_is_atom(env, head)) {
      // Handle common atom-based options
      if  (!arterial::set_atom_sockopt(head, am_keepalive, fd, SOL_SOCKET,  SO_KEEPALIVE, err)
        && !arterial::set_atom_sockopt(head, am_nodelay,   fd, IPPROTO_TCP, TCP_NODELAY, err)
        && !arterial::set_atom_sockopt(head, am_reuseaddr, fd, SOL_SOCKET,  SO_REUSEADDR, err)
      )
        return false;
    } else if (enif_get_tuple(env, head, &tuple_arity, &tuple_elements) && tuple_arity == 2 &&
               enif_is_atom(env, tuple_elements[0])) {
      // Handle tuple-based options {Option, Value}
      if  (!arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepalive, fd, SOL_SOCKET,  SO_KEEPALIVE, err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_nodelay,   fd, IPPROTO_TCP, TCP_NODELAY, err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_linger,
            [fd](int v) {
              struct linger l = {.l_onoff = 1, .l_linger = v /* seconds */};
              return setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
            }, err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_reuseaddr,     fd, SOL_SOCKET,  SO_REUSEADDR,     err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_sndbuf,        fd, SOL_SOCKET,  SO_SNDBUF,        err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_rcvbuf,        fd, SOL_SOCKET,  SO_RCVBUF,        err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_priority,      fd, SOL_SOCKET,  SO_PRIORITY,      err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_tos,           fd, IPPROTO_IP,  IP_TOS,           err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_user_timeout,  fd, IPPROTO_TCP, TCP_USER_TIMEOUT, err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_cork,          fd, IPPROTO_TCP, TCP_CORK,         err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_quickack,      fd, IPPROTO_TCP, TCP_QUICKACK,     err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_rcvlowat,      fd, SOL_SOCKET,  SO_RCVLOWAT,      err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_sndlowat,      fd, SOL_SOCKET,  SO_SNDLOWAT,      err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepidle,      fd, IPPROTO_TCP, TCP_KEEPIDLE,     err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepintvl,     fd, IPPROTO_TCP, TCP_KEEPINTVL,    err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_keepcnt,       fd, IPPROTO_TCP, TCP_KEEPCNT,      err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_multicast_ttl,
            [fd](int v) {
              unsigned char ttl = (unsigned char)v;
              return setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
            }, err)
        && !arterial::set_int_or_bool_sockopt(env, tuple_elements, am_multicast_loop, fd, IPPROTO_IP, IP_MULTICAST_LOOP, err)
        && !arterial::set_tuple_sockopt<std::tuple<bool, int>>(env, tuple_elements, am_linger,
            [fd](const std::tuple<bool, int>& tup) {
              struct linger l = {.l_onoff = std::get<0>(tup) ? 1 : 0, .l_linger = std::get<1>(tup) /* seconds */};
              return setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l));
            }, err)
        // Handle {multicast_if, {A, B, C, D}} format for interface address
        && !set_tuple_sockopt<std::tuple<unsigned int, unsigned int, unsigned int, unsigned int>>
            (env, tuple_elements, am_multicast_if,
            [fd](const auto& arg) {
              auto& [o0, o1, o2, o3] = arg;
              struct in_addr interface_addr;
              interface_addr.s_addr = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);
              return setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &interface_addr, sizeof(interface_addr));
            }, err)
        // Handle {add_membership, {{A,B,C,D}, {E,F,G,H}}} format
        && !set_addr_sockopt(env, tuple_elements, am_multicast_if, fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, err)
        // Handle {drop_membership, {{A,B,C,D}, {E,F,G,H}}} format
        && !set_addr_sockopt(env, tuple_elements, am_multicast_if, fd, IPPROTO_IP, IP_DROP_MEMBERSHIP, err)
      )
        return false;
    } else {
      return false;
    }
  }

  return true;
}

} // namespace arterial