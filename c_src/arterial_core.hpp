#pragma once

#include "arterial_connection.hpp"
#include "enif.hpp"
#include "throttle.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstring>
#include <ctime>
#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/ioctl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <type_traits>
#include <unistd.h>
#include <vector>

#ifdef HAVE_OPENSSL
#include <openssl/err.h>
#include <openssl/opensslv.h>
#include <openssl/ssl.h>
#endif

namespace arterial {

using namespace nifpp;

// Forward declarations
struct PoolStripe;

// 1. The protocol dispatcher
template <typename Visitor>
void visit_protocol(ProtocolType proto, Visitor&& visitor)
{
  switch (proto) {
    case PROTO_SSL: visitor(std::integral_constant<ProtocolType, PROTO_SSL>{}); break;
    case PROTO_UDP: visitor(std::integral_constant<ProtocolType, PROTO_UDP>{}); break;
    default:        visitor(std::integral_constant<ProtocolType, PROTO_TCP>{}); break;
  }
}

// A tiny helper mapping struct
struct ProtocolMap {
  const char*  name;
  ProtocolType type;
};

// The dispatcher loop
template <typename Visitor>
auto visit_protocol_str(const char* protocol_str, Visitor&& visitor)
{
  // 1. Define the supported protocols in a clean data table
  static constexpr ProtocolMap mapping[] = {
      {"tcp", PROTO_TCP},
      {"udp", PROTO_UDP},
#ifdef HAVE_OPENSSL
      {"ssl", PROTO_SSL},
#endif
  };

  // 2. Search for a matching string
  for (const auto& entry : mapping) {
    if (strcmp(protocol_str, entry.name) == 0) {
      // Found it! Execute the template logic via runtime-to-compile-time switch
      switch (entry.type) {
        case PROTO_TCP: return visitor(std::integral_constant<ProtocolType, PROTO_TCP>{});
        case PROTO_UDP: return visitor(std::integral_constant<ProtocolType, PROTO_UDP>{});
#ifdef HAVE_OPENSSL
        case PROTO_SSL: return visitor(std::integral_constant<ProtocolType, PROTO_SSL>{});
#endif
      }
    }
  }

  // 3. Fallback sentinel object (empty/null variant indicator)
  return visitor(std::integral_constant<ProtocolType, PROTO_UNKNOWN>{});
}

//=============================================================================
// NIF Utility Functions
//=============================================================================

// Resolve {PoolRef, StripeId, SlotId} to a Connection&, or nullptr if any index is out of range.
// This is used by handle_readable/3, handle_writable/3, close_slot/3, etc.
inline Connection* resolve_slot(
    ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[], PoolContext** out_ctx);

// Time spacing throttling check - returns true if the request was allowed
inline bool throttle_allow(PoolContext* ctx, Connection& slot);

// Return true, and parse an integer or boolean term into `value`
inline bool get_int_or_bool_option(ErlNifEnv* env, ERL_NIF_TERM opt, int& value)
{
  if (get(env, opt, value)) return true;
  if (enif_is_identical(opt, am_true)) {
    value = 1;
    return true;
  }
  if (enif_is_identical(opt, am_false)) {
    value = 0;
    return true;
  }
  return false;
}

//===========================================================================
// Socket option helper functions
//===========================================================================

// Helper function to set socket options using string name matching
template <typename SetOptFunc>
inline bool set_sockopt_by_name(
    const char* opt_name, const char* target_name, int opt_value, SetOptFunc set_func)
{
  if (strcmp(opt_name, target_name) == 0)
    return set_func(opt_value) == 0; // setsockopt returns 0 on success
  return false;                      // Name doesn't match - continue to next option
}

} // namespace arterial

// Include implementation details after forward declarations
#include "arterial_pool.hpp"
#include "arterial_socket.hpp"

// Implementation of functions that require complete PoolContext definition
namespace arterial {

// Resolve {PoolRef, StripeId, SlotId} to a Connection&, or nullptr if any index is out of range
inline Connection* resolve_slot(
    ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[], PoolContext** out_ctx)
{
  PoolContext* ctx;
  unsigned int stripe_id, slot_id;

  assert(argc == 3);

  if (!get(env, argv[0], ctx) || !ctx ||
      !get(env, argv[1], stripe_id, (unsigned int)(ctx->stripe_count - 1)) ||
      !ctx->stripes[stripe_id] || !get(env, argv[2], slot_id)) [[unlikely]]
    return nullptr;

  auto& stripe = *ctx->stripes[stripe_id];
  if (slot_id >= stripe.capacity) [[unlikely]]
    return nullptr;

  *out_ctx = ctx;
  return &stripe.slots[slot_id];
}

// Time spacing throttling check - returns true if the request was allowed
inline bool throttle_allow(PoolContext* ctx, Connection& slot)
{
  return ctx->get_throttle_rate_per_sec() == 0          // No throttling configured
      || slot.throttle.add(1, arterial::now_utc()) > 0; // check if we can add one request
}

} // namespace arterial
