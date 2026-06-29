#pragma once

#include <cstdint>
#include <cstddef>
#include "enif.hpp"

namespace arterial {

//=============================================================================
// Core Types and Enumerations
//=============================================================================

enum SlotStatus : uint32_t {
  SLOT_EMPTY             = 0,
  SLOT_AVAILABLE         = 1,
  SLOT_LEASED            = 2,
  SLOT_WRITE_POLLING     = 3,
  SLOT_CONNECTING        = 4,
  SLOT_SSL_HANDSHAKE     = 5,
  SLOT_BUSY              = 6,
  // FIFO Mode 3 statuses
  SLOT_FIFO_RESERVED     = 100,  // Mode 3: Connection reserved for single request
  SLOT_FIFO_REQUEST_SENT = 101,  // Mode 3: Request sent, awaiting reply
  SLOT_FIFO_DRAINING     = 102   // Mode 3: Processing reply, about to release
};

enum ProtocolType : uint32_t {
  PROTO_TCP,
  PROTO_UDP,
  PROTO_SSL
};

//=============================================================================
// Utility Templates
//=============================================================================

// Simple optional implementation for C++14 compatibility
template<typename T>
struct simple_optional {
  simple_optional() = default;
  simple_optional(const T& value) : m_has_value(true) {
    new(m_storage) T(value);
  }
  ~simple_optional() {
    if (m_has_value)
      reinterpret_cast<T*>(m_storage)->~T();
  }

  bool     has_value()      const { return m_has_value; }
  explicit operator bool()  const { return m_has_value; }
  T&       operator*()            { return *reinterpret_cast<T*>(m_storage); }
  const T& operator*()      const { return *reinterpret_cast<const T*>(m_storage); }
private:
  bool            m_has_value = false;
  alignas(T) char m_storage[sizeof(T)];
};

//=============================================================================
// Constants
//=============================================================================

// Maximum connections per stripe (must be power of 2 for efficient bit operations)
static constexpr std::size_t MAX_SLOTS_PER_STRIPE = 64;

// FIFO queue constants
static constexpr std::size_t FIFO_QUEUE_SIZE = 64;
static constexpr std::size_t FIFO_QUEUE_MASK = FIFO_QUEUE_SIZE - 1;

//=============================================================================
// Pool Utilization Stats
//=============================================================================

// Calculate pool utilization statistics
struct PoolUtilization {
  std::size_t total_slots;
  std::size_t available_slots;
  std::size_t busy_slots;
  double      utilization_percent;
};

//=============================================================================
// Bit-level functions
//=============================================================================

// Compatibility function for counting trailing zeros (needed by other headers)
inline int count_trailing_zeros(uint64_t value) {
#if (__cplusplus >= 202002L) || (defined(_MSVC_LANG) && _MSVC_LANG >= 202002L)
  return std::countr_zero(value);
#elif defined(__has_builtin) && __has_builtin(__builtin_ctzll)
  return value ? __builtin_ctzll(value) : 64;
#else
  #warning "No support for GCC __builtin_ctzll"
  // Fallback implementation
  if (value == 0) return 64;
  int count = 0;
  while ((value & 1) == 0) {
    value >>= 1;
    count++;
  }
  return count;
#endif
}

// Count one bits in an integer (needed by other headers)
inline size_t count_one_bits(uint64_t value) {
#if (__cplusplus >= 202002L) || (defined(_MSVC_LANG) && _MSVC_LANG >= 202002L)
  return std::popcount(value);
#elif defined(__has_builtin) && __has_builtin(_mm_popcnt_u64)
  return _mm_popcnt_u64(value);
#elif defined(__has_builtin) && __has_builtin(__builtin_popcountll)
  return __builtin_popcountll(value);
#else
  #error "No support for GCC __builtin_popcountll"
#endif
}

} // namespace arterial