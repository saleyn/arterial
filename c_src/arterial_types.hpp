#pragma once

#include <algorithm>
#include <cstring>
#include <type_traits>
#include <cstdint>
#include <cstddef>
#include <utility>  // for std::exchange
#include <unistd.h> // for close()
#include "enif.hpp"
#include "arterial_types.hpp"

namespace arterial {

using namespace nifpp;
using IP4Tuple =
  std::tuple<unsigned int, unsigned int, unsigned int, unsigned int>;

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
  PROTO_UNKNOWN = 0,
  PROTO_TCP     = 1,
  PROTO_UDP     = 2,
  PROTO_SSL     = 3
};

//=============================================================================
// The protocol dispatcher
//=============================================================================
/*
template <typename Visitor>
void visit_protocol(ProtocolType proto, Visitor&& visitor) {
  switch (proto) {
    case PROTO_SSL: 
      return visitor(std::integral_constant<ProtocolType, PROTO_SSL>{}); 
    case PROTO_UDP: 
      return visitor(std::integral_constant<ProtocolType, PROTO_UDP>{}); 
    default:        
      return visitor(std::integral_constant<ProtocolType, PROTO_TCP>{}); 
  }
}

// A tiny helper mapping struct
struct ProtocolMap {
  atom         name;
  ProtocolType type;
};

// The dispatcher loop
template <typename Visitor>
auto visit_protocol_by_name(atom protocol_name, Visitor&& visitor) {
  // 1. Define the supported protocols in a clean data table
  static constexpr ProtocolMap mapping[] = {
    {am_tcp, PROTO_TCP},
    {am_udp, PROTO_UDP},
    #ifdef HAVE_OPENSSL
    {am_ssl, PROTO_SSL},
    #endif
  };

  // 2. Search for a matching string
  for (const auto& entry : mapping) {
    if (protocol_name == entry.name) {
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
*/

//=============================================================================
// RAII File Descriptor Wrapper
//=============================================================================

/// @brief RAII wrapper for POSIX file descriptors
///
/// Automatically closes file descriptors when the object goes out of scope.
/// Supports move semantics and prevents accidental copying.
///
/// @example
/// ```cpp
/// auto timer_fd = FileDescriptor::create([]() {
///   return timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
/// });
/// if (timer_fd) {
///   // Use timer_fd.get() with enif_select
///   // Automatic cleanup when timer_fd goes out of scope
/// }
/// ```
class FileDescriptor {
public:
  /// @brief Default constructor - creates invalid fd
  FileDescriptor() noexcept = default;

  /// @brief Explicit constructor from file descriptor
  /// @param fd File descriptor to manage (must be valid)
  explicit FileDescriptor(int fd) noexcept : m_fd(fd) {}

  /// @brief Destructor - automatically closes the file descriptor
  ~FileDescriptor() noexcept {
    close();
  }

  /// @brief Move constructor
  FileDescriptor(FileDescriptor&& other) noexcept
    : m_fd(std::exchange(other.m_fd, INVALID_FD)) {}

  /// @brief Move assignment operator
  FileDescriptor& operator=(FileDescriptor&& other) noexcept {
    if (this != &other) {
      close();
      m_fd = std::exchange(other.m_fd, INVALID_FD);
    }
    return *this;
  }

  /// @brief Copy operations are deleted to prevent double-close
  FileDescriptor(const FileDescriptor&) = delete;
  FileDescriptor& operator=(const FileDescriptor&) = delete;

  /// @brief Factory method for safe fd creation
  /// @param creator Function that creates the file descriptor
  /// @return FileDescriptor wrapper, or empty wrapper if creation failed
  template<typename Creator>
  static FileDescriptor create(Creator&& creator) {
    int fd = creator();
    return (fd >= 0) ? FileDescriptor(fd) : FileDescriptor();
  }

  /// @brief Get the raw file descriptor
  /// @return File descriptor value, or INVALID_FD if not valid
  int get() const noexcept { return m_fd; }

  explicit operator int() const { return m_fd; }

  /// @brief Check if the file descriptor is valid
  /// @return true if fd >= 0
  bool is_valid() const noexcept { return m_fd >= 0; }

  /// @brief Boolean conversion - true if valid
  explicit operator bool() const noexcept { return is_valid(); }

  /// @brief Release ownership of the file descriptor
  /// @return The file descriptor value (caller becomes responsible for closing)
  int release() noexcept { return std::exchange(m_fd, INVALID_FD); }

  /// @brief Reset to a new file descriptor (closes current fd)
  /// @param fd New file descriptor to manage
  void reset(int fd = INVALID_FD) noexcept { close(); m_fd = fd; }

  /// @brief Manual close (safe to call multiple times)
  void close() noexcept { if (m_fd >= 0) { ::close(m_fd); m_fd = INVALID_FD; } }

private:
  static constexpr int INVALID_FD = -1;
  int m_fd = INVALID_FD;
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

// Forward declaration for the per-connection monitor resource.
// Full definition lives in arterial_pool.hpp after PoolContext is complete.
struct SlotRef;

} // namespace arterial