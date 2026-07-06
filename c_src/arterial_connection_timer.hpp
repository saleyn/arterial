#pragma once

#include "enif.hpp"
#include "arterial_types.hpp"
#include <memory>

// Platform-specific includes
#ifdef __linux__
#include <sys/timerfd.h>
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#include <sys/event.h>
#include <sys/types.h>
#include <sys/time.h>
#endif

#include <unistd.h>
#include <fcntl.h>

// Generic fallback headers (for platforms without timerfd/kqueue)
#if !defined(__linux__) && !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__OpenBSD__) && !defined(__NetBSD__)
#include <thread>
#include <chrono>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <queue>
#include <vector>
#endif

namespace arterial {

// Forward declarations
struct Connection;

//=============================================================================
// Simple Per-Connection Timeout FD Creation (Low-level API)
//=============================================================================

/// @brief Create a timeout file descriptor that becomes ready after timeout_ms
/// @param timeout_ms Timeout in milliseconds
/// @return File descriptor to register with enif_select, or -1 on error
///
/// Usage:
///   1. int timeout_fd = create_connection_timeout_fd(5000);  // 5 second timeout
///   2. Register timeout_fd with enif_select
///   3. When timeout_fd becomes ready → connection timed out
///   4. When connection succeeds → call close_connection_timeout_fd(timeout_fd)
inline int create_connection_timeout_fd(uint64_t timeout_ms);

/// @brief Close and cleanup a timeout fd created by create_connection_timeout_fd
/// @param timeout_fd File descriptor returned by create_connection_timeout_fd
inline void close_connection_timeout_fd(int timeout_fd);

//=============================================================================
// RAII Connection Timeout Management (High-level C++ API)
//=============================================================================

/// @brief RAII connection timeout using platform-optimal timers
///
/// This class provides strong RAII guarantees:
/// - Automatic cleanup on destruction
/// - Move-only semantics (no copying)
/// - Exception-safe setup and cleanup
/// - Clear ownership model
///
/// Usage:
///   auto timeout = ConnectionTimeout::create(5000);  // 5 second timeout
///   if (timeout) {
///     int fd = timeout->get_fd();  // Register fd with enif_select
///     // timeout automatically cleaned up on scope exit
///   }
///
class ConnectionTimeout {
public:
  /// @brief Factory method to create timeout with RAII guarantees
  /// @param timeout_ms Timeout in milliseconds
  /// @return unique_ptr to ConnectionTimeout, nullptr on failure
  static std::unique_ptr<ConnectionTimeout> create(uint64_t timeout_ms) {
    int timeout_fd = create_connection_timeout_fd(timeout_ms);
    if (timeout_fd < 0) {
      return nullptr;  // Failed to create timeout
    }

    // Use private constructor - can't fail after this point
    return std::unique_ptr<ConnectionTimeout>(new ConnectionTimeout(timeout_fd));
  }

  /// @brief Destructor - automatically cleans up timeout resources
  ~ConnectionTimeout() noexcept {
    cleanup();
  }

  /// @brief Move constructor - transfer ownership
  ConnectionTimeout(ConnectionTimeout&& other) noexcept
    : m_timeout_fd(other.m_timeout_fd) {
    other.m_timeout_fd = -1;  // Transfer ownership
  }

  /// @brief Move assignment - transfer ownership with cleanup
  ConnectionTimeout& operator=(ConnectionTimeout&& other) noexcept {
    if (this != &other) {
      cleanup();  // Clean up current resource
      m_timeout_fd = other.m_timeout_fd;
      other.m_timeout_fd = -1;  // Transfer ownership
    }
    return *this;
  }

  /// @brief Check if timeout is currently active
  bool is_active() const noexcept { return m_timeout_fd >= 0; }

  /// @brief Get the timeout file descriptor (for enif_select)
  /// @return File descriptor, or -1 if inactive
  int get_fd() const noexcept { return m_timeout_fd; }

  /// @brief Explicitly cancel timeout (optional - destructor will do this)
  void cancel() noexcept { cleanup(); }

  /// @brief Release ownership of the timeout fd (advanced usage)
  /// @return The file descriptor (caller takes ownership)
  /// @warning Caller must call close_connection_timeout_fd() on the returned fd
  int release() noexcept {
    int fd = m_timeout_fd;
    m_timeout_fd = -1;  // Release ownership
    return fd;
  }

private:
  /// @brief Private constructor - use create() factory method instead
  explicit ConnectionTimeout(int timeout_fd) noexcept : m_timeout_fd(timeout_fd) {}

  /// @brief Internal cleanup method
  void cleanup() noexcept {
    if (m_timeout_fd >= 0) {
      close_connection_timeout_fd(m_timeout_fd);
      m_timeout_fd = -1;
    }
  }

  int m_timeout_fd;  // Platform-specific timeout file descriptor (-1 = inactive)

  // Non-copyable (move-only)
  ConnectionTimeout(const ConnectionTimeout&) = delete;
  ConnectionTimeout& operator=(const ConnectionTimeout&) = delete;
};

//=============================================================================
// RAII Timeout Guard for Automatic Scope-Based Management
//=============================================================================

/// @brief RAII guard for automatic timeout management within a scope
///
/// Usage:
///   {
///     TimeoutGuard guard(conn, 5000);  // Setup timeout
///     // ... connection attempt ...
///     if (connection_succeeds) {
///       guard.dismiss();  // Don't treat timeout as failure
///     }
///     // guard automatically handles cleanup on scope exit
///   }
///
class TimeoutGuard {
public:
  /// @brief Constructor - automatically sets up timeout
  /// @param conn Connection to manage timeout for
  /// @param timeout_ms Timeout in milliseconds (0 = no timeout)
  /// @param setup_enif_select Whether to register with enif_select automatically
  TimeoutGuard(Connection& conn, uint64_t timeout_ms, bool setup_enif_select = false)
    : m_conn(conn), m_dismissed(timeout_ms == 0) {

    if (timeout_ms > 0) {
      m_timeout_fd = create_connection_timeout_fd(timeout_ms);
      m_active = (m_timeout_fd >= 0);

      if (m_active && setup_enif_select) {
        // TODO: Add enif_select registration if needed
        // This would require ErlNifEnv* and PoolContext* parameters
      }
    }
  }

  /// @brief Destructor - automatically cancels timeout if not dismissed
  ~TimeoutGuard() noexcept {
    if (m_active && !m_dismissed && m_timeout_fd >= 0) {
      close_connection_timeout_fd(m_timeout_fd);
    }
  }

  /// @brief Dismiss the timeout guard (connection succeeded)
  void dismiss() noexcept {
    m_dismissed = true;
  }

  /// @brief Check if timeout is active and not dismissed
  bool is_active() const noexcept { return m_active && !m_dismissed; }

  /// @brief Get the timeout file descriptor
  int get_fd() const noexcept { return m_timeout_fd; }

private:
  Connection& m_conn;
  int m_timeout_fd{-1};
  bool m_active{false};
  bool m_dismissed{false};

  // Non-copyable, non-movable (tied to specific connection)
  TimeoutGuard(const TimeoutGuard&) = delete;
  TimeoutGuard& operator=(const TimeoutGuard&) = delete;
  TimeoutGuard(TimeoutGuard&&) = delete;
  TimeoutGuard& operator=(TimeoutGuard&&) = delete;
};

//=============================================================================
// Integration with Connection struct
//=============================================================================

/// @brief Setup connection timeout and return fd for enif_select
/// @param conn Connection to setup timeout for
/// @param timeout_ms Timeout in milliseconds (0 = no timeout)
/// @return timeout fd for enif_select, or -1 if no timeout or error
inline int setup_connection_timeout_fd(Connection& conn, uint64_t timeout_ms);

/// @brief Cancel connection timeout if active
inline void cancel_connection_timeout(Connection& conn);

} // namespace arterial