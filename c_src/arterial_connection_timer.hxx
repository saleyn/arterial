#pragma once

#include "arterial_connection_timer.hpp"

namespace arterial {

//=============================================================================
// Linux timerfd Implementation
//=============================================================================
#ifdef __linux__

inline int create_connection_timeout_fd(uint64_t timeout_ms) {
  // Create timerfd for this specific connection timeout
  int timer_fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC | TFD_NONBLOCK);
  if (timer_fd < 0) {
    return -1;  // Failed to create timerfd
  }

  // Set up the timer to fire once after timeout_ms
  struct itimerspec timer_spec = {};
  timer_spec.it_value.tv_sec = timeout_ms / 1000;
  timer_spec.it_value.tv_nsec = (timeout_ms % 1000) * 1000000;

  if (timerfd_settime(timer_fd, 0, &timer_spec, nullptr) < 0) {
    close(timer_fd);
    return -1;  // Failed to set timer
  }

  // This fd is now ready to be registered with enif_select
  // When it becomes readable, the connection has timed out
  return timer_fd;
}

inline void close_connection_timeout_fd(int timeout_fd) {
  if (timeout_fd >= 0) {
    close(timeout_fd);
  }
}

#endif

//=============================================================================
// macOS/BSD kqueue Implementation
//=============================================================================
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)

inline int create_connection_timeout_fd(uint64_t timeout_ms) {
  // Create kqueue for this specific connection timeout
  int kq = kqueue();
  if (kq < 0) {
    return -1;  // Failed to create kqueue
  }

  // Set up timer event to fire once after timeout_ms
  struct kevent kev;
  EV_SET(&kev, 1, EVFILT_TIMER, EV_ADD | EV_ONESHOT, 0, timeout_ms, nullptr);

  if (kevent(kq, &kev, 1, nullptr, 0, nullptr) < 0) {
    close(kq);
    return -1;  // Failed to set timer
  }

  // This fd is now ready to be registered with enif_select
  // When it becomes readable, the connection has timed out
  return kq;
}

inline void close_connection_timeout_fd(int timeout_fd) {
  if (timeout_fd >= 0) {
    close(timeout_fd);
  }
}

#endif

//=============================================================================
// Generic Fallback Implementation (other platforms)
//=============================================================================
#if !defined(__linux__) && !defined(__APPLE__) && !defined(__FreeBSD__) && !defined(__OpenBSD__) && !defined(__NetBSD__)

// Generic timer entry for single-thread timer management
struct GenericTimerEntry {
  int m_pipe_write_fd;
  int m_pipe_read_fd;  // This is what gets returned for enif_select
  std::chrono::steady_clock::time_point m_expiration;
  std::atomic<bool> m_cancelled{false};

  GenericTimerEntry(uint64_t timeout_ms) {
    int pipefd[2];
    if (pipe(pipefd) < 0) {
      m_pipe_read_fd = m_pipe_write_fd = -1;
      return;
    }

    m_pipe_read_fd = pipefd[0];
    m_pipe_write_fd = pipefd[1];

    // Make read end non-blocking for enif_select, the writing end is blocking
    fcntl(m_pipe_read_fd, F_SETFL, O_NONBLOCK);

    m_expiration = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(timeout_ms);
  }

  ~GenericTimerEntry() {
    if (m_pipe_read_fd >= 0) close(m_pipe_read_fd);
    if (m_pipe_write_fd >= 0) close(m_pipe_write_fd);
  }

  void cancel() {
    m_cancelled.store(true);
  }

  void fire() {
    if (!m_cancelled.load()) {
      // Signal timeout by writing to pipe
      char timeout_signal = 1;
      ssize_t result = write(m_pipe_write_fd, &timeout_signal, 1);
      (void)result;  // Suppress unused warning
    }
  }

  // For priority queue ordering (earliest expiration first)
  bool operator>(const GenericTimerEntry& other) const {
    return m_expiration > other.m_expiration;
  }
};

// Timer entry structure (ported from arterial_timer.hxx)
struct GenericTimerQueueEntry {
  uint64_t expiration_time_ms;  // Absolute expiration time in milliseconds
  uint64_t timer_id;            // Unique timer identifier
  int timeout_pipe_write;       // Write end of timeout pipe (triggers select)

  bool operator<(const GenericTimerQueueEntry& other) const {
    return expiration_time_ms > other.expiration_time_ms; // Min-heap (earliest first)
  }
};

// Single-thread timer manager (ported from GlobalTimerManager in arterial_timer.hxx)
class GenericTimerManager {
public:
  static GenericTimerManager& instance() {
    static GenericTimerManager manager;
    return manager;
  }

  ~GenericTimerManager() {
    shutdown();
  }

  void initialize() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_worker_thread.joinable()) {
      m_shutdown_requested = false;
      m_worker_thread = std::thread([this]() { worker_loop(); });
    }
  }

  void shutdown() {
    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_shutdown_requested = true;
    }
    m_condition.notify_all();

    if (m_worker_thread.joinable())
      m_worker_thread.join();
  }

  int create_timer(uint64_t timeout_ms) {
    auto timer = std::make_unique<GenericTimerEntry>(timeout_ms);
    if  (timer->m_pipe_read_fd < 0)
      return -1;  // Pipe creation failed

    uint64_t timer_id   = get_next_timer_id();
    uint64_t expiration = get_current_time_ms() + timeout_ms;

    GenericTimerQueueEntry queue_entry = {
      .expiration_time_ms = expiration,
      .timer_id = timer_id,
      .timeout_pipe_write = timer->m_pipe_write_fd
    };

    int fd = timer->m_pipe_read_fd;

    {
      std::lock_guard<std::mutex> lock(m_mutex);
      m_timers[fd] = std::move(timer);
      m_timer_queue.push(queue_entry);
      m_active_timers[timer_id] = queue_entry;
    }
    m_condition.notify_one();

    return fd;
  }

  void remove_timer(int fd) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_timers.find(fd);
    if (it != m_timers.end()) {
      it->second->cancel();
      m_timers.erase(it);
    }
  }

private:
  GenericTimerManager() : m_next_timer_id(1) {}

  uint64_t get_next_timer_id() {
    return m_next_timer_id.fetch_add(1);
  }

  uint64_t get_current_time_ms() {
    auto now = std::chrono::steady_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  }

  void worker_loop() {
    while (!m_shutdown_requested.load()) {
      std::unique_lock<std::mutex> lock(m_mutex);

      // Process expired timers (ported from arterial_timer.hxx logic)
      uint64_t now_ms = get_current_time_ms();

      while (!m_timer_queue.empty()) {
        const auto& entry = m_timer_queue.top();

        if (entry.expiration_time_ms > now_ms) {
          break; // No more expired timers
        }

        // Check if timer is still active (not cancelled)
        auto it = m_active_timers.find(entry.timer_id);
        if (it != m_active_timers.end()) {
          // Timer is still active - trigger timeout by writing to pipe
          char timeout_signal = 1;
          ssize_t written = write(entry.timeout_pipe_write, &timeout_signal, 1);
          (void)written; // Suppress unused variable warning

          // Remove from active timers
          m_active_timers.erase(it);
        }

        m_timer_queue.pop();
      }

      // Calculate wait time for next timer (ported from arterial_timer.hxx)
      if (m_timer_queue.empty()) {
        // No timers - wait indefinitely
        m_condition.wait(lock);
      } else {
        // Wait until next timer expires
        uint64_t next_expiration = m_timer_queue.top().expiration_time_ms;
        uint64_t wait_ms = (next_expiration > now_ms) ? (next_expiration - now_ms) : 0;

        auto wait_duration = std::chrono::milliseconds(wait_ms);
        m_condition.wait_for(lock, wait_duration);
      }
    }
  }

  std::thread m_worker_thread;
  std::mutex m_mutex;
  std::condition_variable m_condition;
  std::atomic<bool> m_shutdown_requested{false};
  std::atomic<uint64_t> m_next_timer_id;

  // Timer storage (ported from GlobalTimerManager)
  std::unordered_map<int, std::unique_ptr<GenericTimerEntry>> m_timers;
  std::priority_queue<GenericTimerQueueEntry> m_timer_queue;
  std::unordered_map<uint64_t, GenericTimerQueueEntry> m_active_timers; // For O(1) cancellation
};

// Thread-safe registry wrapper for compatibility
class GenericTimerRegistry {
public:
  static GenericTimerRegistry& instance() {
    static GenericTimerRegistry registry;
    return registry;
  }

  GenericTimerRegistry() {
    GenericTimerManager::instance().initialize();
  }

  int create_timer(uint64_t timeout_ms) {
    return GenericTimerManager::instance().create_timer(timeout_ms);
  }

  void remove_timer(int fd) {
    GenericTimerManager::instance().remove_timer(fd);
  }
};

inline int create_connection_timeout_fd(uint64_t timeout_ms) {
  return GenericTimerRegistry::instance().create_timer(timeout_ms);
}

inline void close_connection_timeout_fd(int timeout_fd) {
  if (timeout_fd >= 0) {
    GenericTimerRegistry::instance().remove_timer(timeout_fd);
  }
}

#endif

//=============================================================================
// RAII Connection Integration Implementation
//=============================================================================

/// @brief Setup connection timeout with RAII guarantees
/// @param conn Connection to setup timeout for
/// @param timeout_ms Timeout in milliseconds (0 = no timeout)
/// @return timeout fd for enif_select, or -1 if no timeout or error
inline int setup_connection_timeout_fd(Connection& conn, uint64_t timeout_ms) {
  if (timeout_ms == 0)
    return -1;  // No timeout requested

  // Create new timeout with RAII factory method
  auto timeout = ConnectionTimeout::create(timeout_ms);
  if (!timeout)
    return -1;  // Failed to create timeout

  int fd = timeout->get_fd();

  // Transfer ownership to connection
  conn.timer = std::move(timeout);

  return fd;
}

/// @brief Cancel connection timeout.
/// @param conn  Connection to cancel timeout for
/// @param env   Unused — kept for call-site compatibility during transition
/// @param ctx   Unused — kept for call-site compatibility during transition
///
/// Closes the timer fd directly without going through enif_select(STOP).
/// Rationale: the timer fd was registered with enif_select_read (one-shot).
/// Calling close() on a Linux fd automatically removes it from epoll, so the
/// OTP poller will never deliver an event for it.  The alternative —
/// enif_select(STOP) + pool_resource_stop — creates an fd-reuse race:
/// pool_resource_stop calls close() synchronously (is_direct_call=1) inside
/// the current NIF call, freeing the fd number before OTP finishes its
/// internal deregistration cleanup.  A concurrent timerfd_create() or
/// socket() call on another scheduler thread may then get the same number and
/// register it with enif_select, triggering a "stealing control" warning and,
/// worse, delivering the new fd's events to the wrong enif_select registration.
inline void cancel_connection_timeout(Connection& conn) {
  // RAII destructor calls close_connection_timeout_fd() which closes the fd.
  // Linux close() atomically removes the fd from all epoll/io_uring interest
  // sets — no enif_select(STOP) needed.
  conn.timer.reset();
}

/// @brief Create RAII timeout guard for automatic scope-based management
/// @param conn Connection to manage timeout for
/// @param timeout_ms Timeout in milliseconds
/// @return TimeoutGuard that automatically manages the timeout
inline TimeoutGuard create_timeout_guard(Connection& conn, uint64_t timeout_ms) {
  return TimeoutGuard(conn, timeout_ms);
}

} // namespace arterial