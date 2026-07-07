#pragma once

#include "arterial_types.hpp"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>

namespace arterial {

// NIF type handling - use real type when headers are available, placeholder otherwise
#ifdef __ERL_NIF_H__
using NifPid = ErlNifPid; // Use real type when NIF headers are available
#else
using NifPid = void*; // Placeholder for header-only usage
#endif

//===========================================================================
// FIFO Queue Entry Management
//===========================================================================

// Entry in the FIFO reservation queue
struct FifoQueueEntry {
  NifPid            m_requester_pid{};
  uint64_t          m_reservation_id{0};
  std::atomic<bool> m_valid{false};       // Entry is valid and waiting
  uint64_t          m_enqueue_time_us{0}; // For timeout tracking
  uint64_t          m_timeout_ms{0};

  // Copy constructor for safe copying (atomic values copied by load/store)
  FifoQueueEntry(const FifoQueueEntry& other)
      : m_requester_pid(other.m_requester_pid), m_reservation_id(other.m_reservation_id),
        m_valid(other.m_valid.load(std::memory_order_acquire)),
        m_enqueue_time_us(other.m_enqueue_time_us), m_timeout_ms(other.m_timeout_ms)
  {}

  // Assignment operator
  FifoQueueEntry& operator=(const FifoQueueEntry& other)
  {
    if (this != &other) {
      m_requester_pid  = other.m_requester_pid;
      m_reservation_id = other.m_reservation_id;
      m_valid.store(other.m_valid.load(std::memory_order_acquire), std::memory_order_release);
      m_enqueue_time_us = other.m_enqueue_time_us;
      m_timeout_ms      = other.m_timeout_ms;
    }
    return *this;
  }

  // Default constructor
  FifoQueueEntry() = default;

  // Reset entry to empty state
  void reset()
  {
    m_requester_pid  = {};
    m_reservation_id = 0;
    m_valid.store(false, std::memory_order_release);
    m_enqueue_time_us = 0;
    m_timeout_ms      = 0;
  }

  // Check if this entry has timed out
  bool is_timed_out() const
  {
    uint64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          std::chrono::steady_clock::now().time_since_epoch())
                          .count();
    return now_us > m_enqueue_time_us + (m_timeout_ms * 1000);
  }
};

//===========================================================================
// Lock-Free FIFO Reservation Queue
//===========================================================================

// Bounded lock-free circular queue for FIFO reservation requests
// Capacity: 64 entries to match stripe slot capacity
class FifoReservationQueue {
  static constexpr std::size_t           QUEUE_SIZE = FIFO_QUEUE_SIZE;
  static constexpr std::size_t           QUEUE_MASK = FIFO_QUEUE_MASK;

  std::array<FifoQueueEntry, QUEUE_SIZE> m_entries{};
  std::atomic<uint64_t>                  m_head{0}; // Next dequeue pos
  std::atomic<uint64_t>                  m_tail{0}; // Next enqueue pos
  std::atomic<bool>                      m_initialized{false};

  // Atomic counter for generating unique reservation IDs
  static std::atomic<uint64_t>           s_reservation_counter;

public:
  //===========================================================================
  // Queue Management
  //===========================================================================

  // Initialize queue on first use (zero overhead when unused)
  void initialize()
  {
    if (!m_initialized.load(std::memory_order_acquire)) {
      for (auto& entry : m_entries) entry.reset();
      m_head.store(0, std::memory_order_relaxed);
      m_tail.store(0, std::memory_order_relaxed);
      m_initialized.store(true, std::memory_order_release);
    }
  }

  // Check if queue has been initialized
  bool is_initialized() const { return m_initialized.load(std::memory_order_acquire); }

  //===========================================================================
  // Queue Operations
  //===========================================================================

  // Enqueue a reservation request (returns false if queue is full)
  bool enqueue(NifPid pid, uint64_t timeout_ms)
  {
    if (!m_initialized.load(std::memory_order_acquire)) initialize();

    uint64_t current_tail = m_tail.load(std::memory_order_acquire);
    uint64_t next_tail    = current_tail + 1;
    uint64_t current_head = m_head.load(std::memory_order_acquire);

    // Check if queue is full (reserve one slot to distinguish full/empty)
    if ((next_tail & QUEUE_MASK) == (current_head & QUEUE_MASK)) return false; // Queue is full

    // Try to claim the tail slot
    if (!m_tail.compare_exchange_weak(
            current_tail, next_tail, std::memory_order_acq_rel, std::memory_order_acquire)) {
      return false; // Another thread claimed it
    }

    // Fill the entry
    auto& entry             = m_entries[current_tail & QUEUE_MASK];
    entry.m_requester_pid   = pid;
    entry.m_reservation_id  = s_reservation_counter.fetch_add(1, std::memory_order_relaxed);
    entry.m_timeout_ms      = timeout_ms;
    entry.m_enqueue_time_us = std::chrono::duration_cast<std::chrono::microseconds>(
                                  std::chrono::steady_clock::now().time_since_epoch())
                                  .count();

    // Mark entry as valid (this makes it visible to dequeue)
    entry.m_valid.store(true, std::memory_order_release);
    return true;
  }

  // Dequeue next waiting request (returns empty optional if queue is empty)
  simple_optional<FifoQueueEntry> dequeue()
  {
    if (!m_initialized.load(std::memory_order_acquire))
      return simple_optional<FifoQueueEntry>{}; // Queue not initialized = empty

    uint64_t current_head = m_head.load(std::memory_order_acquire);
    uint64_t current_tail = m_tail.load(std::memory_order_acquire);

    // Check if queue is empty
    if ((current_head & QUEUE_MASK) == (current_tail & QUEUE_MASK))
      return simple_optional<FifoQueueEntry>{};

    auto& entry = m_entries[current_head & QUEUE_MASK];

    // Wait for entry to be valid (handles race with enqueue)
    if (!entry.m_valid.load(std::memory_order_acquire)) return simple_optional<FifoQueueEntry>{};

    // Check timeout
    if (entry.is_timed_out()) {
      // Entry has timed out - skip it and advance head
      entry.reset();
      m_head.compare_exchange_weak(current_head, current_head + 1, std::memory_order_acq_rel);
      return simple_optional<FifoQueueEntry>{};
    }

    // Try to claim the head slot
    if (!m_head.compare_exchange_weak(
            current_head, current_head + 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
      return simple_optional<FifoQueueEntry>{}; // Another thread claimed it
    }

    // Copy entry data before resetting
    FifoQueueEntry result = entry;
    entry.reset();
    return simple_optional<FifoQueueEntry>{result};
  }

  //===========================================================================
  // Queue Metrics
  //===========================================================================

  // Get current queue size (approximate - may be stale due to concurrency)
  std::size_t size() const
  {
    if (!m_initialized.load(std::memory_order_acquire)) return 0;
    uint64_t head = m_head.load(std::memory_order_acquire);
    uint64_t tail = m_tail.load(std::memory_order_acquire);
    return (tail - head) & QUEUE_MASK;
  }

  // Check if queue is empty
  bool empty() const { return size() == 0; }

  // Check if queue is full
  bool full() const
  {
    if (!m_initialized.load(std::memory_order_acquire)) return false;
    uint64_t head = m_head.load(std::memory_order_acquire);
    uint64_t tail = m_tail.load(std::memory_order_acquire);
    return ((tail + 1) & QUEUE_MASK) == (head & QUEUE_MASK);
  }

  //===========================================================================
  // Maintenance Operations
  //===========================================================================

  // Clean up timed-out entries (called periodically)
  void cleanup_timeouts()
  {
    if (!m_initialized.load(std::memory_order_acquire)) return;
    // Timeout cleanup happens naturally during dequeue operations
    // This could be enhanced with a background cleanup thread if needed
  }

  // Reset queue to initial state
  void reset()
  {
    for (auto& entry : m_entries) entry.reset();
    m_head.store(0, std::memory_order_relaxed);
    m_tail.store(0, std::memory_order_relaxed);
    m_initialized.store(false, std::memory_order_release);
  }
};

} // namespace arterial