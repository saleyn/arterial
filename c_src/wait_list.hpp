//-----------------------------------------------------------------------------
/// \file   wait_list.hpp
/// \author Serge Aleynikov
//-----------------------------------------------------------------------------
/// \brief A bounded, lock-free, multi-producer/multi-consumer queue used to
/// hold asynchronous checkout requests that arrived while every connection
/// in the pool was busy (throttled or backlog-full).
///
/// This is deliberately NOT built on top of pool_fifo.hpp/pool_lifo.hpp: that
/// FIFO never recycles its nodes back to a free-list while the structure is
/// live (a Connection, once registered, stays in the FIFO forever), so its
/// lagging-tail design is safe there. A wait-list needs the opposite pattern
/// -- claim a slot, use it briefly, return it for reuse, repeatedly -- which
/// corrupts pool_fifo's shared next-pointer field under concurrent reuse (a
/// node can be recycled into the free-list while an in-flight CheckIn on
/// another thread still dereferences it as the FIFO's lagging tail). Rather
/// than retrofit hazard-pointer/epoch-based reclamation onto pool_fifo.hpp,
/// this uses a classic bounded MPMC ring buffer (Vyukov-style): slots are
/// addressed by position modulo capacity, and a per-slot sequence number
/// (not a free-list) disambiguates readiness, so no node is ever shared
/// between two different "roles" the way pool_fifo's chained nodes are.
//-----------------------------------------------------------------------------
#pragma once

#include <erl_nif.h>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace arterial {

//-----------------------------------------------------------------------------
/// @brief Bounded MPMC ring buffer. `capacity` must be a power of 2.
//-----------------------------------------------------------------------------
template <typename T>
struct MpmcRing {
  explicit MpmcRing(size_t capacity)
  : m_mask(capacity - 1)
  , m_slots(capacity)
  {
    for (size_t i = 0; i < capacity; ++i)
      m_slots[i].seq.store(i, std::memory_order_relaxed);
    m_enqueue_pos.store(0, std::memory_order_relaxed);
    m_dequeue_pos.store(0, std::memory_order_relaxed);
  }

  size_t Capacity() const { return m_slots.size(); }

  /// @brief Try to push `value`. @return false if the ring is full.
  bool TryPush(T value)
  {
    Slot* slot;
    auto pos = m_enqueue_pos.load(std::memory_order_relaxed);
    for (;;) {
      slot = &m_slots[pos & m_mask];
      auto seq  = slot->seq.load(std::memory_order_acquire);
      auto diff = intptr_t(seq) - intptr_t(pos);
      if (diff == 0) {
        if (m_enqueue_pos.compare_exchange_weak(pos, pos+1, std::memory_order_relaxed))
          break;
      } else if (diff < 0) {
        return false; // full
      } else {
        pos = m_enqueue_pos.load(std::memory_order_relaxed);
      }
    }
    slot->value = std::move(value);
    slot->seq.store(pos+1, std::memory_order_release);
    return true;
  }

  /// @brief Try to pop the oldest pushed value. @return false if empty.
  bool TryPop(T& out)
  {
    Slot* slot;
    auto pos = m_dequeue_pos.load(std::memory_order_relaxed);
    for (;;) {
      slot = &m_slots[pos & m_mask];
      auto seq  = slot->seq.load(std::memory_order_acquire);
      auto diff = intptr_t(seq) - intptr_t(pos+1);
      if (diff == 0) {
        if (m_dequeue_pos.compare_exchange_weak(pos, pos+1, std::memory_order_relaxed))
          break;
      } else if (diff < 0) {
        return false; // empty
      } else {
        pos = m_dequeue_pos.load(std::memory_order_relaxed);
      }
    }
    out = std::move(slot->value);
    slot->seq.store(pos + m_mask + 1, std::memory_order_release);
    return true;
  }

private:
  struct Slot {
    std::atomic<size_t> seq;
    T                   value;
  };

  size_t                            m_mask;
  std::vector<Slot>                 m_slots;
  alignas(64) std::atomic<size_t>   m_enqueue_pos;
  alignas(64) std::atomic<size_t>   m_dequeue_pos;
};

//-----------------------------------------------------------------------------
/// @brief One asynchronous checkout request waiting for pool capacity.
/// `waiter_id` is an internal id (scoped only to the wait-list/waiter TTL
/// registry) used to track this entry's deadline before it has been
/// assigned a connection (and therefore a real wire-level ext_req_id).
/// `expire_at` is the absolute deadline (microseconds) covering the whole
/// call -- both the time spent queued and, once a connection is assigned,
/// the time spent in-flight -- so it's reused unchanged for the in-flight
/// registry once this waiter is serviced.
//-----------------------------------------------------------------------------
struct WaitEntry {
  uint64_t  waiter_id = 0;
  ErlNifPid pid{};
  uint32_t  samples   = 0;
  uint64_t  expire_at = 0;
};

//-----------------------------------------------------------------------------
/// @brief Fixed-capacity FIFO of pending checkout requests. `capacity == 0`
/// disables the feature entirely (TryPush always fails, as if the ring were
/// permanently full) at effectively zero cost.
//-----------------------------------------------------------------------------
struct WaitList {
  explicit WaitList(size_t capacity)
  : m_ring(capacity ? RoundUpPow2(capacity) : 1)
  , m_enabled(capacity > 0)
  {}

  bool   Enabled()  const { return m_enabled; }
  size_t Capacity() const { return m_ring.Capacity(); }

  bool TryEnqueue(WaitEntry entry)
  {
    return m_enabled && m_ring.TryPush(std::move(entry));
  }

  bool TryDequeue(WaitEntry& out)
  {
    return m_enabled && m_ring.TryPop(out);
  }

private:
  static size_t RoundUpPow2(size_t n)
  {
    size_t p = 1;
    while (p < n) p <<= 1;
    return p;
  }

  MpmcRing<WaitEntry> m_ring;
  bool                m_enabled;
};

} // namespace arterial
