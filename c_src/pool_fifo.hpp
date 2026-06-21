//-----------------------------------------------------------------------------
/// \brief Lock-free FIFO object pool
/// \author Serge Aleynikov
///
/// Implementation of an object pool that owns a fixed set of objects and
/// allows to check them out/in concurrently.  The objects are lined up in the
/// FIFO order.
///
/// Objects can be made available/unavailable. Marking an object unavailable
/// will cause a CheckOut operation to skip it.  The unavailable object will
/// never be used until it's explicitly made available, which would perform
/// a CheckIn of the object to the end of the pool.
///
/// When an object is made unavailable, this is done lazily. What this implies
/// is that if the next call to CheckOut() detects that an object is
/// unavailable, it will be removed from the pool of available objects.
///
/// Internally this is a Vyukov-style bounded MPMC ring buffer (see
/// https://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue),
/// not a Michael & Scott linked-list queue: the set of objects in this pool
/// is always bounded (capacity == pool_size, fixed at construction), so a
/// fixed-size array of slots with per-slot sequence numbers avoids pointer
/// chasing, ABA hazards, and "is the queue empty" edge cases entirely --
/// there's no dummy node, no linked list, and no node ever needs its `next`
/// pointer mutated by anyone but its own current owner. Each ring slot has
/// an atomic `sequence` counter; producers/consumers claim a slot by
/// CAS-incrementing a ticket counter (`m_enqueue_pos`/`m_dequeue_pos`) and
/// then spin briefly until that slot's `sequence` confirms it's their turn
/// -- the same technique used by folly::ProducerConsumerQueue and
/// moodycamel::ConcurrentQueue's underlying block-free design. An earlier
/// hand-rolled Michael & Scott adaptation (kept in git history) had at
/// least three distinct concurrency bugs found via stress testing
/// (CheckIn() corrupting m_tail, a stale-tail empty-queue race, a stale
/// `.next` pointer causing a self-loop); this design has no equivalent
/// surface for those bugs to live in.
//-----------------------------------------------------------------------------
// Created: 2010-02-03
//-----------------------------------------------------------------------------
#pragma once

#include "pool_lifo.hpp"
#include "arterial_util.hpp"
#include <thread>

namespace arterial {

struct PooledNodeTraitsFIFO {
  using NextT = std::atomic<NodeIndex>;

  struct NextNode {
    explicit NextNode(int idx = -1) { SetNext(NodeIndex(idx, 0)); }
    NextNode(NextNode&& rhs)        { SetNext(rhs.m_next);        }

    void SetNext(NodeIndex next) { m_next.store(next, std::memory_order_release); }

    NodeIndex  Value() const { return m_next.load(std::memory_order_acquire); }
    NextT&     Ref()         { return m_next; }

    int        Index() const { return Value().Index(); }
  private:
    NextT      m_next;
  };
};

template <typename T, typename Traits = PooledNodeTraitsFIFO>
using BasePoolNodeFIFO = BasePoolNode<T, Traits>;

//-----------------------------------------------------------------------------
/// @brief Object pool with FIFO ordering and CheckIn/CheckOut semantics
//-----------------------------------------------------------------------------
template<DerivedFromPooledObject NodeT>
struct BaseObjectPoolFIFO : public BaseObjectPoolLIFO<NodeT> {
  using BaseT     = BaseObjectPoolLIFO<NodeT>;
  using T         = typename BaseT::T;
  using ObjT      = typename BaseT::ObjT;
  using TraitsT   = typename NodeT::TraitsT;
  using NextNodeT = typename TraitsT::NextT;

  static_assert(std::is_same_v<std::atomic<NodeIndex>, typename NodeT::TraitsT::NextT>);

  explicit BaseObjectPoolFIFO(size_t size) : BaseT(size) { Init(size); }

  template<typename InitFun>
  BaseObjectPoolFIFO(size_t size, InitFun const& init) : BaseT(size, init) { Init(size); }

  template <typename U> requires IsObjOrUniqPtr<U, T>
  explicit BaseObjectPoolFIFO(std::vector<U>& objects) : BaseT(objects) { Init(objects.size()); }

  ~BaseObjectPoolFIFO() {}

  /// @brief Put the node to the end of the pool of available nodes.
  ///
  /// This node must be one of the m_nodes owned by the pool.
  void CheckIn(const ObjT& nd);

  /// @brief Remove the next available node from the head of the FIFO queue
  ObjT* CheckOut();

  /// @brief Make the node available for use
  bool MakeAvailable(T* obj);

  /// @brief Make the node available for use
  bool MakeAvailable(NodeT& node);

  /// @brief Make the node unavailable for use
  bool MakeUnavailable(T* obj);

  /// @brief Make the node unavailable for use
  ///
  /// The next CheckOut() call that encounters this node will remove it from
  /// the pool.
  ///
  /// @return true if the node was already unavailable
  bool MakeUnavailable(NodeT& node) { return node.Available(false); }


  /// Returns true if the queue is empty.
  bool Empty() const;

  /// This method does not return an accurate count in multi-threaded use.
  /// Intenced for debugging only!
  /// @return size of the queue.
  size_t UnsafeSize() const;

  /// @brief Call given lambda for each node currently sitting in the ring
  /// buffer (i.e. checked-in/available, not currently checked out).
  /// This method is not safe to use concurrently with CheckOut()/CheckIn()
  /// -- intended for debugging only, like UnsafeSize().
  template <typename Fun>
  void ForEach(Fun const& fun);

private:
  /// @brief One ring buffer cell: the pool node index it currently holds,
  /// guarded by a sequence number that encodes the cell's state machine
  /// (see Vyukov's bounded MPMC queue write-up). A cell starts at
  /// `sequence == cell_index` (empty, ready for the first enqueue at that
  /// position); after a successful enqueue at ticket `pos`, `sequence`
  /// becomes `pos + 1` (full, ready for the dequeuer expecting that
  /// ticket); after the matching dequeue, `sequence` becomes
  /// `pos + capacity` (empty again, ready for the *next* enqueue that
  /// wraps around to this cell).
  struct Cell {
    std::atomic<uint64_t>  sequence;
    NodeIndex              node_idx;
  };

  // std::atomic is neither copyable nor movable, so Cell can't live in a
  // std::vector<Cell> that needs to resize/reallocate -- allocate the
  // backing storage once, up front, and never move it.
  std::unique_ptr<Cell[]>   m_ring;
  size_t                    m_capacity = 0;
  uint64_t                  m_mask = 0; // m_capacity - 1 (m_capacity is a power of 2)
  alignas(64) std::atomic<uint64_t> m_enqueue_pos{0};
  alignas(64) std::atomic<uint64_t> m_dequeue_pos{0};

  // One flag per *pool node* (indexed by NodeIndex, unlike m_ring which is
  // indexed by ticket-position-mod-capacity): true while that node
  // currently has a live entry somewhere in the ring. CheckIn() on a node
  // that's already enqueued is a real, expected scenario -- e.g.
  // SweepTimeouts() releasing a connection's last in-flight request can
  // race with that same connection's caller separately calling
  // checkin_connection/2,4 for the same request/connection, and both
  // paths call ConnectionPool::CheckIn() unconditionally (see
  // arterial.hpp). Enqueuing the same node index twice would corrupt the
  // ring's ticket/sequence bookkeeping (one extra enqueue ticket with no
  // matching dequeue ever arriving), so CheckIn() must detect this and
  // skip the second, redundant enqueue rather than ever assume it's the
  // caller's job to avoid it.
  std::unique_ptr<std::atomic<bool>[]> m_in_ring;

  void Init(size_t size)
  {
    m_capacity = RoundUpPow2(size ? size : 1);
    m_mask     = m_capacity - 1;
    m_ring     = std::make_unique<Cell[]>(m_capacity);
    for (size_t i = 0; i < m_capacity; ++i)
      m_ring[i].sequence.store(i, std::memory_order_relaxed);
    // Cells beyond `size` (padding up to the next power of 2) are never
    // targeted by any enqueue, since CheckIn() is only ever called with a
    // real node index and EnqueueNode() below claims tickets, not specific
    // cell indices -- but capacity in ticket-space must still match
    // `m_ring.size()` for the modulo/mask arithmetic to stay consistent,
    // so the unused padding cells simply sit idle.

    m_in_ring = std::make_unique<std::atomic<bool>[]>(size ? size : 1);
    for (size_t i = 0; i < (size ? size : 1); ++i)
      m_in_ring[i].store(false, std::memory_order_relaxed);
  }

  /// @brief Push `idx` (a real pool node index) onto the ring. @return
  /// false if the ring's enqueue ticket for this attempt landed on a cell
  /// whose slot from `capacity` enqueues ago hasn't been freed by a
  /// matching dequeue yet -- this can happen transiently even when the
  /// number of live nodes is within capacity, since it reflects whether a
  /// concurrent dequeue has *finished*, not the true live-item count.
  /// Callers that must not fail (CheckIn()) retry until it succeeds.
  bool EnqueueNode(NodeIndex idx)
  {
    auto pos = m_enqueue_pos.load(std::memory_order_relaxed);
    for (;;) {
      auto&    cell = m_ring[pos & m_mask];
      auto     seq  = cell.sequence.load(std::memory_order_acquire);
      int64_t  diff = int64_t(seq) - int64_t(pos);

      if (diff == 0) {
        if (m_enqueue_pos.compare_exchange_weak
            (pos, pos + 1, std::memory_order_relaxed))
          break;
        // CAS failed: `pos` was refreshed to the current value by the
        // failed compare_exchange_weak; loop and re-evaluate that cell.
      } else if (diff < 0) {
        return false; // target cell not freed yet; caller should retry
      } else {
        pos = m_enqueue_pos.load(std::memory_order_relaxed);
      }
    }

    auto& cell = m_ring[pos & m_mask];
    cell.node_idx = idx;
    cell.sequence.store(pos + 1, std::memory_order_release);
    return true;
  }

  /// @brief Pop the next node index off the ring. @return Invalid if empty.
  NodeIndex DequeueNode()
  {
    auto pos = m_dequeue_pos.load(std::memory_order_relaxed);
    for (;;) {
      auto&    cell = m_ring[pos & m_mask];
      auto     seq  = cell.sequence.load(std::memory_order_acquire);
      int64_t  diff = int64_t(seq) - int64_t(pos + 1);

      if (diff == 0) {
        if (m_dequeue_pos.compare_exchange_weak
            (pos, pos + 1, std::memory_order_relaxed))
          break;
      } else if (diff < 0)
        return NodeIndex(); // ring empty
      else
        pos = m_dequeue_pos.load(std::memory_order_relaxed);
    }

    auto& cell = m_ring[pos & m_mask];
    auto  idx  = cell.node_idx;
    cell.sequence.store(pos + m_capacity, std::memory_order_release);
    return idx;
  }
};

template <typename T>
using ObjectPoolFIFO = BaseObjectPoolFIFO<BasePoolNode<T, PooledNodeTraitsFIFO>>;

//-----------------------------------------------------------------------------
// IMPLEMENTATION
//-----------------------------------------------------------------------------

template<DerivedFromPooledObject NodeT>
BaseObjectPoolFIFO<NodeT>::ObjT*
BaseObjectPoolFIFO<NodeT>::CheckOut()
{
  for (;;) {
    auto idx = DequeueNode();
    if (idx.Invalid())
      return nullptr;

    assert(idx.Index() >= 0 && idx.Index() < int(this->m_nodes.size()));
    m_in_ring[idx.Index()].store(false, std::memory_order_release);
    auto* node = &this->m_nodes[idx.Index()];

    // If the fetched node became unavailable while waiting in the ring,
    // drop it (don't re-enqueue) and take the next one -- mirrors the
    // original lazy-removal semantics: MakeAvailable() is what re-enqueues
    // it later.
    if (!node->Available())
      continue;

    return node;
  }
}

template<DerivedFromPooledObject NodeT>
void BaseObjectPoolFIFO<NodeT>::CheckIn(const BaseObjectPoolFIFO<NodeT>::ObjT& nd)
{
  assert(nd.Magic() == this->m_magic);  // Make sure the node belongs to the pool
  assert(nd.Index() >= 0 && nd.Index() < int(this->m_nodes.size()));

  // Checking in a node that's already sitting in the ring is a real,
  // expected scenario (see m_in_ring's doc comment) -- detect it and skip
  // the redundant enqueue, which would otherwise corrupt the ring's
  // ticket/sequence bookkeeping (an extra enqueue ticket with no matching
  // dequeue ever arriving to balance it, since the node is only really
  // sitting in one ring slot no matter how many times CheckIn() is called).
  bool already_in = m_in_ring[nd.Index()].exchange(true, std::memory_order_acq_rel);
  if (already_in)
    return;

  // EnqueueNode() can return false transiently even though the number of
  // live nodes never exceeds capacity: a producer's enqueue ticket can
  // legitimately outrun a *concurrent, not-yet-finished* dequeue's release
  // of that same cell (Vyukov's algorithm reports "full" based on whether
  // the target cell has been freed yet, not on a true live-item count).
  // The in-flight dequeue is guaranteed to complete on its own (it isn't
  // blocked on anything), so spin until the cell is free -- there is no
  // caller-visible fallback for CheckIn() failing, the node must go back.
  while (!EnqueueNode(NodeIndex(nd.Index(), 0)))
    std::this_thread::yield();
}

template<DerivedFromPooledObject NodeT>
bool BaseObjectPoolFIFO<NodeT>::MakeAvailable(T* obj)
{
  auto   node =  this->Find(obj);
  return node && MakeAvailable(*node);
}

template<DerivedFromPooledObject NodeT>
bool BaseObjectPoolFIFO<NodeT>::MakeAvailable(NodeT& node)
{
  auto success = node.Available(true);

  // Place the node in the pool
  if (success)
    CheckIn(node);

  return success;
}

template<DerivedFromPooledObject NodeT>
bool BaseObjectPoolFIFO<NodeT>::MakeUnavailable(T* obj)
{
  auto   node =  this->Find(obj);
  return node && MakeUnavailable(*node);
}

template<DerivedFromPooledObject NodeT>
bool BaseObjectPoolFIFO<NodeT>::Empty() const
{
  return m_dequeue_pos.load(std::memory_order_relaxed)
      == m_enqueue_pos.load(std::memory_order_relaxed);
}

template<DerivedFromPooledObject NodeT>
size_t BaseObjectPoolFIFO<NodeT>::UnsafeSize() const
{
  auto enq = m_enqueue_pos.load(std::memory_order_relaxed);
  auto deq = m_dequeue_pos.load(std::memory_order_relaxed);
  return enq >= deq ? size_t(enq - deq) : 0;
}

template<DerivedFromPooledObject NodeT>
template<typename Fun>
void BaseObjectPoolFIFO<NodeT>::ForEach(Fun const& fun)
{
  auto deq = m_dequeue_pos.load(std::memory_order_relaxed);
  auto enq = m_enqueue_pos.load(std::memory_order_relaxed);
  for (auto pos = deq; pos < enq; ++pos) {
    auto& cell = m_ring[pos & m_mask];
    fun(static_cast<ObjT&>(this->m_nodes[cell.node_idx.Index()]));
  }
}

} // namespace arterial
