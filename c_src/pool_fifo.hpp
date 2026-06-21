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
///
/// The node-storage/bookkeeping layer below (`NodeIndex`, `PooledObject`,
/// `BasePoolNode`, `BaseObjectPoolFIFO`'s `m_nodes`/`Construct()`/`Get()`/
/// `Find()`) was originally shared with a separate LIFO pool variant
/// (`pool_lifo.hpp`, since removed) via inheritance from a common
/// `BaseObjectPoolLIFO` base. That LIFO pool's own CAS-based free-list
/// CheckOut()/CheckIn() was never used by any production code -- only this
/// FIFO pool's ring-based CheckOut()/CheckIn() (which fully overrode the
/// base class's) ever backed `ConnectionPool` -- so the storage layer was
/// folded directly into this file and the unused LIFO checkout/checkin
/// algorithm was dropped instead of carried along as dead code.
//-----------------------------------------------------------------------------
// Created: 2010-02-03
//-----------------------------------------------------------------------------
#pragma once

#include "arterial_util.hpp"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <map>
#include <memory>
#include <thread>
#include <vector>

#ifndef LIKELY
#define LIKELY(Cond)   __builtin_expect(!!(Cond), 1)
#endif
#ifndef UNLIKELY
#define UNLIKELY(Cond) __builtin_expect(!!(Cond), 0)
#endif

namespace arterial {

inline static uint32_t NewMagic()
{
  srand((unsigned)time(NULL));
  return rand();
}

//-----------------------------------------------------------------------------
/// @brief A versioned index of a node inside a pool for solving the ABA problem
//-----------------------------------------------------------------------------
struct NodeIndex {
  NodeIndex() noexcept                 : m_index(-1),  m_vsn(0)     {}
  NodeIndex(int idx, uint32_t vsn = 0) : m_index(idx), m_vsn(vsn)   {}

  uint32_t  Vsn()        const { return m_vsn;                       }
  int       Index()      const { return m_index;                     }
  void      Set(NodeIndex idx) { Set(idx.m_index, idx.m_vsn);        }
  void      Set(int idx, uint32_t vsn) { m_index = idx; m_vsn = vsn; }

  bool      Valid()      const { return m_index >= 0;                }
  bool      Invalid()    const { return m_index <  0;                }

  void      Reset()            { m_index = -1; m_vsn = 0;            }

  bool      operator!()  const { return m_index < 0;                 }

  bool      operator==(NodeIndex const& rhs) const { return *(uint64_t*)this == *(uint64_t*)&rhs; }
  bool      operator!=(NodeIndex const& rhs) const { return *(uint64_t*)this != *(uint64_t*)&rhs; }
private:
  int       m_index;
  uint32_t  m_vsn;
};

//-----------------------------------------------------------------------------
/// @brief An object holder that gets checked out and checked into the pool.
///
/// An instance of this class doesn't own the contained object. The ownership
/// is assumed by the derived class, which is a node in the pool
/// (e.g. BasePoolNode<T>).
///
/// @tparam T - a type that represents a pooled resource
//-----------------------------------------------------------------------------
template <typename T>
struct PooledObject {
  explicit PooledObject()
  : m_obj(nullptr), m_available(false), m_index(-1), m_magic(0)
  {}

  PooledObject(T* obj, int idx, bool avail, uint32_t magic)
  : m_obj(obj)
  , m_available(avail)
  , m_index(idx)
  , m_magic(magic)
  {}

  PooledObject(PooledObject&& rhs)
  : m_obj(rhs.m_obj)
  , m_available(rhs.m_available.load(std::memory_order_relaxed))
  , m_index(rhs.m_index)
  , m_magic(rhs.m_magic)
  {}

  PooledObject(PooledObject const& rhs)
  : m_obj(rhs.m_obj)
  , m_available(rhs.m_available.load(std::memory_order_relaxed))
  , m_index(rhs.m_index)
  , m_magic(rhs.m_magic)
  {}

  T*         Value()            { return m_obj;        }
  T const*   Value()      const { return m_obj;        }

  /// @brief Return the index of this node in the pool
  int        Index()      const { return m_index;      }
  /// @brief Return the unique magic number of the pool this node belongs to
  uint32_t   Magic()      const { return m_magic;      }

  bool       Valid()      const { return m_index >= 0; }
  bool       Invalid()    const { return m_index <  0; }

  bool       Available()  const { return m_available.load(std::memory_order_relaxed); }
  /// @brief Returns true if the object's available status changed
  bool       Available(bool v)  { return v ^ m_available.exchange(v); }
protected:
  using AtomicBool = std::atomic<bool>;

  T*         m_obj;
  AtomicBool m_available;
  int        m_index;
  uint32_t   m_magic; /// Magic number of the pool this node belongs to

  void       Invalidate()       { m_index = -1; }

  void operator=(PooledObject const&) = delete;
  void operator=(PooledObject&&)      = delete;
};

//-----------------------------------------------------------------------------
/// @brief A concept that only accepts T classes derived from
/// PooledObject<...> specializations.
///
/// NOTE: we cannot just use the following concept definition:
///
///    template<class T, class U>
///    concept Derived = std::is_base_of_v<U, T>;
///
/// because the U class is templated (PooledObject<T>), and
/// BaseObjectPoolFIFO would have to be defined as:
///
///    template <Derived<PooledObject<T>> NodeT>
///    class BaseObjectPoolFIFO
///
/// which is invalid.
//-----------------------------------------------------------------------------
template <class T>
concept DerivedFromPooledObject = requires(T obj) {
  []<typename U>(PooledObject<U>&){}(obj);
};

template<class U, class ObjT>
concept IsObjOrUniqPtr = std::is_same_v<U, ObjT*>
                      || std::is_same_v<U, std::unique_ptr<ObjT>>;

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

//-----------------------------------------------------------------------------
/// @brief A pool node that owns a pooled object of type T.
//-----------------------------------------------------------------------------
template <typename T, typename Traits = PooledNodeTraitsFIFO>
struct BasePoolNode : PooledObject<T> {
  using ValueT      = T;
  using ObjT        = PooledObject<T>;
  using TraitsT     = Traits;
  using NextNodeT   = typename Traits::NextNode;
  using NextT       = typename Traits::NextT;

  /// NOTE: the `args` arguments must not be moved here but copied because the
  /// caller uses the same arguments to initialize every object in the pool.
  template <typename... Args>
  explicit BasePoolNode(uint32_t magic, bool avail, int idx = -1, Args... args)
  : PooledObject<T>(new T(std::forward<Args>(args)...), idx, avail, magic)
  , m_obj_owner(this->PooledObject<T>::m_obj) // Take ownership
  , m_next(std::max(-1, idx-1))
  {}

  explicit BasePoolNode(T* obj, uint32_t magic, bool avail, int idx = -1)
  : PooledObject<T>(obj, idx, avail, magic)
  , m_obj_owner(this->PooledObject<T>::m_obj) // Take ownership
  , m_next(idx+1)
  {}

  void SetNext(NodeIndex next) { m_next.SetNext(next); }

  /// @brief NodeIndex of the next node in the pool
  NodeIndex  Next()      const { return m_next.Value(); }
  NextT&     NextRef()         { return m_next.Ref();   }

private:
  std::unique_ptr<T> m_obj_owner;
  NextNodeT          m_next;
};

template <typename T, typename Traits = PooledNodeTraitsFIFO>
using BasePoolNodeFIFO = BasePoolNode<T, Traits>;

//-----------------------------------------------------------------------------
/// @brief Object pool with FIFO ordering and CheckIn/CheckOut semantics
//-----------------------------------------------------------------------------
template<DerivedFromPooledObject NodeT>
struct BaseObjectPoolFIFO {
  using T         = typename NodeT::ValueT;
  using ObjT      = typename NodeT::ObjT;
  using TraitsT   = typename NodeT::TraitsT;
  using NextNodeT = typename TraitsT::NextT;

  static_assert(std::is_same_v<std::atomic<NodeIndex>, typename NodeT::TraitsT::NextT>);

  explicit BaseObjectPoolFIFO(size_t size) : m_magic(NewMagic())
  {
    auto init = [this](auto i){ return NodeT(m_magic, false, i); };
    Construct(size, init);
    InitRing(size);
  }

  template<typename InitFun>
  BaseObjectPoolFIFO(size_t size, InitFun const& init) : m_magic(NewMagic())
  {
    Construct(size, init);
    InitRing(size);
  }

  template <typename U> requires IsObjOrUniqPtr<U, T>
  explicit BaseObjectPoolFIFO(std::vector<U>& objects) : m_magic(NewMagic())
  {
    auto new_node = [this, &objects](auto i) {
      return NodeT(Ptr(objects[i]), m_magic, false, i);
    };
    Construct(objects.size(), new_node);
    InitRing(objects.size());
  }

  ~BaseObjectPoolFIFO() {}

  /// @brief Pool maximum capacity
  size_t Capacity() const { return m_nodes.size(); }

  /// @brief Put the node to the end of the pool of available nodes.
  ///
  /// This node must be one of the m_nodes owned by the pool.
  void CheckIn(const ObjT& nd);

  /// @brief Remove the next available node from the head of the FIFO queue
  ObjT* CheckOut();

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T* Get(ObjT& n) { return LIKELY(n.Valid()) ? n.Value() : nullptr; }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T const* Get(ObjT const& n) const { return LIKELY(n.Valid()) ? n.Value() : nullptr; }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T* Get(NodeIndex n) { return UNLIKELY(!n) ? nullptr : m_nodes[n.Index()].Value(); }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T const*
  Get(NodeIndex n) const { return UNLIKELY(!n) ? nullptr : m_nodes[n.Index()].Get(); }

  NodeT const*
  Get(uint32_t idx) const { return LIKELY(idx < m_nodes.size()) ? &m_nodes[idx] : nullptr; }

  NodeT*
  Get(uint32_t idx) { return LIKELY(idx < m_nodes.size()) ? &m_nodes[idx] : nullptr; }

  NodeT const* Find(const T* obj) const;
  NodeT*       Find(const T* obj);

  /// @brief Magic number unique to this pool
  uint32_t Magic() const { return m_magic; }

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
  static T* Ptr(T* p) { return p; }
  static T* Ptr(std::unique_ptr<T>& p) { return p.release(); }

  template <typename Init>
  void Construct(size_t size, Init const& fun);

  uint32_t                 m_magic;     // Unique ID of this pool
  std::vector<NodeT>       m_nodes;
  std::map<const T*, int>  m_nodes_map; // Resolves m_nodes index

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

  void InitRing(size_t size)
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
template <typename Init>
void BaseObjectPoolFIFO<NodeT>::Construct(size_t size, Init const& fun)
{
  m_nodes.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    m_nodes.emplace_back(fun(i));
    m_nodes_map.emplace(std::make_pair(m_nodes.back().Value(), i));
  }

  assert(m_nodes.size() == size);
}

template<DerivedFromPooledObject NodeT>
NodeT* BaseObjectPoolFIFO<NodeT>::Find(const T* obj)
{
  auto   it =  m_nodes_map.find(obj);
  return it == m_nodes_map.end() ? nullptr : &m_nodes[it->second];
}

template<DerivedFromPooledObject NodeT>
NodeT const* BaseObjectPoolFIFO<NodeT>::Find(const T* obj) const
{
  auto   it =  m_nodes_map.find(obj);
  return it == m_nodes_map.end() ? nullptr : &m_nodes[it->second];
}

template<DerivedFromPooledObject NodeT>
BaseObjectPoolFIFO<NodeT>::ObjT*
BaseObjectPoolFIFO<NodeT>::CheckOut()
{
  for (;;) {
    auto idx = DequeueNode();
    if (idx.Invalid())
      return nullptr;

    assert(idx.Index() >= 0 && idx.Index() < int(m_nodes.size()));
    m_in_ring[idx.Index()].store(false, std::memory_order_release);
    auto* node = &m_nodes[idx.Index()];

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
  assert(nd.Magic() == m_magic);  // Make sure the node belongs to the pool
  assert(nd.Index() >= 0 && nd.Index() < int(m_nodes.size()));

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
  auto   node =  Find(obj);
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
  auto   node =  Find(obj);
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
    fun(static_cast<ObjT&>(m_nodes[cell.node_idx.Index()]));
  }
}

} // namespace arterial
