//-----------------------------------------------------------------------------
/// \brief Lock-free FIFO object pool
/// \author Serge Aleynikov
///
/// Implementation is derived from:
/// https://github.com/saleyn/utxx/blob/master/include/utxx/container/concurrent_fifo.hpp
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
//-----------------------------------------------------------------------------
// Created: 2010-02-03
//-----------------------------------------------------------------------------
#pragma once

#include "pool_lifo.hpp"

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

  template<typename... Args>
  BaseObjectPoolFIFO(size_t size, Args&&... args)
    : BaseT(size, std::forward<Args&&>(args)...)
  {
    Init();
  }

  template <IsObjOrUniqPtr<ObjT> T>
  BaseObjectPoolFIFO(std::vector<T> const& objects)
    : BaseT(objects)
  {
    Init();
  }

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
  bool MakeUnavailable(NodeT& node) { return node->Available(false); }


  /// Returns true if the queue is empty.
  bool Empty() const;

  /// This method does not return an accurate count in multi-threaded use.
  /// Intenced for debugging only!
  /// @return size of the queue.
  size_t UnsafeSize() const;

  /// @brief Call given lambda for each node in the pool
  template <typename Fun>
  void ForEach(Fun const& fun);
private:
  alignas(64) std::atomic<NodeIndex> m_head;
  alignas(64) std::atomic<NodeIndex> m_tail;

  void Init()
  {
    m_head.store(NodeIndex(), std::memory_order_relaxed);
    m_tail.store(NodeIndex(), std::memory_order_relaxed);
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
  NodeIndex old_head, next_index;
  size_t vsn = 0;

  while (true) {
    // (a) This acquire-load synchronizes with the release-CAS
    old_head = m_head.load(std::memory_order_acquire);
    if (old_head.Invalid())
      return nullptr;

    auto& old_node = this->m_nodes[old_head.Index()];
    auto& next     = old_node.NextRef(); // atomic by reference
    auto  old_next = old_node.Next();    // atomic by value

    vsn = old_next.Invalid() ? 0 : vsn == 0 ? ++this->m_vsn : vsn;

    // (b) This acquire-load synchronizes with the release-CAS
    next_index = NodeIndex(old_next.Index(), vsn);

    if (m_head.load(std::memory_order_acquire) != old_head)
      continue;

    // (c) This acquire-load synchronizes with the release-CAS
    auto old_tail = m_tail.load(std::memory_order_acquire);
    if (old_head == old_tail) { // tail is falling behind
      // (d) This release-CAS synchronizes with the acquire-load (c)
      m_tail.compare_exchange_weak
        (old_tail, next, std::memory_order_release, std::memory_order_relaxed);
      continue;
    }

    // (e) This release-CAS synchronizes with the acquire-load (a)
    if (!m_head.compare_exchange_weak
        (old_head, next, std::memory_order_release, std::memory_order_relaxed))
      continue;

    assert(old_head.Valid() && old_head.Index() < this->m_nodes.size());

    auto node = &this->m_nodes[old_head.Index()];

    // If the fetched node became unavailable while waiting in the queue,
    // skip it and take the next one.
    if (!node->Available()) {
      vsn = 0;  // Reset the vsn so that it gets incremented when needed
      continue;
    }

    return node;
  }
}

template<DerivedFromPooledObject NodeT>
void BaseObjectPoolFIFO<NodeT>::CheckIn(const BaseObjectPoolFIFO<NodeT>::ObjT& nd)
{
  assert(nd.Magic() == this->m_magic);  // Make sure the node belongs to the pool
  assert(nd.Index() >= 0 && nd.Index() < this->m_nodes.size());

  auto node_idx = nd.Index();
  auto vsn      = ++this->m_vsn;

  NodeIndex new_tail(node_idx, vsn), old_tail, next_index;
  // Clear the next node pointer, as the "new_tail" will become the new tail
  this->m_nodes[node_idx].SetNext(NodeIndex());

  // Update tail with the new item
  do {
    // (a) This acquire-load synchronizes with the release-CAS
    old_tail   = m_tail.load(std::memory_order_acquire);
    NextNodeT    empty_node;
    auto& next = old_tail.Valid()
                ? this->m_nodes[old_tail.Index()].NextRef()
                : empty_node;

    // (b) This acquire-load synchronizes with the release-CAS
    next_index = NodeIndex(next.load(std::memory_order_acquire).Index(), old_tail.Valid() ? vsn : 0);

    // Did another thread change the current tail?
    // (c) This acquire-load synchronizes with the release-CAS
    if (old_tail != m_tail.load(std::memory_order_acquire))
      continue;

    if (next_index.Valid()) {
      // Tail is falling behind, update the the latest tail (only one of
      // the threads will succeed here), and repeat the attempt to fetch
      // the lastest tail.
      // (d) This release-CAS synchronizes with the acquire-load (a)
      m_tail.compare_exchange_weak
        (old_tail, next_index, std::memory_order_release, std::memory_order_relaxed);
      continue;
    }

    NodeIndex empty;

    // If no other thread added any item behind the current tail, append the
    // item to the end, and we are done
    // (e) This release-CAS synchronizes with the acquire-load (b)
    if (next.compare_exchange_weak
        (empty, new_tail, std::memory_order_release, std::memory_order_relaxed))
      break;
  } while (true);

  // If we made it here, the new_tail contains the newly enqued node_idx
  // Possibly replace the actual tail (only one of the threads will succeed).
  // (f) This release-CAS synchronizes with the acquire-load (a)
  m_tail.compare_exchange_weak
    (old_tail, new_tail, std::memory_order_release, std::memory_order_relaxed);

  assert(m_tail.load(std::memory_order_relaxed).Valid());

  NodeIndex empty;

  // If the head was unassigned, point to the tail (the queue has 1 element)
  m_head.compare_exchange_strong
    (empty, m_tail, std::memory_order_release, std::memory_order_relaxed);
}

template<DerivedFromPooledObject NodeT>
bool BaseObjectPoolFIFO<NodeT>::MakeAvailable(ObjT* obj)
{
  auto   node =  Find(obj);
  return node && MakeAvailable(*node);
}

template<DerivedFromPooledObject NodeT>
bool BaseObjectPoolFIFO<NodeT>::MakeAvailable(NodeT& node)
{
  auto success = !node.Available(true);

  // Place the node in the pool
  if (success)
    CheckIn(*node);

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
  auto   head = m_head.load(std::memory_order_relaxed);
  if   (!head)  return true;
  auto&  next = this->m_nodes[head.Index()].Next();
  return next.load(std::memory_order_relaxed).Invalid();
}

template<DerivedFromPooledObject NodeT>
size_t BaseObjectPoolFIFO<NodeT>::UnsafeSize() const {
  size_t result = 0;
  for (NodeIndex h = m_head.load(std::memory_order_relaxed);
        h.Valid();
        h = this->m_nodes[h.Index()].Next())
    result++;
  return result;
}

template<DerivedFromPooledObject NodeT>
template<typename Fun>
void BaseObjectPoolFIFO<NodeT>::ForEach(Fun const& fun)
{
  for (NodeIndex h = m_head.load(std::memory_order_relaxed);
        h.Valid();
        h = this->m_nodes[h.Index()].Next())
    fun(static_cast<ObjT&>(this->m_nodes[h.Index()]));
}

} // namespace arterial