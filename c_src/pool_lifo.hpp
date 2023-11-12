/// @brief Lock-free LIFO object pool.
///
/// Implementation of an object pool that owns a fixed set of objects and
/// allows to check them out/in concurrently.
///
/// Objects can be made available/unavailable. Marking an object unavailable
/// will remove it from the object pool until it's made back to be available.
/// When an object is made unavailable, this is done lazily. What this implies
/// is that if the next call to CheckOut() detects that an object is
/// unavailable, it will be removed from the pool of free objects and placed
/// in the "unavailable" list.
///
/// @author Serge Aleynikov
#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>
#include <map>
#include <set>
#include <stdexcept>

#define LIKELY(Cond)   __builtin_expect(!!(Cond), 1)
#define UNLIKELY(Cond) __builtin_expect(!!(Cond), 0)

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
  : m_obj(nullptr), m_index(-1), m_available(false), m_magic(0)
  {}

  PooledObject(T* obj, int idx, bool avail, uint32_t magic)
  : m_obj(obj)
  , m_index(idx)
  , m_available(avail)
  , m_magic(magic)
  {}

  PooledObject(PooledObject&& rhs)
  : m_obj(rhs.m_obj)
  , m_index(rhs.m_index)
  , m_available(rhs.m_available.load(std::memory_order_relaxed))
  , m_magic(rhs.m_magic)
  {}

  PooledObject(PooledObject const& rhs)
  : m_obj(rhs.m_obj)
  , m_index(rhs.m_index)
  , m_available(rhs.m_available)
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
struct PooledNodeTraits {
  using NextT = NodeIndex;

  struct NextNode {
    explicit NextNode(int idx = -1) : m_next(idx, 0) {}
    NextNode(NextNode&& rhs) : m_next(rhs.m_next) {}

    void SetNext(NodeIndex next) { m_next = next; }

    NodeIndex  Value()     const { return m_next; }
    NodeIndex& Ref()             { return m_next; }

    int        Index()     const { return m_next.Index(); }
  private:
    NodeIndex m_next;
  };
};

//-----------------------------------------------------------------------------
/// @brief A pool node that owns a pooled object of type T.
//-----------------------------------------------------------------------------
template <typename T, typename Traits = PooledNodeTraits>
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
  : PooledObject<T>(new T(std::forward(args)...), idx, avail, magic)
  , m_obj_owner(this->PooledObject<T>::m_obj) // Take ownership
  , m_next(std::max(-1, idx-1))
  {}

  explicit BasePoolNode(T* obj, uint32_t magic, bool avail, int idx = -1)
  : PooledObject<T>(obj, idx, avail, magic)
  , m_obj_owner(this->PooledObject<T>::m_obj) // Take ownership
  , m_next(idx+1)
  {}

  // BasePoolNode(BasePoolNode&& rhs)
  //   : PooledObject<T>(rhs.Get(), rhs.Index(), rhs.Magic())
  //   , m_next(rhs.m_next)
  // {
  //   rhs.Invalidate();
  // }

  void SetNext(NodeIndex next) { m_next.SetNext(next); }

  /// @brief NodeIndex of the next node in the pool
  NodeIndex  Next()      const { return m_next.Value(); }
  NextT&     NextRef()         { return m_next.Ref();   }

private:
  std::unique_ptr<T> m_obj_owner;
  NextNodeT          m_next;
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
/// because the U class is templated (PooledObject<T>), and BaseObjectPoolLIFO
/// would have to be defined as:
///
///    template <Derived<PooledObject<T>> NodeT>
///    class BaseObjectPoolLIFO
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

//-----------------------------------------------------------------------------
/// @brief Lock-free object pool
///
/// The pool holds N nodes containing objects of type `NodeT::ValueT`.
///
/// Use functions CheckOut() and CheckIn() to get/put an available object
/// in LIFO order.
///
/// @tparam NodeT a pooled resource type that must be derived from BasePoolNode.
//-----------------------------------------------------------------------------
template<DerivedFromPooledObject NodeT>
struct BaseObjectPoolLIFO {
  using T       = typename NodeT::ValueT;
  using ObjT    = typename NodeT::ObjT;
  using TraitsT = typename NodeT::TraitsT;

  BaseObjectPoolLIFO(size_t size) : m_magic(NewMagic())
  {
    auto init = [this](auto i){ return NodeT(m_magic, false, i); };
    Construct(size, init);
  }

  template<typename Init>
  BaseObjectPoolLIFO(size_t size, Init const& init) : m_magic(NewMagic())
  {
    Construct(size, init);
  }

  template <IsObjOrUniqPtr<ObjT> T>
  BaseObjectPoolLIFO(std::vector<T> const& objects) : m_magic(NewMagic())
  {
    auto new_node = [this, &objects](auto i) {
      return NodeT(Ptr(objects[i]), m_magic, false, i);
    };

    Construct(objects.size(), new_node);
  }

private:
  static ObjT* Ptr(ObjT* p) { return p; }
  static ObjT* Ptr(std::unique_ptr<ObjT>& p) { return p.release(); }

  template <typename Init>
  void Construct(size_t size, Init const& fun);
public:
  /// @brief Pool maximum capacity
  size_t Capacity() const { return m_nodes.size(); }

  /// @brief Acquire an object from the object pool.
  ///
  /// The object must be returned back to the pool using CheckIn() function.
  ///
  /// @return The node index of a checked out node. Use the Get() function to
  /// obtain the contained object pointer.
  ObjT* CheckOut();

  /// @brief Release the node back to pool
  ///
  /// This node must be previously checked out by a call to CheckOut().
  void CheckIn(const ObjT& nd);

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

  /// @brief Return the number of available nodes in the pool
  size_t UnsafeSize() const;

  /// @brief Checkout N items from the queue
  std::pair<bool, std::vector<T*>> UnsafeReserve(size_t n);

  /// @brief Call given lambda for each node in the pool
  template <typename Fun>
  void ForEach(Fun const& fun);

protected:
  uint32_t                           m_magic;     // Unique ID of this pool
  std::vector<NodeT>                 m_nodes;
  std::map<const T*, int>            m_nodes_map; // Resolves m_nodes index
  alignas(64) std::atomic<NodeIndex> m_free_list_head;
  std::atomic<uint32_t>              m_vsn;       // Global vsn counter
};

template <typename T>
using ObjectPoolLIFO = BaseObjectPoolLIFO<BasePoolNode<T, PooledNodeTraits>>;

//-----------------------------------------------------------------------------
// IMPLEMENTATION
//-----------------------------------------------------------------------------

template<DerivedFromPooledObject NodeT>
template <typename Init>
void BaseObjectPoolLIFO<NodeT>::Construct(size_t size, Init const& fun)
{
  m_nodes.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    m_nodes.emplace_back(fun(i));
    m_nodes_map.emplace(std::make_pair(m_nodes.back().Value(), i));
  }

  assert(m_nodes.size() == size);

  // Mark the end of the list by invalidating the next pointer of the
  // last node in the list
  m_nodes[size-1].SetNext(NodeIndex());

  // Initially the free list is empty. The nodes become available by
  // explicitely calling MakeAvailable(obj).
  m_free_list_head.store(NodeIndex());
}

template<DerivedFromPooledObject NodeT>
BaseObjectPoolLIFO<NodeT>::ObjT*
BaseObjectPoolLIFO<NodeT>::CheckOut()
{
  auto old_head = m_free_list_head.load(std::memory_order_relaxed);
  do {
    if (old_head.Invalid()) [[unlikely]] { break; }

    auto& node = m_nodes[old_head.Index()];

    NodeIndex new_head(node.Next().Index(), ++m_vsn);

    if (m_free_list_head.compare_exchange_strong
          (old_head, new_head, std::memory_order_relaxed))
      return &node;
  } while (true);

  return nullptr;
}

template<DerivedFromPooledObject NodeT>
void BaseObjectPoolLIFO<NodeT>::CheckIn(const ObjT& nd)
{
  assert(nd.Magic() == m_magic);  // Make sure the node belongs to the pool
  assert(nd.Index() <  m_nodes.size());

  auto node_idx = nd.Index();

  if (node_idx < 0) [[unlikely]] { return; }

  auto& node = m_nodes[node_idx];

  auto curr_head = m_free_list_head.load(std::memory_order_relaxed);
  do {
    node.SetNext(curr_head);
    NodeIndex new_head(node_idx, ++m_vsn);
    if (m_free_list_head.compare_exchange_weak(curr_head, new_head,
          std::memory_order_release, std::memory_order_relaxed))
      break;
  } while (true);
}

template<DerivedFromPooledObject NodeT>
NodeT* BaseObjectPoolLIFO<NodeT>::Find(const T* obj)
{
  auto   it =  m_nodes_map.find(obj);
  return it == m_nodes_map.end() ? nullptr : &m_nodes[it->second];
}

template<DerivedFromPooledObject NodeT>
NodeT const* BaseObjectPoolLIFO<NodeT>::Find(const T* obj) const
{
  auto   it =  m_nodes_map.find(obj);
  return it == m_nodes_map.end() ? nullptr : &m_nodes[it->second];
}


template<DerivedFromPooledObject NodeT>
size_t BaseObjectPoolLIFO<NodeT>::UnsafeSize() const
{
  size_t result = 0;
  auto   cursor = m_free_list_head.load(std::memory_order_relaxed);

  while (cursor.Valid()) {
    result++;
    cursor = m_nodes[cursor.Index()].Next();
  }

  return result;
}

template<DerivedFromPooledObject NodeT>
std::pair<bool, std::vector<typename BaseObjectPoolLIFO<NodeT>::T*>>
BaseObjectPoolLIFO<NodeT>::UnsafeReserve(size_t n)
{
  size_t result = 0;
  auto   cursor = m_free_list_head.load(std::memory_order_relaxed);

  auto   vec = std::vector<T*>();
  vec.reserve(n);

  while (cursor.Valid() && vec.size() < n) {
    vec.push_back(cursor.Get()->ID());
    cursor = m_nodes[cursor.Index()].Next();
  }

  auto success = vec.size() == n;
  if  (success)
    m_free_list_head.exchange(cursor);

  return std::make_pair(success, vec);
}

/// @brief Call given lambda for each node in the pool
template<DerivedFromPooledObject NodeT>
template <typename Fun>
void BaseObjectPoolLIFO<NodeT>::ForEach(Fun const& fun)
{
  auto   cursor = m_free_list_head.load(std::memory_order_relaxed);

  while (cursor.Valid()) {
    cursor = m_nodes[cursor.Index()].Next();
    fun(static_cast<ObjT&>(m_nodes[cursor.Index()]));
  }
}

}   // namespace arterial