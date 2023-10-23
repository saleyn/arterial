/// @brief Lock-free object pool.
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

#define LIKELY(Cond) __builtin_expect(Cond, true)

namespace arterial {

/// @brief A versioned index of a node inside a pool for solving the ABA problem
struct NodeIndex {
  NodeIndex() noexcept                 : m_index(-1),  m_vsn(0)     {}
  NodeIndex(int idx, uint32_t vsn = 0) : m_index(idx), m_vsn(vsn)   {}

  uint32_t  Vsn()        const { return m_vsn;                       }
  int       Index()      const { return m_index;                     }
  void      Set(NodeIndex idx) { Set(idx.m_index, idx.m_vsn);        }
  void      Set(int idx, uint32_t vsn) { m_index = idx; m_vsn = vsn; }

  bool      Valid()      const { return m_index >= 0;                }
  bool      Invalid()    const { return m_index <  0;                }
private:
  int       m_index;
  uint32_t  m_vsn;
};

/// @brief An object holder that gets checked out and checked into the pool.
///
/// An instance of this class doesn't own the contained object. The ownership
/// is assumed by the derived class, which is a node in the pool
/// (e.g. BasePoolNode<T>).
///
/// @tparam T - a type that represents a pooled resource
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
    , m_available(rhs.m_available)
    , m_magic(rhs.m_magic)
  {}

  PooledObject(PooledObject const& rhs)
    : m_obj(rhs.m_obj)
    , m_index(rhs.m_index)
    , m_available(rhs.m_available)
    , m_magic(rhs.m_magic)
  {}

  T*         Value()              { return m_obj;        }
  T const*   Value()        const { return m_obj;        }

  /// @brief Return the index of this node in the pool
  int        Index()      const { return m_index;      }
  /// @brief Return the unique magic number of the pool this node belongs to
  uint32_t   Magic()      const { return m_magic;      }

  bool       Valid()      const { return m_index >= 0; }
  bool       Invalid()    const { return m_index <  0; }

  bool       Available()  const { return m_available;  }
  void       Available(bool v)  { m_available = v;     }
protected:
  T*         m_obj;
  int        m_index;
  bool       m_available;
  uint32_t   m_magic; /// Magic number of the pool this node belongs to

  void       Invalidate()       { m_index = -1;        }

  void operator=(PooledObject const&) = delete;
  void operator=(PooledObject&&)      = delete;
};

/// @brief A pool node that owns a pooled object of type T.
template <typename T>
struct BasePoolNode : PooledObject<T> {
  using ValueT = T;

  /// NOTE: the `args` arguments must not be moved here but copied because the
  /// caller uses the same arguments to initialize every object in the pool.
  template <typename... Args>
  explicit BasePoolNode(uint32_t magic, bool avail, int idx = -1, Args... args)
    : PooledObject<T>(new T(std::forward(args)...), idx, avail, magic)
    , m_obj_owner(this->PooledObject<T>::m_obj) // Take ownership
    , m_next(std::max(-1, idx-1))
  {}

  explicit BasePoolNode(T* obj, uint32_t magic, bool avail, int idx = -1)
    : PooledObject<T>(obj), idx, avail, magic)
    , m_obj_owner(this->PooledObject<T>::m_obj) // Take ownership
    , m_next(std::max(-1, idx-1))
  {}

  // BasePoolNode(BasePoolNode&& rhs)
  //   : PooledObject<T>(rhs.Get(), rhs.Index(), rhs.Magic())
  //   , m_next(rhs.m_next)
  // {
  //   rhs.Invalidate();
  // }

  void SetNext(NodeIndex next)         { m_next.Set(next);      }
  void SetNext(int next, uint32_t vsn) { m_next.Set(next, vsn); }

  /// @brief NodeIndex of the next node in the pool
  NodeIndex  Next()              const { return m_next;         }
  NodeIndex& Next()                    { return m_next;         }

private:
  std::unique_ptr<T> m_obj_owner;
  NodeIndex          m_next;
};

/// @brief A concept that only accepts T classes derived from
/// PooledObject<...> specializations.
///
/// NOTE: we cannot just use the following concept definition:
///
///    template<class T, class U>
///    concept Derived = std::is_base_of_v<U, T>;
///
/// because the U class is templated (PooledObject<T>), and BaseObjectPool
/// would have to be defined as:
///
///    template <Derived<PooledObject<T>> NodeT>
///    class BaseObjectPool
///
/// which is invalid.
template <class T>
concept DerivedFromPooledObject = requires(T obj) {
  []<typename U>(PooledObject<U>&){}(obj);
};

/// @brief Lock-free object pool
///
/// The pool holds N nodes containing objects of type `NodeT::ValueT`.
///
/// Use functions CheckOut() and CheckIn() to get/put an available object.
///
/// @tparam NodeT a pooled resource type that must be derived from BasePoolNode.
template<DerivedFromPooledObject NodeT>
struct BaseObjectPool {
  using T    = typename NodeT::ValueT;
  using ObjT = PooledObject<T>;

  template<typename... Args>
  BaseObjectPool(size_t size, Args&&... args)
    : m_magic(NewMagic())
  {
    m_nodes.reserve(size);
    for (size_t i = 0; i < size; ++i) {
      m_nodes.emplace_back(NodeT(m_magic, false, i, std::forward(args)...));
      m_nodes_map.emplace(std::make_pair(m_nodes.back().Value(), i));
      m_unavail_nodes.emplace(i);
    }

    assert(m_nodes.size() == size);
    assert(m_unavail_nodes.size() == size);

    m_free_list_head.store(NodeIndex());
  }

  template<class U>
  concept IsObjOrUniqPtr = std::is_same_v<U, ObjT*>
                        || std::is_same_v<U, std::unique_ptr<ObjT>>;

  template <IsObjOrUniqPtr T>
  BaseObjectPool(std::vector<T> const& objects)
    : m_magic(NewMagic())
  {
    m_nodes.reserve(objects.size());
    for (size_t i = 0; i < objects.size(); ++i) {
      m_nodes.emplace_back(NodeT(Ptr(objects[i]), m_magic, false, i));
      m_nodes_map.emplace(std::make_pair(m_nodes.back().Value(), i));
      m_unavail_nodes.emplace(i);
    }

    assert(m_nodes.size() == size);
    assert(m_unavail_nodes.size() == size);

    m_free_list_head.store(NodeIndex());
  }

private:
  static ObjT* Ptr(ObjT* p) { return p; }
  static ObjT* Ptr(std::unique_ptr<ObjT>& p) { return p.release(); }

public:
  /// @brief Pool maximum capacity
  size_t Capacity() const { return m_nodes.size(); }

  /// @brief Acquire an object from the object pool.
  ///
  /// The object must be returned back to the pool using CheckIn() function.
  ///
  /// @return The node index of a checked out node. Use the Get() function to
  /// obtain the contained object pointer.
  ObjT const& CheckOut()
  {
    auto old_head = m_free_list_head.load(std::memory_order_relaxed);
    do {
      if (old_head.Invalid()) [[unlikely]] { break; }

      NodeT const& node = m_nodes[old_head.Index()];

      NodeIndex new_head(node.Next().Index(), ++m_vsn);

      if (m_free_list_head.compare_exchange_strong
            (old_head, new_head, std::memory_order_relaxed)) {
        // If the checked out node is unavailable, move it to the unavailable
        // set and continue CheckIng out another node
        if (!node.Available()) [[unlikely]] {
          std::lock_guard<std::mutex> g(m_unavail_mutex);
          m_unavail_nodes.emplace(node.Index());
          continue;
        }
        return node;
      }
    } while (true);

    static const ObjT s_obj;
    return s_obj;
  }

  /// @brief Release the node back to pool
  ///
  /// This node must be previously checked out by a call to CheckOut().
  void CheckIn(const ObjT& nd)
  {
    assert(nd.Magic() == m_magic);  // Make sure the node belongs to the pool
    CheckIn(nd.Index());
  }

  NodeT* Find(const T* obj)
  {
    auto   it =  m_nodes_map.find(obj);
    return it == m_nodes_map.end() ? nullptr : &m_nodes[it->second];
  }

  NodeT const* Find(const T* obj) const
  {
    auto   it =  m_nodes_map.find(obj);
    return it == m_nodes_map.end() ? nullptr : &m_nodes[it->second];
  }

  /// @brief Make the node available for use
  bool MakeAvailable(T* obj)
  {
    auto node = Find(obj);

    if (!node) [[unlikely]]
      return false;

    if (node->Available()) [[unlikely]]  // Already available
      return true;

    std::lock_guard<std::mutex> g(m_unavail_mutex);
    auto erased = m_unavail_nodes.erase(node->Index());

    if (node->Available()) [[unlikely]]  // Already made available by another thread
      return true;

    node->Available(true);

    if (erased)
      CheckIn(node->Index());

    return erased;
  }

  bool MakeUnavailable(T* obj)
  {
    auto node = Find(obj);

    if (!node) [[unlikely]]
      return false;

    node->Available(false);

    return true;
  }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T* Get(ObjT& ni)
  {
    return LIKELY(ni.Valid()) ? ni.Value() : nullptr;
  }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T const* Get(ObjT const& ni) const
  {
    return LIKELY(ni.Valid()) ? ni.Value() : nullptr;
  }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T* Get(NodeIndex ni)
  {
    return LIKELY(ni.Valid()) ? m_nodes[ni.Index()].Value() : nullptr;
  }

  /// @brief Get the object associated with the given node index
  /// @return NULL if the node index is invalid, or else resolve the node
  T const* Get(NodeIndex ni) const
  {
    return LIKELY(ni.Valid()) ? m_nodes[ni.Index()].Get() : nullptr;
  }

  /// @brief Magic number unique to this pool
  uint32_t Magic() const { return m_magic; }

  /// @brief Return the number of available nodes in the pool
  size_t UnsafeFreeCapacity() const
  {
    size_t result = 0;
    auto   cursor = m_free_list_head.load(std::memory_order_relaxed);

    while (cursor.Valid()) {
      result++;
      cursor = m_nodes[cursor.Index()].Next();
    }

    return result;
  }

  /// @brief Call given lambda for each node in the pool
  template <typename Fun>
  void ForEach(Fun const& fun)
  {
    for (auto& node : m_nodes)
      fun(static_cast<ObjT&>(node));
  }

private:
  uint32_t                           m_magic;     // Unique ID of this pool
  std::vector<NodeT>                 m_nodes;
  std::map<const T*, int>            m_nodes_map; // Resolves m_nodes index
  alignas(64) std::atomic<NodeIndex> m_free_list_head;
  std::mutex                         m_unavail_mutex;
  std::set<int>                      m_unavail_nodes;
  std::atomic<uint32_t>              m_vsn;       // Global vsn counter

  /// @brief Return the node identified by `node_idx` back to pool
  void CheckIn(int node_idx)
  {
    if (node_idx < 0) [[unlikely]] { return; }

    assert(node_idx < m_nodes.size());
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

  static uint32_t NewMagic()
  {
    srand((unsigned)time(NULL));
    return rand();
  }
};

template <typename T>
using ObjectPool = BaseObjectPool<BasePoolNode<T>>;

}   // namespace arterial