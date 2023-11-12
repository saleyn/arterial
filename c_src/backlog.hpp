#pragma once

#include <forward_list>
#include <vector>
#include <set>
#include <cstdint>
#include "nifpp.h"

namespace arterial {

template <class T, bool Init>
struct CircularBuffer;

//-----------------------------------------------------------------------------
/// @brief Information about a request sent over the current connection
//-----------------------------------------------------------------------------
template <typename BaseReqID, typename ReqID>
class RequestInfo {
  static constexpr const s_bitsize = sizeof(BaseReqID)*8;
  static constexpr const s_lo_mask = (1 << s_bitsize)-1;
  static constexpr const s_hi_mask = s_lo_mask << s_bitsize;
public:

  BaseReqID req_id;             // Internal request ID
  ReqID     ext_req_id;         // External last assigned semi-unique request ID 
  uint64_t  ts_create;
  uint64_t  ts_expire;
  ErlNifPid pid;

  template <typename VsnCount>
  void Update(time_val now, uint32_t ttl_us, VsnCount& vsn)
  {
    ext_req_id = ReqID((++vsn) << s_bitsize) & s_hi_mask | (req_id & s_lo_mask);
    ts_create  = now.microseconds();
    ts_expire  = ts_create + ttl_us;
  }

  static BaseReqID DecodeReqID(ReqID id) { return id & s_lo_mask; }
};

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests in FIFO order 
//-----------------------------------------------------------------------------
template <typename BaseReqID, typename ReqID, bool FIFO = true>
struct BackLog  : CircularBuffer<RequestInfo<BaseReqID, ReqID>, false> {
  using BaseT   = CircularBuffer<RequestInfo<BaseReqID, ReqID>, false>;
  using Req     = RequestInfo<BaseReqID, ReqID>;
  using InitFun = void (*)(ReqID i, Req& req);

  explicit BackLog(size_t capacity)
  : BaseT(capacity, [](size_t i, Req& item) { item = Req(); item.req_id = i; })
  {
    static_assert(capacity <= std::numeric_limits<ReqID>::max());
    if constexpr (!FIFO)
      for (auto& r : m_buffer)
        m_available.emplace(r.req_id);
  }

  /// @brief Push the next available request to backlog
  /// @return nullptr if the backlog is full
  template <typename Vsn>
  Req const* Push(time_val now, uint32_t ttl_us, Vsn& vsn)
  {
    if (Full()) return nullptr;
    if constexpr (FIFO) {
      auto fun = [now, ttl_us, &vsn](Req& req) { req.Update(now, ttl, vsn); };
      return BaseT::Enqueue(fun);
    }
  }

  /// @brief Pop the head item from the backlog
  /// @return nullptr if the queue is empty
  Req* Pop() { return m_used.Dequeue(); }

  Req&       operator[](size_t i)       { assert(i < m_buffer.size()); return m_requests[i]; }
  const Req& operator[](size_t i) const { assert(i < m_buffer.size()); return m_requests[i]; }

  bool Full() const
  {
    if constexpr (FIFO)
      return BaseT::Full();
    else
      return m_available.size() == BaseT::Size();
  }

  bool Empty() const
  {
    if constexpr (FIFO)
      return BaseT::Empty();
    else
      return m_available.empty();
  }
private:
  std::set<BaseReqID> m_available;
};

//-----------------------------------------------------------------------------
// CircularBuffer
//-----------------------------------------------------------------------------
template <class T, bool Init = false>
struct BaseCircularBuffer
{
  explicit BaseCircularBuffer(size_t capacity) : m_buffer(capacity) {}

  template <typename InitFun>
  explicit BaseCircularBuffer(size_t capacity, InitFun init)
  {
    resize(capacity, init);
  }

  /// @brief Return true if this circular buffer is empty
  bool   Empty() const { return m_head == m_tail; }

  /// @brief Return true if this circular buffer is full
  bool   Full()  const { return m_head == (m_tail + 1) % m_buffer.size(); }

  /// @brief Return the remaining capacity of the buffer
  size_t Capacity() const { return m_buffer.size() - size(); }

  /// @brief Return the size of this circular buffer
  size_t Size()  const {
    auto   tail = m_tail >= m_head ? m_tail : m_buffer.size() - m_tail;
    return tail - m_head;
  }

  /// @brief Remove all items from the buffer and set its size to 0
  void Clear()
  {
    m_head = m_tail = 0;
    if (Init)
      for (auto& item : m_buffer)
        item = T();
  }

  /// @brief Resize the buffer to the given capacity, and initialize items
  ///        using given `init` initialization lambda.
  /// @param capacity  new buffer capacity 
  /// @param init      the lambda initializer `(size_t i, T& item) -> void`
  template <typename InitFun>
  void Resize(size_t capacity, InitFun init)
  {
    m_buffer.resize(capacity);
    m_buffer.shrink_to_fit();
    if (init)
      for (auto i = 0u; i < m_buffer.size(); ++i)
        init(i, std::ref(m_buffer[i]));
    else if (Init)
      for (auto i = 0u; i < m_buffer.size(); ++i)
        m_buffer[i] = T();
      
    m_head = m_tail = 0;
  }

protected:
  /// @brief Add an item to the circular buffer
  template <typename Fun>
  bool Enqueue(Fun const& fun)
  {
    if (Full()) [[unlikely]]
      return false;

    fun(m_buffer[m_tail]);  // insert item at back of buffer
    m_tail = ++m_tail % m_buffer.size(); // increment tail
  }

  /// @brief Remove the head item from this buffer and return it.
  /// @return nullptr if the queue is empty, otherwise the head item.
  T* Dequeue()
  {
    if (Empty())
      return nullptr;

    auto p = &m_buffer[m_head];
    if (Init)
      m_buffer[m_head] = T();            // set item at head to be empty
    m_head = ++m_head % m_buffer.size(); // move head foward

    return p;
  }

  /// @brief Return the front item (only valid if buffer is not empty)
  const T& Front() const { return m_buffer[m_head]; }
  T&       Front()       { return m_buffer[m_head]; }
  /// @brief Return the back item (only valid if buffer is not empty)
  const T& Back()  const { return m_buffer[(m_tail ? m_tail : m_buffer.size())-1]; }
  T&       Back()        { return m_buffer[(m_tail ? m_tail : m_buffer.size())-1]; }

protected:
  std::vector<T> m_buffer;
  size_t m_head = 0;
  size_t m_tail = 0;
  size_t m_capacity;
};

} // namespace arterial