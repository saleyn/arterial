#pragma once

#include <forward_list>
#include <vector>
#include <set>
#include <cstdint>
#include <limits>
#include "nifpp.h"

namespace arterial {

using BaseReqID    = uint16_t;  // Internal request ID
using ReqID        = uint32_t;  // Request ID for sending over the wire

//-----------------------------------------------------------------------------
/// @brief Information about a request sent over the current connection
//-----------------------------------------------------------------------------
class RequestInfo {
  static constexpr const uint32_t s_bitsize = sizeof(BaseReqID)*8;
  static constexpr const uint32_t s_lo_mask = (1 << s_bitsize)-1;
  static constexpr const uint32_t s_hi_mask = s_lo_mask << s_bitsize;
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
// Forward declarations
//-----------------------------------------------------------------------------
template <class T, bool Init>
struct BaseCircularBuffer;

struct AbstractBackLog;
struct FIFOBackLog;
struct RandomAccessBackLog;

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests in FIFO order.
///
/// It should be used in cases when the wire-level protocol doesn't support
/// passing a request ID in a request/reply, and the request/reply order is
/// strictly FIFO. 
//-----------------------------------------------------------------------------
struct FIFOBackLog : protected AbstractBackLog
{
  using BaseT = AbstractBackLog;

  explicit FIFOBackLog(size_t capacity)
  : AbstractBackLog(capacity)
  {}

  /// @brief Checkout the next available request from the tail of the backlog
  /// FIFO queue to be sent to a server for processing.
  /// When a reply is received, use CheckIn() function to added it back to the
  /// backlog.
  /// @return nullptr if the backlog is full
  Req* CheckOut(time_val now, uint32_t ttl_us, uint32_t& vsn) override
  {
    if (IsFull()) return nullptr;
    auto fun = [now, ttl_us, &vsn](Req& req) { req.Update(now, ttl_us, vsn); };
    return BaseT::Enqueue(fun);
  }

  /// @brief Pop the head item from the backlog.
  /// NOTE: The FIFO backlog is used when the wire protocol doesn't support 
  /// passing request IDs to the server and back. It's expected that the replies
  /// to requests are in the FIFO order, so we don't need to check the ReqID,
  /// because the client doesn't know it, and it's inferred from the backlog's
  /// queue.  
  /// @return nullptr if the queue is empty
  Req* CheckIn(ReqID) override { return this->Dequeue(); }

  bool Full()  const override { return IsFull();  }
  bool Empty() const override { return BaseT::Empty(); }

private:
  bool IsFull()  const override { return BaseT::Full();  }
};

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests in random order.
///
/// It should be used in cases when the wire-level protocol supports
/// passing a request ID in a request/reply, so the replies don't follow the
/// FIFO order of requests.
//-----------------------------------------------------------------------------
struct RandomAccessBackLog : protected AbstractBackLog
{
  explicit RandomAccessBackLog(size_t capacity) : AbstractBackLog(capacity) {}

  /// @brief Checkout the next available request from the backlog's capacity
  /// to be sent to a server for processing.
  /// When a reply is received, use CheckIn() function to added it back to the
  /// backlog.
  /// @return nullptr if the backlog is full
  Req* CheckOut(time_val now, uint32_t ttl_us, uint32_t& vsn) override
  {
    if (Empty()) return nullptr;
    
    auto idx = *m_available.begin();
    m_available.erase(m_available.begin());

    auto res = &this->m_buffer[idx];
    res->Update(now, ttl_us, vsn);
    return res;
  }

  /// @brief Pop the head item from the backlog
  /// @return nullptr if the queue is empty
  Req* CheckIn(ReqID ext_req_id) override
  {
    auto req_id = RequestInfo::DecodeReqID(ext_req_id);
    if (req_id >= this->Capacity()) [[unlikely]]
      return nullptr;

    m_available.insert(req_id);

    return &this->m_buffer[req_id];
  }

  bool Full()  const override { return m_available.empty(); }
  bool Empty() const override { return m_available.size() == BaseT::Size(); }
private:
  std::set<BaseReqID> m_available;
};

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests in FIFO order 
//-----------------------------------------------------------------------------
struct AbstractBackLog : protected BaseCircularBuffer<RequestInfo, false>
{
  using BaseT   = BaseCircularBuffer<RequestInfo, false>;
  using Req     = RequestInfo;
  using InitFun = void (*)(ReqID i, Req& req);

  /// @brief Factory method used to create a FIFO/RandomAccess backlog
  static AbstractBackLog* Create(size_t capacity, bool fifo)
  {
    return fifo
         ? dynamic_cast<AbstractBackLog*>(new FIFOBackLog(capacity))
         : dynamic_cast<AbstractBackLog*>(new RandomAccessBackLog(capacity));
  }
protected:
  explicit AbstractBackLog(size_t capacity)
  : BaseT(capacity, [](size_t i, Req& item) { item = Req(); item.req_id = i; })
  {
    assert(capacity <= std::numeric_limits<BaseReqID>::max());
  }

  /// @brief Checkout the next available request from the backlog to be sent
  /// to a server for processing.
  /// When reply is received, use CheckIn() function to added it back to the
  /// backlog. 
  /// @return nullptr if the backlog is empty
  virtual Req* CheckOut(time_val now, uint32_t ttl_us, uint32_t& vsn) = 0;

  /// @brief CheckIn a previously checked out request back to the pool.
  /// @return nullptr if unsuccessful
  virtual Req* CheckIn(ReqID ext_req_id) = 0;

  Req&       operator[](size_t i)       { assert(i < m_buffer.size()); return m_requests[i]; }
  const Req& operator[](size_t i) const { assert(i < m_buffer.size()); return m_requests[i]; }

  virtual bool Full()  const = 0;
  virtual bool Empty() const = 0;
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
  Req* Enqueue(Fun const& fun)
  {
    if (Full()) [[unlikely]]
      return false;

    auto& req = m_buffer[m_tail];
    fun(req);  // insert item at back of buffer
    m_tail = ++m_tail % m_buffer.size(); // increment tail
    return &req;
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