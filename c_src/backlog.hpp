#pragma once

#include <vector>
#include <set>
#include <atomic>
#include <cstdint>
#include <limits>
#include <cassert>
#include <erl_nif.h>
#include "throttle.hpp"

namespace arterial {

using BaseReqID    = uint16_t;  // Internal request ID
using ReqID        = uint32_t;  // Request ID for sending over the wire

//-----------------------------------------------------------------------------
/// @brief Information about a request sent over the current connection
//-----------------------------------------------------------------------------
struct RequestInfo {
  static constexpr const uint32_t s_bitsize = sizeof(BaseReqID)*8;
  static constexpr const uint32_t s_lo_mask = (1u << s_bitsize)-1;
  static constexpr const uint32_t s_hi_mask = ~s_lo_mask;

  BaseReqID req_id     = 0;     // Internal request ID
  ReqID     ext_req_id = 0;     // External last assigned semi-unique request ID
  uint64_t  ts_create  = 0;
  uint64_t  ts_expire  = 0;
  ErlNifPid pid{};

  /// @brief Refresh this request's metadata when it's checked out for use.
  void Update(uint64_t now_us, uint32_t ttl_us, std::atomic<uint32_t>& vsn)
  {
    ext_req_id = (ReqID(++vsn) << s_bitsize & s_hi_mask) | (req_id & s_lo_mask);
    ts_create  = now_us;
    ts_expire  = now_us + ttl_us;
  }

  static BaseReqID DecodeReqID(ReqID id) { return BaseReqID(id & s_lo_mask); }
};

//-----------------------------------------------------------------------------
// Forward declarations
//-----------------------------------------------------------------------------
struct AbstractBackLog;
struct FIFOBackLog;
struct RandomAccessBackLog;

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests.
///
/// Concrete subclasses determine whether requests are checked back in FIFO
/// order (when the wire protocol carries no request ID) or by request ID
/// (when the wire protocol echoes the ID, so replies may be out of order).
//-----------------------------------------------------------------------------
struct AbstractBackLog {
  using Req = RequestInfo;

  explicit AbstractBackLog(size_t capacity)
  : m_buffer(capacity)
  {
    assert(capacity <= std::numeric_limits<BaseReqID>::max());
    for (size_t i = 0; i < capacity; ++i)
      m_buffer[i].req_id = BaseReqID(i);
  }

  virtual ~AbstractBackLog() = default;

  /// @brief Factory method used to create a FIFO/RandomAccess backlog
  static AbstractBackLog* Create(size_t capacity, bool fifo);

  /// @brief Checkout the next available request from the backlog to be sent
  /// to a server for processing.
  /// When a reply is received, use CheckIn() to add it back to the backlog.
  /// @return nullptr if the backlog is empty/full
  virtual Req* CheckOut(uint64_t now_us, uint32_t ttl_us, std::atomic<uint32_t>& vsn) = 0;

  /// @brief Check a previously checked-out request back into the backlog.
  /// @return nullptr if unsuccessful
  virtual Req* CheckIn(ReqID ext_req_id) = 0;

  /// @brief Reserve `n` request slots without sending anything yet.
  /// @return {true, ids} if `n` slots were available and reserved (checked
  /// out); otherwise {false, {}} and no slots are reserved. Each id is the
  /// request's wire-level ext_req_id (not its internal slot index), since
  /// that's what's needed both on the wire and to key a timeout registry
  /// across slot reuse.
  std::pair<bool, std::vector<ReqID>>
  UnsafeReserve(size_t n, uint64_t now_us = 0, uint32_t ttl_us = 0)
  {
    std::vector<ReqID> ids;
    ids.reserve(n);

    while (ids.size() < n) {
      auto req = CheckOut(now_us, ttl_us, m_vsn);
      if (!req) break;
      ids.push_back(req->ext_req_id);
    }

    if (ids.size() == n)
      return std::make_pair(true, std::move(ids));

    // Not enough free slots: roll back what we reserved.
    for (auto id : ids)
      CheckIn(id);

    return std::make_pair(false, std::vector<ReqID>());
  }

  virtual bool   Full()     const = 0;
  virtual bool   Empty()    const = 0;
  size_t         Capacity() const { return m_buffer.size(); }

  /// @brief List every currently checked-out-but-not-checked-in request's
  /// wire-level `ext_req_id`, in arbitrary order (callers needing FIFO
  /// send order should sort/zip against their own bookkeeping; this just
  /// enumerates "what's still outstanding"). Used by disconnect
  /// notification (see ConnectionPool::OnConnectionDown()) to tell a
  /// connection's owning processes which of their in-flight requests were
  /// never confirmed before the socket died.
  virtual std::vector<ReqID> OutstandingReqIDs() const = 0;

  /// @brief Bulk/cumulative ack (mode (f-2); see FIFOBackLog::CheckInUpTo
  /// for the real implementation). Meaningless for a random-access
  /// backlog -- replies there are already individually id-matched, so
  /// there's no FIFO "everything before this" to collapse -- hence the
  /// default no-op here; only FIFOBackLog overrides it.
  /// @return every `ext_req_id` released (oldest first), empty if
  /// `upto_ext_req_id` doesn't match any outstanding slot or this backlog
  /// doesn't support cumulative ack at all.
  virtual std::vector<ReqID> CheckInUpTo(ReqID /*upto_ext_req_id*/) { return {}; }

protected:
  std::vector<Req>      m_buffer;
  std::atomic<uint32_t> m_vsn{0};
};

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests in FIFO order.
///
/// It should be used in cases when the wire-level protocol doesn't support
/// passing a request ID in a request/reply, and the request/reply order is
/// strictly FIFO.
//-----------------------------------------------------------------------------
struct FIFOBackLog : AbstractBackLog
{
  explicit FIFOBackLog(size_t capacity)
  : AbstractBackLog(capacity)
  {}

  /// @brief Checkout the next available request from the tail of the backlog
  /// FIFO queue to be sent to a server for processing.
  /// @return nullptr if the backlog is full
  Req* CheckOut(uint64_t now_us, uint32_t ttl_us, std::atomic<uint32_t>& vsn) override
  {
    if (Full()) return nullptr;

    auto& req = m_buffer[m_tail];
    req.Update(now_us, ttl_us, vsn);
    m_tail = (m_tail + 1) % m_buffer.size();
    ++m_size;
    return &req;
  }

  /// @brief Pop the head item from the backlog.
  /// NOTE: the FIFO backlog is used when the wire protocol doesn't support
  /// passing request IDs to the server and back, so the reply is assumed to
  /// correspond to the oldest outstanding request.
  /// @return nullptr if the queue is empty
  Req* CheckIn(ReqID) override
  {
    if (Empty()) return nullptr;

    auto& req = m_buffer[m_head];
    m_head = (m_head + 1) % m_buffer.size();
    --m_size;
    return &req;
  }

  /// @brief Bulk/cumulative ack (mode (f-2)): pop every outstanding slot
  /// from the head up to and including the one whose `ext_req_id` equals
  /// `upto_ext_req_id`, in FIFO order. For a protocol whose acks carry a
  /// cumulative sequence number rather than per-message ids, the caller
  /// (Erlang side) is responsible for mapping that sequence number to the
  /// `ext_req_id` of the corresponding `checkout`-returned slot -- this
  /// only knows FIFO order, not the wire protocol's own numbering scheme.
  /// @return the `ext_req_id` of every slot popped (oldest first), empty
  /// if `upto_ext_req_id` doesn't match any currently outstanding slot
  /// (the backlog is left unmodified in that case -- this is "all or
  /// nothing": a caller acking an id that was already checked in, or was
  /// never outstanding, gets no partial effect).
  std::vector<ReqID> CheckInUpTo(ReqID upto_ext_req_id) override
  {
    std::vector<ReqID> popped;

    for (size_t i = 0, idx = m_head; i < m_size; ++i, idx = (idx + 1) % m_buffer.size()) {
      popped.push_back(m_buffer[idx].ext_req_id);
      if (m_buffer[idx].ext_req_id == upto_ext_req_id) {
        m_head  = (idx + 1) % m_buffer.size();
        m_size -= popped.size();
        return popped;
      }
    }

    return {}; // upto_ext_req_id not found among outstanding slots: no-op
  }

  bool Full()  const override { return m_size == m_buffer.size(); }
  bool Empty() const override { return m_size == 0; }

  std::vector<ReqID> OutstandingReqIDs() const override
  {
    std::vector<ReqID> ids;
    ids.reserve(m_size);
    for (size_t i = 0, idx = m_head; i < m_size; ++i, idx = (idx + 1) % m_buffer.size())
      ids.push_back(m_buffer[idx].ext_req_id);
    return ids;
  }

private:
  size_t m_head = 0;
  size_t m_tail = 0;
  size_t m_size = 0;
};

//-----------------------------------------------------------------------------
/// @brief BackLog maintains a backlog of used requests in random order.
///
/// It should be used in cases when the wire-level protocol supports passing
/// a request ID in a request/reply, so replies don't need to follow the
/// FIFO order of requests.
//-----------------------------------------------------------------------------
struct RandomAccessBackLog : AbstractBackLog
{
  explicit RandomAccessBackLog(size_t capacity) : AbstractBackLog(capacity)
  {
    for (size_t i = 0; i < capacity; ++i)
      m_available.insert(BaseReqID(i));
  }

  /// @brief Checkout the next available request slot to be sent to a server
  /// for processing.
  /// @return nullptr if the backlog is full
  Req* CheckOut(uint64_t now_us, uint32_t ttl_us, std::atomic<uint32_t>& vsn) override
  {
    if (Full()) return nullptr;

    auto idx = *m_available.begin();
    m_available.erase(m_available.begin());

    auto& req = m_buffer[idx];
    req.Update(now_us, ttl_us, vsn);
    return &req;
  }

  /// @brief Check a previously checked-out request back into the backlog,
  /// resolved by its externally-visible (wire) request ID.
  /// @return nullptr if unsuccessful (unknown/double check-in)
  Req* CheckIn(ReqID ext_req_id) override
  {
    auto req_id = RequestInfo::DecodeReqID(ext_req_id);
    if (req_id >= Capacity()) [[unlikely]]
      return nullptr;

    auto [_, inserted] = m_available.insert(req_id);
    if (!inserted) [[unlikely]]
      return nullptr; // double check-in

    return &m_buffer[req_id];
  }

  bool Full()  const override { return m_available.empty(); }
  bool Empty() const override { return m_available.size() == Capacity(); }

  std::vector<ReqID> OutstandingReqIDs() const override
  {
    std::vector<ReqID> ids;
    ids.reserve(Capacity() - m_available.size());
    for (BaseReqID i = 0; i < Capacity(); ++i)
      if (m_available.find(i) == m_available.end())
        ids.push_back(m_buffer[i].ext_req_id);
    return ids;
  }

private:
  std::set<BaseReqID> m_available;
};

inline AbstractBackLog* AbstractBackLog::Create(size_t capacity, bool fifo)
{
  return fifo ? static_cast<AbstractBackLog*>(new FIFOBackLog(capacity))
              : static_cast<AbstractBackLog*>(new RandomAccessBackLog(capacity));
}

} // namespace arterial
