#pragma once
// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file   reactor.hpp
/// \brief  Arterial I/O Reactor — single-threaded async I/O engine.
///
/// The Reactor runs on a dedicated background thread, owns an io_uring ring
/// (Linux) or kqueue (macOS), and drives all socket connect / read / write /
/// timeout operations asynchronously.  From the Erlang NIF side every
/// connection request is non-blocking: the NIF enqueues a ConnectRequest and
/// returns immediately; the reactor thread completes the connect, then posts
/// the result back to the caller's Erlang process.
///
/// Design
/// ------
///   • One Reactor singleton per NIF (one thread per PoolContext, or one
///     global reactor shared across pools — both topologies are supported).
///   • All operations arrive via a lock-free command queue (ReactorCmd).
///   • The reactor thread loops on Wait(), dispatches CQE/kevent results,
///     and calls per-fd Handler callbacks.
///   • Timeouts are driven by timerfd/EVFILT_TIMER registered in the same
///     multiplexer, so they arrive as ordinary CQEs with no extra threads.
///
/// Handler model
/// -------------
///   Each registered fd is associated with a FdEntry that carries:
///     - on_readable  : called when the fd is readable
///     - on_writable  : called when the fd is writable (one-shot, re-armed on request)
///     - on_error     : called on HUP/ERR
///     - on_timeout   : called when a timeout registered for this fd fires
///     - user_data    : arbitrary pointer for the handler callbacks
///
/// Connect flow (from NIF thread → reactor thread → Erlang process)
/// -----------------------------------------------------------------
///   1. NIF calls Reactor::connect(fd, addr, timeout_ms, pid, opaque)
///      This enqueues a ConnectCmd into the command ring and writes to
///      the wakeup eventfd.
///   2. Reactor thread wakes up, prepares IORING_OP_CONNECT (or registers
///      write-interest via epoll/kqueue for EINPROGRESS sockets), and arms
///      a one-shot timeout via timerfd/EVFILT_TIMER.
///   3a. Connect CQE fires → cancel timeout → send {ok, SlotId} to pid.
///   3b. Timeout fires first → cancel connect → send {error, timeout} to pid.
///
/// All message sends to Erlang are done via enif_send() inside the reactor
/// thread using a dedicated ErlNifEnv per send (the reactor holds a NULL env
/// suitable for calls from non-Erlang threads).
//-----------------------------------------------------------------------------
// Copyright (c) 2026 Serge Aleynikov <saleyn@gmail.com>
//-----------------------------------------------------------------------------

#include "reactor_platform.hpp"

#include <atomic>
#include <cassert>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <netinet/in.h>
#include <sys/ioctl.h>
#include <sys/socket.h>

#ifdef REACTOR_TEST_STUB_NIF
#include "erl_nif_stub.h"
#else
#include <erl_nif.h>
#endif

namespace arterial {

//==============================================================================
// Handler callbacks
//==============================================================================

struct FdEntry;

/// Called when fd is readable.  Return value: 0 = keep, <0 = remove fd.
using ReadHandler    = std::move_only_function<int(int fd, void* user_data)>;
/// Called when fd is writable (one-shot, re-arm with arm_write).
using WriteHandler   = std::move_only_function<int(int fd, void* user_data)>;
/// Called on error / HUP.
using ErrorHandler   = std::move_only_function<void(int fd, void* user_data)>;
/// Called when the per-fd timeout fires.
using TimeoutHandler = std::move_only_function<void(int fd, void* user_data)>;

/// io_uring only: called when a linked CONNECT+LINK_TIMEOUT CQE pair completes.
/// @param mask  REACTOR_EV_CONNECT alone = success;
///              | REACTOR_EV_HUP = timed out;
///              | REACTOR_EV_ERR = refused/reset.
using ConnectHandler = std::move_only_function<void(uint32_t mask)>;

struct FdEntry {
  int            fd          = -1;
  int            timer_id    = -1;   ///< -1 = no timeout registered
  void*          user_data   = nullptr;
  ReadHandler    on_readable;
  WriteHandler   on_writable;
  ErrorHandler   on_error;
  TimeoutHandler on_timeout;
  ConnectHandler on_connect;         ///< io_uring linked-connect only
  bool           write_armed = false;
};

//==============================================================================
// Command queue
// A trivially-copyable command struct + a bounded MPSC ring lets NIF threads
// post commands to the reactor thread without locks on the hot path.
//==============================================================================

enum class CmdType : uint8_t {
  Nop           = 0,
  AddFd         = 1,   ///< register a new fd with read interest
  RemoveFd      = 2,   ///< deregister an fd and close it
  ArmWrite      = 3,   ///< arm one-shot write-readiness for an existing fd
  SetTimeout    = 4,   ///< set/reset per-fd timeout
  CancelTimeout = 5,
  Connect       = 6,   ///< initiate async connect (see ConnectCmd)
  Stop          = 255,
};

struct ConnectParams {
  int                fd;
  struct sockaddr_in addr;
  uint64_t           timeout_ms;
  ErlNifPid          caller_pid;
  uint32_t           stripe_id;
  uint32_t           slot_id;
};

struct SetTimeoutParams {
  int      fd;
  uint64_t timeout_ms;
};

struct ReactorCmd {
  CmdType          type    = CmdType::Nop;
  int              fd      = -1;   // add_fd / remove_fd / arm_write / CancelTimeout
  ConnectParams    connect = {};   // connect
  SetTimeoutParams timeout = {};   // set_timeout
};

//------------------------------------------------------------------------------
// Simple wait-free MPSC ring (single consumer = reactor thread).
// Capacity must be power-of-2.
//------------------------------------------------------------------------------
template <typename T>
class MpscRing {
  struct Slot {
    std::atomic<unsigned> seq;
    T                     val;
  };

  unsigned                          m_cap;
  unsigned                          m_mask;
  alignas(64) std::atomic<unsigned> m_head{0};
  alignas(64) std::atomic<unsigned> m_tail{0};
  std::unique_ptr<Slot[]>           m_slots;

public:
  explicit MpscRing(unsigned cap = 4096)
  {
    cap    = upper_power_of_two(cap);
    m_cap  = cap;
    m_mask = cap - 1;
    m_slots.reset(new Slot[cap]);
    for (unsigned i = 0; i < m_cap; ++i)
      m_slots[i].seq.store(i, std::memory_order_relaxed);
  }

  bool push(const T& v)
  {
    unsigned head = m_head.load(std::memory_order_relaxed);
    for (;;) {
      auto&   s   = m_slots[head & m_mask];
      auto    seq = s.seq.load(std::memory_order_acquire);
      auto    diff = (int)seq - (int)head;
      if (diff == 0) {
        if (m_head.compare_exchange_weak(head, head + 1, std::memory_order_relaxed)) {
          s.val = v;
          s.seq.store(head + 1, std::memory_order_release);
          return true;
        }
      } else if (diff < 0) {
        return false; // full
      } else {
        head = m_head.load(std::memory_order_relaxed);
      }
    }
  }

  bool pop(T& v)
  {
    unsigned tail = m_tail.load(std::memory_order_relaxed);
    Slot&    s    = m_slots[tail & m_mask];
    unsigned seq  = s.seq.load(std::memory_order_acquire);
    int      diff = (int)seq - (int)(tail + 1);
    if (diff != 0) return false;
    v = s.val;
    m_tail.store(tail + 1, std::memory_order_relaxed);
    s.seq.store(tail + m_cap, std::memory_order_release);
    return true;
  }
};

//==============================================================================
// Reactor
//==============================================================================

struct Reactor {
  static constexpr int s_max_events     = 256;
  static constexpr int s_cmd_ring_cap   = 4096;
  static constexpr int s_poll_ms        = -1;         ///< block until event
  static constexpr int s_default_fd_vec = 64 * 1024;  ///< default vector pre-size

  explicit Reactor(std::string ident         = "arterial_reactor",
                   int         fd_vec_size   = s_default_fd_vec,
                   unsigned    cmds_ring_cap = 4096);

  ~Reactor() { stop(); }

  /// Start the reactor thread; blocks until the thread signals ready.
  void start(ErlNifPid owner_pid = ErlNifPid{});

  /// Stop the reactor thread and wait for it to exit.
  void stop();

  bool running() const { return m_running.load(std::memory_order_relaxed); }
  bool valid()   const { return m_valid; }

  //----------------------------------------------------------------------------
  // Registration API (thread-safe — may be called from any thread)
  //----------------------------------------------------------------------------

  /// Store handlers for fd without registering with the kernel multiplexer.
  void register_handlers(int            fd,
                         ReadHandler    on_read,
                         ErrorHandler   on_error,
                         WriteHandler   on_write   = {},
                         TimeoutHandler on_timeout = {},
                         void*          user_data  = nullptr);

  /// Register fd for persistent read notifications.
  void add_fd(int            fd,
              ReadHandler    on_read,
              ErrorHandler   on_error,
              WriteHandler   on_write   = {},
              TimeoutHandler on_timeout = {},
              void*          user_data  = nullptr);

  /// Deregister fd from the reactor (closes it).
  void remove_fd(int fd);

  /// Arm one-shot write-readiness on an already-registered fd.
  void arm_write(int fd);

  /// Set / reset a timeout for an fd.  Passing 0 cancels any existing timeout.
  void set_timeout(int fd, uint64_t timeout_ms);

  /// Initiate an async TCP connect.
  void connect(int              fd,
               struct sockaddr_in addr,
               uint64_t         timeout_ms,
               ErlNifPid        caller_pid,
               uint32_t         stripe_id,
               uint32_t         slot_id,
               ReadHandler      on_readable,
               WriteHandler     on_writable,
               ErrorHandler     on_error,
               TimeoutHandler   on_timeout,
               void*            user_data);

  //----------------------------------------------------------------------------
  // Accessors
  //----------------------------------------------------------------------------
  const std::string& ident()  const { return m_ident; }
  reactor_handle_t   handle() const { return m_handle; }

#if defined(REACTOR_BACKEND_URING)
  struct io_uring* ring() { return reactor_uring(m_handle); }
#endif

private:
  void close_handles();  ///< close m_handle and m_wakeup_rd; safe to call multiple times
  void post(const ReactorCmd& cmd);
  void run_loop();
  void dispatch(const reactor_event_t& ev);
  void drain_commands();
  void do_add_fd(int fd);
  void do_remove_fd(int fd);
  void do_arm_write(int fd);
  void do_set_timeout(int fd, uint64_t timeout_ms);
  void do_cancel_timeout(int fd);
  void fire_timeout(int fd);
  void do_connect(const ConnectParams& p);
  void send_connect_result(const ErlNifPid& pid, uint32_t stripe, uint32_t slot, bool ok);
  void send_timeout(const ErlNifPid& pid, uint32_t stripe, uint32_t slot);
  void send_reactor_exit(int err);

  FdEntry* find_entry(int fd);
  FdEntry& ensure_entry(int fd);
  void     clear_entry(int fd);

  //----------------------------------------------------------------------------
  // Timer id → owner fd mapping.
  // kqueue idents are sparse (0x7000'0000+) → unordered_map.
  // Linux timerfd values are small OS fds → vector for O(1) cache-friendly access.
  //----------------------------------------------------------------------------
#if defined(REACTOR_BACKEND_KQUEUE)
  struct TimerMap {
    std::unordered_map<int, int> data;
    void set(int tid, int fd)      { data[tid] = fd; }
    int  get(int tid) const        { auto it = data.find(tid); return it != data.end() ? it->second : -1; }
    void clear(int tid)            { data.erase(tid); }
    bool contains(int tid) const   { return data.count(tid) > 0; }
  };
#else
  struct TimerVec {
    std::vector<int> data;
    void set(int tid, int fd)      { grow(tid); data[tid] = fd; }
    int  get(int tid) const        { return (tid >= 0 && tid < (int)data.size()) ? data[tid] : -1; }
    void clear(int tid)            { if (tid >= 0 && tid < (int)data.size()) data[tid] = -1; }
    bool contains(int tid) const   { return get(tid) >= 0; }
  private:
    void grow(int tid)
    {
      if (tid >= (int)data.size())
        data.resize(std::max(tid + 1, (int)data.size() * 2), -1);
    }
  };
  using TimerMap = TimerVec;
#endif

  //----------------------------------------------------------------------------
  // State
  //----------------------------------------------------------------------------
  bool                             m_valid{true};
  std::string                      m_ident;
  reactor_handle_t                 m_handle;
  int                              m_wakeup_rd;  ///< eventfd read end
  int                              m_wakeup_wr;  ///< eventfd write end (== wakeup_rd on Linux)
  std::atomic<bool>                m_running;
  std::thread                      m_thread;
  ErlNifPid                        m_owner_pid{};

  std::mutex                       m_ready_mu;
  std::condition_variable          m_ready_cv;
  bool                             m_ready{false};

  std::vector<FdEntry>             m_entries;
  MpscRing<ReactorCmd>             m_cmds;

  std::mutex                       m_entries_overflow_mu;
  std::unordered_map<int, FdEntry> m_entries_overflow;

  std::mutex                       m_timers_mu;
  TimerMap                         m_timer_to_fd;
};

} // namespace arterial
