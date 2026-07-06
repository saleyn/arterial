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
#include <sys/socket.h>

#ifdef REACTOR_TEST_STUB_NIF
#  include "erl_nif_stub.h"
#else
#  include <erl_nif.h>
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
  CmdType type = CmdType::Nop;
  // Payload — we don't use a union to avoid UB; zero-init unused fields.
  int              fd      = -1;   // add_fd / remove_fd / arm_write / CancelTimeout
  ConnectParams    connect = {};   // connect
  SetTimeoutParams timeout = {};   // set_timeout
};

//------------------------------------------------------------------------------
// Simple wait-free MPSC ring (single consumer = reactor thread).
// Capacity must be power-of-2.
//------------------------------------------------------------------------------
template <typename T, unsigned Cap>
class MpscRing {
  static_assert((Cap & (Cap-1)) == 0, "Cap must be a power of 2");
  static constexpr unsigned kMask = Cap - 1;

  struct Slot {
    std::atomic<unsigned> seq;
    T                     val;
  };

  alignas(64) std::atomic<unsigned> m_head{0};
  alignas(64) std::atomic<unsigned> m_tail{0};
  alignas(64) Slot                  m_slots[Cap];

public:
  MpscRing()
  {
    for (unsigned i = 0; i < Cap; ++i)
      m_slots[i].seq.store(i, std::memory_order_relaxed);
  }

  bool push(const T& v)
  {
    unsigned head = m_head.load(std::memory_order_relaxed);
    for (;;) {
      auto&   s = m_slots[head & kMask];
      auto  seq = s.seq.load(std::memory_order_acquire);
      auto diff = (int)seq - (int)head;
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
    Slot& s = m_slots[tail & kMask];
    unsigned seq = s.seq.load(std::memory_order_acquire);
    int diff = (int)seq - (int)(tail + 1);
    if (diff != 0) return false;
    v = s.val;
    m_tail.store(tail +  1, std::memory_order_relaxed);
    s.seq.store(tail + Cap, std::memory_order_release);
    return true;
  }
};

//==============================================================================
// Reactor
//==============================================================================

class Reactor {
public:
  static constexpr int kMaxEvents    = 256;
  static constexpr int kCmdRingCap   = 4096;
  static constexpr int kPollMs       = -1;         ///< block until event (wakeup fd unblocks on commands)
  static constexpr int kDefaultFdVec = 64 * 1024; ///< default vector pre-size

  //----------------------------------------------------------------------------
  /// Create a reactor.  Does NOT start the background thread yet.
  /// @param ident       Human-readable name (for logging/debugging).
  /// @param fd_vec_size Pre-allocated vector size for fast fd lookup (no mutex
  ///                    for fds < fd_vec_size).  Fds ≥ fd_vec_size spill into
  ///                    a mutex-guarded overflow map.  Default: 64k entries.
  //----------------------------------------------------------------------------
  explicit Reactor(std::string ident = "arterial_reactor",
                   int fd_vec_size   = kDefaultFdVec)
    : m_ident(std::move(ident))
    , m_handle(reactor_create())
    , m_wakeup_rd(reactor_eventfd_create())
    , m_wakeup_wr(reactor_eventfd_write_fd(m_wakeup_rd))
    , m_running(false)
    , m_entries(fd_vec_size)  // value-init: each FdEntry has fd=-1
  {
    // Register the wakeup fd for read events (persistent, edge-triggered).
    reactor_add(m_handle, m_wakeup_rd, REACTOR_EV_IN | REACTOR_EV_ET);
  }

  ~Reactor() { stop(); }

  /// Start the reactor thread and block until it signals ready.
  ///
  /// @param owner_pid  Erlang pid to receive
  ///   `{arterial_reactor_exit, Ident, Reason}` if the reactor loop exits
  ///   for any reason other than an explicit stop() call.
  ///   Pass a zero-initialised ErlNifPid{} (the default) to opt out.
  ///
  /// The reactor thread sets the ready condition before entering its first
  /// reactor_wait() call, guaranteeing that any add_fd/connect/set_timeout
  /// posted immediately after start() returns will be seen by the reactor
  /// on the very first drain_commands() cycle — no race between the caller
  /// and the reactor on startup.
  void start(ErlNifPid owner_pid = ErlNifPid{})
  {
    bool expected = false;
    if (!m_running.compare_exchange_strong(expected, true))
      return; // already running

    m_owner_pid = owner_pid;
    std::unique_lock<std::mutex> lk(m_ready_mu);
    m_ready = false;
    m_thread = std::thread(&Reactor::run_loop, this);
    // Block until the reactor thread signals it has entered its event loop.
    m_ready_cv.wait(lk, [this]{ return m_ready; });
  }

  /// stop the reactor thread and wait for it to exit.
  void stop()
  {
    bool expected = true;
    if (!m_running.compare_exchange_strong(expected, false))
      return;
    ReactorCmd cmd; cmd.type = CmdType::Stop;
    post(cmd);
    if (m_thread.joinable()) m_thread.join();
    reactor_eventfd_close(m_wakeup_rd);
    reactor_destroy(m_handle);
  }

  bool running() const { return m_running.load(std::memory_order_relaxed); }

  //----------------------------------------------------------------------------
  // Registration API (thread-safe — may be called from any thread)
  //----------------------------------------------------------------------------

  /// Store handlers for fd without registering with the kernel multiplexer.
  /// Used by arm_connect to set up write/error handlers before arm_write
  /// registers EPOLLOUT — avoids a redundant EPOLLIN poll on connecting sockets.
  void register_handlers(int fd,
                        ReadHandler    on_read,
                        ErrorHandler   on_error,
                        WriteHandler   on_write   = {},
                        TimeoutHandler on_timeout = {},
                        void*          user_data  = nullptr)
  {
    auto& e       = ensure_entry(fd);
    e.fd          = fd;
    e.user_data   = user_data;
    e.on_readable = std::move(on_read);
    e.on_writable = std::move(on_write);
    e.on_error    = std::move(on_error);
    e.on_timeout  = std::move(on_timeout);
    // No command posted — kernel registration happens separately.
  }

  /// Register fd for persistent read notifications.
  void add_fd(int fd,
             ReadHandler    on_read,
             ErrorHandler   on_error,
             WriteHandler   on_write   = {},
             TimeoutHandler on_timeout = {},
             void*          user_data  = nullptr)
  {
    {
      auto& e       = ensure_entry(fd);  // locking handled inside for overflow fds
      e.fd          = fd;
      e.user_data   = user_data;
      e.on_readable = std::move(on_read);
      e.on_writable = std::move(on_write);
      e.on_error    = std::move(on_error);
      e.on_timeout  = std::move(on_timeout);
    }
    ReactorCmd cmd{}; cmd.type = CmdType::AddFd; cmd.fd = fd;
    post(cmd);
  }

  /// Deregister fd from the reactor (closes it).
  void remove_fd(int fd)
  {
    ReactorCmd cmd{}; cmd.type = CmdType::RemoveFd; cmd.fd = fd;
    post(cmd);
  }

  /// Arm one-shot write-readiness on an already-registered fd.
  void arm_write(int fd)
  {
    ReactorCmd cmd{}; cmd.type = CmdType::ArmWrite; cmd.fd = fd;
    post(cmd);
  }

  /// Set / reset a timeout for an fd.  When it fires, on_timeout() is called.
  /// Passing 0 cancels any existing timeout.
  void set_timeout(int fd, uint64_t timeout_ms)
  {
    if (timeout_ms == 0) {
      ReactorCmd cmd{}; cmd.type = CmdType::CancelTimeout; cmd.fd = fd;
      post(cmd);
    } else {
      ReactorCmd cmd{}; cmd.type = CmdType::SetTimeout;
      cmd.timeout = {fd, timeout_ms};
      post(cmd);
    }
  }

  //----------------------------------------------------------------------------
  // Connect API (thread-safe)
  //
  // Initiate an async TCP connect.  The caller must create a non-blocking
  // socket (socket() + fcntl(O_NONBLOCK)) but must NOT call connect() —
  // the reactor thread performs connect() itself on the socket fd, so this
  // NIF call is truly non-blocking (no syscall with potential blocking).
  //
  // When the connection completes or times out the reactor sends to caller_pid:
  //   success → {arterial_event, StripeId, SlotId, connect_result, ok}
  //   failure → {arterial_event, StripeId, SlotId, connect_result, connect_failed}
  //   timeout → {arterial_event, StripeId, SlotId, timeout}
  //
  // on_readable / on_writable / on_error / on_timeout are installed as
  // the I/O handlers for this fd after the connect completes.
  //----------------------------------------------------------------------------
  void connect(int            fd,
               struct sockaddr_in addr,
               uint64_t       timeout_ms,
               ErlNifPid      caller_pid,
               uint32_t       stripe_id,
               uint32_t       slot_id,
               ReadHandler    on_readable,
               WriteHandler   on_writable,
               ErrorHandler   on_error,
               TimeoutHandler on_timeout,
               void*          user_data)
  {
    {
      auto& e        = ensure_entry(fd);  // locking handled inside for overflow fds
      e.fd           = fd;
      e.user_data    = user_data;
      e.on_readable  = std::move(on_readable);
      e.on_writable  = std::move(on_writable);
      e.on_error     = std::move(on_error);
      e.on_timeout   = std::move(on_timeout);
    }
    ReactorCmd cmd{};
    cmd.type = CmdType::Connect;
    cmd.connect = { fd, addr, timeout_ms, caller_pid, stripe_id, slot_id };
    post(cmd);
  }

  //----------------------------------------------------------------------------
  // Accessors
  //----------------------------------------------------------------------------
  const std::string& ident()  const { return m_ident; }
  reactor_handle_t   handle() const { return m_handle; }

#if defined(REACTOR_BACKEND_URING)
  struct io_uring* ring() { return reactor_uring(m_handle); }
#endif

private:
  //----------------------------------------------------------------------------
  // Internal helpers
  //----------------------------------------------------------------------------

  void post(const ReactorCmd& cmd)
  {
    while (!m_cmds.push(cmd)) {
      // Ring full — spin briefly (should be extremely rare).
      std::this_thread::yield();
    }
    uint64_t one = 1;
    reactor_eventfd_write(m_wakeup_wr, one);
  }

  //----------------------------------------------------------------------------
  // Reactor loop
  //----------------------------------------------------------------------------
  void run_loop()
  {
    reactor_event_t events[kMaxEvents];

    // Signal the caller of start() that we are ready.
    // This happens before the first reactor_wait() so any commands posted
    // immediately after start() returns are guaranteed to be seen.
    {
      std::lock_guard<std::mutex> lk(m_ready_mu);
      m_ready = true;
    }
    m_ready_cv.notify_one();

    // Drain any commands that arrived before the thread started.
    drain_commands();

    bool abnormal_exit = false;
    int  exit_errno    = 0;

    while (m_running.load(std::memory_order_relaxed)) {
      int nev = reactor_wait(m_handle, events, kMaxEvents, kPollMs);
      if (nev < 0 && errno != EINTR) {
        abnormal_exit = true;
        exit_errno    = errno;
        break;
      }

      // Drain commands after wait so wakeup-triggered commands are processed
      // in the same cycle that woke us, not deferred to the next iteration.
      drain_commands();

      for (int i = 0; i < nev; ++i)
        dispatch(events[i]);
    }

    // Drain any last commands before exit.
    drain_commands();

    // Notify the owner if the loop exited for a reason other than stop().
    // stop() sets m_running=false before the break condition fires, so
    // a normal shutdown has m_running==false AND abnormal_exit==false.
    if (abnormal_exit)
      send_reactor_exit(exit_errno);
  }

  void dispatch(const reactor_event_t& ev)
  {
    int      fd   = reactor_ev_fd(ev);
    uint32_t mask = reactor_ev_mask(ev);

    if (fd == m_wakeup_rd) {
      uint64_t val;
      reactor_eventfd_read(m_wakeup_rd, val);
      return;
    }

    // Check if this is a timer fd.  Extract owner under the timer lock,
    // then release before calling fire_timeout to avoid lock inversion
    // (fire_timeout → do_cancel_timeout → m_timers_mu, find_entry → m_entries_mu).
    {
      int owner_fd = -1;
      {
        std::lock_guard<std::mutex> lk(m_timers_mu);
        owner_fd = m_timer_to_fd.get(fd);
      }
      if (owner_fd >= 0) {
        uint64_t exp = 0;
        reactor_timerfd_read(fd, exp);
        fire_timeout(owner_fd);
        return;
      }
    }

    // Regular I/O fd.
    FdEntry* e = find_entry(fd);
    if (!e) return;

    // io_uring linked-connect CQE (REACTOR_EV_CONNECT synthetic flag).
    if (mask & REACTOR_EV_CONNECT) {
      if (e->on_connect) {
        ConnectHandler cb = std::move(e->on_connect);
        e->on_connect = {};
        cb(mask);
      }
      return;
    }

    // When write is armed (async connect in progress) any readiness event —
    // including EPOLLHUP/EPOLLERR — must go through on_writable first so it
    // can call getsockopt(SO_ERROR) and report the real outcome.
    // Only route to on_error when no write is pending.
    if (e->write_armed && e->on_writable) {
      e->write_armed = false;
      int rc = e->on_writable(fd, e->user_data);
      if (rc < 0) { do_remove_fd(fd); return; }
      // After connect completes, fall through to check for immediately
      // available read data in the same event.
    } else if (reactor_is_error(mask)) {
      if (e->on_error) e->on_error(fd, e->user_data);
      return;
    }
    if (reactor_is_readable(mask) && e->on_readable) {
      int rc = e->on_readable(fd, e->user_data);
      if (rc < 0) { do_remove_fd(fd); return; }
    }
    // Second writable check: handles re-arm after on_writable returned 0
    // and write_armed was set again (e.g. partial send).
    if (reactor_is_writable(mask) && e->on_writable && e->write_armed) {
      e->write_armed = false;
      int rc = e->on_writable(fd, e->user_data);
      if (rc < 0) do_remove_fd(fd);
    }
  }

  //----------------------------------------------------------------------------
  // Command dispatch (runs on reactor thread only)
  //----------------------------------------------------------------------------
  void drain_commands()
  {
    ReactorCmd cmd;
    while (m_cmds.pop(cmd)) {
      switch (cmd.type) {
        case CmdType::Stop:          m_running.store(false);           return;
        case CmdType::AddFd:         do_add_fd   (cmd.fd);             break;
        case CmdType::RemoveFd:      do_remove_fd(cmd.fd);             break;
        case CmdType::ArmWrite:      do_arm_write(cmd.fd);             break;
        case CmdType::SetTimeout:    do_set_timeout(cmd.timeout.fd,
                                                    cmd.timeout.timeout_ms); break;
        case CmdType::CancelTimeout: do_cancel_timeout(cmd.fd);        break;
        case CmdType::Connect:       do_connect(cmd.connect);          break;
        default: break;
      }
    }
  }

  void do_add_fd(int fd)
  {
    // Register for read interest.  Only adds EPOLLIN if on_readable is set.
    FdEntry* e = find_entry(fd);
    if (!e || e->fd < 0) return;
    if (!e->on_readable) return;  // arm_connect case: no EPOLLIN needed yet

    // Use LEVEL-TRIGGERED (no EPOLLET) for data sockets so that any data
    // arriving during the cancel+re-add window of reactor_mod is not silently
    // lost.  With level-triggered polling, the next reactor_wait iteration
    // re-fires EPOLLIN as long as unread bytes remain in the kernel buffer,
    // eliminating the race between arm_read and in-flight server replies.
    try {
      reactor_mod(m_handle, fd, REACTOR_EV_IN | REACTOR_EV_RDHUP);
    } catch (...) {}
  }

  void do_remove_fd(int fd)
  {
    reactor_del(m_handle, fd);
    do_cancel_timeout(fd);
    clear_entry(fd);
    ::close(fd);
  }

  void do_arm_write(int fd)
  {
    FdEntry* e = find_entry(fd);
    if (!e) return;
    e->write_armed = true;
    // Include EPOLLIN only when a read handler exists (post-connect I/O).
    // During connect phase, on_readable is empty — register EPOLLOUT only.
    // After connect, if on_readable is set, use level-triggered so data
    // that arrives during re-registration is not missed.
    uint32_t ev = REACTOR_EV_OUT | REACTOR_EV_RDHUP;
    if (e->on_readable) ev |= REACTOR_EV_IN;
    else                ev |= REACTOR_EV_ET;  // edge-triggered ok for connect-only
    try {
      reactor_mod(m_handle, fd, ev);
    } catch (...) {
      // fd was closed before the arm_write cmd was processed; treat as gone.
      do_remove_fd(fd);
    }
  }

  void do_set_timeout(int fd, uint64_t timeout_ms)
  {
    FdEntry* e = find_entry(fd);
    if (!e) return;

    // Cancel any existing timer first.
    do_cancel_timeout(fd);

    int tid = reactor_timerfd_create();
    if (tid < 0) return;

    if (reactor_timerfd_arm(m_handle, tid, timeout_ms, 0) < 0) {
      reactor_timerfd_close(m_handle, tid);
      return;
    }

#if defined(REACTOR_OS_LINUX)
    // On Linux, timerfd is a real fd — register it for read events.
    try {
      reactor_add(m_handle, tid, REACTOR_EV_IN | REACTOR_EV_ONESHOT);
    } catch (...) {
      reactor_timerfd_close(m_handle, tid);
      return;
    }
#endif
    // On kqueue, EVFILT_TIMER fires directly without a separate fd registration.

    e->timer_id = tid;
    {
      std::lock_guard<std::mutex> lk(m_timers_mu);
      m_timer_to_fd.set(tid, fd);
    }
  }

  void do_cancel_timeout(int fd)
  {
    FdEntry* e = find_entry(fd);
    if (!e || e->timer_id < 0) return;
    int tid = e->timer_id;
    e->timer_id = -1;

#if defined(REACTOR_OS_LINUX)
    reactor_del(m_handle, tid);
#endif
    reactor_timerfd_close(m_handle, tid);
    {
      std::lock_guard<std::mutex> lk(m_timers_mu);
      m_timer_to_fd.clear(tid);
    }
  }

  void fire_timeout(int fd)
  {
    FdEntry* e = find_entry(fd);
    if (!e) return;
    int tid = e->timer_id;
    e->timer_id = -1;
    {
      std::lock_guard<std::mutex> lk(m_timers_mu);
      m_timer_to_fd.clear(tid);
    }
    reactor_timerfd_close(m_handle, tid);
    if (e->on_timeout) e->on_timeout(fd, e->user_data);
  }

  //----------------------------------------------------------------------------
  // Async connect
  //
  // The socket has already been set non-blocking and connect() returned
  // EINPROGRESS.  We register write-interest to detect completion and arm a
  // timeout timer.  Both the connect-complete and timeout handlers call
  // enif_send() to notify the Erlang caller.
  //----------------------------------------------------------------------------
  void do_connect(const ConnectParams& p)
  {
    FdEntry* e = find_entry(p.fd);
    if (!e) return;

    ErlNifPid pid    = p.caller_pid;
    uint32_t  stripe = p.stripe_id;
    uint32_t  slot   = p.slot_id;
    int       fd     = p.fd;

    auto close_fd = [this, fd]() {
      reactor_del(m_handle, fd);
      ::close(fd);
      clear_entry(fd);
    };

#if defined(REACTOR_BACKEND_URING)
    //--------------------------------------------------------------------------
    // io_uring path — two linked SQEs, no syscall on the reactor thread:
    //
    //   SQE[0]: IORING_OP_CONNECT  fd → addr     IOSQE_IO_LINK
    //   SQE[1]: IORING_OP_LINK_TIMEOUT  ts        (linked to SQE[0])
    //
    // Exactly one CQE pair arrives:
    //   connect succeeds: CQE[0].res=0,          CQE[1].res=-ECANCELED
    //   timeout fires:    CQE[0].res=-ECANCELED,  CQE[1].res=0
    //   connect fails:    CQE[0].res=-Exxx,       CQE[1].res=-ECANCELED
    //
    // reactor_wait translates the connect CQE into a synthetic reactor_event_t
    // with REACTOR_EV_CONNECT in the mask; dispatch() reads that flag here.
    // The Timeout CQE is silently dropped in reactor_wait.
    //
    // No timerfd, no poll_add, no separate wakeup — one kernel round-trip.
    //--------------------------------------------------------------------------
    {
      struct io_uring* ring = reactor_uring(m_handle);

      // ts must outlive io_uring_submit() — declare in outer scope.
      struct __kernel_timespec ts{};

      // SQE[0]: CONNECT (linked → SQE[1] will cancel it on timeout)
      struct io_uring_sqe* sqe_connect = io_uring_get_sqe(ring);
      if (!sqe_connect) { io_uring_submit(ring); sqe_connect = io_uring_get_sqe(ring); }
      if (!sqe_connect) { send_connect_result(pid, stripe, slot, false); close_fd(); return; }

      io_uring_prep_connect(sqe_connect, fd,
                            reinterpret_cast<const sockaddr*>(&p.addr),
                            sizeof(p.addr));
      io_uring_sqe_set_data64(sqe_connect, reactor_userdata(fd, ReactorOp::Connect));
      // IOSQE_IO_LINK chains SQE[1] — if connect completes before timeout,
      // the kernel automatically cancels the linked timeout.
      if (p.timeout_ms > 0)
        sqe_connect->flags |= IOSQE_IO_LINK;

      if (p.timeout_ms > 0) {
        // SQE[1]: LINK_TIMEOUT — cancels SQE[0] if it doesn't finish in time.
        struct io_uring_sqe* sqe_timeout = io_uring_get_sqe(ring);
        if (!sqe_timeout) { io_uring_submit(ring); sqe_timeout = io_uring_get_sqe(ring); }
        if (!sqe_timeout) {
          // Can't get a second SQE: clear the link flag and proceed without timeout.
          sqe_connect->flags &= ~IOSQE_IO_LINK;
        } else {
          ts.tv_sec  = p.timeout_ms / 1000;
          ts.tv_nsec = (p.timeout_ms % 1000) * 1'000'000L;
          io_uring_prep_link_timeout(sqe_timeout, &ts, 0);
          io_uring_sqe_set_data64(sqe_timeout, reactor_userdata(fd, ReactorOp::Timeout));
        }
      }

      io_uring_submit(ring); // kernel copies ts during this call

      // Capture the caller's on_timeout before overwriting on_connect.
      TimeoutHandler uot = std::move(e->on_timeout);

      // dispatch() calls on_connect(mask) when reactor_wait synthesises
      // a REACTOR_EV_CONNECT event for this fd.
      e->on_connect = [this, pid, stripe, slot, fd, close_fd,
                       uot = std::move(uot)](uint32_t mask) mutable {
        if (mask & (REACTOR_EV_ERR | REACTOR_EV_HUP)) {
          // REACTOR_EV_HUP without ERR = timed out (-ECANCELED on connect SQE)
          bool is_timeout = !(mask & REACTOR_EV_ERR);
          // Call on_error before close_fd clears the entry.
          FdEntry* entry = find_entry(fd);
          ErrorHandler onerr = entry ? std::move(entry->on_error) : ErrorHandler{};
          void*         ud   = entry ? entry->user_data : nullptr;
          close_fd();
          if (is_timeout) {
            send_timeout(pid, stripe, slot);
            if (uot) uot(fd, nullptr);
          } else {
            if (onerr) onerr(fd, ud);
            send_connect_result(pid, stripe, slot, false);
          }
        } else {
          // REACTOR_EV_CONNECT alone = success.
          // Install read-interest for subsequent I/O; the caller's on_readable
          // (set via Reactor::connect) is already in e->on_readable.
          try {
            reactor_add(m_handle, fd, REACTOR_EV_IN | REACTOR_EV_RDHUP);
          } catch (...) {}
          // Invoke on_writable if set — this is the "connection ready, send
          // first request" callback (equivalent to the epoll write-ready path).
          FdEntry* entry = find_entry(fd);
          if (entry && entry->on_writable) {
            WriteHandler wh = std::move(entry->on_writable);
            entry->on_writable = {};
            entry->write_armed = false;
            wh(fd, entry->user_data);
          }
          send_connect_result(pid, stripe, slot, true);
        }
      };
      return;
    }
#endif

    //--------------------------------------------------------------------------
    // epoll / kqueue path — call ::connect() on the reactor thread.
    // Returns immediately (EINPROGRESS) or completes synchronously.
    //--------------------------------------------------------------------------
    int rc = ::connect(fd, reinterpret_cast<const sockaddr*>(&p.addr), sizeof(p.addr));
    if (rc == 0) {
      send_connect_result(pid, stripe, slot, true);
      return;
    }
    if (errno != EINPROGRESS) {
      send_connect_result(pid, stripe, slot, false);
      close_fd();
      return;
    }

    // EINPROGRESS: watch for write-ready (= connect complete).
    e->write_armed = true;
    e->on_writable = [this, pid, stripe, slot, fd](int, void*) -> int {
      int so_err = 0; socklen_t len = sizeof(so_err);
      getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len);
      do_cancel_timeout(fd);
      send_connect_result(pid, stripe, slot, so_err == 0);
      return 0;
    };

    TimeoutHandler user_on_timeout = std::move(e->on_timeout);
    e->on_timeout = [this, pid, stripe, slot, fd, close_fd,
                     uot = std::move(user_on_timeout)](int ifd, void* ud) mutable {
      close_fd();
      send_timeout(pid, stripe, slot);
      if (uot) uot(ifd, ud);
    };

    try {
      reactor_add(m_handle, fd,
                  REACTOR_EV_OUT | REACTOR_EV_IN | REACTOR_EV_ET | REACTOR_EV_RDHUP);
    } catch (...) {
      send_connect_result(pid, stripe, slot, false);
      close_fd();
      return;
    }
    if (p.timeout_ms > 0)
      do_set_timeout(fd, p.timeout_ms);
  }

  //----------------------------------------------------------------------------
  // Erlang message helpers
  // These run from the reactor thread, so they use a process-independent env.
  //----------------------------------------------------------------------------
  void send_connect_result(const ErlNifPid& pid,
                            uint32_t stripe, uint32_t slot, bool ok)
  {
    ErlNifEnv* env = enif_alloc_env();
    if (!env) return;
    ERL_NIF_TERM msg = enif_make_tuple5(env,
      enif_make_atom(env, "arterial_event"),
      enif_make_uint(env, stripe),
      enif_make_uint(env, slot),
      enif_make_atom(env, "connect_result"),
      enif_make_atom(env, ok ? "ok" : "connect_failed"));
    enif_send(nullptr, const_cast<ErlNifPid*>(&pid), env, msg);
    enif_free_env(env);
  }

  void send_timeout(const ErlNifPid& pid, uint32_t stripe, uint32_t slot)
  {
    ErlNifEnv* env = enif_alloc_env();
    if (!env) return;
    ERL_NIF_TERM msg = enif_make_tuple4(env,
      enif_make_atom(env, "arterial_event"),
      enif_make_uint(env, stripe),
      enif_make_uint(env, slot),
      enif_make_atom(env, "timeout"));
    enif_send(nullptr, const_cast<ErlNifPid*>(&pid), env, msg);
    enif_free_env(env);
  }

  // Sent to m_owner_pid when the reactor loop exits abnormally (reactor_wait
  // returned a hard error).  Message: {arterial_reactor_exit, Ident, Errno}
  void send_reactor_exit(int err)
  {
    // Detect unset pid by comparing against a zero-initialised one.
    // enif_compare_pids is not available here (non-Erlang thread context),
    // so we use memcmp against a known-zero sentinel.
    static const ErlNifPid kZeroPid{};
    if (std::memcmp(&m_owner_pid, &kZeroPid, sizeof(ErlNifPid)) == 0) return;
    ErlNifEnv* env = enif_alloc_env();
    if (!env) return;
    ERL_NIF_TERM msg = enif_make_tuple3(env,
      enif_make_atom(env, "arterial_reactor_exit"),
      enif_make_atom(env, m_ident.c_str()),  // ident as atom, no encoding arg needed
      enif_make_int(env, err));
    enif_send(nullptr, &m_owner_pid, env, msg);
    enif_free_env(env);
  }

  //----------------------------------------------------------------------------
  // Entry table: pre-sized vector (fast, no mutex) + overflow map (mutex-guarded).
  //
  // m_entries is sized at construction and NEVER resized afterwards, so
  // any fd < m_entries.size() can be accessed without a mutex: NIF threads
  // write via ensure_entry() before posting the command; the reactor thread
  // reads/clears only after receiving that command — the MPSC ring provides
  // the necessary memory ordering fence between the two threads.
  //
  // Fds ≥ m_entries.size() are rare (process exhausted the preallocated range)
  // and fall through to m_entries_overflow, which is mutex-guarded.
  //----------------------------------------------------------------------------

  FdEntry* find_entry(int fd)
  {
    if (fd < 0) [[unlikely]] return nullptr;
    if (fd < static_cast<int>(m_entries.size())) {
      FdEntry& e = m_entries[fd];
      return (e.fd >= 0) ? &e : nullptr;
    }
    std::lock_guard<std::mutex> lk(m_entries_overflow_mu);
    auto it = m_entries_overflow.find(fd);
    return (it != m_entries_overflow.end()) ? &it->second : nullptr;
  }

  // Obtain (or create) the entry for fd.  Called from NIF threads.
  FdEntry& ensure_entry(int fd)
  {
    if (fd < static_cast<int>(m_entries.size())) {
      // No mutex needed: m_entries is never resized after construction.
      return m_entries[fd];
    }
    std::lock_guard<std::mutex> lk(m_entries_overflow_mu);
    return m_entries_overflow[fd];
  }

  // Reset an entry to the default (fd = -1) state.  Called from reactor thread.
  void clear_entry(int fd)
  {
    if (fd < 0) return;
    if (fd < static_cast<int>(m_entries.size())) {
      m_entries[fd] = FdEntry{};   // no mutex — reactor thread only
      return;
    }
    std::lock_guard<std::mutex> lk(m_entries_overflow_mu);
    m_entries_overflow.erase(fd);
  }

#if defined(REACTOR_BACKEND_KQUEUE)
  // kqueue timer idents start at 0x7000'0000 — too sparse for a vector.
  // Wrap unordered_map with the same set/get/clear/contains API as TimerVec.
  struct TimerMap {
    std::unordered_map<int,int> data;
    void set(int tid, int fd)       { data[tid] = fd; }
    int  get(int tid) const         { auto it = data.find(tid); return it != data.end() ? it->second : -1; }
    void clear(int tid)             { data.erase(tid); }
    bool contains(int tid) const    { return data.count(tid) > 0; }
  };
#else
  // Linux timerfd values are small OS fds — a vector is O(1) and cache-friendly.
  // Indexed by timer_id (= timerfd number); value is owner fd.
  struct TimerVec {
    std::vector<int> data;   // data[timer_id] = owner_fd, or -1 if free
    void  set(int tid, int fd)    { grow(tid); data[tid] = fd; }
    int   get(int tid) const      { return (tid >= 0 && tid < (int)data.size()) ? data[tid] : -1; }
    void  clear(int tid)          { if (tid >= 0 && tid < (int)data.size()) data[tid] = -1; }
    bool  contains(int tid) const { return get(tid) >= 0; }
  private:
    void grow(int tid) {
      if (tid >= (int)data.size())
        data.resize(std::max(tid + 1, (int)data.size() * 2), -1);
    }
  };
  using TimerMap = TimerVec;
#endif

  //----------------------------------------------------------------------------
  // State
  //----------------------------------------------------------------------------
  std::string       m_ident;
  reactor_handle_t  m_handle;
  int               m_wakeup_rd;  ///< eventfd read end
  int               m_wakeup_wr;  ///< eventfd write end (== wakeup_rd on Linux)
  std::atomic<bool> m_running;
  std::thread       m_thread;
  ErlNifPid         m_owner_pid{};  ///< receives exit notification on abnormal stop

  // Ready-signal: start() blocks until the reactor thread sets m_ready=true,
  // guaranteeing the caller can post commands without a startup race.
  std::mutex              m_ready_mu;
  std::condition_variable m_ready_cv;
  bool                    m_ready{false};

  MpscRing<ReactorCmd, kCmdRingCap> m_cmds;

  // Fast path: fixed-size heap array, allocated once at construction —
  // no locking needed for in-range fds.  Heap allocation avoids placing
  // Pre-sized vector, sized at construction and never resized.
  // No locking needed for fds in [0, size()).  Default: 64k entries.
  std::vector<FdEntry> m_entries;  // m_entries[fd].fd == -1 → slot free

  // Slow path: fds >= m_entries.size().  Protected by its own mutex.
  std::mutex                         m_entries_overflow_mu;
  std::unordered_map<int, FdEntry>   m_entries_overflow;

  // timer_id → owner fd.  Protected separately to reduce contention.
  std::mutex m_timers_mu;
  TimerMap   m_timer_to_fd;
};

} // namespace arterial
