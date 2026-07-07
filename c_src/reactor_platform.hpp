#pragma once
// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
/// \file   reactor_platform.hpp
/// \brief  Platform abstraction for the arterial I/O reactor.
///
/// Three backend modes are selected at compile time:
///
///   REACTOR_BACKEND_URING   — io_uring (Linux, requires liburing ≥ 2.x)
///   REACTOR_BACKEND_EPOLL   — epoll    (Linux fallback)
///   REACTOR_BACKEND_KQUEUE  — kqueue   (macOS / BSD)
///
/// Default selection:
///   Linux:  REACTOR_BACKEND_URING if <liburing.h> is present and
///           REACTOR_NO_URING is not defined, else REACTOR_BACKEND_EPOLL.
///   macOS/BSD: always REACTOR_BACKEND_KQUEUE.
///
/// Runtime WSL detection (REACTOR_BACKEND_URING only):
///   On WSL the io_uring backend automatically falls back to pure epoll at
///   runtime.  reactor_create() reads /proc/version; if "Microsoft" or "WSL"
///   appears in the string it builds an epoll handle instead of an io_uring
///   handle.  All reactor_add/mod/del/wait calls transparently dispatch to
///   the epoll path when the handle is an epoll fd.  No code changes needed
///   in callers; the io_uring code path (IORING_OP_POLL_ADD for readiness,
///   IORING_OP_CONNECT for connect) is used on native Linux only.
///
/// io_uring backend (native Linux):
///   Uses IORING_OP_POLL_ADD for all fd readiness (sockets, timerfd, wakeup
///   eventfd).  Uses IORING_OP_CONNECT + linked IORING_OP_LINK_TIMEOUT for
///   async connect.  No companion epoll — pure io_uring throughout.
///
/// Public API (same on all platforms):
///
///   Types:
///     reactor_handle_t        — multiplexer handle
///     reactor_event_t         — fired-event structure
///
///   Event flags (REACTOR_EV_*):
///     REACTOR_EV_IN, REACTOR_EV_OUT, REACTOR_EV_ERR,
///     REACTOR_EV_HUP, REACTOR_EV_RDHUP, REACTOR_EV_ET, REACTOR_EV_ONESHOT
///
///   Multiplexer:
///     reactor_handle_t reactor_create()
///     void             reactor_destroy(reactor_handle_t)
///     void             reactor_add    (reactor_handle_t, int fd, uint32_t ev)
///     void             reactor_mod    (reactor_handle_t, int fd, uint32_t ev)
///     void             reactor_del    (reactor_handle_t, int fd)
///     int              reactor_wait   (reactor_handle_t, reactor_event_t*, int, int ms)
///     int              reactor_ev_fd  (const reactor_event_t&)
///     uint32_t         reactor_ev_mask(const reactor_event_t&)
///
///   Eventfd / wakeup:
///     int  reactor_eventfd_create()
///     int  reactor_eventfd_write_fd(int efd)
///     int  reactor_eventfd_read (int efd, uint64_t& val)
///     int  reactor_eventfd_write(int wfd, uint64_t  val)
///     void reactor_eventfd_close(int efd)
///
///   Timerfd:
///     int  reactor_timerfd_create()
///     int  reactor_timerfd_arm  (reactor_handle_t, int id, uint64_t init_ms, uint64_t intv_ms)
///     int  reactor_timerfd_read (int id, uint64_t& expirations)
///     void reactor_timerfd_close(reactor_handle_t, int id)
///
/// io_uring-specific (only when REACTOR_BACKEND_URING):
///   struct io_uring* reactor_uring(reactor_handle_t) — get the raw ring
///                                                       (nullptr on WSL)
///   bool             reactor_is_epoll(reactor_handle_t) — true when the
///                    handle is a pure-epoll fallback (WSL or no io_uring)
///
/// All inline; no separate .cpp file required.
//-----------------------------------------------------------------------------
// Copyright (c) 2015 Serge Aleynikov <saleyn@gmail.com>
// io_uring backend 2026
//-----------------------------------------------------------------------------

#include <cassert>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

//------------------------------------------------------------------------------
// Platform & backend detection
//------------------------------------------------------------------------------
#if defined(__linux__)
#  define REACTOR_OS_LINUX 1
#elif defined(__APPLE__) && defined(__MACH__)
#  define REACTOR_OS_MACOS  1
#  define REACTOR_OS_KQUEUE 1
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#  define REACTOR_OS_BSD    1
#  define REACTOR_OS_KQUEUE 1
#else
#  error "reactor: unsupported platform"
#endif

#if defined(REACTOR_OS_LINUX)
#  if !defined(REACTOR_NO_URING) && __has_include(<liburing.h>)
#    define REACTOR_BACKEND_URING 1
#  else
#    define REACTOR_BACKEND_EPOLL 1
#  endif
#else
#  define REACTOR_BACKEND_KQUEUE 1
#endif

//------------------------------------------------------------------------------
// Backend-specific includes
//------------------------------------------------------------------------------
#if defined(REACTOR_BACKEND_URING)
#  include <sys/epoll.h>
#  include <sys/eventfd.h>
#  include <sys/timerfd.h>
#  include <liburing.h>
#  include <memory>
#  include <mutex>
#elif defined(REACTOR_BACKEND_EPOLL)
#  include <sys/epoll.h>
#  include <sys/eventfd.h>
#  include <sys/timerfd.h>
#else // kqueue
#  include <sys/event.h>
#  include <sys/time.h>
#  include <atomic>
#  include <mutex>
#  include <unordered_map>
#endif

namespace arterial {

//==============================================================================
// Unified event-flag constants (REACTOR_EV_*)
//==============================================================================
#if defined(REACTOR_BACKEND_EPOLL) || defined(REACTOR_BACKEND_URING)

using reactor_event_t                        = struct epoll_event;
using reactor_handle_t                       = int;

static constexpr uint32_t REACTOR_EV_IN      = EPOLLIN;
static constexpr uint32_t REACTOR_EV_OUT     = EPOLLOUT;
static constexpr uint32_t REACTOR_EV_ERR     = EPOLLERR;
static constexpr uint32_t REACTOR_EV_HUP     = EPOLLHUP;
static constexpr uint32_t REACTOR_EV_RDHUP   = EPOLLRDHUP;
static constexpr uint32_t REACTOR_EV_ET      = EPOLLET;
static constexpr uint32_t REACTOR_EV_ONESHOT = EPOLLONESHOT;

#else // kqueue

struct reactor_event_t { int fd; uint32_t mask; };
using reactor_handle_t                       = int;

static constexpr uint32_t REACTOR_EV_IN      = 0x0001u;
static constexpr uint32_t REACTOR_EV_OUT     = 0x0002u;
static constexpr uint32_t REACTOR_EV_ERR     = 0x0004u;
static constexpr uint32_t REACTOR_EV_HUP     = 0x0008u;
static constexpr uint32_t REACTOR_EV_RDHUP   = 0x0010u;
static constexpr uint32_t REACTOR_EV_ET      = 0x0020u;
static constexpr uint32_t REACTOR_EV_ONESHOT = 0x0040u;

#endif

// Synthetic flag used by reactor_wait (io_uring only) to signal a completed
// async connect CQE.  Always zero on epoll/kqueue so dispatch() can test it
// unconditionally on all backends without a compile-time guard.
static constexpr uint32_t REACTOR_EV_CONNECT = 0x8000'0000u;

/// Round up to the nearest power of 2.  Returns 1 for 0.
inline unsigned upper_power_of_two(unsigned n)
{
  if (n == 0) return 1;
  if (n & (n - 1)) {
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n++;
  }
  return n;
}

inline bool reactor_is_error   (uint32_t m) { return m & (REACTOR_EV_ERR | REACTOR_EV_HUP); }
inline bool reactor_is_readable(uint32_t m) { return m & REACTOR_EV_IN;  }
inline bool reactor_is_writable(uint32_t m) { return m & REACTOR_EV_OUT; }

//==============================================================================
// reactor_handle_t helpers
//==============================================================================

/// Extract fd from a fired event.
inline int reactor_ev_fd(const reactor_event_t& ev)
{
#if defined(REACTOR_BACKEND_EPOLL) || defined(REACTOR_BACKEND_URING)
  return ev.data.fd;
#else
  return ev.fd;
#endif
}

/// Extract event mask from a fired event.
inline uint32_t reactor_ev_mask(const reactor_event_t& ev)
{
#if defined(REACTOR_BACKEND_EPOLL) || defined(REACTOR_BACKEND_URING)
  return ev.events;
#else
  return ev.mask;
#endif
}

//==============================================================================
// io_uring backend
// ---------------------------------------------------------------------------
// Uses IORING_OP_POLL_ADD for all fd readiness (sockets, timerfd, wakeup
// eventfd) and IORING_OP_CONNECT + linked IORING_OP_LINK_TIMEOUT for async
// connect.  No companion epoll — pure io_uring throughout.
//
// Runtime WSL fallback: reactor_create() reads /proc/version at first call.
// If the string contains "Microsoft" or "WSL", the handle table slot is left
// null and a plain epoll fd is returned as a negative-offset handle instead
// (see URING_EPOLL_BIAS below).  All API functions check reactor_is_epoll()
// and route to plain epoll_ctl / epoll_wait on that path.
//
// user_data encoding (64-bit) for io_uring SQEs:
//   bits 63-32  fd  (int32_t, -1 for non-fd ops like timeout)
//   bits 31-0   op  (ReactorOp enum)
//==============================================================================
#if defined(REACTOR_BACKEND_URING)

enum class ReactorOp : uint32_t {
  PollAdd   = 0,  // io_uring_prep_poll_add — fd readiness
  PollMod   = 1,  // io_uring_prep_poll_update
  Connect   = 2,  // io_uring_prep_connect (main SQE of a linked pair)
  Recv      = 3,  // io_uring_prep_recv
  Send      = 4,  // io_uring_prep_send
  Timeout   = 5,  // io_uring_prep_link_timeout (linked to Connect)
  Cancel    = 6,  // io_uring_prep_cancel
};

inline uint64_t reactor_userdata(int fd, ReactorOp op) noexcept
{
  return (uint64_t(uint32_t(fd)) << 32) | uint32_t(op);
}
inline int        reactor_userdata_fd(uint64_t ud) noexcept { return int(int32_t(ud >> 32)); }
inline ReactorOp  reactor_userdata_op(uint64_t ud) noexcept { return ReactorOp(uint32_t(ud));  }

//------------------------------------------------------------------------------
// Per-handle io_uring context
//------------------------------------------------------------------------------
struct ReactorUringCtx {
  struct io_uring ring;

  ReactorUringCtx() { memset(&ring, 0, sizeof(ring)); }
  ~ReactorUringCtx()
  {
    if (ring.ring_fd > 0) io_uring_queue_exit(&ring);
  }
};

// Handles in the range [URING_EPOLL_BIAS, URING_EPOLL_BIAS + max_epoll_fd)
// represent pure-epoll fallback handles (used on WSL).  We encode them as
// (epoll_fd + URING_EPOLL_BIAS) so reactor_is_epoll() can distinguish them
// from io_uring table indices [0, s_max_rings) by a single comparison.
static constexpr int URING_EPOLL_BIAS = 0x4000'0000;

namespace detail {
  static constexpr int s_max_rings = 256;
  inline ReactorUringCtx** uring_table()
  {
    static ReactorUringCtx* t[s_max_rings] = {};
    return t;
  }
  inline std::mutex& uring_mutex() { static std::mutex m; return m; }

  /// Returns true if this process is running on WSL.
  /// Result is cached after the first call.
  inline bool is_wsl()
  {
    static int cached = -1;
    if (cached >= 0) return cached != 0;
    cached = 0;
    char buf[512] = {};
    int fd = ::open("/proc/version", O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
      ssize_t n = ::read(fd, buf, sizeof(buf) - 1);
      ::close(fd);
      if (n > 0) {
        buf[n] = '\0';
        if (::strstr(buf, "Microsoft") || ::strstr(buf, "WSL"))
          cached = 1;
      }
    }
    return cached != 0;
  }
} // namespace detail

/// Returns true when the handle represents a pure-epoll fallback (WSL).
inline bool reactor_is_epoll(reactor_handle_t h) { return h >= URING_EPOLL_BIAS; }
/// Extract the underlying epoll fd from an epoll-fallback handle.
inline int  reactor_epoll_fd(reactor_handle_t h) { return h - URING_EPOLL_BIAS; }

/// Create an io_uring-backed reactor handle (or a pure-epoll handle on WSL).
inline reactor_handle_t reactor_create(unsigned entries = 4096,
                                       unsigned /*epoll_entries*/ = 4096)
{
  // On WSL, io_uring POLL_ADD is unreliable — use plain epoll.
  if (detail::is_wsl()) {
    int epfd = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd < 0)
      throw std::runtime_error(std::string("epoll_create1 (WSL fallback): ") + strerror(errno));
    return URING_EPOLL_BIAS + epfd;
  }

  std::unique_ptr<ReactorUringCtx> ctx(new ReactorUringCtx());

  struct io_uring_params params{};
  if (io_uring_queue_init_params(entries, &ctx->ring, &params) < 0)
    throw std::runtime_error(std::string("io_uring_queue_init: ") + strerror(errno));

  std::lock_guard<std::mutex> lg(detail::uring_mutex());
  for (int i = 0; i < detail::s_max_rings; ++i) {
    auto& tab = detail::uring_table()[i];
    if (!tab) {
      tab = ctx.release();
      return i;
    }
  }
  throw std::runtime_error("reactor_create: too many rings");
}

inline void reactor_destroy(reactor_handle_t h)
{
  if (reactor_is_epoll(h)) {
    ::close(reactor_epoll_fd(h));
    return;
  }
  if (h < 0 || h >= detail::s_max_rings) return;
  std::lock_guard<std::mutex> lg(detail::uring_mutex());
  delete detail::uring_table()[h];
  detail::uring_table()[h] = nullptr;
}

inline ReactorUringCtx& reactor_ctx(reactor_handle_t h)
{
  assert(!reactor_is_epoll(h));
  assert(size_t(h) < detail::s_max_rings);
  return *detail::uring_table()[h];
}

/// Get the raw io_uring for submitting custom SQEs (nullptr on WSL/epoll path).
inline struct io_uring* reactor_uring(reactor_handle_t h)
{
  if (reactor_is_epoll(h)) return nullptr;
  return &reactor_ctx(h).ring;
}

// ---- Pure epoll helpers used by the WSL fallback path ----------------------

inline void reactor_epoll_add(int epfd, int fd, uint32_t ev)
{
  epoll_event e{}; e.events = ev | EPOLLONESHOT; e.data.fd = fd;
  if (::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &e) < 0) {
    if (errno == EEXIST)
      ::epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &e);
  }
}

inline void reactor_epoll_del(int epfd, int fd)
{
  ::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
}

// ---- io_uring POLL_ADD helpers for pure-uring path ------------------------

inline void reactor_poll_add(reactor_handle_t h, int fd, uint32_t ev)
{
  struct io_uring* ring = &reactor_ctx(h).ring;
  // Cancel any existing poll for this fd before adding a new one.
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) { io_uring_submit(ring); sqe = io_uring_get_sqe(ring); }
  if (!sqe) return; // ring full, drop
  // IORING_POLL_UPDATE_EVENTS: update interest without cancel+re-add cost.
  // We always cancel+re-add for simplicity (one-shot semantics).
  io_uring_prep_poll_add(sqe, fd, ev);
  sqe->flags |= IOSQE_ASYNC;  // avoid busy-poll
  io_uring_sqe_set_data64(sqe, reactor_userdata(fd, ReactorOp::PollAdd));
}

inline void reactor_poll_remove(reactor_handle_t h, int fd)
{
  struct io_uring* ring = &reactor_ctx(h).ring;
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  if (!sqe) { io_uring_submit(ring); sqe = io_uring_get_sqe(ring); }
  if (!sqe) return;
  io_uring_prep_cancel64(sqe, reactor_userdata(fd, ReactorOp::PollAdd), 0);
  io_uring_sqe_set_data64(sqe, reactor_userdata(fd, ReactorOp::Cancel));
}

// ---- Unified add/mod/del/wait — dispatch to uring or epoll ----------------

inline void reactor_add(reactor_handle_t h, int fd, uint32_t ev)
{
  if (reactor_is_epoll(h)) {
    reactor_epoll_add(reactor_epoll_fd(h), fd, ev);
    return;
  }
  // io_uring: POLL_ADD for fd readiness (one-shot; re-arm after each event).
  reactor_poll_add(h, fd, ev);
}

inline void reactor_del(reactor_handle_t h, int fd)
{
  if (reactor_is_epoll(h)) {
    reactor_epoll_del(reactor_epoll_fd(h), fd);
    return;
  }
  reactor_poll_remove(h, fd);
}

inline void reactor_mod(reactor_handle_t h, int fd, uint32_t ev)
{
  // For both backends, mod = cancel existing interest + re-add with new mask.
  reactor_add(h, fd, ev);
}

// reactor_add_wakeup is identical to reactor_add.  It exists so reactor.hpp
// can use a single call site for wakeup fd registration.
inline void reactor_add_wakeup(reactor_handle_t h, int fd, uint32_t ev)
{
  reactor_add(h, fd, ev);
}

/// Wait for fd readiness events and async connect completions.
///
/// On WSL (epoll path): blocks in epoll_wait.
///
/// On native Linux (io_uring path): submits pending SQEs, then blocks until
/// at least one CQE arrives or timeout elapses.  PollAdd CQEs are translated
/// into synthetic epoll_event entries.  Connect CQEs get REACTOR_EV_CONNECT.
/// Cancel/Timeout CQEs are silently consumed.  All seen CQEs are advanced.
inline int reactor_wait(reactor_handle_t h, reactor_event_t* buf, int maxev,
                        int timeout_ms,
                        struct io_uring_cqe** /*unused*/ = nullptr,
                        int*                 /*unused*/ = nullptr)
{
  if (reactor_is_epoll(h)) {
    RETRY_WAIT:
    int n = ::epoll_wait(reactor_epoll_fd(h), buf, maxev, timeout_ms);
    if (n < 0) {
      if (errno == EINTR) [[unlikely]]
        goto RETRY_WAIT;
      return 0;
    }
    return n;
  }

  auto& ctx  = reactor_ctx(h);
  auto& ring = ctx.ring;

  // Submit all pending SQEs (POLL_ADD + any CONNECT).
  io_uring_submit(&ring);

  // Block until at least one CQE arrives or timeout elapses.
  struct __kernel_timespec  ts{};
  struct __kernel_timespec* tsp = nullptr;
  if (timeout_ms >= 0) {
    ts.tv_sec  = timeout_ms / 1000;
    ts.tv_nsec = (long)(timeout_ms % 1000) * 1'000'000L;
    tsp = &ts;
  }

  struct io_uring_cqe* cqe;
  int wait_ret = io_uring_wait_cqe_timeout(&ring, &cqe, tsp);
  if (wait_ret < 0) {
    if (wait_ret == -ETIME || wait_ret == -EINTR) return 0;
    return -1;
  }

  // Drain all available CQEs.  io_uring_peek_cqe / cqe_seen advances one at
  // a time; we count each seen CQE and accumulate events for the caller.
  int nev = 0;
  while (io_uring_peek_cqe(&ring, &cqe) == 0) {
    uint64_t  ud = io_uring_cqe_get_data64(cqe);
    ReactorOp op = reactor_userdata_op(ud);
    int       fd = reactor_userdata_fd(ud);

    if (nev < maxev) {
      if (op == ReactorOp::PollAdd && cqe->res > 0) {
        // PollAdd CQE res encodes the poll event mask (POLLIN/POLLOUT/etc.).
        // The kernel uses poll(2) bit values which match EPOLLIN/EPOLLOUT.
        buf[nev].data.fd = fd;
        buf[nev].events  = (uint32_t)cqe->res;
        ++nev;
      } else if (op == ReactorOp::Connect) {
        buf[nev].data.fd = fd;
        if (cqe->res == 0)
          buf[nev].events = REACTOR_EV_CONNECT;
        else if (cqe->res == -ECANCELED)
          buf[nev].events = REACTOR_EV_CONNECT | REACTOR_EV_HUP;   // timed out
        else
          buf[nev].events = REACTOR_EV_CONNECT | REACTOR_EV_ERR;   // refused/reset
        ++nev;
      }
      // PollAdd with res <= 0 (cancelled/error), Timeout, Cancel: silently drop.
    }
    io_uring_cqe_seen(&ring, cqe);  // advance ring regardless
  }
  return nev;
}

#endif // REACTOR_BACKEND_URING

//==============================================================================
// epoll backend (Linux fallback / compile-time selection)
//==============================================================================
#if defined(REACTOR_BACKEND_EPOLL)

inline reactor_handle_t reactor_create(unsigned /*entries*/ = 0, unsigned = 0)
{
  int fd = ::epoll_create1(EPOLL_CLOEXEC);
  if (fd < 0) throw std::runtime_error(std::string("epoll_create1: ") + strerror(errno));
  return fd;
}
inline void reactor_destroy(reactor_handle_t h) { if (h >= 0) ::close(h); }

inline void reactor_add(reactor_handle_t h, int fd, uint32_t ev)
{
  epoll_event e{}; e.events = ev; e.data.fd = fd;
  if (::epoll_ctl(h, EPOLL_CTL_ADD, fd, &e) < 0)
    throw std::runtime_error(std::string("epoll_ctl ADD fd=") + std::to_string(fd) + ": " + strerror(errno));
}
inline void reactor_mod(reactor_handle_t h, int fd, uint32_t ev)
{
  epoll_event e{}; e.events = ev; e.data.fd = fd;
  if (::epoll_ctl(h, EPOLL_CTL_MOD, fd, &e) < 0) {
    if (errno == ENOENT)
      ::epoll_ctl(h, EPOLL_CTL_ADD, fd, &e);  // first registration
    else
      throw std::runtime_error(std::string("epoll_ctl MOD fd=") + std::to_string(fd) + ": " + strerror(errno));
  }
}
inline void reactor_del(reactor_handle_t h, int fd)
{
  ::epoll_ctl(h, EPOLL_CTL_DEL, fd, nullptr);
}
inline int reactor_wait(reactor_handle_t h, reactor_event_t* buf, int maxev, int timeout_ms)
{
  return ::epoll_wait(h, buf, maxev, timeout_ms);
}

#endif // REACTOR_BACKEND_EPOLL

//==============================================================================
// Linux shared: eventfd + timerfd  (used by both epoll and io_uring)
//==============================================================================
#if defined(REACTOR_OS_LINUX)

inline int reactor_eventfd_create()
{
  int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) throw std::runtime_error(std::string("eventfd: ") + strerror(errno));
  return fd;
}
inline int  reactor_eventfd_write_fd(int efd)     { return efd; }
inline void reactor_eventfd_close(int efd)         { if (efd >= 0) ::close(efd); }
inline int  reactor_eventfd_read(int efd, uint64_t& val)
{
  return (::read(efd, &val, 8) == 8) ? 0 : -1;
}
inline int reactor_eventfd_write(int wfd, uint64_t val)
{
  return (::write(wfd, &val, 8) == 8) ? 0 : -1;
}

inline int reactor_timerfd_create()
{
  int fd = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (fd < 0) throw std::runtime_error(std::string("timerfd_create: ") + strerror(errno));
  return fd;
}
inline int reactor_timerfd_arm(
    reactor_handle_t /*kq*/, int timer_id, uint64_t initial_ms, uint64_t interval_ms)
{
  itimerspec ts{};
  ts.it_value.tv_sec     = initial_ms  / 1000;
  ts.it_value.tv_nsec    = (initial_ms  % 1000) * 1'000'000L;
  ts.it_interval.tv_sec  = interval_ms / 1000;
  ts.it_interval.tv_nsec = (interval_ms % 1000) * 1'000'000L;
  return ::timerfd_settime(timer_id, 0, &ts, nullptr);
}
inline int  reactor_timerfd_read(int tid, uint64_t& exp)
{
  return (::read(tid, &exp, 8) == 8) ? 0 : -1;
}
inline void reactor_timerfd_close(reactor_handle_t /*kq*/, int tid)
{
  if (tid >= 0) ::close(tid);
}

#endif // REACTOR_OS_LINUX

//==============================================================================
// kqueue backend (macOS / BSD)
//==============================================================================
#if defined(REACTOR_BACKEND_KQUEUE)

inline reactor_handle_t reactor_create(unsigned = 0, unsigned = 0)
{
  int kq = ::kqueue();
  if (kq < 0) throw std::runtime_error(std::string("kqueue: ") + strerror(errno));
  return kq;
}
inline void reactor_destroy(reactor_handle_t h) { if (h >= 0) ::close(h); }

namespace detail {
inline void kq_to_kevent(int fd, uint32_t ev, bool add, struct kevent* out, int& n)
{
  n = 0;
  unsigned short flags = add ? (EV_ADD | EV_CLEAR) : EV_DELETE;
  if (add && (ev & REACTOR_EV_ONESHOT)) flags = EV_ADD | EV_ONESHOT;
  if (ev & REACTOR_EV_IN)
    EV_SET(&out[n++], fd, EVFILT_READ,  flags, 0, 0, (void*)(uintptr_t)fd);
  if (ev & REACTOR_EV_OUT)
    EV_SET(&out[n++], fd, EVFILT_WRITE, flags, 0, 0, (void*)(uintptr_t)fd);
}
} // namespace detail

inline void reactor_add(reactor_handle_t kq, int fd, uint32_t ev)
{
  struct kevent ch[2]; int n;
  detail::kq_to_kevent(fd, ev, true, ch, n);
  if (n > 0 && ::kevent(kq, ch, n, nullptr, 0, nullptr) < 0)
    throw std::runtime_error(std::string("kevent ADD: ") + strerror(errno));
}
inline void reactor_mod(reactor_handle_t kq, int fd, uint32_t ev)
{
  struct kevent del[2]; int n;
  detail::kq_to_kevent(fd, REACTOR_EV_IN | REACTOR_EV_OUT, false, del, n);
  ::kevent(kq, del, n, nullptr, 0, nullptr);
  reactor_add(kq, fd, ev);
}
inline void reactor_del(reactor_handle_t kq, int fd)
{
  struct kevent del[2]; int n;
  detail::kq_to_kevent(fd, REACTOR_EV_IN | REACTOR_EV_OUT, false, del, n);
  ::kevent(kq, del, n, nullptr, 0, nullptr);
}
inline int reactor_wait(reactor_handle_t kq, reactor_event_t* buf, int maxev, int timeout_ms)
{
  struct kevent raw[64];
  int cap = (maxev < 64) ? maxev : 64;
  struct timespec ts{}, *tsp = nullptr;
  if (timeout_ms >= 0) {
    ts.tv_sec  = timeout_ms / 1000;
    ts.tv_nsec = (timeout_ms % 1000) * 1'000'000L;
    tsp = &ts;
  }
  int nev = ::kevent(kq, nullptr, 0, raw, cap, tsp);
  if (nev <= 0) return nev;
  for (int i = 0; i < nev; ++i) {
    buf[i].fd   = (int)raw[i].ident;
    buf[i].mask = 0;
    if (raw[i].filter == EVFILT_READ)  buf[i].mask |= REACTOR_EV_IN;
    if (raw[i].filter == EVFILT_WRITE) buf[i].mask |= REACTOR_EV_OUT;
    if (raw[i].flags  & EV_ERROR)      buf[i].mask |= REACTOR_EV_ERR;
    if (raw[i].flags  & EV_EOF)        buf[i].mask |= REACTOR_EV_HUP;
  }
  return nev;
}

// ---- eventfd via pipe (macOS) -----------------------------------------------
namespace detail {
inline std::mutex& pipe_mtx() { static std::mutex m; return m; }
inline std::unordered_map<int,int>& pipe_map()
{
  static std::unordered_map<int,int> m; return m;
}
} // namespace detail

inline int reactor_eventfd_create()
{
  int fds[2];
  if (::pipe(fds) < 0) throw std::runtime_error(std::string("pipe: ") + strerror(errno));
  ::fcntl(fds[0], F_SETFL, O_NONBLOCK | O_CLOEXEC);
  ::fcntl(fds[1], F_SETFL, O_NONBLOCK | O_CLOEXEC);
  std::lock_guard<std::mutex> lg(detail::pipe_mtx());
  detail::pipe_map()[fds[0]] = fds[1];
  return fds[0];
}
inline int reactor_eventfd_write_fd(int efd)
{
  std::lock_guard<std::mutex> lg(detail::pipe_mtx());
  auto it = detail::pipe_map().find(efd);
  return (it != detail::pipe_map().end()) ? it->second : -1;
}
inline void reactor_eventfd_close(int efd)
{
  if (efd < 0) return;
  std::lock_guard<std::mutex> lg(detail::pipe_mtx());
  auto it = detail::pipe_map().find(efd);
  if (it != detail::pipe_map().end()) { ::close(it->second); detail::pipe_map().erase(it); }
  ::close(efd);
}
inline int reactor_eventfd_read(int efd, uint64_t& val)
{
  char buf[256]; ssize_t n, total = 0;
  while ((n = ::read(efd, buf, sizeof(buf))) > 0) total += n;
  if (total == 0 && errno != EAGAIN) return -1;
  val = (uint64_t)total; return 0;
}
inline int reactor_eventfd_write(int wfd, uint64_t /*val*/)
{
  char b = 1; return (::write(wfd, &b, 1) == 1) ? 0 : -1;
}

// ---- timerfd via EVFILT_TIMER (macOS) ---------------------------------------
namespace detail {
inline std::atomic<int>& timer_counter() { static std::atomic<int> c{0x7000'0000}; return c; }
} // namespace detail

inline int reactor_timerfd_create() { return ++detail::timer_counter(); }

inline int reactor_timerfd_arm(
    reactor_handle_t kq, int tid, uint64_t initial_ms, uint64_t interval_ms)
{
  struct kevent del;
  EV_SET(&del, tid, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
  ::kevent(kq, &del, 1, nullptr, 0, nullptr);

  short flags = EV_ADD | EV_ENABLE;
  if (!interval_ms) flags |= EV_ONESHOT;
  uint64_t period = interval_ms ? interval_ms : initial_ms;
  if (!period) period = 1;

  struct kevent kev;
  EV_SET(&kev, tid, EVFILT_TIMER, flags, NOTE_MSECONDS, (intptr_t)period, nullptr);
  return (::kevent(kq, &kev, 1, nullptr, 0, nullptr) < 0) ? -1 : 0;
}
inline int  reactor_timerfd_read(int /*tid*/, uint64_t& exp) { exp = 1; return 0; }
inline void reactor_timerfd_close(reactor_handle_t kq, int tid)
{
  if (tid < 0) return;
  struct kevent del;
  EV_SET(&del, tid, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
  ::kevent(kq, &del, 1, nullptr, 0, nullptr);
}

#endif // REACTOR_BACKEND_KQUEUE

} // namespace arterial
