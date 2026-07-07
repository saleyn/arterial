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
///           The build system sets REACTOR_NO_URING automatically on WSL
///           (detected via /proc/version at make time) so the epoll backend
///           is always used on WSL without any runtime branching.
///   macOS/BSD: always REACTOR_BACKEND_KQUEUE.
///
/// io_uring backend:
///   Uses IORING_OP_POLL_ADD for all fd readiness (sockets, timerfd, wakeup
///   eventfd).  Uses IORING_OP_CONNECT + linked IORING_OP_LINK_TIMEOUT for
///   async connect.  Pure io_uring — no companion epoll.
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
///     int              reactor_wait   (reactor_handle_t, reactor_event_t*, int ms)
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
///
/// All inline; no separate .cpp file required.
//-----------------------------------------------------------------------------
// Copyright (c) 2015 Serge Aleynikov <saleyn@gmail.com>
// io_uring backend 2026
//-----------------------------------------------------------------------------

#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>

//------------------------------------------------------------------------------
// Platform & backend detection
//------------------------------------------------------------------------------
#if defined(__linux__)
#define REACTOR_OS_LINUX 1
#elif defined(__APPLE__) && defined(__MACH__)
#define REACTOR_OS_MACOS  1
#define REACTOR_OS_KQUEUE 1
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
#define REACTOR_OS_BSD    1
#define REACTOR_OS_KQUEUE 1
#else
#error "reactor: unsupported platform"
#endif

#if defined(REACTOR_OS_LINUX)
#if !defined(REACTOR_NO_URING) && __has_include(<liburing.h>)
#define REACTOR_BACKEND_URING 1
#else
#define REACTOR_BACKEND_EPOLL 1
#endif
#else
#define REACTOR_BACKEND_KQUEUE 1
#endif

//------------------------------------------------------------------------------
// Backend-specific includes
//------------------------------------------------------------------------------
#if defined(REACTOR_BACKEND_URING)
#include <liburing.h>
#include <memory>
#include <mutex>
#include <sys/epoll.h> // epoll_event used as reactor_event_t
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#elif defined(REACTOR_BACKEND_EPOLL)
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#else // kqueue
#include <atomic>
#include <mutex>
#include <sys/event.h>
#include <sys/time.h>
#include <unordered_map>
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

struct reactor_event_t {
  int      fd;
  uint32_t mask;
};
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
inline unsigned           upper_power_of_two(unsigned n)
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

inline bool reactor_is_error(uint32_t m)
{ return m & (REACTOR_EV_ERR | REACTOR_EV_HUP); }
inline bool reactor_is_readable(uint32_t m)
{ return m & REACTOR_EV_IN; }
inline bool reactor_is_writable(uint32_t m)
{ return m & REACTOR_EV_OUT; }

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
// Pure io_uring: IORING_OP_POLL_ADD for all fd readiness, IORING_OP_CONNECT
// + linked IORING_OP_LINK_TIMEOUT for async connect.  No epoll at all.
//
// user_data encoding (64-bit):
//   bits 63-32  fd  (int32_t, -1 for non-fd ops like timeout)
//   bits 31-0   op  (ReactorOp enum)
//==============================================================================
#if defined(REACTOR_BACKEND_URING)

enum class ReactorOp : uint32_t {
  PollAdd = 0, // io_uring_prep_poll_add — fd readiness (one-shot)
  Connect = 1, // io_uring_prep_connect (main SQE of a linked pair)
  Timeout = 2, // io_uring_prep_link_timeout (linked to Connect)
  Cancel  = 3, // io_uring_prep_cancel
};

inline uint64_t reactor_userdata(int fd, ReactorOp op) noexcept
{ return (uint64_t(uint32_t(fd)) << 32) | uint32_t(op); }
inline int reactor_userdata_fd(uint64_t ud) noexcept
{ return int(int32_t(ud >> 32)); }
inline ReactorOp reactor_userdata_op(uint64_t ud) noexcept
{ return ReactorOp(uint32_t(ud)); }

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

namespace detail {
  static constexpr int     s_max_rings = 256;
  inline ReactorUringCtx** uring_table()
  {
    static ReactorUringCtx* t[s_max_rings] = {};
    return t;
  }
  inline std::mutex& uring_mutex()
  {
    static std::mutex m;
    return m;
  }
} // namespace detail

/// Create an io_uring-backed reactor handle.  Returns -1 on failure.
inline reactor_handle_t reactor_create(unsigned entries = 4096, unsigned /*unused*/ = 0)
{
  std::unique_ptr<ReactorUringCtx> ctx(new ReactorUringCtx());

  struct io_uring_params           params{};
  if (io_uring_queue_init_params(entries, &ctx->ring, &params) < 0) return -1;

  std::lock_guard<std::mutex> lg(detail::uring_mutex());
  for (int i = 0; i < detail::s_max_rings; ++i) {
    auto& slot = detail::uring_table()[i];
    if (!slot) {
      slot = ctx.release();
      return i;
    }
  }
  return -1;
}

inline void reactor_destroy(reactor_handle_t h)
{
  if (h < 0 || h >= detail::s_max_rings) return;
  std::lock_guard<std::mutex> lg(detail::uring_mutex());
  delete detail::uring_table()[h];
  detail::uring_table()[h] = nullptr;
}

inline ReactorUringCtx& reactor_ctx(reactor_handle_t h)
{
  assert(h >= 0 && h < detail::s_max_rings);
  return *detail::uring_table()[h];
}

/// Get the raw io_uring for submitting custom SQEs.
inline struct io_uring* reactor_uring(reactor_handle_t h)
{ return &reactor_ctx(h).ring; }

// Submit a POLL_ADD SQE for fd.  One-shot: fires once, must be re-armed.
inline int reactor_add(reactor_handle_t h, int fd, uint32_t ev)
{
  struct io_uring*     ring = &reactor_ctx(h).ring;
  struct io_uring_sqe* sqe  = io_uring_get_sqe(ring);
  if (!sqe) {
    io_uring_submit(ring);
    sqe = io_uring_get_sqe(ring);
  }
  if (!sqe) return -1;
  io_uring_prep_poll_add(sqe, fd, ev);
  sqe->flags |= IOSQE_ASYNC;
  io_uring_sqe_set_data64(sqe, reactor_userdata(fd, ReactorOp::PollAdd));
  return 0;
}

// Cancel any outstanding POLL_ADD for fd (e.g. before closing or re-arming).
inline int reactor_del(reactor_handle_t h, int fd)
{
  struct io_uring*     ring = &reactor_ctx(h).ring;
  struct io_uring_sqe* sqe  = io_uring_get_sqe(ring);
  if (!sqe) {
    io_uring_submit(ring);
    sqe = io_uring_get_sqe(ring);
  }
  if (!sqe) return -1;
  io_uring_prep_cancel64(sqe, reactor_userdata(fd, ReactorOp::PollAdd), 0);
  io_uring_sqe_set_data64(sqe, reactor_userdata(fd, ReactorOp::Cancel));
  return 0;
}

// mod = cancel + re-add with new interest mask.
inline int reactor_mod(reactor_handle_t h, int fd, uint32_t ev)
{
  reactor_del(h, fd);
  return reactor_add(h, fd, ev);
}

inline void reactor_add_wakeup(reactor_handle_t h, int fd, uint32_t ev)
{ reactor_add(h, fd, ev); }

/// Wait for CQEs.  Submits pending SQEs, blocks until at least one CQE
/// arrives or timeout_ms elapses, then drains all available CQEs.
///
/// PollAdd CQEs → synthetic epoll_event with the poll mask in .events.
/// Connect CQEs → synthetic epoll_event with REACTOR_EV_CONNECT flag.
/// Cancel/Timeout CQEs → silently consumed.
inline int reactor_wait(reactor_handle_t h, reactor_event_t* buf, int maxev, int timeout_ms)
{
  auto& ring = reactor_ctx(h).ring;

  io_uring_submit(&ring);

  struct __kernel_timespec  ts{};
  struct __kernel_timespec* tsp = nullptr;
  if (timeout_ms >= 0) {
    ts.tv_sec  = timeout_ms / 1000;
    ts.tv_nsec = (long)(timeout_ms % 1000) * 1'000'000L;
    tsp        = &ts;
  }

  struct io_uring_cqe* cqe;
  int                  ret = io_uring_wait_cqe_timeout(&ring, &cqe, tsp);
  if (ret < 0) {
    if (ret == -ETIME || ret == -EINTR) return 0;
    return -1;
  }

  int nev = 0;
  while (io_uring_peek_cqe(&ring, &cqe) == 0) {
    uint64_t  ud = io_uring_cqe_get_data64(cqe);
    ReactorOp op = reactor_userdata_op(ud);
    int       fd = reactor_userdata_fd(ud);

    if (nev < maxev) {
      if (op == ReactorOp::PollAdd && cqe->res > 0) {
        buf[nev].data.fd = fd;
        buf[nev].events  = (uint32_t)cqe->res;
        ++nev;
      } else if (op == ReactorOp::Connect) {
        buf[nev].data.fd = fd;
        if (cqe->res == 0)
          buf[nev].events = REACTOR_EV_CONNECT;
        else if (cqe->res == -ECANCELED)
          buf[nev].events = REACTOR_EV_CONNECT | REACTOR_EV_HUP; // timed out
        else
          buf[nev].events = REACTOR_EV_CONNECT | REACTOR_EV_ERR; // refused/reset
        ++nev;
      }
      // PollAdd with res<=0 (cancelled/closed), Timeout, Cancel: silently drop.
    }
    io_uring_cqe_seen(&ring, cqe);
  }
  return nev;
}

#endif // REACTOR_BACKEND_URING

//==============================================================================
// epoll backend (Linux — used on WSL and when liburing is absent)
//==============================================================================
#if defined(REACTOR_BACKEND_EPOLL)

inline reactor_handle_t reactor_create(unsigned /*entries*/ = 0, unsigned = 0)
{
  int fd = ::epoll_create1(EPOLL_CLOEXEC);
  return fd; // -1 on failure, caller checks
}
inline void reactor_destroy(reactor_handle_t h)
{
  if (h >= 0) ::close(h);
}

inline int reactor_add(reactor_handle_t h, int fd, uint32_t ev)
{
  epoll_event e{};
  e.events  = ev;
  e.data.fd = fd;
  return ::epoll_ctl(h, EPOLL_CTL_ADD, fd, &e);
}
inline int reactor_mod(reactor_handle_t h, int fd, uint32_t ev)
{
  epoll_event e{};
  e.events  = ev;
  e.data.fd = fd;
  if (::epoll_ctl(h, EPOLL_CTL_MOD, fd, &e) < 0) {
    if (errno == ENOENT) return ::epoll_ctl(h, EPOLL_CTL_ADD, fd, &e);
    return -1;
  }
  return 0;
}
inline int reactor_del(reactor_handle_t h, int fd)
{ return ::epoll_ctl(h, EPOLL_CTL_DEL, fd, nullptr); }
inline int reactor_wait(reactor_handle_t h, reactor_event_t* buf, int maxev, int timeout_ms)
{
  int n = ::epoll_wait(h, buf, maxev, timeout_ms);
  return (n < 0 && errno == EINTR) ? 0 : n;
}

#endif // REACTOR_BACKEND_EPOLL

//==============================================================================
// Linux shared: eventfd + timerfd  (both epoll and io_uring backends)
//==============================================================================
#if defined(REACTOR_OS_LINUX)

inline int reactor_eventfd_create()
{
  return ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC); // -1 on failure
}
inline int reactor_eventfd_write_fd(int efd)
{ return efd; }
inline void reactor_eventfd_close(int efd)
{
  if (efd >= 0) ::close(efd);
}
inline int reactor_eventfd_read(int efd, uint64_t& val)
{ return (::read(efd, &val, 8) == 8) ? 0 : -1; }
inline int reactor_eventfd_write(int wfd, uint64_t val)
{ return (::write(wfd, &val, 8) == 8) ? 0 : -1; }

inline int reactor_timerfd_create()
{
  return ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC); // -1 on failure
}
inline int
reactor_timerfd_arm(reactor_handle_t /*h*/, int tid, uint64_t initial_ms, uint64_t interval_ms)
{
  itimerspec ts{};
  ts.it_value.tv_sec     = initial_ms / 1000;
  ts.it_value.tv_nsec    = (initial_ms % 1000) * 1'000'000L;
  ts.it_interval.tv_sec  = interval_ms / 1000;
  ts.it_interval.tv_nsec = (interval_ms % 1000) * 1'000'000L;
  return ::timerfd_settime(tid, 0, &ts, nullptr);
}
inline int reactor_timerfd_read(int tid, uint64_t& exp)
{ return (::read(tid, &exp, 8) == 8) ? 0 : -1; }
inline void reactor_timerfd_close(reactor_handle_t /*h*/, int tid)
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
  return ::kqueue(); // -1 on failure
}
inline void reactor_destroy(reactor_handle_t h)
{
  if (h >= 0) ::close(h);
}

namespace detail {
  inline void kq_to_kevent(int fd, uint32_t ev, bool add, struct kevent* out, int& n)
  {
    n                    = 0;
    unsigned short flags = add ? (EV_ADD | EV_CLEAR) : EV_DELETE;
    if (add && (ev & REACTOR_EV_ONESHOT)) flags = EV_ADD | EV_ONESHOT;
    if (ev & REACTOR_EV_IN) EV_SET(&out[n++], fd, EVFILT_READ, flags, 0, 0, (void*)(uintptr_t)fd);
    if (ev & REACTOR_EV_OUT) EV_SET(&out[n++], fd, EVFILT_WRITE, flags, 0, 0, (void*)(uintptr_t)fd);
  }
} // namespace detail

inline int reactor_add(reactor_handle_t kq, int fd, uint32_t ev)
{
  struct kevent ch[2];
  int           n;
  detail::kq_to_kevent(fd, ev, true, ch, n);
  if (n > 0 && ::kevent(kq, ch, n, nullptr, 0, nullptr) < 0) return -1;
  return 0;
}
inline int reactor_mod(reactor_handle_t kq, int fd, uint32_t ev)
{
  struct kevent del[2];
  int           n;
  detail::kq_to_kevent(fd, REACTOR_EV_IN | REACTOR_EV_OUT, false, del, n);
  ::kevent(kq, del, n, nullptr, 0, nullptr);
  return reactor_add(kq, fd, ev);
}
inline int reactor_del(reactor_handle_t kq, int fd)
{
  struct kevent del[2];
  int           n;
  detail::kq_to_kevent(fd, REACTOR_EV_IN | REACTOR_EV_OUT, false, del, n);
  return ::kevent(kq, del, n, nullptr, 0, nullptr) < 0 ? -1 : 0;
}
inline int reactor_wait(reactor_handle_t kq, reactor_event_t* buf, int maxev, int timeout_ms)
{
  struct kevent   raw[64];
  int             cap = (maxev < 64) ? maxev : 64;
  struct timespec ts{}, *tsp = nullptr;
  if (timeout_ms >= 0) {
    ts.tv_sec  = timeout_ms / 1000;
    ts.tv_nsec = (timeout_ms % 1000) * 1'000'000L;
    tsp        = &ts;
  }
  int nev = ::kevent(kq, nullptr, 0, raw, cap, tsp);
  if (nev <= 0) return nev;
  for (int i = 0; i < nev; ++i) {
    buf[i].fd   = (int)raw[i].ident;
    buf[i].mask = 0;
    if (raw[i].filter == EVFILT_READ) buf[i].mask |= REACTOR_EV_IN;
    if (raw[i].filter == EVFILT_WRITE) buf[i].mask |= REACTOR_EV_OUT;
    if (raw[i].flags & EV_ERROR) buf[i].mask |= REACTOR_EV_ERR;
    if (raw[i].flags & EV_EOF) buf[i].mask |= REACTOR_EV_HUP;
  }
  return nev;
}

// ---- eventfd via pipe (macOS) -----------------------------------------------
namespace detail {
  inline std::mutex& pipe_mtx()
  {
    static std::mutex m;
    return m;
  }
  inline std::unordered_map<int, int>& pipe_map()
  {
    static std::unordered_map<int, int> m;
    return m;
  }
} // namespace detail

inline int reactor_eventfd_create()
{
  int fds[2];
  if (::pipe(fds) < 0) return -1;
  ::fcntl(fds[0], F_SETFL, O_NONBLOCK | O_CLOEXEC);
  ::fcntl(fds[1], F_SETFL, O_NONBLOCK | O_CLOEXEC);
  std::lock_guard<std::mutex> lg(detail::pipe_mtx());
  detail::pipe_map()[fds[0]] = fds[1];
  return fds[0];
}
inline int reactor_eventfd_write_fd(int efd)
{
  std::lock_guard<std::mutex> lg(detail::pipe_mtx());
  auto                        it = detail::pipe_map().find(efd);
  return (it != detail::pipe_map().end()) ? it->second : -1;
}
inline void reactor_eventfd_close(int efd)
{
  if (efd < 0) return;
  std::lock_guard<std::mutex> lg(detail::pipe_mtx());
  auto                        it = detail::pipe_map().find(efd);
  if (it != detail::pipe_map().end()) {
    ::close(it->second);
    detail::pipe_map().erase(it);
  }
  ::close(efd);
}
inline int reactor_eventfd_read(int efd, uint64_t& val)
{
  char    buf[256];
  ssize_t n, total = 0;
  while ((n = ::read(efd, buf, sizeof(buf))) > 0) total += n;
  if (total == 0 && errno != EAGAIN) return -1;
  val = (uint64_t)total;
  return 0;
}
inline int reactor_eventfd_write(int wfd, uint64_t /*val*/)
{
  char b = 1;
  return (::write(wfd, &b, 1) == 1) ? 0 : -1;
}

// ---- timerfd via EVFILT_TIMER (macOS) ---------------------------------------
namespace detail {
  inline std::atomic<int>& timer_counter()
  {
    static std::atomic<int> c{0x7000'0000};
    return c;
  }
} // namespace detail

inline int reactor_timerfd_create()
{ return ++detail::timer_counter(); }

inline int
reactor_timerfd_arm(reactor_handle_t kq, int tid, uint64_t initial_ms, uint64_t interval_ms)
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
inline int reactor_timerfd_read(int /*tid*/, uint64_t& exp)
{
  exp = 1;
  return 0;
}
inline void reactor_timerfd_close(reactor_handle_t kq, int tid)
{
  if (tid < 0) return;
  struct kevent del;
  EV_SET(&del, tid, EVFILT_TIMER, EV_DELETE, 0, 0, nullptr);
  ::kevent(kq, &del, 1, nullptr, 0, nullptr);
}

#endif // REACTOR_BACKEND_KQUEUE

} // namespace arterial
