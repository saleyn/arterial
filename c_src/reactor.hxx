#pragma once
// vim:ts=2:sw=2:et
// Implementation of Reactor member functions — included once from arterial_nif.cpp.
// All definitions are in namespace arterial.

#include "reactor.hpp"

namespace arterial {

//==============================================================================
// Construction / destruction
//==============================================================================

Reactor::Reactor(std::string ident, int fd_vec_size, unsigned cmds_ring_cap)
  : m_ident(std::move(ident))
  , m_handle(reactor_create())
  , m_wakeup_rd(reactor_eventfd_create())
  , m_wakeup_wr(reactor_eventfd_write_fd(m_wakeup_rd))
  , m_running(false)
  , m_entries(fd_vec_size)
  , m_cmds(cmds_ring_cap)
{
  if (m_handle < 0 || m_wakeup_rd < 0) {
    m_valid = false;
    close_handles();
    return;
  }
  if (reactor_add(m_handle, m_wakeup_rd, REACTOR_EV_IN | REACTOR_EV_ET) < 0) {
    m_valid = false;
    close_handles();
    return;
  }
  m_valid = true;
}

//==============================================================================
// Public API
//==============================================================================

void Reactor::start(ErlNifPid owner_pid)
{
  bool expected = false;
  if (!m_running.compare_exchange_strong(expected, true))
    return; // already running

  m_owner_pid = owner_pid;
  std::unique_lock<std::mutex> lk(m_ready_mu);
  m_ready  = false;
  m_thread = std::thread(&Reactor::run_loop, this);
  m_ready_cv.wait(lk, [this] { return m_ready; });
}

void Reactor::close_handles()
{
  reactor_eventfd_close(m_wakeup_rd);
  reactor_destroy(m_handle);
  m_wakeup_rd = -1;
  m_handle    = -1;
}

void Reactor::stop()
{
  bool expected = true;
  if (!m_running.compare_exchange_strong(expected, false))
    return;
  ReactorCmd cmd;
  cmd.type = CmdType::Stop;
  post(cmd);
  if (m_thread.joinable()) m_thread.join();
  close_handles();
}

void Reactor::register_handlers(int            fd,
                                ReadHandler    on_read,
                                ErrorHandler   on_error,
                                WriteHandler   on_write,
                                TimeoutHandler on_timeout,
                                void*          user_data)
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

void Reactor::add_fd(int            fd,
                     ReadHandler    on_read,
                     ErrorHandler   on_error,
                     WriteHandler   on_write,
                     TimeoutHandler on_timeout,
                     void*          user_data)
{
  {
    auto& e       = ensure_entry(fd);
    e.fd          = fd;
    e.user_data   = user_data;
    e.on_readable = std::move(on_read);
    e.on_writable = std::move(on_write);
    e.on_error    = std::move(on_error);
    e.on_timeout  = std::move(on_timeout);
  }
  ReactorCmd cmd{};
  cmd.type = CmdType::AddFd;
  cmd.fd   = fd;
  post(cmd);
}

void Reactor::remove_fd(int fd)
{
  ReactorCmd cmd{};
  cmd.type = CmdType::RemoveFd;
  cmd.fd   = fd;
  post(cmd);
}

void Reactor::arm_write(int fd)
{
  ReactorCmd cmd{};
  cmd.type = CmdType::ArmWrite;
  cmd.fd   = fd;
  post(cmd);
}

void Reactor::set_timeout(int fd, uint64_t timeout_ms)
{
  if (timeout_ms == 0) {
    ReactorCmd cmd{};
    cmd.type = CmdType::CancelTimeout;
    cmd.fd   = fd;
    post(cmd);
  } else {
    ReactorCmd cmd{};
    cmd.type    = CmdType::SetTimeout;
    cmd.timeout = {fd, timeout_ms};
    post(cmd);
  }
}

void Reactor::connect(int              fd,
                       struct sockaddr_in addr,
                       uint64_t         timeout_ms,
                       ErlNifPid        caller_pid,
                       uint32_t         stripe_id,
                       uint32_t         slot_id,
                       ReadHandler      on_readable,
                       WriteHandler     on_writable,
                       ErrorHandler     on_error,
                       TimeoutHandler   on_timeout,
                       void*            user_data)
{
  {
    auto& e       = ensure_entry(fd);
    e.fd          = fd;
    e.user_data   = user_data;
    e.on_readable = std::move(on_readable);
    e.on_writable = std::move(on_writable);
    e.on_error    = std::move(on_error);
    e.on_timeout  = std::move(on_timeout);
  }
  ReactorCmd cmd{};
  cmd.type    = CmdType::Connect;
  cmd.connect = {fd, addr, timeout_ms, caller_pid, stripe_id, slot_id};
  post(cmd);
}

//==============================================================================
// Private helpers
//==============================================================================

void Reactor::post(const ReactorCmd& cmd)
{
  while (!m_cmds.push(cmd))
    std::this_thread::yield(); // ring full — extremely rare
  uint64_t one = 1;
  reactor_eventfd_write(m_wakeup_wr, one);
}

//==============================================================================
// Reactor loop
//==============================================================================

void Reactor::run_loop()
{
  reactor_event_t events[s_max_events];

  {
    std::lock_guard<std::mutex> lk(m_ready_mu);
    m_ready = true;
  }
  m_ready_cv.notify_one();

  drain_commands();

  bool abnormal_exit = false;
  int  exit_errno    = 0;

  while (m_running.load(std::memory_order_relaxed)) {
    int nev = reactor_wait(m_handle, events, s_max_events, s_poll_ms);
    if (nev < 0 && errno != EINTR) {
      abnormal_exit = true;
      exit_errno    = errno;
      break;
    }
    drain_commands();
    for (int i = 0; i < nev; ++i)
      dispatch(events[i]);
  }

  drain_commands();

  if (abnormal_exit)
    send_reactor_exit(exit_errno);
}

void Reactor::dispatch(const reactor_event_t& ev)
{
  int      fd   = reactor_ev_fd(ev);
  uint32_t mask = reactor_ev_mask(ev);

  if (fd == m_wakeup_rd) {
    uint64_t val;
    reactor_eventfd_read(m_wakeup_rd, val);
    reactor_mod(m_handle, m_wakeup_rd, REACTOR_EV_IN | REACTOR_EV_ET);
    return;
  }

  // Check if this is a timer fd.  Extract owner under the timer lock,
  // then release before calling fire_timeout to avoid lock inversion.
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

  FdEntry* e = find_entry(fd);
  if (!e) return;

  // io_uring linked-connect CQE.
  if (mask & REACTOR_EV_CONNECT) {
    if (e->on_connect) {
      ConnectHandler cb = std::move(e->on_connect);
      e->on_connect     = {};
      cb(mask);
    }
    return;
  }

  // When write is armed, any readiness event goes through on_writable first
  // so getsockopt(SO_ERROR) can report the real connect outcome.
  if (e->write_armed && e->on_writable) {
    e->write_armed = false;
    int rc         = e->on_writable(fd, e->user_data);
    if (rc < 0) { do_remove_fd(fd); return; }
  } else if (reactor_is_error(mask)) {
    if (e->on_error) e->on_error(fd, e->user_data);
    return;
  }

  if (reactor_is_readable(mask) && e->on_readable) {
    int rc = e->on_readable(fd, e->user_data);
    if (rc < 0) { do_remove_fd(fd); return; }
#if defined(REACTOR_BACKEND_URING)
    if (e->on_readable)
      reactor_add(m_handle, fd, REACTOR_EV_IN | REACTOR_EV_RDHUP);
#endif
  }

  // Second writable check: re-arm after on_writable returned 0.
  if (reactor_is_writable(mask) && e->on_writable && e->write_armed) {
    e->write_armed = false;
    int rc         = e->on_writable(fd, e->user_data);
    if (rc < 0) do_remove_fd(fd);
  }
}

//==============================================================================
// Command dispatch (reactor thread only)
//==============================================================================

void Reactor::drain_commands()
{
  ReactorCmd cmd;
  while (m_cmds.pop(cmd)) {
    switch (cmd.type) {
      case CmdType::Stop:          m_running.store(false);                             return;
      case CmdType::AddFd:         do_add_fd(cmd.fd);                                  break;
      case CmdType::RemoveFd:      do_remove_fd(cmd.fd);                               break;
      case CmdType::ArmWrite:      do_arm_write(cmd.fd);                               break;
      case CmdType::SetTimeout:    do_set_timeout(cmd.timeout.fd, cmd.timeout.timeout_ms); break;
      case CmdType::CancelTimeout: do_cancel_timeout(cmd.fd);                          break;
      case CmdType::Connect:       do_connect(cmd.connect);                            break;
      default:                     break;
    }
  }
}

void Reactor::do_add_fd(int fd)
{
  FdEntry* e = find_entry(fd);
  if (!e || e->fd < 0) return;
  if (!e->on_readable) return;  // arm_connect: no EPOLLIN needed yet
  reactor_mod(m_handle, fd, REACTOR_EV_IN | REACTOR_EV_RDHUP);
  // If data arrived before the poll was registered, dispatch directly.
  int bytes = 0;
  if (::ioctl(fd, FIONREAD, &bytes) == 0 && bytes > 0 && e->on_readable) {
    int rc = e->on_readable(fd, e->user_data);
    if (rc < 0) do_remove_fd(fd);
  }
}

void Reactor::do_remove_fd(int fd)
{
  reactor_del(m_handle, fd);
  do_cancel_timeout(fd);
  clear_entry(fd);
  ::close(fd);
}

void Reactor::do_arm_write(int fd)
{
  FdEntry* e = find_entry(fd);
  if (!e) return;
  e->write_armed = true;
  uint32_t ev    = REACTOR_EV_OUT | REACTOR_EV_RDHUP;
  if (e->on_readable) ev |= REACTOR_EV_IN;
  else                ev |= REACTOR_EV_ET;
  if (reactor_mod(m_handle, fd, ev) < 0)
    do_remove_fd(fd);  // fd closed before arm_write was processed
}

void Reactor::do_set_timeout(int fd, uint64_t timeout_ms)
{
  FdEntry* e = find_entry(fd);
  if (!e) return;

  do_cancel_timeout(fd);

  int tid = reactor_timerfd_create();
  if (tid < 0) return;

  if (reactor_timerfd_arm(m_handle, tid, timeout_ms, 0) < 0) {
    reactor_timerfd_close(m_handle, tid);
    return;
  }

#if defined(REACTOR_OS_LINUX)
  if (reactor_add(m_handle, tid, REACTOR_EV_IN | REACTOR_EV_ONESHOT) < 0) {
    reactor_timerfd_close(m_handle, tid);
    return;
  }
#endif

  e->timer_id = tid;
  {
    std::lock_guard<std::mutex> lk(m_timers_mu);
    m_timer_to_fd.set(tid, fd);
  }
}

void Reactor::do_cancel_timeout(int fd)
{
  FdEntry* e = find_entry(fd);
  if (!e || e->timer_id < 0) return;
  int tid     = e->timer_id;
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

void Reactor::fire_timeout(int fd)
{
  FdEntry* e = find_entry(fd);
  if (!e) return;
  int tid     = e->timer_id;
  e->timer_id = -1;
  {
    std::lock_guard<std::mutex> lk(m_timers_mu);
    m_timer_to_fd.clear(tid);
  }
  reactor_timerfd_close(m_handle, tid);
  if (e->on_timeout) e->on_timeout(fd, e->user_data);
}

//==============================================================================
// Async connect
//==============================================================================

void Reactor::do_connect(const ConnectParams& p)
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
  {
    reactor_del(m_handle, fd);

    struct io_uring*         ring = reactor_uring(m_handle);
    struct __kernel_timespec ts{};

    struct io_uring_sqe* sqe_connect = io_uring_get_sqe(ring);
    if (!sqe_connect) { io_uring_submit(ring); sqe_connect = io_uring_get_sqe(ring); }
    if (!sqe_connect) {
      send_connect_result(pid, stripe, slot, false);
      close_fd();
      return;
    }

    io_uring_prep_connect(sqe_connect, fd,
                          reinterpret_cast<const sockaddr*>(&p.addr), sizeof(p.addr));
    io_uring_sqe_set_data64(sqe_connect, reactor_userdata(fd, ReactorOp::Connect));
    if (p.timeout_ms > 0) sqe_connect->flags |= IOSQE_IO_LINK;

    if (p.timeout_ms > 0) {
      struct io_uring_sqe* sqe_timeout = io_uring_get_sqe(ring);
      if (!sqe_timeout) { io_uring_submit(ring); sqe_timeout = io_uring_get_sqe(ring); }
      if (!sqe_timeout) {
        sqe_connect->flags &= ~IOSQE_IO_LINK;
      } else {
        ts.tv_sec  = p.timeout_ms / 1000;
        ts.tv_nsec = (p.timeout_ms % 1000) * 1'000'000L;
        io_uring_prep_link_timeout(sqe_timeout, &ts, 0);
        io_uring_sqe_set_data64(sqe_timeout, reactor_userdata(fd, ReactorOp::Timeout));
      }
    }

    io_uring_submit(ring);

    TimeoutHandler uot = std::move(e->on_timeout);

    e->on_connect = [this, pid, stripe, slot, fd, close_fd,
                     uot = std::move(uot)](uint32_t mask) mutable {
      if (mask & (REACTOR_EV_ERR | REACTOR_EV_HUP)) {
        bool         is_timeout = !(mask & REACTOR_EV_ERR);
        FdEntry*     entry      = find_entry(fd);
        ErrorHandler onerr      = entry ? std::move(entry->on_error) : ErrorHandler{};
        void*        ud         = entry ? entry->user_data : nullptr;
        close_fd();
        if (is_timeout) {
          send_timeout(pid, stripe, slot);
          if (uot) uot(fd, nullptr);
        } else {
          if (onerr) onerr(fd, ud);
          send_connect_result(pid, stripe, slot, false);
        }
      } else {
        reactor_add(m_handle, fd, REACTOR_EV_IN | REACTOR_EV_RDHUP);
        FdEntry* entry = find_entry(fd);
        if (entry && entry->on_writable) {
          WriteHandler wh    = std::move(entry->on_writable);
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

  // epoll / kqueue path
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

  if (reactor_add(m_handle, fd,
                  REACTOR_EV_OUT | REACTOR_EV_IN | REACTOR_EV_ET | REACTOR_EV_RDHUP) < 0) {
    send_connect_result(pid, stripe, slot, false);
    close_fd();
    return;
  }
  if (p.timeout_ms > 0) do_set_timeout(fd, p.timeout_ms);
}

//==============================================================================
// Erlang message helpers (reactor thread — process-independent env)
//==============================================================================

void Reactor::send_connect_result(const ErlNifPid& pid,
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

void Reactor::send_timeout(const ErlNifPid& pid, uint32_t stripe, uint32_t slot)
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

void Reactor::send_reactor_exit(int err)
{
  static const ErlNifPid kZeroPid{};
  if (std::memcmp(&m_owner_pid, &kZeroPid, sizeof(ErlNifPid)) == 0) return;
  ErlNifEnv* env = enif_alloc_env();
  if (!env) return;
  ERL_NIF_TERM msg = enif_make_tuple3(env,
    enif_make_atom(env, "arterial_reactor_exit"),
    enif_make_atom(env, m_ident.c_str()),
    enif_make_int(env, err));
  enif_send(nullptr, &m_owner_pid, env, msg);
  enif_free_env(env);
}

//==============================================================================
// Entry table
//==============================================================================

FdEntry* Reactor::find_entry(int fd)
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

FdEntry& Reactor::ensure_entry(int fd)
{
  if (fd < static_cast<int>(m_entries.size()))
    return m_entries[fd];  // no mutex — vector never resized after construction
  std::lock_guard<std::mutex> lk(m_entries_overflow_mu);
  return m_entries_overflow[fd];
}

void Reactor::clear_entry(int fd)
{
  if (fd < 0) return;
  if (fd < static_cast<int>(m_entries.size())) {
    m_entries[fd] = FdEntry{};  // no mutex — reactor thread only
    return;
  }
  std::lock_guard<std::mutex> lk(m_entries_overflow_mu);
  m_entries_overflow.erase(fd);
}

} // namespace arterial
