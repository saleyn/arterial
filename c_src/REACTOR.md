# Arterial I/O Reactor

A single-threaded, fully asynchronous I/O engine for the arterial NIF layer.
Runs on a dedicated background thread; all NIF calls enqueue commands and
return immediately.

## Backends

| Platform | Backend | Selected when |
|---|---|---|
| Linux | **io_uring** | `<liburing.h>` present and `REACTOR_NO_URING` not defined |
| Linux | **epoll** | `-DREACTOR_NO_URING` or no liburing |
| macOS / BSD | **kqueue** | always |

The public API is identical on all three.

---

## Architecture

```
  NIF thread(s)                    Reactor thread
  ─────────────                    ──────────────
  socket() + fcntl(O_NONBLOCK)
  reactor.Connect(fd, …) ────────► do_connect()
  reactor.AddFd(fd, …)   ────────►   ::connect() (EINPROGRESS)
  reactor.ArmWrite(fd)   ────────►   reactor_add(fd, EPOLLOUT|EPOLLIN)
  reactor.SetTimeout(fd) ────────►   reactor_timerfd_arm(50ms)
  return to Erlang immediately               │
                                    io_uring/epoll/kqueue waits…
                                             │
                                    socket write-ready → on_writable()
                                    OR timer fires     → on_timeout()
                                             │
                                    enif_send(pid, {arterial_event,…})
```

Commands travel through a **lock-free MPSC ring** (4 096 slots).
A wakeup eventfd (Linux) or pipe (macOS) wakes the reactor thread
immediately when a command is posted; there is no polling delay.

---

## Files

| File | Purpose |
|---|---|
| `reactor_platform.hpp` | Platform primitives — all inline, no `.cpp` needed |
| `reactor.hpp` | `Reactor` class — the engine |
| `erl_nif_stub.h` | Stub NIF types for C++-only unit tests |
| `reactor_test.cpp` | 13 standalone C++ tests (`make test_reactor`) |

---

## Quick start

```cpp
#include "reactor.hpp"
using namespace arterial;

// Create and start one reactor per pool (or share one globally).
Reactor reactor("my_pool");
reactor.Start();

// … register fds, connect, etc. …

reactor.Stop();  // joins the background thread
```

---

## API reference

### Lifecycle

```cpp
Reactor r("ident");   // creates epoll/kqueue/io_uring handle; does NOT start thread
r.Start();            // spawns background thread
r.Stop();             // signals thread, joins it, destroys handle
bool live = r.Running();
```

### `AddFd` — watch an fd for I/O events

```cpp
r.AddFd(
  fd,
  // on_readable: called every time fd has data to read.
  // Return 0 to keep the fd registered; return <0 to remove it.
  [](int fd, void* ud) -> int {
    char buf[4096];
    ssize_t n;
    while ((n = ::read(fd, buf, sizeof(buf))) > 0)
      process(buf, n);
    return 0;
  },
  // on_error: called on EPOLLHUP / EPOLLERR / EOF.
  [](int fd, void* ud) {
    cleanup(fd);
  },
  // on_writable (optional): one-shot, only fires after ArmWrite().
  [](int fd, void* ud) -> int {
    flush_pending_writes(fd);
    return 0;
  },
  // on_timeout (optional): fires after SetTimeout(); one-shot.
  [](int fd, void* ud) {
    handle_idle_timeout(fd);
  },
  my_context_ptr   // void* user_data, passed back to every handler
);
```

**Read notifications are persistent** (edge-triggered): once registered the
reactor keeps delivering `on_readable` on every new data arrival without
needing to re-register.

**Write notifications are one-shot**: `on_writable` fires exactly once per
`ArmWrite()` call.  Re-arm after each flush to get the next notification.

### `RemoveFd` — deregister and close

```cpp
r.RemoveFd(fd);   // posts command; reactor thread closes fd when processed
```

The fd is closed inside the reactor thread. Do not `close()` it yourself
after calling `RemoveFd`.

### `ArmWrite` — request a write-ready notification

```cpp
r.ArmWrite(fd);   // on_writable fires once when fd can accept data
```

Typical pattern for a send buffer:

```cpp
// In a NIF function:
append_to_send_buffer(fd, data);
reactor.ArmWrite(fd);

// In on_writable:
[](int fd, void* ud) -> int {
  auto& ctx = *static_cast<Conn*>(ud);
  while (!ctx.send_buf.empty()) {
    ssize_t n = ::send(fd, ctx.send_buf.data(), ctx.send_buf.size(), MSG_NOSIGNAL);
    if (n < 0 && errno == EAGAIN) {
      reactor.ArmWrite(fd);   // re-arm for next opportunity
      return 0;
    }
    ctx.send_buf.consume(n);
  }
  return 0;
}
```

### `SetTimeout` — per-fd deadline

```cpp
r.SetTimeout(fd, 5000);   // on_timeout fires after 5 000 ms
r.SetTimeout(fd, 0);      // cancel any pending timeout
```

Timeouts are one-shot.  To implement a sliding deadline (reset on each
message received):

```cpp
// on_readable:
[](int fd, void* ud) -> int {
  read_data(fd);
  reactor.SetTimeout(fd, 30'000);   // push deadline 30 s into the future
  return 0;
}
// on_timeout:
[](int fd, void* ud) {
  close_idle_connection(fd);
  reactor.RemoveFd(fd);
}
```

### `Connect` — async TCP connect with timeout

```cpp
int fd = ::socket(AF_INET, SOCK_STREAM, 0);
::fcntl(fd, F_SETFL, O_NONBLOCK);
// Do NOT call ::connect() — the reactor does it on its thread.

struct sockaddr_in addr{};
addr.sin_family      = AF_INET;
addr.sin_port        = htons(9000);
addr.sin_addr.s_addr = inet_addr("10.0.0.1");

reactor.Connect(
  fd,
  addr,
  5000,           // timeout_ms (0 = no timeout)
  owner_pid,      // ErlNifPid to receive the result message
  stripe_id,      // ─┐ encoded into the result message
  slot_id,        // ─┘
  // on_readable — installed for post-connect I/O
  [](int fd, void* ud) -> int {
    read_response(fd, ud);
    return 0;
  },
  // on_writable — one-shot send
  [](int fd, void* ud) -> int {
    send_request(fd, ud);
    return 0;
  },
  // on_error
  [](int fd, void* ud) { handle_disconnect(fd, ud); },
  // on_timeout — connect-phase timeout notification (user-supplied)
  [](int fd, void* ud) { log_connect_timeout(fd); },
  &my_context
);
```

The reactor sends one of these messages to `owner_pid`:

```erlang
{arterial_event, StripeId, SlotId, connect_result, ok}           % connected
{arterial_event, StripeId, SlotId, connect_result, connect_failed} % refused/reset
{arterial_event, StripeId, SlotId, timeout}                        % timed out
```

The Erlang process receives the message and calls back into the NIF
(e.g. `arterial_nif:handle_connection_timeout/3`) for any cleanup that
requires NIF-side state.

---

## Scenarios

### 1. Persistent TCP reader

```cpp
int fd = open_connected_socket();
reactor.AddFd(fd,
  [](int fd, void* ud) -> int {
    auto& conn = *static_cast<MyConn*>(ud);
    char buf[65536];
    ssize_t n;
    while ((n = ::recv(fd, buf, sizeof(buf), 0)) > 0)
      conn.codec.feed(buf, n);
    if (n == 0) return -1;  // peer closed → RemoveFd
    return 0;               // EAGAIN → wait for next event
  },
  [](int fd, void* ud) {
    static_cast<MyConn*>(ud)->on_disconnect();
  },
  {}, {}, &my_conn
);
```

### 2. Request–response with send timeout

```cpp
// After connect succeeds:
reactor.ArmWrite(fd);              // send request immediately
reactor.SetTimeout(fd, 2000);      // 2 s to get a response

// on_writable:
[](int fd, void* ud) -> int {
  auto& ctx = *static_cast<Ctx*>(ud);
  ::send(fd, ctx.req.data(), ctx.req.size(), MSG_NOSIGNAL);
  return 0;
}

// on_readable:
[](int fd, void* ud) -> int {
  auto& ctx = *static_cast<Ctx*>(ud);
  read_response(fd, ctx);
  reactor.SetTimeout(fd, 0);   // cancel timeout on success
  return 0;
}

// on_timeout:
[](int fd, void* ud) {
  auto& ctx = *static_cast<Ctx*>(ud);
  ctx.reply_timeout();
  // fd was already closed by the reactor in do_cancel_timeout
}
```

### 3. Idle-connection watchdog

```cpp
reactor.AddFd(fd,
  [](int fd, void* ud) -> int {
    read_and_process(fd, ud);
    reactor.SetTimeout(fd, 30'000);   // slide deadline on every read
    return 0;
  },
  [](int fd, void* ud) { handle_error(fd, ud); },
  {}, // no write handler
  [](int fd, void* ud) {
    // 30 s of silence — close the connection
    send_notification(static_cast<Ctx*>(ud), "idle_timeout");
    reactor.RemoveFd(fd);
  }
);
reactor.SetTimeout(fd, 30'000);   // arm initial deadline
```

### 4. Fan-out — many connections, one reactor

```cpp
Reactor reactor("pool");
reactor.Start();

for (auto& server : server_list) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ::fcntl(fd, F_SETFL, O_NONBLOCK);
  reactor.Connect(fd, server.addr, 3000, server.pid,
                  server.stripe, server.slot,
                  make_read_handler(&server),
                  make_write_handler(&server),
                  make_error_handler(&server),
                  make_timeout_handler(&server),
                  &server);
}
// All connects run concurrently on the single reactor thread.
```

### 5. Write flush with back-pressure

```cpp
struct SendCtx {
  std::vector<char> buf;
  Reactor*          reactor;
};

// Enqueue data from any thread:
void enqueue_write(SendCtx* ctx, const char* data, size_t n) {
  ctx->buf.insert(ctx->buf.end(), data, data + n);
  ctx->reactor->ArmWrite(ctx->fd);
}

// on_writable fires on the reactor thread:
[](int fd, void* ud) -> int {
  auto* ctx = static_cast<SendCtx*>(ud);
  while (!ctx->buf.empty()) {
    ssize_t n = ::send(fd, ctx->buf.data(), ctx->buf.size(), MSG_NOSIGNAL);
    if (n > 0) {
      ctx->buf.erase(ctx->buf.begin(), ctx->buf.begin() + n);
    } else if (errno == EAGAIN) {
      ctx->reactor->ArmWrite(fd);   // kernel buffer full — re-arm
      return 0;
    } else {
      return -1;   // error → RemoveFd
    }
  }
  return 0;
}
```

---

## Platform primitives

`reactor_platform.hpp` exposes the underlying primitives directly if you need
to integrate with existing code or submit custom io_uring SQEs.

```cpp
// Multiplexer
reactor_handle_t h = reactor_create();
reactor_add (h, fd, REACTOR_EV_IN | REACTOR_EV_ET);
reactor_mod (h, fd, REACTOR_EV_IN | REACTOR_EV_OUT | REACTOR_EV_ET);
reactor_del (h, fd);
int n = reactor_wait(h, events, 256, 10 /*ms*/);
reactor_destroy(h);

// Wakeup event (eventfd on Linux, pipe on macOS)
int efd  = reactor_eventfd_create();
int wfd  = reactor_eventfd_write_fd(efd);   // Linux: same; macOS: write-end of pipe
reactor_eventfd_write(wfd, 1);
uint64_t val;
reactor_eventfd_read(efd, val);
reactor_eventfd_close(efd);

// Timer (timerfd on Linux, EVFILT_TIMER on macOS)
int tid = reactor_timerfd_create();
reactor_timerfd_arm(h, tid, 500 /*initial ms*/, 500 /*interval ms, 0=one-shot*/);
uint64_t expirations;
reactor_timerfd_read(tid, expirations);
reactor_timerfd_close(h, tid);

// io_uring raw ring (Linux io_uring backend only)
#if defined(REACTOR_BACKEND_URING)
struct io_uring* ring = reactor_uring(h);
struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
io_uring_prep_send(sqe, fd, buf, len, 0);
io_uring_sqe_set_data64(sqe, my_user_data);
io_uring_submit(ring);
#endif
```

Event flag constants are the same on all platforms:

| Constant | Meaning |
|---|---|
| `REACTOR_EV_IN` | fd is readable |
| `REACTOR_EV_OUT` | fd is writable |
| `REACTOR_EV_ERR` | error condition |
| `REACTOR_EV_HUP` | hang-up (peer closed) |
| `REACTOR_EV_RDHUP` | read half-closed |
| `REACTOR_EV_ET` | edge-triggered (don't re-fire until state changes) |
| `REACTOR_EV_ONESHOT` | fire once then auto-deregister |

---

## Thread safety

| Call site | Safe to call from |
|---|---|
| `reactor.Start()` / `Stop()` | any thread, once |
| `reactor.AddFd()` | any NIF / C++ thread |
| `reactor.RemoveFd()` | any NIF / C++ thread |
| `reactor.ArmWrite()` | any NIF / C++ thread |
| `reactor.SetTimeout()` | any NIF / C++ thread |
| `reactor.Connect()` | any NIF / C++ thread |
| Handler callbacks | **reactor thread only** |
| `reactor_platform.hpp` primitives | single-threaded use only (no internal locking) |

Handler callbacks are always invoked on the reactor thread.  They must not
block.  To hand work back to an Erlang process use `enif_send()` with a
freshly allocated env:

```cpp
[](int fd, void* ud) -> int {
  ErlNifEnv* env = enif_alloc_env();
  ERL_NIF_TERM msg = enif_make_tuple2(env,
    enif_make_atom(env, "data"),
    enif_make_int(env, fd));
  enif_send(nullptr, &owner_pid, env, msg);
  enif_free_env(env);
  return 0;
}
```

The first argument of `enif_send` is `nullptr` (process-independent env) —
correct for calls made from a non-Erlang thread.

---

## Building

```makefile
# Build the NIF .so (io_uring auto-detected):
make -C c_src

# Force epoll fallback:
make -C c_src CXXFLAGS="-DREACTOR_NO_URING"

# Build and run the standalone C++ tests (no Erlang needed):
make -C c_src test_reactor
```

Test output example:

```
Backend: io_uring

  1. eventfd create/write/read/close                      ok
  2. timerfd create/arm/fire/close                        ok
  3. reactor add/mod/del/wait (socketpair + pipe)         ok
  4. Reactor Start/Stop lifecycle                         ok
  5. Reactor AddFd — readable notification (pipe)         ok
  6. Reactor ArmWrite — one-shot write notification       ok
  7. Reactor SetTimeout — fires after ~50 ms              ok
  8. Reactor SetTimeout — cancel before firing            ok
  9. Multiple fds registered simultaneously               ok
  10. RemoveFd — no further notifications after removal   ok
  11. Stress — rapid timeout resets                       ok
  12. Reactor::Connect — success to local listener        ok
  13. Reactor::Connect — timeout (non-routable addr)      ok

Results: 77 passed, 0 failed
```
