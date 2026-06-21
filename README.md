![Banner](https://github.com/saleyn/arterial/blob/main/assets/arterial-banner.png?raw=true)

[![build](https://github.com/saleyn/arterial/actions/workflows/erlang.yaml/badge.svg)](https://github.com/saleyn/arterial/actions/workflows/erlang.yaml)
[![Hex.pm](https://img.shields.io/hexpm/v/arterial.svg)](https://hex.pm/packages/arterial)
[![Hex.pm](https://img.shields.io/hexpm/dt/arterial.svg)](https://hex.pm/packages/glazer)

`arterial` is a high-performance Erlang/Elixir connection-pooling library for
clients that talk to a server (or cluster of servers) over SSL/TCP/UDP.
The design is structurally a shared-nothing/lock-free concurrent model.
The pool, per-connection backlog tracking, and rate throttling are implemented
in a C++ NIF for performance; connection lifecycle, supervision, and socket
I/O stay on the Erlang side.

It's protocol-agnostic: you supply a module that knows how to encode
requests and decode responses for your wire protocol, and Arterial handles
checking out a connection, tracking in-flight requests, throttling,
reconnecting on failure, and timing out asynchronous requests that never
get a reply.

**Supported features:**

* Lock-free, atomic CAS–based connection pool (C++ NIF) with FIFO checkout
  order and ABA-safe versioned node indices.
* Per-connection request backlog, in either FIFO order (no wire-level
  request ID required) or random-access order (wire-level request ID,
  out-of-order replies supported), up to 65,536 in-flight requests per
  connection.
* Per-connection rate throttling (token-bucket style) checked on every
  checkout.
* A synchronous request API (`arterial_client:call/3`) where the caller
  owns the socket for the duration of one request/reply.
* An in-flight request registry (`unordered_map_with_ttl`, owned by each
  pool's `ConnectionPool` instance) for the asynchronous path: register a
  request with `arterial_nif:track_inflight/5`, and if no matching
  `checkin_connection/4` happens before its TTL expires, the owning process
  receives `{arterial_timeout, Pool, ReqID}` and the request's backlog slot
  (and the connection itself) is automatically released back to the pool.
  TTLs can be either a single fixed duration per pool or set individually
  per request.
* An optional, lock-free, bounded queue-when-busy wait-list
  (`arterial_nif:checkout_async/3`): when every connection is busy, a
  caller can be queued (up to a fixed capacity set at pool creation)
  instead of failing immediately, and is serviced automatically as soon as
  a connection frees up, or timed out if it waits too long. Disabled by
  default (zero behavioral/cost impact unless explicitly enabled); see
  [Queue-when-busy checkouts](#queue-when-busy-checkouts).
* A per-pool `arterial_sweeper` process that periodically evicts expired
  in-flight requests and expired queued waiters by calling
  `arterial_nif:sweep_timeouts/1`.
* Automatic reconnect with exponential backoff per connection
  (`arterial_connection`).

## Installation

Arterial is not yet published to Hex. Add it as a `rebar3` dependency directly
from its Git repository:

```erlang
{deps, [
  {arterial, {git, "https://github.com/saleyn/arterial.git", {branch, "main"}}}
]}.
```

## Architecture

```mermaid
flowchart TD
    subgraph arterial [Arterial]
    A[fa:fa-person-military-pointing Arterial Sup] --->|supervises| P(fa:fa-layer-group Pool)
    A[fa:fa-person-military-pointing Arterial Sup] --->|supervises| CS[fa:fa-person-military-pointing Connection Mgr Sup]
    B>fa:fa-arrow-down-9-1 Backlog] -.-> P
    style B stroke-width:1px,color:#666,stroke-dasharray: 5 5
    CS -->|supervises| C1(fa:fa-link Connection1)
    CS -->|supervises| C2(fa:fa-link Connection2)
    CS -->|supervises| CN(fa:fa-link ConnectionN)
    P -.-|monitors| C1
    P -.-|monitors| C2
    P -.-|monitors| CN
    end
    C1 o-.-o S([Server])
    C2 o-.-o S
    CN o-.-o S
```

`arterial_app` is a top-level `simple_one_for_one` supervisor that spawns one
`arterial_pool` supervisor per named pool. Each `arterial_pool` creates the
pool's C++-backed `ConnectionPool` NIF resource (which tracks which
connections are checked out/available, their per-connection request
backlog, throttles, and in-flight async request registry), then starts one
`arterial_connection` worker per connection slot plus one `arterial_sweeper`
that periodically evicts timed-out in-flight requests.

### Erlang layer

| Module | Responsibility |
|---|---|
| `arterial_app` | Application + top supervisor (`simple_one_for_one` of pools). |
| `arterial_pool` | Per-pool `supervisor`; creates the NIF pool resource, then starts one `arterial_connection` worker per connection slot and one `arterial_sweeper`. |
| `arterial_connection` | One `gen_server` per pooled connection. Owns the socket, runs the connect/reconnect state machine with exponential backoff, and drives the user-supplied client callback module (`arterial_client`). |
| `arterial_sweeper` | Per-pool timer process; calls `arterial_nif:sweep_timeouts/1` on an interval to evict expired in-flight async requests. |
| `arterial_socket` | Thin wrapper over Erlang's `socket` module for `tcp`/`udp` connect, plus `ssl` (requires OTP 28+ — see [SSL/TLS guide](docs/ssl-guide.md)). |
| `arterial_client` | Behaviour for the connection lifecycle/codec (`init/1`, `setup/2`, `handle_request/2`, `handle_data/2`, `handle_timeout/2`, `terminate/2`); also implements `call/3`, the synchronous request API. |
| `arterial_protocol` | Behaviour for the wire transport and per-request codec (`connect/3`, `send/2`, `recv/2`, `encode_request/3`, `decode_reply/2`). |
| `arterial_proxy` | **Not yet working.** Originally intended as the asynchronous request/queuing path; largely superseded by `arterial_nif:checkout_async/3` (see [Implementation status](#implementation-status)) — whether this module still needs a rewrite is an open follow-up. |
| `arterial_nif` | Thin Erlang wrapper around the C++ NIF (pool create/destroy, checkout/checkin, checkout_async, availability, in-flight tracking, sweep). |
| `arterial_util` | Small helpers: random element selection, timeout/expiration arithmetic. |
| `arterial` | Shared public types (`client/0`, `socket/0`, `protocol/0`, etc). |

### C++ NIF layer (`c_src/`)

| File | Responsibility |
|---|---|
| `arterial.hpp` / `arterial.cpp` | `ConnectionPool`: wraps the lock-free FIFO pool of connections, the in-flight async request registry, and the optional queue-when-busy wait-list; NIF entry points (`create_nif`, `checkout_nif`, `checkin_nif`, `checkout_async_nif`, `make_available_nif`/`make_unavailable_nif`, `track_inflight_nif`, `sweep_timeouts_nif`). |
| `connection.hpp` | `Connection`: one pooled connection's id, socket term, request backlog, and vector of throttles. |
| `backlog.hpp` | Per-connection in-flight request tracking — see [Backlog modes](#backlog-modes). |
| `pool_fifo.hpp` / `pool_lifo.hpp` | Lock-free, atomic CAS–based object pools (FIFO built on top of a LIFO base) used to hand out `Connection*` objects with `CheckOut`/`CheckIn`/`MakeAvailable`/`MakeUnavailable` semantics and ABA-safe versioned node indices. |
| `wait_list.hpp` | `WaitList`: a bounded, lock-free MPMC ring buffer (Vyukov-style) of queued `checkout_async` callers, used by the optional queue-when-busy feature — see [Queue-when-busy checkouts](#queue-when-busy-checkouts). |
| `throttle.hpp` | `basic_time_spacing_throttle<T>` — a token-bucket-style rate limiter (ported from [utxx's `rate_throttler`](https://github.com/saleyn/utxx/blob/master/include/utxx/rate_throttler.hpp)) that checks elapsed time on each call instead of running a timer thread. |
| `unordered_map_with_ttl.hpp` | Hash map + sorted LRU eviction list, used to track in-flight asynchronous requests and their deadlines (either one fixed TTL per pool, or a per-request deadline kept sorted on insert); also used to track deadlines for queued waiters. |
| `nifpp.h` | Generic C++/Erlang NIF interop helpers (term conversion, resource wrapping). |
| `test_pool.cpp` | Standalone tests for the FIFO/LIFO object pools. |

### Connection checkout

`ConnectionPool::CheckOut` (`c_src/arterial.hpp`) walks the FIFO pool of
connections; for each candidate it checks whether all of the connection's
throttles currently have room for the requested number of samples, and
whether its backlog has free request slots. If both checks pass, the slot(s)
are reserved and the connection is returned to the caller; otherwise the
connection is checked back in at the tail of the pool and the next one is
tried, up to one full pass over the pool.

### Backlog modes

A connection's backlog tracks requests that have been sent but not yet
answered. Two implementations exist, chosen per the wire protocol's
capabilities (`backlog.hpp`):

* **`FIFOBackLog`** — for protocols whose messages carry no request ID.
  Replies are assumed to arrive in the same order requests were sent;
  checkout/checkin happens at the head/tail of a circular buffer.
* **`RandomAccessBackLog`** — for protocols that embed a request ID in each
  message, so replies can come back out of order. Slots are tracked in a
  `std::set` of free indices and looked up by decoded request ID on checkin.

Both are bounded by a 16-bit internal index (`BaseReqID`), so a connection
supports **at most 65,536 in-flight requests**.

### Asynchronous in-flight timeouts

Each `ConnectionPool` owns an `unordered_map_with_ttl<ReqID, {pid,
Connection*}>` registry, separate from the per-connection backlog, keyed by
the request's wire-level `ext_req_id`:

1. After checking out a connection for an asynchronous request, call
   `arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), TtlUs)` to
   register it. `TtlUs` is ignored if the pool was created with a fixed
   timeout (`create/6`'s last argument); otherwise each request keeps its
   own deadline, and the registry's eviction list stays sorted by deadline
   regardless of insertion order.
2. If a reply arrives, call `checkin_connection/4` as usual — this also
   untracks the request from the registry.
3. If nothing calls `checkin_connection/4` before the deadline, the next
   `arterial_nif:sweep_timeouts/1` call (run periodically by
   `arterial_sweeper`) sends `{arterial_timeout, Pool, ReqID}` to the owning
   process **and** releases the request's backlog slot and returns the
   connection to the pool — equivalent to what `checkin_connection/4` would
   have done, just triggered by the timeout instead of an explicit checkin.

### Queue-when-busy checkouts

By default, checking out a connection when every connection in the pool is
busy (throttled or backlog-full) fails immediately with
`{error, no_connection}` — this is unchanged and is what both
`checkout_connection/2` and `checkout_async/3` do when the feature described
here is not enabled.

`arterial_nif:checkout_async/3` adds an **optional** alternative: instead of
failing immediately, the caller can be queued and serviced automatically as
soon as capacity frees up. The feature is opt-in per pool, controlled by
`create/7`'s `MaxWaiters` argument (`0`, the default via `create/5` and
`create/6`, disables it entirely at effectively zero cost — a disabled
`WaitList` never allocates more than a single dummy ring slot and every
`checkout_async/3` call behaves exactly like `checkout_connection/2`).

The wait-list itself (`c_src/wait_list.hpp`) is a fixed-capacity, lock-free
MPMC ring buffer (Vyukov-style) of queued callers — bounded so it can never
grow without limit, and built independently of `pool_fifo.hpp` because that
structure is unsafe under repeated claim/release cycling (see the comment at
the top of `wait_list.hpp` for why).

`checkout_async(Pool, Pid, TtlUs)` has three possible outcomes:

1. **A connection is available immediately** — returns `{ok, Map}`, exactly
   like `checkout_connection/2`. No message follows.
2. **Every connection is busy, but the wait-list has room** — the request is
   queued and `{queued, WaiterID}` is returned immediately. `Pid` will later
   receive exactly one of:
   * `{arterial_ready, Pool, ReqID, ConnID, Socket, ReqIDs}` once a
     connection is assigned — call `checkin_connection/4` with the real
     wire-level `ReqID`/`ReqIDs` from this message when done, exactly as for
     a synchronous checkout.
   * `{arterial_timeout, Pool, WaiterID}` if `TtlUs` microseconds pass while
     still queued, without ever being assigned a connection.
3. **Every connection is busy and the wait-list is disabled or full** —
   returns `{error, no_connection}`, same as `checkout_connection/2`.

`TtlUs` bounds the *entire* call, from the moment `checkout_async/3` is
invoked until a reply is checked in — covering both time spent queued and
time spent in-flight once a connection is assigned. It's ignored (like
`track_inflight/5`'s `TtlUs`) if the pool uses a fixed per-pool timeout.

Queued waiters are serviced from two places, both of which drain the
wait-list in FIFO order (oldest queued caller first):

* `checkin_connection/2,4` — after releasing a connection back to the pool,
  it immediately tries to service the head of the wait-list with that
  connection before the connection is offered to any new, non-queued
  checkout.
* `sweep_timeouts/1` — besides evicting expired in-flight requests (see
  above), it also evicts expired queued waiters (sending them
  `{arterial_timeout, Pool, WaiterID}`), and any connection capacity freed
  up by evicting a *different*, timed-out in-flight request is immediately
  offered to the head of the wait-list.

## Protocol

It is the responsibility of the user to provide a module for encoding and decoding messages over the selected socket transport. Below are the types of common use cases for the protocol:

1. Each wire-level message contains a 32-bit (or 64-bit) request ID. In this case the requests can be sent asynchronously and replies are not required to arrive in FIFO order. A backlog setting is required to be set to a value below 65,536. No more than 64k asynchronous in-flight requests are supported.

2. Wire-level messages don't encode a request ID. In this case the replies are expected to come in the FIFO order of sent requests, and there are two possibilities of use:

    1. If a client needs to send requests asynchronously, then it's required to allocate a backlog below 65k.

    2. A client reserves the connection for the lifetime of a request, and awaits the response from the server synchronously. In this case no backlog is needed, and the default backlog setting of 1 is used.

### Behaviours to implement

Two behaviours decouple the wire transport from request/response encoding:

* **`arterial_protocol`** ([src/arterial_protocol.erl](src/arterial_protocol.erl)) — the transport and per-request codec: `connect/3`, `close/1`, `send/2`, `recv/2`, `setopts/2`, `encode_request/3`, `decode_reply/2`.
* **`arterial_client`** ([src/arterial_client.erl](src/arterial_client.erl)) — the connection lifecycle: `init/1`, `setup/2`, `handle_request/2`, `handle_data/2`, `handle_timeout/2` (optional), `terminate/2`. Also implements `call/3`, the synchronous request API used by callers (the caller's own process owns the socket for the call's duration via `arterial_protocol`; `arterial_connection`'s `gen_server` is not involved in the call itself).

Pool/connection options (`t:arterial_client:options/0`) include `address`/`ip`
(or `addresses`, for failover across several), `port`, `protocol` (`tcp` |
`udp` | `ssl` — see [docs/ssl-guide.md](docs/ssl-guide.md) for `ssl`, which
needs OTP 28+), `reconnect`, `reconnect_time`, `bounce_interval_ms`,
`socket_options`, and `tls_options` (only used when `protocol => ssl`).
`t:arterial_pool:options/0` (passed to `arterial_pool:start_link/2`)
additionally controls `size`, `backlog`, `fifo`, `fixed_timeout_us`,
`sweep_interval_ms`, `protocol` (here, the wire codec module — not the
same `protocol` key as above), `client`, and `client_opts` (the
per-connection options map above).

For a full step-by-step walkthrough of implementing both behaviours,
wiring them into a pool, and testing against a real server, see
[docs/clent-guide.md](docs/clent-guide.md). For the `ssl` transport
specifically (OTP 28+ required), see [docs/ssl-guide.md](docs/ssl-guide.md).

## Documentation

This README is the primary documentation; module-level `@doc`/`-spec`
comments cover the public API of each module listed above. Arterial is not
yet published to Hex, so there are no hosted hexdocs.

## License

Arterial uses [MIT License](LICENSE). You can use the source code freely
in any project, including commercial applications, as long as you give
credit by publishing the contents of the LICENSE file somewhere in your
documentation.