-module(arterial_nif).

-moduledoc """
NIF bindings to the raw-socket connection-pool engine (`c_src/arterial_nif.cpp`)
-- the low-level half of `arterial`'s NIF-resident-I/O connection
pool backend (see `arterial_pool`'s moduledoc for the full picture).

This NIF performs the actual `read(2)`/
`write(2)`/`writev(2)` syscalls itself, directly inside whichever Erlang
process calls in: `send_and_release/3` writes synchronously inside the
calling process (typically the request's own caller, see
`arterial_client:call/3`), and `handle_readable/3`/`handle_writable/3`
perform a read/flush synchronously inside whichever process calls them
(expected to be the slot's registered "owner" process, normally an
`arterial_connection` worker).

Sockets are organized into `Stripes` (assigned by the caller, e.g. by
scheduler id, to spread atomic-CAS contention across cores) of up to 64
`Slots` each (one real socket per slot) -- a single stripe's free/busy
state lives in one lock-free `uint64` bitmask, which is also why 64 is a
hard per-stripe cap.

There is no callback invoked automatically by the NIF runtime when a
socket becomes readable/writable (no such mechanism exists in
`erl_nif.h`): `enif_select` only ever delivers a message --
`{arterial_event, StripeId, SlotId, read | write | closed}` -- to
the slot's owner pid, which must then call `handle_readable/3` or
`handle_writable/3` to actually do the I/O and re-arm the next
notification. `closed` needs no further NIF call; the fd is already
gone by the time it's delivered.

## Protocol Support

This NIF supports three socket protocols via `connect_proto/8` family:
- **TCP** - Reliable stream protocol with optional TLS/SSL
- **UDP** - Unreliable datagram protocol with multicast support
- **SSL** - TLS/SSL over TCP (requires OpenSSL)

**Multicast support:** UDP sockets can be configured with multicast
options including TTL, loopback control, interface selection, and
group membership management. See `connect_proto_with_opts/9`.
""".

-export([init/0]).
-export([init_pool/2, init_pool/3, configure_throttle/3, register_socket/4, connect/7, connect_async/6, connect_proto/8, connect_async_proto/7, send_and_release/3, send_on_slot/4]).
-export([connect_with_opts/8, connect_proto_with_opts/9]).
-export([handle_readable/3, handle_writable/3, close_slot/3, handle_connection_timeout/3]).
-export([reactor_listen/2, reactor_accept/3, reactor_close_fd/2, reactor_register_client/4]).
-export([is_slot_available/3, set_slot_available/3, set_slot_unavailable/3]).
-export([reserve_fifo_connection/3, send_fifo_request/6, release_fifo_connection/4, fifo_connection_status/3, handle_fifo_reply/4]).
-export([reserve_send_fifo_request/5]). % New combined function (#3)
-export([register_corr/6, unregister_corr/3, lookup_and_remove_corr/3,
         corr_count/2, drain_corr_map/3, sweep_corr_map/2]).
-export([info/0]).

-on_load(init/0).

-define(LIBNAME, arterial_nif).
-define(NOT_LOADED_ERROR,
  erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})).

-doc "Opaque NIF resource returned by `init_pool/2`, identifying one pool.".
-type pool_ref() :: reference().

-export_type([pool_ref/0]).

%%%-----------------------------------------------------------------------------
%%% Public API (thin NIF bindings)
%%%-----------------------------------------------------------------------------

-doc """
Create a pool resource with `NumStripes` stripes of `SlotsPerStripe` slots
each (`SlotsPerStripe` must be `=< 64`). Every slot starts unregistered
(no socket); use `register_socket/4` to attach a real, already-connected
socket's file descriptor to one.

## Examples

```
1> arterial_nif:init_pool(4, 8).
{ok, PoolRef}
```
""".
-spec init_pool(non_neg_integer(), non_neg_integer()) ->
  {ok, pool_ref()} | {error, max_slots_exceeded_64}.
init_pool(_NumStripes, _SlotsPerStripe) ->
  ?NOT_LOADED_ERROR.

-doc """
Like `init_pool/2` but with an explicit per-stripe corr-table size.
`CorrTableSize` is rounded up to the next power of two.  Must be large
enough to hold all in-flight requests across the stripe simultaneously
(defaults to `max(256, next_pow2(SlotsPerStripe * 16))` when omitted).
""".
-spec init_pool(non_neg_integer(), non_neg_integer(), pos_integer()) ->
  {ok, pool_ref()} | {error, max_slots_exceeded_64}.
init_pool(_NumStripes, _SlotsPerStripe, _CorrTableSize) ->
  ?NOT_LOADED_ERROR.

-doc """
Configure throttling for a pool resource. Sets up time-spacing throttle
with `RatePerSec` requests per second over a `WindowMsec` millisecond window.
Setting `RatePerSec` to 0 disables throttling entirely.

The throttle enforces a minimum interval of `WindowMsec / RatePerSec`
milliseconds between successive requests on each connection slot.

## Examples

```
1> arterial_nif:configure_throttle(PoolRef, 100, 1000).  % 100 req/sec, 1s window
ok
2> arterial_nif:configure_throttle(PoolRef, 50, 500).    % 50 req/sec, 0.5s window
ok
```
""".
-spec configure_throttle(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
configure_throttle(_PoolRef, _RatePerSec, _WindowMs) ->
  ?NOT_LOADED_ERROR.

-doc """
Hand off raw file descriptor `RawFd` (e.g. extracted from an OTP
`socket()` via `socket:getopt(Sock, otp, fd)`) to stripe `StripeId` of
`PoolRef`, under an idle slot chosen automatically. Sets `RawFd`
non-blocking and arms its first read-readiness notification, targeted at
`OwnerPid` -- every future `{arterial_event, StripeId, SlotId, _}`
message for this slot (read, write, or closed) is sent to that same pid
for as long as the slot stays registered.

**Prefer `connect/7`** for a brand-new outgoing connection: `RawFd` here
still has another resource (whatever opened it, e.g. the `socket()`
term's own `esock` resource) believing it owns it too, and erts logs a
"stealing control of fd=N" warning both when this call takes it over and
again, potentially against a since-reused fd number, when that other
owner's resource is eventually garbage collected. Kept for callers that
genuinely need to register an fd opened by something other than this
NIF (there's no other way to get one in).

## Examples

```
1> arterial_nif:register_socket(PoolRef, 0, RawFd, self()).
{ok, SlotId}
```
""".
-spec register_socket(pool_ref(), non_neg_integer(), non_neg_integer(), pid()) ->
  {ok, non_neg_integer()} |
  {error, failed_to_set_nonblocking | stripe_full}.
register_socket(_PoolRef, _StripeId, _RawFd, _OwnerPid) ->
  ?NOT_LOADED_ERROR.

-doc """
Open a brand-new IPv4 TCP socket, connect it to `IP`:`Port` (waiting up
to `TimeoutMs` milliseconds), and -- on success -- claim an idle slot of
stripe `StripeId` for it, exactly like `register_socket/4`. `Nodelay`
sets `TCP_NODELAY` on the new socket when `true`.

Unlike `register_socket/4`, the fd is opened by this NIF and never has
any other owner at any point: nothing else (no Erlang `socket()` term,
no `prim_socket`/`esock` resource) ever believes it owns this fd, so
there is no "stealing control of fd=N" warning and no fd-reuse risk on
teardown. This runs as a dirty, IO-bound NIF (`connect/2` can block for
up to `TimeoutMs`), so it never ties up a regular scheduler thread.

## Examples

```
1> arterial_nif:connect(PoolRef, 0, {127,0,0,1}, 9000, 5000, true, self()).
{ok, SlotId}
2> arterial_nif:connect(PoolRef, 0, {127,0,0,1}, 1, 200, true, self()).
{error, connect_failed}
```
""".
-spec connect(pool_ref(), non_neg_integer(), inet:ip4_address(),
              arterial:inet_port(), non_neg_integer(), boolean(), pid()) ->
  {ok, non_neg_integer()} |
  {error, socket_failed | failed_to_set_nonblocking | connect_failed | timeout | stripe_full}.
connect(_PoolRef, _StripeId, _IP, _Port, _TimeoutMs, _Nodelay, _OwnerPid) ->
  ?NOT_LOADED_ERROR.

-doc """
Open a brand-new IPv4 TCP socket and start connecting to `IP`:`Port`
asynchronously. Unlike `connect/7`, this function returns immediately
after starting the connection attempt.

Returns `{ok, SlotId}` if connection completes immediately,
`{ok, connecting, SlotId}` if connection is in progress, or
`{error, Reason}` if the connection attempt fails immediately.

When an async connection completes, sends a message to `OwnerPid`:
`{arterial_event, StripeId, SlotId, connect_result, Result}`
where `Result` is `ok` for success or an error atom for failure.

`Nodelay` sets `TCP_NODELAY` on the new socket when `true`.

## Examples

```
1> arterial_nif:connect_async(PoolRef, 0, {127,0,0,1}, 9000, true, self()).
{ok, 0}
2> arterial_nif:connect_async(PoolRef, 0, {127,0,0,1}, 9001, true, self()).
{ok, connecting, 1}
```
""".
-spec connect_async(pool_ref(), non_neg_integer(), inet:ip4_address(),
                   arterial:inet_port(), boolean(), pid()) ->
  {ok, non_neg_integer()} | {ok, connecting, non_neg_integer()} |
  {error, socket_failed | failed_to_set_nonblocking | connect_failed | stripe_full}.
connect_async(_PoolRef, _StripeId, _IP, _Port, _Nodelay, _OwnerPid) ->
  ?NOT_LOADED_ERROR.

-doc """
Open a brand-new IPv4 socket with the specified protocol and connect to
`IP`:`Port` (waiting up to `TimeoutMs` milliseconds). Supports `tcp`, `udp`,
and `ssl` protocols. This is the protocol-aware version of `connect/7`.

For `tcp`: behaves identically to `connect/7`.
For `udp`: creates a UDP socket and optionally "connects" it to the remote address.
For `ssl`: creates a TCP socket and performs SSL handshake.

`Nodelay` sets `TCP_NODELAY` for TCP/SSL protocols (ignored for UDP).

## Examples

```
1> arterial_nif:connect_proto(PoolRef, 0, {127,0,0,1}, 9000, 5000, tcp, true, self()).
{ok, 0}
2> arterial_nif:connect_proto(PoolRef, 0, {127,0,0,1}, 9001, 5000, udp, false, self()).
{ok, 1}
```
""".
-spec connect_proto(pool_ref(), non_neg_integer(), inet:ip4_address(),
                   arterial:inet_port(), non_neg_integer(), tcp | udp | ssl, boolean(), pid()) ->
  {ok, non_neg_integer()} |
  {error, socket_failed | failed_to_set_nonblocking | connect_failed |
          timeout       | stripe_full               | unsupported_protocol}.
connect_proto(_PoolRef, _StripeId, _IP, _Port, _TimeoutMs, _Protocol, _Nodelay, _OwnerPid) ->
  ?NOT_LOADED_ERROR.

-doc """
Async version of `connect_proto/8`. Opens a socket with the specified protocol
and starts connecting asynchronously.

## Examples

```
1> arterial_nif:connect_async_proto(PoolRef, 0, {127,0,0,1}, 9000, tcp, true, self()).
{ok, connecting, 0}
2> arterial_nif:connect_async_proto(PoolRef, 0, {127,0,0,1}, 9001, udp, false, self()).
{ok, 1}
```
""".
-spec connect_async_proto(pool_ref(), non_neg_integer(), inet:ip4_address(),
                         arterial:inet_port(), tcp | udp | ssl, boolean(), pid()) ->
  {ok, non_neg_integer()} | {ok, connecting, non_neg_integer()} |
  {error, socket_failed | failed_to_set_nonblocking | connect_failed | stripe_full | unsupported_protocol}.
connect_async_proto(_PoolRef, _StripeId, _IP, _Port, _Protocol, _Nodelay, _OwnerPid) ->
  ?NOT_LOADED_ERROR.

-doc """
Write `IoList` (a list of binaries) to **any** currently idle slot of stripe
`StripeId`, chosen automatically via a lock-free CAS over the stripe's
lease bitmask.

The calling process does not pick which slot is used and does not learn it
in advance. The returned `SlotId` is the only way to discover which physical
connection carried the write — useful when recording a correlation-id for
later disconnect-notification bookkeeping (see `arterial_connection`).

`writev(2)` runs synchronously inside the calling process. If the kernel
buffer cannot accept all bytes immediately, the remainder is buffered and
flushed asynchronously by the slot owner's `handle_writable/3` call;
`{ok, SlotId}` is returned as soon as the bytes are accepted by the kernel
or the NIF's own pending buffer.

Returns `{error, pool_busy}` when every slot in the stripe is currently
leased or not yet registered. Callers are expected to retry against a
different stripe (see `arterial_client`); this NIF never spreads one
request across stripes.

## When to use

Use `send_and_release/3` for the **server-side echo / fan-out** pattern,
where many slots exist in a stripe and any idle connection can carry the
next outbound message:

- The stripe acts as an anonymous connection pool.
- The caller does not care *which* connection is used.
- At most one Erlang process per slot (the slot owner) is waiting for an
  inbound event on that slot, so routing is unambiguous regardless of which
  slot is chosen.

A typical server echo loop looks like:

```erlang
handle({arterial_event, StripeId, _SlotId, read, Bin}) ->
    {ok, _} = arterial_nif:send_and_release(PoolRef, StripeId, [Bin]).
```

**Do not use** `send_and_release/3` when multiple independent Erlang
processes each own a distinct slot in the same stripe and each must receive
its own reply. In that case use `send_on_slot/4`.

## Examples

```
1> arterial_nif:send_and_release(PoolRef, 0, [<<1,2,3>>]).
{ok, 0}
```
""".
-spec send_and_release(pool_ref(), non_neg_integer(), [binary()]) ->
  {ok, non_neg_integer()} |
  {error, pool_busy | write_failed}.
send_and_release(_PoolRef, _StripeId, _IoList) ->
  ?NOT_LOADED_ERROR.

-doc """
Write `IoList` to a **specific** slot identified by `{StripeId, SlotId}`.

Unlike `send_and_release/3`, this function targets a named slot directly
rather than scanning the stripe for any idle connection. The lease bit for
`SlotId` is claimed atomically, the data is written synchronously via
`writev(2)`, and the bit is released on completion. A partial write
(EAGAIN on a non-loopback socket) buffers the remainder and arms a writable
callback exactly as `send_and_release/3` does.

Returns `ok` (not `{ok, SlotId}`) because the slot identity is already
known to the caller.

Returns `{error, pool_busy}` when the target slot is currently leased by
another writer (e.g. a concurrent partial-write flush is in progress).

## When to use

Use `send_on_slot/4` for the **client-side request** pattern, where each
Erlang process owns a dedicated slot and must ensure its request travels
over its own TCP connection so the reply is delivered back to it:

- Multiple worker processes share a single stripe (fewer stripes than
  connections, i.e. `STRIPES < CONNS`).
- Each process connected via `connect_proto_with_opts/9` and received a
  specific `SlotId`.
- The process must call `send_on_slot` with that `SlotId` so the server
  echo comes back to the correct `owner_pid`.

A typical NIF client worker looks like:

```erlang
nif_client_worker(PoolRef, StripeId, SlotId, Msg) ->
    ok = arterial_nif:send_on_slot(PoolRef, StripeId, SlotId, [Msg]),
    receive
        {arterial_event, StripeId, SlotId, read, Reply} -> Reply
    after 5000 -> error(timeout)
    end.
```

If you used `send_and_release/3` here instead, the write could land on a
*different* slot owned by another worker process. That process would receive
the server's reply while the original caller blocks forever — a deadlock
that only manifests when two or more slots share a stripe.

## When `send_and_release/3` is safe for clients

`send_and_release/3` is safe for client sends when every stripe holds
**exactly one slot** (i.e. `init_pool(NConns, 1)`). There is no ambiguity
because there is only one connection to pick. This is the default pool shape
when `STRIPES=0` (one stripe per connection) and is why the bug only
appears with `STRIPES < CONNS`.

## Examples

```
1> arterial_nif:send_on_slot(PoolRef, 0, 1, [<<1,2,3>>]).
ok
```
""".
-spec send_on_slot(pool_ref(), non_neg_integer(), non_neg_integer(), [binary()]) ->
  ok | {error, pool_busy | write_failed}.
send_on_slot(_PoolRef, _StripeId, _SlotId, _IoList) ->
  ?NOT_LOADED_ERROR.

-doc """
Called by a slot's owner process upon receiving
`{arterial_event, StripeId, SlotId, read}`: reads whatever is
currently available on the slot's socket (one `ioctl(FIONREAD)` + one
`read(2)`, synchronously inside the calling process) and re-arms the next
read-readiness notification.

Returns `{ok, Binary}` (possibly `<<>>` on a spurious wakeup -- still
re-armed, safe to ignore) with the raw bytes read, or `closed` if the
peer closed the connection (or the read failed for any other reason) --
the fd is already gone and deselected by the time this returns; no
further cleanup call is needed.

## Examples

```
1> arterial_nif:handle_readable(PoolRef, 0, SlotId).
{ok, <<"...">>}
```
""".
-spec handle_readable(pool_ref(), non_neg_integer(), non_neg_integer()) ->
  {ok, binary()} | closed.
handle_readable(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Called by a slot's owner process upon receiving
`{arterial_event, StripeId, SlotId, write}`: flushes as much of the
slot's pending write buffer (left over from a `send_and_release/3` that
couldn't complete immediately) as the kernel will currently accept.

Returns `ok` whether or not the buffer is now fully flushed (re-arming
the next write-readiness notification itself if not) -- the slot becomes
available for a new `send_and_release/3` exactly when fully flushed, with
no separate signal to the original caller (which already got `{ok,
SlotId}` back from `send_and_release/3` regardless). Returns `closed` if
the connection died before the buffer could be flushed.

## Examples

```
1> arterial_nif:handle_writable(PoolRef, 0, SlotId).
ok
```
""".
-spec handle_writable(pool_ref(), non_neg_integer(), non_neg_integer()) ->
  ok | closed.
handle_writable(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Force-close slot `SlotId` of stripe `StripeId` (e.g. `arterial_bouncer`
recycling a connection, or `arterial_connection` tearing one down) --
deselects and closes its fd without sending any `closed` notification
(the caller already knows; compare the `closed` event delivered by
`handle_readable/3`/`send_and_release/3` discovering a dead peer on their
own). The slot becomes unregistered and its stripe bit reverts to
"unavailable" until a future `register_socket/4` reuses it.

## Examples

```
1> arterial_nif:close_slot(PoolRef, 0, SlotId).
ok
```
""".
-spec close_slot(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
close_slot(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Handle connection timeout by cleaning up the connection slot and releasing resources.
This function should be called when a connection timeout message is received.

## Parameters
- `PoolRef`: Reference to the connection pool
- `StripeId`: Stripe identifier
- `SlotId`: Slot identifier

## Returns
`ok` on successful cleanup.

## Examples
```
% Called when timeout message received
1> arterial_nif:handle_connection_timeout(PoolRef, StripeId, SlotId).
ok
```
""".
-spec handle_connection_timeout(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
handle_connection_timeout(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Enhanced `connect/7` with socket options support. Like `connect/7` but accepts
an additional list of socket options to apply to the socket before connecting.

**Basic socket options:**
- Atoms: `keepalive`, `nodelay`, `reuseaddr`
- Tuples: `{keepalive, true}`, `{sndbuf, 8192}`, `{rcvbuf, 8192}`, `{priority, 0..6}`, `{tos, integer()}`, `{linger, {boolean(), integer()}}`

**Multicast options (UDP protocol only):**
- `{multicast_ttl, 0..255}` - Multicast TTL (time-to-live) hop limit
- `{multicast_loop, boolean()}` - Enable/disable multicast loopback
- `{multicast_if, {A,B,C,D}}` - Interface address for outgoing multicast packets
- `{add_membership, {{MA,MB,MC,MD}, {IA,IB,IC,ID}}}` - Join multicast group (multicast addr, interface addr)
- `{drop_membership, {{MA,MB,MC,MD}, {IA,IB,IC,ID}}}` - Leave multicast group (multicast addr, interface addr)

## Examples

```
1> arterial_nif:connect_with_opts(PoolRef, 0, {127,0,0,1}, 9000, 5000, true, self(), [keepalive, {sndbuf, 16384}]).
{ok, SlotId}
2> % UDP multicast example
2> arterial_nif:connect_proto_with_opts(PoolRef, 0, {239,1,1,1}, 12345, 5000, udp, false, self(),
2>   [{multicast_ttl, 16}, {add_membership, {{239,1,1,1}, {0,0,0,0}}}]).
{ok, SlotId}
```
""".
-spec connect_with_opts(pool_ref(), non_neg_integer(), inet:ip4_address(),
                        arterial:inet_port(), non_neg_integer(), boolean(),
                        pid(), [atom() | {atom(), term()}]) ->
  {ok, non_neg_integer()} |
  {error, socket_failed | failed_to_set_nonblocking | connect_failed | timeout | stripe_full}.
connect_with_opts(_PoolRef, _StripeId, _IP, _Port, _TimeoutMs, _Nodelay, _OwnerPid, _SocketOpts) ->
  ?NOT_LOADED_ERROR.

-doc """
Enhanced `connect_proto/8` with socket options support. Like `connect_proto/8`
but accepts an additional list of socket options. Supports all protocols (tcp, udp, ssl)
with protocol-specific options.

**Socket options:** Same as `connect_with_opts/8`. Multicast options are only
applicable when `Protocol` is `udp`.

## Examples

```
1> arterial_nif:connect_proto_with_opts(PoolRef, 0, {127,0,0,1}, 9000, 5000, tcp, true, self(), [keepalive]).
{ok, SlotId}
2> % UDP multicast receiver
2> arterial_nif:connect_proto_with_opts(PoolRef, 1, {239,1,1,1}, 12345, 5000, udp, false, self(),
2>   [{multicast_ttl, 1}, {multicast_loop, false}, {add_membership, {{239,1,1,1}, {192,168,1,100}}}]).
{ok, SlotId}
```
""".
-spec connect_proto_with_opts(pool_ref(), non_neg_integer(), inet:ip4_address(),
                              arterial:inet_port(), non_neg_integer(),
                              tcp | udp | ssl, boolean(), pid(), [atom() | {atom(), term()}]) ->
  {ok, non_neg_integer()} |
  {error, socket_failed | failed_to_set_nonblocking | connect_failed | timeout | stripe_full | unsupported_protocol}.
connect_proto_with_opts(_PoolRef, _StripeId, _IP, _Port, _TimeoutMs, _Protocol, _Nodelay, _OwnerPid, _SocketOpts) ->
  ?NOT_LOADED_ERROR.

-doc """
Check if a connection slot is available for new sends. This is the authoritative
source for connection availability, replacing the dual state tracking system.

Checks both that the slot has an active connection (`SLOT_AVAILABLE` status)
and is not currently leased for I/O operations.

## Examples

```
1> arterial_nif:is_slot_available(PoolRef, 0, 0).
true
2> arterial_nif:is_slot_available(PoolRef, 0, 1).
false
```
""".
-spec is_slot_available(pool_ref(), non_neg_integer(), non_neg_integer()) -> boolean().
is_slot_available(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Mark a connection slot as available for new sends. Used when a connection
completes successfully and is ready to handle requests.

This updates both the slot status to `SLOT_AVAILABLE` and clears the lease mask
bit to make the slot available for `send_and_release/3`.

## Examples

```
1> arterial_nif:set_slot_available(PoolRef, 0, 0).
ok
```
""".
-spec set_slot_available(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
set_slot_available(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Mark a connection slot as unavailable for new sends. Used when a connection
is being bounced, is disconnected, or is otherwise not ready for new requests.

This updates the slot status and sets the lease mask bit to prevent the slot
from being used by `send_and_release/3`.

## Examples

```
1> arterial_nif:set_slot_unavailable(PoolRef, 0, 0).
ok
```
""".
-spec set_slot_unavailable(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
set_slot_unavailable(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

%%%-----------------------------------------------------------------------------
%%% FIFO Mode 3 functions
%%%-----------------------------------------------------------------------------

-doc """
Reserve a connection slot for FIFO Mode 3 operation. Atomically reserves
a connection slot for exclusive use by the calling process, with a timeout.

Returns `{ok, SlotId, ConnectionInfo}` if a slot is successfully reserved,
or `{error, Reason}` if no slots are available or other error occurs.
""".
-spec reserve_fifo_connection(pool_ref(), non_neg_integer(),
                             non_neg_integer()) ->
  {ok, non_neg_integer(), term()} | {error, term()}.
reserve_fifo_connection(_PoolRef, _StripeId, _TimeoutMs) ->
  ?NOT_LOADED_ERROR.

-doc """
Send request data through a reserved FIFO connection slot.

The slot must have been previously reserved with `reserve_fifo_connection/3`.
Returns `{ok, BytesSent}` on success or `{error, Reason}` on failure.
""".
-spec send_fifo_request(pool_ref(), non_neg_integer(), non_neg_integer(),
                       [binary()], non_neg_integer(), pid()) ->
  {ok, non_neg_integer()} | {error, term()}.
send_fifo_request(_PoolRef, _StripeId, _SlotId, _IoList, _TimeoutMs, _RequesterPid) ->
  ?NOT_LOADED_ERROR.

-doc """
Release a reserved FIFO connection slot back to the pool.

Should be called after processing the response from a FIFO request.
Returns `ok` on success.
""".
-spec release_fifo_connection(pool_ref(), non_neg_integer(),
                             non_neg_integer(), term()) ->
  ok | {error, term()}.
release_fifo_connection(_PoolRef, _StripeId, _SlotId, _Result) ->
  ?NOT_LOADED_ERROR.

-doc """
Get the status and statistics of a FIFO connection slot.

Returns information about the current state of the slot including
whether it's reserved, connection status, and timing statistics.
""".
-spec fifo_connection_status(pool_ref(), non_neg_integer(),
                            non_neg_integer()) ->
  {ok, term()} | {error, term()}.
fifo_connection_status(_PoolRef, _StripeId, _SlotId) ->
  ?NOT_LOADED_ERROR.

-doc """
Handle incoming reply data for a FIFO request.

Called when data is received on a FIFO connection slot to route
the reply to the appropriate waiting process.
""".
-spec handle_fifo_reply(pool_ref(), non_neg_integer(), non_neg_integer(),
                       binary()) ->
  ok | {error, term()}.
handle_fifo_reply(_PoolRef, _StripeId, _SlotId, _ReplyData) ->
  ?NOT_LOADED_ERROR.

-doc """
Combined reserve and send FIFO request operation (#3 - NIF call optimization).

This function combines connection reservation and request sending into a single
NIF call to reduce overhead. It reserves a connection (with queuing/waiting if
necessary), sends the request, and returns the reservation info for later release.

This is a performance optimization that reduces the Reserve -> Send pattern
from 2 NIF calls to 1, while maintaining the ability to release separately
for error handling flexibility.

## Parameters

- `PoolRef`: Pool context reference
- `StripeId`: Which stripe to reserve from (0-based)
- `RequestData`: List of binaries containing request data to send
- `ReservationTimeoutMs`: Timeout for connection reservation
- `RequestTimeoutMs`: Timeout for sending the request

## Returns

- `{ok, fifo_request_sent, StripeId, SlotId, ReservationId}`: Success
- `{error, pool_busy}`: All connections busy (after waiting)
- `{error, write_failed}`: Failed to write to socket
- `{error, timeout}`: Timeout during reservation or send
""".
-spec reserve_send_fifo_request(pool_ref(), non_neg_integer(), [binary()],
                               non_neg_integer(), non_neg_integer()) ->
  {ok, fifo_request_sent, non_neg_integer(), non_neg_integer(), non_neg_integer()} |
  {error, atom()}.
reserve_send_fifo_request(_PoolRef, _StripeId, _RequestData, _ReservationTimeoutMs, _RequestTimeoutMs) ->
  ?NOT_LOADED_ERROR.

%%%-----------------------------------------------------------------------------
%%% Corr-map NIFs (in-NIF correlation-id → caller mapping, replaces ETS table)
%%%-----------------------------------------------------------------------------

-doc """
Register a correlation id in the per-stripe NIF map before sending a request.
`StripeId` is the stripe the request was sent on.  `CorrId` is the wire-level
correlation id.  `CallerPid` is the process waiting for the reply.  `ConnId`
is the slot index (used by `drain_corr_map/3` on disconnect).  `DeadlineUs`
is `os:system_time(microsecond) + TimeoutUs` (used by `sweep_corr_map/2`).
""".
-spec register_corr(pool_ref(), non_neg_integer(), non_neg_integer(),
                    pid(), non_neg_integer(), integer()) -> ok.
register_corr(_PoolRef, _StripeId, _CorrId, _CallerPid, _ConnId, _DeadlineUs) ->
  ?NOT_LOADED_ERROR.

-doc "Remove a previously registered corr entry (on send failure before a reply arrives).".
-spec unregister_corr(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
unregister_corr(_PoolRef, _StripeId, _CorrId) ->
  ?NOT_LOADED_ERROR.

-doc "Return the number of pending corr entries in stripe `StripeId`. Used by bounce drain polling.".
-spec corr_count(pool_ref(), non_neg_integer()) -> non_neg_integer().
corr_count(_PoolRef, _StripeId) ->
  ?NOT_LOADED_ERROR.

-doc """
Atomically remove and return the entry for `CorrId`.
Returns `{CallerPid, ConnId}` when found, `not_found` otherwise.
Called by `arterial_connection` after decoding a reply frame, replacing `ets:take`.
""".
-spec lookup_and_remove_corr(pool_ref(), non_neg_integer(), non_neg_integer()) ->
  {pid(), non_neg_integer()} | not_found.
lookup_and_remove_corr(_PoolRef, _StripeId, _CorrId) ->
  ?NOT_LOADED_ERROR.

-doc """
Remove all corr entries for `ConnId` from the stripe and send
`{arterial_disconnected, Pool, CorrId}` to each waiting caller.
Called by `arterial_connection` on disconnect instead of scanning ETS by ConnId.
""".
-spec drain_corr_map(pool_ref(), non_neg_integer(), non_neg_integer()) -> ok.
drain_corr_map(_PoolRef, _StripeId, _ConnId) ->
  ?NOT_LOADED_ERROR.

-doc """
Scan all stripes for entries whose deadline has passed `NowUs`
(`os:system_time(microsecond)`), remove them, and send
`{arterial_timeout, Pool, CorrId}` to each expired caller.
Returns the count of expired entries.  Called by `arterial_sweeper` instead
of `ets:select`.
""".
-spec sweep_corr_map(pool_ref(), integer()) -> non_neg_integer().
sweep_corr_map(_PoolRef, _NowUs) ->
  ?NOT_LOADED_ERROR.

%% --- Reactor server NIFs ---

-doc "Create a non-blocking TCP listen socket. Returns {ok, {Fd, ActualPort}}.".
reactor_listen(_PoolRef, _Port) -> ?NOT_LOADED_ERROR.

-doc "Register ListenFd with the reactor; sends {arterial_accept, Lfd, Cfd, IP, Port} to OwnerPid on each new connection.".
reactor_accept(_PoolRef, _ListenFd, _OwnerPid) -> ?NOT_LOADED_ERROR.

-doc "Remove Fd from the reactor and close it.".
reactor_close_fd(_PoolRef, _Fd) -> ?NOT_LOADED_ERROR.

-doc "Register a client fd with the NIF pool for read events. Returns {ok, SlotId}.".
reactor_register_client(_PoolRef, _StripeId, _ClientFd, _OwnerPid) -> ?NOT_LOADED_ERROR.

-doc "Return NIF library info.".
info() -> ?NOT_LOADED_ERROR.

%%%-----------------------------------------------------------------------------
%%% NIF loading
%%%-----------------------------------------------------------------------------

-doc false.
init() ->
  SoName = case code:priv_dir(?LIBNAME) of
    {error, bad_name} ->
      case code:which(?MODULE) of
        Filename when is_list(Filename) ->
          Dir = filename:dirname(filename:dirname(Filename)),
          filename:join([Dir, "priv", "arterial_nif"]);
        _ ->
          % More robust fallback using absolute path from current working directory
          {ok, Cwd} = file:get_cwd(),
          filename:join([Cwd, "_build", "default", "lib", "arterial", "priv", "arterial_nif"])
      end;
    Dir ->
      filename:join(Dir, "arterial_nif")
  end,
  erlang:load_nif(SoName, 0).
