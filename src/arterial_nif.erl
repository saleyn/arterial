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
-export([init_pool/2, configure_throttle/3, register_socket/4, connect/7, connect_async/6, connect_proto/8, connect_async_proto/7, send_and_release/3]).
-export([connect_with_opts/8, connect_proto_with_opts/9]).
-export([handle_readable/3, handle_writable/3, close_slot/3]).

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
Write `IoList` (a list of binaries) to any currently idle slot of stripe
`StripeId`, chosen automatically (lock-free CAS over the stripe's lease
bitmask) -- the calling process never picks (or even learns, in advance)
which physical socket it lands on.

Runs the `write(2)`/`writev(2)` syscall synchronously inside the calling
process. If the kernel socket buffer can't accept all of `IoList`
immediately, the remainder is buffered and flushed later via
`handle_writable/3` (called by the slot's owner, not this caller) --
either way, `{ok, SlotId}` is returned as soon as the bytes are *accepted*
(by the kernel or this NIF's own pending buffer), not once a peer
necessarily receives them. The returned `SlotId` is the caller's only way
to know which physical connection carried this write (e.g. to record
alongside a request's correlation id for later disconnect-notification
bookkeeping, see `arterial_connection`).

`{error, no_connections_available}` if every slot in `StripeId` is
currently unregistered or already leased (busy writing/flushing) --
callers are expected to retry against a different `StripeId` themselves
(see `arterial_client`); this NIF never spreads one logical request
across stripes.

## Examples

```
1> arterial_nif:send_and_release(PoolRef, 0, [<<1,2,3>>]).
{ok, SlotId}
```
""".
-spec send_and_release(pool_ref(), non_neg_integer(), [binary()]) ->
  {ok, non_neg_integer()} |
  {error, no_connections_available | write_failed}.
send_and_release(_PoolRef, _StripeId, _IoList) ->
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
