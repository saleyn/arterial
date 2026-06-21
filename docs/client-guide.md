# Implementing an Arterial client/server

This guide walks through everything you need to write to plug a new wire
protocol into `arterial`: the two behaviours you implement
(`arterial_protocol`, `arterial_client`), how they're wired together via
`arterial_pool:start_link/2`, and how the synchronous and asynchronous
request paths actually use them at runtime. It assumes you've read the
README's [Architecture](../README.md#architecture) section first for the
supervision tree/module overview.

The running example is a tiny length-prefixed framing protocol:

```
<<ReqID:32, Len:32, Payload:Len/binary>>
```

This is exactly what [test/test_echo_protocol.erl](../test/test_echo_protocol.erl)
and [test/test_echo_client.erl](../test/test_echo_client.erl) implement;
both are real, runnable code exercised by
[test/test_tcp_server_tests.erl](../test/test_tcp_server_tests.erl) and
[test/test_udp_server_tests.erl](../test/test_udp_server_tests.erl), so you
can read/run those directly instead of (or alongside) this guide.

## The two behaviours, and why there are two

`arterial_protocol` owns **the bytes**: opening/closing the transport,
raw `send`/`recv`, and how one request/reply is encoded/decoded.

`arterial_client` owns **the connection's lifecycle and the asynchronous
request/reply bookkeeping**: post-connect handshake, assigning wire-level
request ids, and reassembling decoded replies out of a stream of raw
`handle_data/2` chunks.

They're separate because the synchronous path
(`arterial_client:call/3`) only ever needs `arterial_protocol` — the
calling process owns the socket directly for the duration of one
request/reply and talks to it itself, without going through the pool's
`arterial_connection` `gen_server` at all. The asynchronous path
(`arterial_nif:checkout_async/3` + `arterial_client:handle_request/2`/
`handle_data/2`) is what actually needs `arterial_client`'s bookkeeping
callbacks, since replies can now arrive on `arterial_connection`'s own
mailbox, decoupled from whichever process issued the request.

If you only need the synchronous path, you still have to implement both
behaviours (`arterial_pool:start_link/2` requires both modules), but
`handle_request/2`/`handle_data/2` can be trivial stubs — see
[Synchronous-only clients](#synchronous-only-clients) below.

## Step 1: implement `arterial_protocol`

See [src/arterial_protocol.erl](../src/arterial_protocol.erl) for the full
callback specs. Six callbacks:

| Callback | Called by | Purpose |
|---|---|---|
| `connect/3` | `arterial_connection` on (re)connect | Open the transport. **Unused if `arterial_connection` opens the socket itself** (see note below). |
| `close/1` | `arterial_connection` on disconnect | Close the transport. Same caveat as `connect/3`. |
| `send/2` | `arterial_client:call/3`, `arterial_connection` | Write `iodata()` to the socket. |
| `recv/2` | `arterial_client:call/3`, `arterial_connection` | Read the next chunk, with a timeout. |
| `setopts/2` | `arterial_connection` after connect | Apply socket options. |
| `encode_request/3` | `arterial_client:call/3`, your `handle_request/2` | Encode one request, given the request id to embed. |
| `decode_reply/2` | `arterial_client:call/3`, your `handle_data/2` | Try to pull one complete reply for `ReqID` off the front of a buffer. |

**Important wrinkle**: if your transport is plain TCP/UDP,
`arterial_connection` opens the socket itself via `arterial_socket:connect/5`
(selected by the `client_opts` `protocol => tcp | udp` option) *before*
your protocol module ever sees it — in that case `connect/3`/`close/1` are
unused stubs, exactly like in `test_echo_protocol`:

```erlang
connect(_Address, _Port, _Opts) -> {error, not_used}.
close(_Sock) -> ok.
```

Only implement real `connect/3`/`close/1` logic if your transport needs
something `arterial_socket` doesn't provide (e.g. TLS, a custom
handshake-before-framing transport).

`send/2`/`recv/2` are thin wrappers around the underlying socket module:

```erlang
send(Sock, Data) ->
  socket:send(Sock, Data).

recv(Sock, Timeout) ->
  socket:recv(Sock, 0, Timeout).
```

`decode_reply/2` must distinguish three outcomes — a complete reply, not
enough bytes yet, or a hard error — so the caller (`call/3` or your
`handle_data/2`) knows whether to keep reading or give up:

```erlang
decode_reply(ReqID, Buffer) ->
  case unframe(Buffer) of
    {ok, ReqID, Payload, Rest} -> {ok, binary_to_term(Payload), Rest};
    {ok, _OtherReqID, _, _}    -> {more, Buffer}; % not this request's reply yet
    more                       -> {more, Buffer}; % incomplete frame
    {error, _} = Error         -> Error
  end.
```

The `{ok, _OtherReqID, _, _}` branch matters for protocols with
`backlog > 1`: a reply for a *different* in-flight request may arrive
first, and `decode_reply/2` for `ReqID` should leave it buffered rather
than consuming/discarding it. The synchronous `call/3` path (below)
checks the unconsumed buffer back into the pool, so it isn't lost.

## Step 2: implement `arterial_client`

See [src/arterial_client.erl](../src/arterial_client.erl) for the full
callback specs. Five callbacks plus one optional:

| Callback | Called when | Purpose |
|---|---|---|
| `init/1` | once per (re)connect, before a socket exists | Build initial per-connection state from `client_opts`. |
| `setup/2` | once per connect, right after `init/1`, socket now open | Post-connect handshake/auth, if any. |
| `handle_request/2` | asynchronous path, per request | Assign wire-level request id(s), encode bytes to send. |
| `handle_data/2` | asynchronous path, per chunk received | Decode zero or more complete replies out of newly arrived bytes. |
| `handle_timeout/2` *(optional)* | asynchronous path, on TTL expiry | Synthesize a response instead of just letting the caller see `{arterial_timeout, Pool, ReqID}`. |
| `terminate/2` | on disconnect (clean or error) | Release any resources held in callback state. |

For a protocol with no handshake (like the echo example), `init/1`/`setup/2`
just carry whatever counter/buffer state `handle_request/2`/`handle_data/2`
need:

```erlang
-record(st, {next_id = 0 :: non_neg_integer(), buf = <<>> :: binary()}).

init(_Options) ->
  {ok, #st{}}.

setup(_Socket, State) ->
  {ok, State}.

handle_request(Request, #st{next_id = ReqID} = State) ->
  {ok, Data} = my_protocol:encode_request(ReqID, Request, infinity),
  {ok, [ReqID], Data, State#st{next_id = ReqID + 1}}.

handle_data(Data, #st{buf = Buf} = State) ->
  decode_all(<<Buf/binary, Data/binary>>, [], State).

decode_all(Buffer, Acc, State) ->
  case my_protocol:unframe(Buffer) of
    {ok, _ReqID, Payload, Rest} ->
      decode_all(Rest, [binary_to_term(Payload) | Acc], State);
    more ->
      {ok, lists:reverse(Acc), State#st{buf = Buffer}}
  end.

terminate(_Reason, _State) -> ok.
```

If your protocol needs a handshake (auth, version negotiation, etc.),
do it in `setup/2` — it runs once, after the socket is open but *before*
the connection is published into the pool (`arterial_nif:set_socket/3` +
`make_available/2`), so no checkout can race a not-yet-authenticated
socket:

```erlang
setup(Socket, State) ->
  case my_protocol:send(Socket, <<"AUTH ", Token/binary>>) of
    ok ->
      case my_protocol:recv(Socket, 5000) of
        {ok, <<"OK">>} -> {ok, State};
        {ok, Other}    -> {error, {auth_rejected, Other}, State};
        {error, _} = E -> {error, E, State}
      end;
    {error, _} = E -> {error, E, State}
  end.
```

A `setup/2` failure tears the connection down and retries with backoff
(via `arterial_connection`'s reconnect state machine) — it never makes a
broken socket available for checkout.

## Step 3: start the pool

Both modules are passed to `arterial_pool:start_link/2`:

```erlang
{ok, _SupPid} = arterial_pool:start_link(my_pool, #{
  size        => 4,
  backlog     => 16,
  fifo        => true,
  protocol    => my_protocol,
  client      => my_client,
  client_opts => #{
    address   => "db.internal",
    port      => 9000,
    protocol  => tcp
  }
}).
```

See `t:arterial_pool:options/0` and `t:arterial_client:options/0` for the
full option lists (pool shape vs. per-connection transport options). This
starts a per-pool supervisor with one `arterial_connection` worker per
connection slot (each independently connecting/reconnecting) plus an
`arterial_sweeper` that periodically evicts timed-out async requests.

In a real application you'd normally start this as a child of your own
supervision tree (or under `arterial_app`, if you're using its top-level
supervisor); the tests in `test_tcp_server_tests`/`test_udp_server_tests`
call `start_link/2` directly because they manage the pool's lifecycle
per-test.

## Step 4: choosing `backlog`/`fifo`, and how requests get matched to replies

This choice depends entirely on what your wire protocol's messages look
like — see the README's [Protocol](../README.md#protocol) section for the
three cases. In short:

* **Messages carry a request id, replies may be out of order** → any
  `backlog` value, `fifo` doesn't matter (`RandomAccessBackLog` is used
  internally); `decode_reply/2` must actually check the id.
* **Messages carry no request id, but you want concurrent in-flight
  requests on one connection** → `backlog > 1`, `fifo => true`
  (`FIFOBackLog`); replies *must* arrive in the order requests were sent.
* **One request reserves the whole connection until its reply arrives**
  (the synchronous `call/3` use case) → `backlog => 1` (the default), `fifo`
  irrelevant.

The echo example uses request ids (`backlog => 1, fifo => true` in the
tests, since each test only issues one call at a time) but the framing
itself supports any backlog size.

## Step 5: calling it

**Synchronous** — block on a single request/reply:

```erlang
{ok, Reply} = arterial_client:call(my_pool, Request, _TimeoutMs = 5000).
```

This is the entire round trip described in
[Architecture > Synchronous path](../README.md#erlang-layer): check out a
connection, `encode_request/3`, `send/2`, loop on `recv/2`/`decode_reply/2`
until a full reply or the timeout expires, then `checkin_connection/4`
with whatever unconsumed buffer is left over (so a partial next-frame
doesn't get dropped).

**Asynchronous** — check out a connection without blocking, get notified
later:

```erlang
case arterial_nif:checkout_async(my_pool, self(), _TtlUs = 5_000_000) of
  {ok, #{conn_id := ConnID, socket := Socket, req_ids := ReqIDs}} ->
    %% send your own request via my_protocol/my_client, encoding it with
    %% ReqIDs, then call:
    ok = arterial_nif:track_inflight(my_pool, ConnID, hd(ReqIDs), self(), 5_000_000);
  {queued, WaiterID} ->
    %% every connection busy, but the pool's wait-list has room -- wait for
    %% {arterial_ready, my_pool, _, ConnID, Socket, ReqIDs} or
    %% {arterial_timeout, my_pool, WaiterID}
    ok;
  {error, no_connection} ->
    %% every connection busy, no wait-list (or it's full)
    ok
end.
```

See the README's [Asynchronous in-flight
timeouts](../README.md#asynchronous-in-flight-timeouts) and
[Queue-when-busy checkouts](../README.md#queue-when-busy-checkouts)
sections for the full message-passing contract — `handle_request/2`/
`handle_data/2` come into play once you're driving `arterial_connection`'s
own mailbox rather than a private `call/3` round trip, which is a
larger undertaking than this guide covers in full; reading
`arterial_connection.erl`'s `client_init/1` is the best starting point if
you go down that path.

## Step 6: the server side (for testing)

`arterial` only implements the client. To test against something, you
need a server speaking the same framing — see
[test/test_tcp_server.erl](../test/test_tcp_server.erl) and
[test/test_udp_server.erl](../test/test_udp_server.erl) for minimal
examples built on the OTP `socket` module. The one nonobvious wrinkle, if
you write your own: `socket` ties a socket's lifetime to whichever
process called `accept/1`, so a one-shot acceptor process must transfer
`controlling_process` to the long-lived connection handler *before* it
exits, or the accepted socket gets closed immediately:

```erlang
{ok, ASock} = socket:accept(LSock),
ConnPid = spawn(fun() -> conn_recv_loop(ASock, <<>>) end),
ok = socket:setopt(ASock, otp, controlling_process, ConnPid).
```

## Synchronous-only clients

If you only ever call `arterial_client:call/3` and never use
`checkout_async/3`, `handle_request/2`/`handle_data/2` are still required
by the behaviour but are never actually invoked — stub them out:

```erlang
handle_request(_Request, State) -> {ok, [], <<>>, State}.
handle_data(_Data, State)       -> {ok, [], State}.
```

(`test_echo_client` keeps real implementations of these for its own
tests' asynchronous-path coverage, but a synchronous-only client doesn't
need them to do anything.)

## Worked end-to-end example

The full, runnable version of everything above is split across:

* [test/test_echo_protocol.erl](../test/test_echo_protocol.erl) — `arterial_protocol`
* [test/test_echo_client.erl](../test/test_echo_client.erl) — `arterial_client`
* [test/test_tcp_server.erl](../test/test_tcp_server.erl) / [test/test_udp_server.erl](../test/test_udp_server.erl) — minimal servers
* [test/test_tcp_server_tests.erl](../test/test_tcp_server_tests.erl) / [test/test_udp_server_tests.erl](../test/test_udp_server_tests.erl) — pool setup + `call/3` usage

Run them directly to see the whole stack working:

```
$ rebar3 eunit --module=test_tcp_server_tests
$ rebar3 eunit --module=test_udp_server_tests
```
