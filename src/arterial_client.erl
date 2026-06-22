-module(arterial_client).

-moduledoc """
Behaviour implemented by callback modules that own a connection's
request/reply lifecycle, plus the synchronous `call/3` request API.

An `arterial_client` callback module is paired with an `arterial_protocol`
module: `arterial_protocol` handles the wire transport and per-request
codec, while `arterial_client` owns per-connection setup/teardown and
(for the asynchronous path) request/reply bookkeeping driven by
`handle_request/2` and `handle_data/2`.

Implementations are passed to `arterial_pool:start_link/2` via the
`client` and `client_opts` options.
""".

-export([call/3, cast/2]).

-optional_callbacks([handle_timeout/2]).

-doc """
Initialize per-connection callback state from `Options` (the pool's
`client_opts`, see `t:options/0`), before a socket exists. Called once
each time `arterial_connection` (re)connects, before `c:setup/2`.
""".
-callback init(Options::map()) ->
  {ok, State::any()} | {error, Reason::term()}.

-doc """
Perform any post-connect handshake/setup on the freshly connected `Socket`
(e.g. authentication). Called once per connection, right after `c:init/1`
succeeds and before the connection is made available for checkout.
""".
-callback setup(Socket::inet:socket(), State::any()) ->
  {ok, State::any()} | {error, Reason::term(), State::any()}.

-doc """
Encode `Request` for the asynchronous path: assign it one or more
wire-level request ids and produce the bytes to send on the wire.
Not used by the synchronous `call/3` path, which talks to
`arterial_protocol` directly.
""".
-callback handle_request(Request::term(), State::any()) ->
  {ok, [RequestID::arterial:request_id()], Data::iodata(), State::any()}.

-doc """
Decode newly received `Data` (asynchronous path) into zero or more
completed responses, given the callback module's own buffering/framing
state.
""".
-callback handle_data(Data::binary(), State::any()) ->
  {ok, [arterial:response()], State::any()} | {error, Reason::term(), State::any()}.

-doc """
Optional: produce a synthetic response for a request that timed out
(asynchronous path) instead of letting the caller only see
`{error, timeout}` from the connection's `arterial_conn_owner`.
""".
-callback handle_timeout(RequestID::arterial:request_id(), State::any()) ->
  {ok, arterial:response(), State::any()} | {error,  Reason::term(), State::term()}.

-doc """
Called when the connection is torn down (cleanly or due to an error), to
let the callback module release any resources held in `State`.
""".
-callback terminate(Reason::any(), State::any()) ->
  ok.

-doc """
Pool/connection options accepted under the `client_opts` key of
`t:arterial_pool:options/0`.

`addresses` (a non-empty list, tried in order on every reconnect until
one connects) takes priority over the single-address `address`/`ip` if
both are given; useful for failing over across a fixed set of
known-good backup addresses. Each entry is either a plain address
(sharing this `port`/`socket_options`/`tls_options`) or a map overriding
any of those for that entry alone -- see
`t:arterial_connection:address_entry/0`, e.g. to reach several
independent server instances on localhost, each on its own port. Once
the whole list has been tried and failed, the connection backs off
(`reconnect_time`) before restarting from the first address again.

`reconnect_time` controls that backoff: a fixed interval (plain
integer), or `{backoff, Min, Max}` for exponential backoff -- see
`t:arterial_connection:reconnect_time/0`. The legacy `reconnect_time_min`/
`reconnect_time_max` pair still works (folded into the equivalent
`{backoff, Min, Max}`) if `reconnect_time` isn't given.

`tls_options` (requires **OTP 28+**) is only used when this connection's
transport (the `client_opts` `protocol` key, distinct from
`t:arterial_pool:options/0`'s `protocol` key, which names the wire codec
module) is `ssl` -- passed straight through to `ssl:connect/3` after
this connection's `socket_options` are applied via the `socket` module.
See `m:arterial_socket`'s moduledoc for why `ssl` needs OTP 28+.

## Examples

```
1> Opts = #{address => "db.internal", port => 5432, protocol => tcp,
2>          reconnect => true, reconnect_time => {backoff, 500, 30000}}.
#{address => "db.internal",port => 5432,protocol => tcp,
  reconnect => true,reconnect_time => {backoff,500,30000}}
3> Opts2 = #{addresses => ["db1.internal", "db2.internal", "db3.internal"],
4>           port => 5432, protocol => tcp}.
#{addresses => ["db1.internal","db2.internal","db3.internal"],
  port => 5432,protocol => tcp}
5> Opts3 = #{addresses => [#{address => "127.0.0.1", port => 9001},
6>                          #{address => "127.0.0.1", port => 9002}],
7>           protocol => tcp}.
#{addresses => [#{address => "127.0.0.1",port => 9001},
                 #{address => "127.0.0.1",port => 9002}],
  protocol => tcp}
8> Opts4 = #{address => "db.internal", port => 5432, protocol => ssl,
9>           tls_options => [{verify, verify_peer}, {cacertfile, "/etc/ssl/ca.pem"}]}.
#{address => "db.internal",port => 5432,protocol => ssl,
  tls_options => [{verify,verify_peer},{cacertfile,"/etc/ssl/ca.pem"}]}
```
""".
-type options() :: #{
  init_options       => arterial_connection:init_options(),
  address            => arterial:inet_address(),
  addresses          => [arterial_connection:address_entry(), ...],
  ip                 => arterial:inet_address(),
  port               => arterial:inet_port(),
  protocol           => tcp | udp | ssl,
  reconnect          => boolean(),
  reconnect_time     => arterial_connection:reconnect_time(),
  reconnect_time_max => arterial:time()   | infinity, % deprecated, use reconnect_time
  reconnect_time_min => arterial:time(),              % deprecated, use reconnect_time
  bounce_interval_ms => non_neg_integer() | infinity,
  socket_options     => arterial:socket_options(),
  tls_options        => arterial:tls_options()
}.

-export_type([options/0]).

%%%-----------------------------------------------------------------------------
%%% Synchronous request API
%%%-----------------------------------------------------------------------------
-doc """
Send `Request` on a connection checked out from `Pool` and block for its
reply (or `Timeout` milliseconds, whichever comes first).

The connection's `arterial_conn_owner` process -- not the calling process
-- owns the socket and does the actual `encode_request`/`send`/`recv`/
`decode_reply` round trip; this call just checks out reservation capacity,
hands the request to the owner via `arterial_conn_owner:send_recv/4`, and
checks the capacity back in once the owner replies. This is what makes
`backlog > 1` (multiple concurrent callers multiplexed onto one
connection) safe: only the owner ever touches the raw socket.

## Examples

```
1> arterial_pool:start_link(my_pool, #{protocol => my_proto, client => my_client,
2>                                      client_opts => #{address => "localhost", port => 9000}}).
{ok,<0.123.0>}
2> arterial_client:call(my_pool, {get, <<"key">>}, 5000).
{ok, <<"value">>}
```
""".
-spec call(arterial_pool:name(), term(), non_neg_integer()) ->
  {ok, arterial:response()} | {error, term()}.
call(Pool, Request, Timeout) ->
  arterial_observe:span([call], #{pool => Pool}, fun() ->
    Result = do_call(Pool, Request, Timeout),
    Outcome = case Result of {ok, _} -> ok; _ -> error end,
    {Result, #{pool => Pool, result => Outcome}}
  end).

do_call(Pool, Request, Timeout) ->
  case checkout(Pool, sync) of
    {ok, ConnID} ->
      try
        arterial_conn_owner:send_recv(Pool, ConnID, Request, Timeout)
      catch
        exit:Reason -> owner_call_exit_error(Reason)
      after
        ok = arterial_nif:checkin_connection(Pool, ConnID)
      end;
    {error, _} = Error ->
      Error
  end.

%% The owner died mid-call (gen_server:call/3 raises rather than
%% returning when its target is gone) -- surface this exactly like any
%% other disconnect, instead of crashing this caller along with it.
%% arterial_pool's supervisor restarts the owner; the checkin in
%% do_call/3's/do_cast/2's `after` still runs regardless, so the dead
%% reservation is never stranded.
owner_call_exit_error({Reason, {gen_server, call, _}}) -> {error, Reason};
owner_call_exit_error(Reason)                          -> {error, Reason}.

%% Wraps arterial_nif:checkout_connection/2 with a [arterial, checkout, ...]
%% span -- shared by call/3 (Mode = sync) and cast/2 (Mode = async).
checkout(Pool, Mode) ->
  arterial_observe:span([checkout], #{pool => Pool, mode => Mode}, fun() ->
    Result = arterial_nif:checkout_connection(Pool, Mode),
    Outcome = case Result of {ok, _} -> ok; {error, Reason} -> Reason end,
    {Result, #{pool => Pool, mode => Mode, outcome => Outcome}}
  end).

-doc """
Send `Request` on a connection checked out from `Pool` without waiting
for (or expecting) any reply -- mode (e) of the backlog/protocol design:
send-and-forget protocols (e.g. fire-and-forget logging/metrics) where
every message is inherently one-way. The reservation is claimed and
released back-to-back within this one call, never actually left
in-flight.

Returns as soon as the bytes are handed to the transport's `send/2`
(e.g. accepted into the OS socket buffer), not when (or whether) the
remote peer actually processes them -- there is no protocol-level
acknowledgement to wait for. If the underlying protocol needs delivery
confirmation, use mode (f) (ack-based protocols) instead, which has a
real reply to correlate (see `c:arterial_protocol:decode_reply/2`'s
moduledoc).

## Examples

```
1> arterial_pool:start_link(my_pool, #{protocol => my_log_proto, client => my_client,
2>                                      client_opts => #{address => "localhost", port => 9000}}).
{ok,<0.123.0>}
2> arterial_client:cast(my_pool, {log, info, <<"started">>}).
ok
```
""".
-spec cast(arterial_pool:name(), term()) -> ok | {error, term()}.
cast(Pool, Request) ->
  arterial_observe:span([cast], #{pool => Pool}, fun() ->
    Result = do_cast(Pool, Request),
    Outcome = case Result of ok -> ok; _ -> error end,
    {Result, #{pool => Pool, result => Outcome}}
  end).

do_cast(Pool, Request) ->
  case checkout(Pool, async) of
    {ok, ConnID} ->
      try
        arterial_conn_owner:send(Pool, ConnID, Request)
      catch
        exit:Reason -> owner_call_exit_error(Reason)
      after
        ok = arterial_nif:checkin_connection(Pool, ConnID)
      end;
    {error, _} = Error ->
      Error
  end.