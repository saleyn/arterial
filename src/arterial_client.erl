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

-export([call/3]).

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
`{arterial_timeout, Pool, RequestID}` (see `arterial_nif:track_inflight/5`).
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
(sharing this `port`/`socket_options`) or a map overriding either (or
both) for that entry alone -- see `t:arterial_connection:address_entry/0`,
e.g. to reach several independent server instances on localhost, each
on its own port. Once the whole list has been tried and failed, the
connection backs off (`reconnect_time`) before restarting from the first
address again.

`reconnect_time` controls that backoff: a fixed interval (plain
integer), or `{backoff, Min, Max}` for exponential backoff -- see
`t:arterial_connection:reconnect_time/0`. The legacy `reconnect_time_min`/
`reconnect_time_max` pair still works (folded into the equivalent
`{backoff, Min, Max}`) if `reconnect_time` isn't given.

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
```
""".
-type options() :: #{
  init_options       => arterial_connection:init_options(),
  address            => arterial:inet_address(),
  addresses          => [arterial_connection:address_entry(), ...],
  ip                 => arterial:inet_address(),
  port               => arterial:inet_port(),
  protocol           => arterial:protocol(),
  reconnect          => boolean(),
  reconnect_time     => arterial_connection:reconnect_time(),
  reconnect_time_max => arterial:time()   | infinity, % deprecated, use reconnect_time
  reconnect_time_min => arterial:time(),              % deprecated, use reconnect_time
  bounce_interval_ms => non_neg_integer() | infinity,
  socket_options     => arterial:socket_options()
}.

-export_type([options/0]).

%%%-----------------------------------------------------------------------------
%%% Synchronous request API
%%%-----------------------------------------------------------------------------
-doc """
Send `Request` on a connection checked out from `Pool` and block for its
reply (or `Timeout` milliseconds, whichever comes first).

The calling process owns the socket directly for the duration of the call:
it checks out a connection, encodes/sends/receives on the raw socket via
the pool's `arterial_protocol` module, then checks the connection back in.
`arterial_connection` (the pool's `gen_server` worker for this connection)
is not involved in the call itself -- it only owns reconnect/health
monitoring between calls.

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
  case arterial_nif:checkout_connection(Pool, sync) of
    {ok, #{
      conn_id  := ConnID,
      protocol := Proto,
      socket   := Socket,
      buffer   := Buffer,
      req_ids  := [ReqID | _] = ReqIDs
    }} ->
      try
        TS = os:system_time(microsecond),
        case Proto:encode_request(ReqID, Request, Timeout) of
          {ok, Data} ->
            case Proto:send(Socket, Data) of
              ok ->
                Expire = arterial_util:calc_expiration(TS, Timeout),
                case receive_response(Socket, Proto, ReqID, Buffer, Expire) of
                  {ok, Result, Rest} ->
                    ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, Rest),
                    {ok, Result};
                  {error, _} = Error ->
                    ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
                    Error
                end;
              {error, _} = Error ->
                ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
                Error
            end;
          {error, _} = Error ->
            ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
            Error
        end
      catch E:R:ST ->
        arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>),
        erlang:raise(E, R, ST)
      end;
    {error, _} = Error ->
      Error
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

-spec receive_response(arterial:socket(), arterial:protocol(),
                        arterial:request_id(), binary(),
                        non_neg_integer() | infinity) ->
  {ok, arterial:response(), binary()} | {error, term()}.
receive_response(Socket, Proto, ReqID, Buffer, Expire) ->
  case Proto:decode_reply(ReqID, Buffer) of
    {ok, Result, Rest} ->
      {ok, Result, Rest};
    {more, Buffer1} ->
      Timeout = arterial_util:calc_timeout(Expire),
      case Proto:recv(Socket, Timeout) of
        {ok, Chunk} ->
          receive_response(Socket, Proto, ReqID, <<Buffer1/binary, Chunk/binary>>, Expire);
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      Error
  end.