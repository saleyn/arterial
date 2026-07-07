-module(arterial_client).

-moduledoc """
Public request API for `arterial_pool` (`arterial`'s second connection-
pool backend, see `arterial_pool`'s moduledoc).

Unlike `arterial_client:call/3` (the original backend, where the caller
checks out and holds a connection for the call's whole round trip),
`call/2,3` here writes the request inline (via
`arterial_nif:send_and_release/3`) and immediately gives up any
claim on "its" connection -- multiplexing is entirely by the wire-level
correlation id the request was encoded with, looked up against the
pool's public ETS table by whichever `arterial_connection` worker
decodes the matching reply off the wire.
""".

-export([call/2, call/3, cast/2]).
-export([new_corr_id/0]).

-include("arterial_pool.hrl").

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc "Equivalent to `call/3` with `Timeout` defaulting to the pool's `default_timeout_ms`.".
-spec call(arterial_pool:name(), term()) -> {ok, arterial:response()} | {error, term()}.
call(Pool, Request) ->
  call(Pool, Request, arterial_pool:default_timeout_ms(Pool)).

-doc """
Encode and send `Request` on any currently available connection of
`Pool` (tried round-robin, starting from a scheduler-affine offset to
spread contention -- see `arterial_pool`'s moduledoc),
then block for its reply (matched purely by wire-level correlation id,
see `c:arterial_codec:decode/1`) or `Timeout` milliseconds, whichever
comes first.

Returns `{error, no_connection}` if every connection is currently
unavailable or throttled, `{error, disconnected}` if the connection that
carried this request dies before a reply arrives (see
`arterial_connection`'s disconnect-notification path), or
`{error, timeout}` if `Timeout` elapses first either way.

## Examples

```
1> arterial_client:call(my_pool, {get, <<"key">>}, 5000).
{ok, <<"value">>}
```
""".
-spec call(arterial_pool:name(), term(), non_neg_integer() | infinity) ->
  {ok, arterial:response()} | {error, term()}.
call(Pool, Request, Timeout) ->
  Dispatcher = arterial_observe:dispatcher(),
  Dispatcher:call(Pool, fun() -> do_call(Pool, Request, Timeout, Dispatcher) end).

do_call(Pool, Request, Timeout, Dispatcher) ->
  #pool_meta{pool_ref = PoolRef, codec = Codec, size = Size} = arterial_pool:pool_meta(Pool),
  CorrId = new_corr_id(),
  Data   = Codec:encode_request(CorrId, Request),
  Start  = erlang:system_info(scheduler_id) rem Size,
  case send_to_any(PoolRef, Size, Start, 0, CorrId, Data, Timeout, Dispatcher, Pool) of
    {ok, ConnID} ->
      await_reply(PoolRef, CorrId, ConnID, Timeout);
    {error, _} = Error ->
      Error
  end.

-doc """
Encode and send `Request` on any currently available connection of
`Pool` without waiting for (or expecting) any reply -- the
send-and-forget path (mode (e) of `arterial`'s protocol design; see the
top-level README's "Protocol" section). Returns as soon as the bytes are
accepted by `arterial_nif:send_and_release/3`, never registering
any correlation-id bookkeeping (there's no `call/3`-style reply to match
it up with later).

## Examples

```
1> arterial_client:cast(my_pool, {log, info, <<"started">>}).
ok
```
""".
-spec cast(arterial_pool:name(), term()) -> ok | {error, term()}.
cast(Pool, Request) ->
  Dispatcher = arterial_observe:dispatcher(),
  Dispatcher:cast(Pool, fun() -> do_cast(Pool, Request, Dispatcher) end).

do_cast(Pool, Request, Dispatcher) ->
  #pool_meta{pool_ref = PoolRef, codec = Codec, size = Size} = arterial_pool:pool_meta(Pool),
  CorrId = new_corr_id(),
  Data   = Codec:encode_request(CorrId, Request),
  Start  = erlang:system_info(scheduler_id) rem Size,
  send_cast_to_any(PoolRef, Size, Start, 0, Data, Dispatcher, Pool).

-doc """
A fresh wire-level correlation id, truncated to 32 bits (the width
`arterial_codec_default`'s framing reserves for it) -- custom
`c:arterial_codec` implementations with a wider/narrower id field
should generate and pass their own instead of relying on this.

Deliberately `[positive]` only, no `monotonic`: nothing here needs
correlation ids globally ordered across processes (each is only ever
matched against the single pool-wide ETS table, never compared to
another id), and `monotonic` forces a single counter shared across every
scheduler -- measurably more contended under concurrency than the
per-scheduler counters backing plain `unique_integer/1`.
""".
-spec new_corr_id() -> non_neg_integer().
new_corr_id() ->
  erlang:unique_integer([positive]) band 16#FFFFFFFF.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

%% Try stripes in scheduler-affine order.  Availability and slot selection are
%% handled entirely inside the NIF's register_and_send (CAS on lease_mask):
%% the Erlang layer just needs to advance to the next stripe on failure and
%% give up after trying all Size stripes.
send_to_any(_PoolRef, Size, _Start, Size, _CorrId, _Data, _Timeout, _Dispatcher, _Pool) ->
  {error, no_connection};
send_to_any(PoolRef, Size, Start, Offset, CorrId, Data, Timeout, Dispatcher, Pool) ->
  ConnID   = (Start + Offset) rem Size,
  Deadline = arterial_util:calc_expiration(os:system_time(microsecond), Timeout),
  case Dispatcher:register_and_send(Pool, PoolRef, ConnID, CorrId, self(), Deadline, Data) of
    {ok,    _SlotId} -> {ok, ConnID};
    {error, _Reason} -> send_to_any(PoolRef, Size, Start, Offset + 1, CorrId, Data, Timeout, Dispatcher, Pool)
  end.

send_cast_to_any(_PoolRef, Size, _Start, Size, _Data, _Dispatcher, _Pool) ->
  {error, no_connection};
send_cast_to_any(PoolRef, Size, Start, Offset, Data, Dispatcher, Pool) ->
  ConnID = (Start + Offset) rem Size,
  case Dispatcher:send_and_release(Pool, PoolRef, ConnID, Data) of
    {ok,    _SlotId} -> ok;
    {error, _Reason} -> send_cast_to_any(PoolRef, Size, Start, Offset + 1, Data, Dispatcher, Pool)
  end.

await_reply(PoolRef, CorrId, ConnID, Timeout) ->
  receive
    {arterial_reply,        CorrId, Reply} -> {ok, Reply};
    {arterial_disconnected, CorrId}        -> {error, disconnected};
    {arterial_timeout,      CorrId}        -> {error, timeout}
  after Timeout ->
    arterial_nif:unregister_corr(PoolRef, ConnID, CorrId),
    {error, timeout}
  end.
