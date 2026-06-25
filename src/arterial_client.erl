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
  case arterial_observe:enabled() of
    false ->
      do_call(Pool, Request, Timeout);
    true ->
      arterial_observe:span([call], #{pool => Pool}, fun() ->
        Result = do_call(Pool, Request, Timeout),
        Outcome = case Result of {ok, _} -> ok; _ -> error end,
        {Result, #{pool => Pool, result => Outcome}}
      end)
  end.

do_call(Pool, Request, Timeout) ->
  CorrId = new_corr_id(),
  Codec = arterial_pool:codec(Pool),
  Data = iolist_to_binary(Codec:encode_request(CorrId, Request)),
  Size = arterial_pool:size(Pool),
  case send_to_any(Pool, Size, [], CorrId, Data, Timeout) of
    ok ->
      await_reply(Pool, CorrId, Timeout);
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
  case arterial_observe:enabled() of
    false ->
      do_cast(Pool, Request);
    true ->
      arterial_observe:span([cast], #{pool => Pool}, fun() ->
        Result = do_cast(Pool, Request),
        Outcome = case Result of ok -> ok; _ -> error end,
        {Result, #{pool => Pool, result => Outcome}}
      end)
  end.

do_cast(Pool, Request) ->
  CorrId = new_corr_id(),
  Codec = arterial_pool:codec(Pool),
  Data = iolist_to_binary(Codec:encode_request(CorrId, Request)),
  Size = arterial_pool:size(Pool),
  send_cast_to_any(Pool, Size, [], Data).

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

send_to_any(_Pool, Size, Tried, _CorrId, _Data, _Timeout) when length(Tried) >= Size ->
  {error, no_connection};
send_to_any(Pool, Size, Tried, CorrId, Data, Timeout) ->
  case next_candidate(Pool, Size, Tried) of
    none ->
      {error, no_connection};
    ConnID ->
      case try_send(Pool, ConnID, CorrId, Data, Timeout) of
        ok    -> ok;
        retry -> send_to_any(Pool, Size, [ConnID | Tried], CorrId, Data, Timeout)
      end
  end.

try_send(Pool, ConnID, CorrId, Data, Timeout) ->
  CorrTable = arterial_pool:corr_table(Pool),
  TS        = os:system_time(microsecond),
  Deadline  = arterial_util:calc_expiration(TS, Timeout),
  %% Inserted before sending: the reply (or even a disconnect) can only
  %% ever be noticed by arterial_connection after the write below
  %% returns, so there's no risk of it being decoded and dropped for
  %% lack of a matching entry yet.
  ets:insert(CorrTable, {CorrId, self(), ConnID, Deadline}),
  PoolRef = arterial_pool:pool_ref(Pool),
  case arterial_observe:enabled() of
    false ->
      case arterial_nif:send_and_release(PoolRef, ConnID, [Data]) of
        {ok, _SlotId} -> ok;
        {error, _Reason} -> ets:delete(CorrTable, CorrId), retry
      end;
    true ->
      case arterial_observe:span([nif, send], #{pool => Pool, conn_id => ConnID}, fun() ->
        case arterial_nif:send_and_release(PoolRef, ConnID, [Data]) of
          {ok, SlotId} -> {{ok, SlotId}, #{result => ok, slot_id => SlotId}};
          {error, Reason} -> {{error, Reason}, #{result => error, reason => Reason}}
        end
      end) of
        {ok, _SlotId} -> ok;
        {error, _Reason} -> ets:delete(CorrTable, CorrId), retry
      end
  end.

send_cast_to_any(_Pool, Size, Tried, _Data) when length(Tried) >= Size ->
  {error, no_connection};
send_cast_to_any(Pool, Size, Tried, Data) ->
  case next_candidate(Pool, Size, Tried) of
    none ->
      {error, no_connection};
    ConnID ->
      PoolRef = arterial_pool:pool_ref(Pool),
      case arterial_observe:enabled() of
        false ->
          case arterial_nif:send_and_release(PoolRef, ConnID, [Data]) of
            {ok, _SlotId}    -> ok;
            {error, _Reason} -> send_cast_to_any(Pool, Size, [ConnID | Tried], Data)
          end;
        true ->
          case arterial_observe:span([nif, send], #{pool => Pool, conn_id => ConnID}, fun() ->
            case arterial_nif:send_and_release(PoolRef, ConnID, [Data]) of
              {ok, SlotId} -> {{ok, SlotId}, #{result => ok, slot_id => SlotId}};
              {error, Reason} -> {{error, Reason}, #{result => error, reason => Reason}}
            end
          end) of
            {ok, _SlotId}    -> ok;
            {error, _Reason} -> send_cast_to_any(Pool, Size, [ConnID | Tried], Data)
          end
      end
  end.

await_reply(Pool, CorrId, Timeout) ->
  receive
    {arterial_reply,        CorrId, Reply} -> {ok, Reply};
    {arterial_disconnected, Pool,  CorrId} -> {error, disconnected};
    {arterial_timeout,      Pool,  CorrId} -> {error, timeout};
    Other ->
      error({invalid_reply, Other, #{pool => Pool, corr_id => CorrId}})
  after Timeout ->
    ets:delete(arterial_pool:corr_table(Pool), CorrId),
    {error, timeout}
  end.

%% Scheduler-affine starting offset, then a linear scan of every
%% remaining ConnID (wrapping), skipping any already in `Tried` and any
%% currently unavailable connection (no socket / disconnected /
%% draining for a bounce). Throttling is handled in the NIF.
next_candidate(Pool, Size, Tried) ->
  Start = erlang:system_info(scheduler_id) rem Size,
  find_candidate(Pool, Size, Tried, Start, 0).

find_candidate(_Pool, Size, _Tried, _Start, Size) ->
  none;
find_candidate(Pool, Size, Tried, Start, Offset) ->
  ConnID = (Start + Offset) rem Size,
  case lists:member(ConnID, Tried) of
    true ->
      find_candidate(Pool, Size, Tried, Start, Offset + 1);
    false ->
      case arterial_pool:is_available(Pool, ConnID) of
        true  -> ConnID;
        false -> find_candidate(Pool, Size, Tried, Start, Offset + 1)
      end
  end.

%% Throttling is now handled directly in the NIF during send_and_release/3
