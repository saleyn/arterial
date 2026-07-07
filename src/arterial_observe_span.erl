-module(arterial_observe_span).

-moduledoc """
Observability dispatcher used when a backend is configured. Each function
wraps the operation in `arterial_observe:span/3`, emitting start/stop events.
Returned by `arterial_observe:dispatcher/0` when `enabled/0` is true.
""".

-export([call/2, cast/2, send_and_release/4, register_and_send/7]).

-doc "Wrap `Fun/0` in a `[call]` span and return its result.".
-spec call(arterial_pool:name(), fun(() -> Result)) -> Result when Result :: term().
call(Pool, Fun) ->
  arterial_observe:span([call], #{pool => Pool}, fun() ->
    Result = Fun(),
    Outcome = case Result of {ok, _} -> ok; _ -> error end,
    {Result, #{pool => Pool, result => Outcome}}
  end).

-doc "Wrap `Fun/0` in a `[cast]` span and return its result.".
-spec cast(arterial_pool:name(), fun(() -> Result)) -> Result when Result :: term().
cast(Pool, Fun) ->
  arterial_observe:span([cast], #{pool => Pool}, fun() ->
    Result = Fun(),
    Outcome = case Result of ok -> ok; _ -> error end,
    {Result, #{pool => Pool, result => Outcome}}
  end).

-doc "Wrap `arterial_nif:send_and_release/3` in a `[nif, send]` span.".
-spec send_and_release(arterial_pool:name(), arterial_nif:pool_ref(), non_neg_integer(), iodata()) ->
  {ok, non_neg_integer()} | {error, term()}.
send_and_release(Pool, PoolRef, ConnID, Data) ->
  arterial_observe:span([nif, send], #{pool => Pool, conn_id => ConnID}, fun() ->
    case arterial_nif:send_and_release(PoolRef, ConnID, [Data]) of
      {ok,    SlotId} -> {{ok,    SlotId}, #{result => ok,    slot_id => SlotId}};
      {error, Reason} -> {{error, Reason}, #{result => error, reason  => Reason}}
    end
  end).

-doc "Wrap `arterial_nif:register_and_send/6` in a `[nif, send]` span.".
-spec register_and_send(arterial_pool:name(), arterial_nif:pool_ref(), non_neg_integer(),
                        non_neg_integer(), pid(), integer(), iodata()) ->
  {ok, non_neg_integer()} | {error, term()}.
register_and_send(Pool, PoolRef, ConnID, CorrId, CallerPid, DeadlineUs, Data) ->
  arterial_observe:span([nif, send], #{pool => Pool, conn_id => ConnID}, fun() ->
    case arterial_nif:register_and_send(PoolRef, ConnID, CorrId, CallerPid, DeadlineUs, [Data]) of
      {ok,    SlotId} -> {{ok,    SlotId}, #{result => ok,    slot_id => SlotId}};
      {error, Reason} -> {{error, Reason}, #{result => error, reason  => Reason}}
    end
  end).
