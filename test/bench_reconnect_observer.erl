-module(bench_reconnect_observer).

-moduledoc """
Simple arterial_observe backend that counts reconnection events during benchmarks.
Used by bench_arterial_fifo to track reconnections.
""".

-behaviour(arterial_observe).

-export([start/1, stop/0, event/3]).
-export([get_count/0, reset_count/0]).

%% Store reconnect count in persistent_term for efficiency
-define(COUNT_KEY, {?MODULE, reconnect_count}).

%%%-----------------------------------------------------------------------------
%%% arterial_observe behaviour
%%%-----------------------------------------------------------------------------

-spec start(term()) -> ok.
start(_Opts) ->
  persistent_term:put(?COUNT_KEY, 0),
  ok.

-spec stop() -> ok.
stop() ->
  persistent_term:erase(?COUNT_KEY),
  ok.

-spec event([atom()], map(), map()) -> ok.
event([arterial, reconnect, success], _Measurements, _Metadata) ->
  % Count successful reconnections
  increment_count();
event([arterial, reconnect, attempt], _Measurements, _Metadata) ->
  % We could count attempts too, but for now just count successes
  ok;
event(_EventName, _Measurements, _Metadata) ->
  % Ignore other events
  ok.

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-spec get_count() -> non_neg_integer().
get_count() ->
  try
    persistent_term:get(?COUNT_KEY, 0)
  catch
    _:_ -> 0
  end.

-spec reset_count() -> ok.
reset_count() ->
  persistent_term:put(?COUNT_KEY, 0),
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

increment_count() ->
  try
    Current = persistent_term:get(?COUNT_KEY, 0),
    persistent_term:put(?COUNT_KEY, Current + 1)
  catch
    _:_ -> ok
  end.