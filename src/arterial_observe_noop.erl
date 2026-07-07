-module(arterial_observe_noop).

-moduledoc """
Observability dispatcher used when no backend is configured. Each function
calls through directly without any span wrapping or event emission.
Returned by `arterial_observe:dispatcher/0` when `enabled/0` is false.
""".

-export([call/2, cast/2, send_and_release/4]).

-doc "Run `Fun/0` and return its result directly, no span wrapping.".
-spec call(arterial_pool:name(), fun(() -> Result)) -> Result when Result :: term().
call(_Pool, Fun) ->
  Fun().

-doc "Run `Fun/0` and return its result directly, no span wrapping.".
-spec cast(arterial_pool:name(), fun(() -> Result)) -> Result when Result :: term().
cast(_Pool, Fun) ->
  Fun().

-doc "Call `arterial_nif:send_and_release/3` directly, no span wrapping.".
-spec send_and_release(arterial_pool:name(), arterial_nif:pool_ref(), non_neg_integer(), iodata()) ->
  {ok, non_neg_integer()} | {error, term()}.
send_and_release(_Pool, PoolRef, ConnID, Data) ->
  arterial_nif:send_and_release(PoolRef, ConnID, [Data]).
