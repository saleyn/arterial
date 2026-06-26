-module(arterial_observe).

-moduledoc """
Generic, pluggable instrumentation facade. Every instrumentation point in
`arterial` calls `span/3`/`event/2,3` here instead of talking to any
specific metrics library directly, so swapping backends never touches
`arterial_client`/`arterial_connection`/`arterial_sweeper`.

## Configuring a backend

The backend actually receiving events is chosen via the `arterial`
application's `observability` env var, read once by `arterial_app`'s
top-level supervisor and passed to `start_link/2` (see below) --
changing it at runtime requires restarting the `arterial_observe`
child (or the whole `arterial` application):

```erlang
{arterial, [{observability, undefined}]}              % default: no backend, span/3 just runs Fun/0
{arterial, [{observability, telemetry}]}               % m:arterial_observe_telemetry
{arterial, [{observability, prometheus}]}              % m:arterial_observe_prometheus
{arterial, [{observability, {telemetry, Opts}}]}       % built-in backend + Opts passed to its start/1
{arterial, [{observability, {prometheus, Opts}}]}
{arterial, [{observability, my_app_metrics}]}          % your own module, Opts defaults to []
{arterial, [{observability, {my_app_metrics, Opts}}]}  % your own module + Opts
```

`telemetry` and `prometheus` are both strictly optional dependencies of
`arterial` (declared in `rebar.config` but absent from `arterial.app.src`'s
`applications`) -- only the configured backend's dependency is ever
actually started, the same lazy pattern `arterial_socket` uses for `ssl`.

## Writing your own backend

Implement the `arterial_observe` behaviour: three callbacks,

```erlang
-callback start(Opts :: term()) -> ok.
-callback stop() -> ok.
-callback event(EventNameSuffix :: [atom()], Measurements :: map(), Metadata :: map()) -> ok.
```

`start/1` is called once, with whatever `Opts` were configured (or `#{}`
if none), when `arterial_app`'s supervisor starts the
`arterial_observe` child -- use it to start/configure the
underlying metrics library and declare any metrics up front (see
`m:arterial_observe_prometheus`'s `start/1` for an example).
`stop/0` is called when that child terminates (normal shutdown or a
crash) -- use it to release whatever `start/1` acquired (typically just
stopping the backend's own application if `start/1` started it; see
either built-in backend's `ensure_started/0` for the `internal`/`external`
bookkeeping pattern that makes this safe to call even when the underlying
application was already running for unrelated reasons before `start/1`
ran). `event/3` receives every emitted event -- `EventNameSuffix` is
always `[arterial | _]`-prefixed (e.g. `[arterial, call, stop]`,
`[arterial, disconnect]`) -- see this module's "Events" section below for
the full catalog, including which `Measurements`/`Metadata` keys come
with each. Set `{observability, my_app_metrics}` and arterial calls
`my_app_metrics:start/1` once at boot, then `my_app_metrics:event/3` per
event; no separate registration step needed.

## Events

All events are emitted under the `[arterial | _]` prefix. `Pool` (the
pool name, `t:arterial_pool:name/0`) is always present in metadata.

* `[arterial, call, start | stop | exception]` -- one span per
  `arterial_client:call/3` call. Metadata: `pool`. Stop metadata adds
  `result` (`ok` or `error`).
* `[arterial, cast, start | stop | exception]` -- one span per
  `arterial_client:cast/2` call. Metadata: `pool`. Stop metadata adds
  `result`.
* `[arterial, checkout, start | stop | exception]` -- one span around a
  connection checkout (`arterial_nif:checkout_connection/2,3`, used
  internally by `call/3`/`cast/2`). Metadata: `pool`, `mode`
  (`sync` | `async`). Stop metadata adds `outcome`
  (`ok` | `no_connection`).
* `[arterial, connect, start | stop | exception]` -- one span per
  `arterial_socket:connect/6` attempt made by `arterial_connection`
  (one per address tried, not just the first). Metadata: `pool`,
  `conn_id`, `address`, `port`. Stop metadata adds `result`.
* `[arterial, disconnect]` -- emitted whenever
  `arterial_connection:disconnect/2` runs (planned bounce or
  unplanned/error-triggered). Metadata: `pool`, `conn_id`, `reason`.
* `[arterial, sweep, stop]` -- one event per `sweep_timeouts/1` call.
  Measurements: `expired_count` (how many in-flight requests timed out
  this sweep). Metadata: `pool`.

Span `stop`/`exception` measurements always include `duration` (native
time units, see `erlang:convert_time_unit/3`) and `monotonic_time`; `start`
measurements include `monotonic_time`. Two events conspicuously absent
from this catalog: a per-request timeout event and a "queued wait time"
event for `arterial_nif:checkout_async/3,4`. Both `{arterial_timeout, Pool,
ReqID}` and `{arterial_ready, Pool, ...}` are sent directly from the C++
NIF layer (`enif_send`) straight to the original caller's mailbox -- there
is no Erlang-side chokepoint in this library to intercept them, since
`arterial` doesn't ship the asynchronous dispatch loop that would receive
them (see `test/arterial_async_driver.erl` for the pattern such a loop
follows). `[arterial, sweep, stop]`'s `expired_count` covers timeout
*volume* in aggregate; a caller building their own dispatch loop on
`checkout_async/3,4` should emit its own queued-wait/per-request-timeout
telemetry around its own receive loop.
""".
-include_lib("kernel/include/logger.hrl").

-callback event(EventNameSuffix :: [atom()], Measurements :: map(), Metadata :: map()) -> ok.

-doc """
Perform initialization of an observability component.
""".
-callback start(Opts :: term()) -> ok.

-doc """
Perform finalization of an observability component.
""".
-callback stop() -> ok.

-export([start_link/2, span/3, event/2, event/3, enabled/0]).

-doc """
Start (and register locally as `?MODULE`) the singleton process that owns
the configured backend's lifecycle: calls `Module:start(Options)` once on
init, and `Module:stop/0` whenever this process terminates (normal
shutdown, or re-raised after `Module:stop/0` if it was killed for another
reason) -- the same crash-then-cleanup-then-propagate shape a
`gen_server`'s `terminate/2` would give you, implemented directly since
this process has no other state or call/cast API to justify a full
`gen_server`.

Intended to be started as a permanent child of `arterial_app`'s
top-level supervisor (see that module's `init/1`), with `Module`/`Options`
resolved there from the `observability` application env var --
not normally called directly. `Module = undefined` (no backend
configured) is a no-op: `span/3` then just runs `Fun/0` with no event
emission, and `event/2,3` are no-ops.
""".
-spec start_link(Module :: module() | undefined, Opts :: term()) -> {ok, pid()}.
start_link(Module, Options) ->
  {ok, spawn_link(fun() -> server_init(Module, Options) end)}.

server_init(Module, Options) ->
  true = register(?MODULE, self()),
  process_flag(trap_exit, true),
  try
    % Resolve module name to actual implementation module
    ResolvedModule = resolve_module(Module),
    case ResolvedModule of
      nil ->
        persistent_term:put(?MODULE, nil);
      _ ->
        case code:ensure_loaded(ResolvedModule) of
          {module, ResolvedModule} ->
            ok = ResolvedModule:start(Options),
            persistent_term:put(?MODULE, ResolvedModule);
          {error, _Reason} ->
            % If module loading fails, disable observability gracefully
            ?LOG_WARNING("~p observability module ~p could not be loaded, disabling observability",
                        [arterial, ResolvedModule]),
            persistent_term:put(?MODULE, nil)
        end
    end
  catch E:R:ST ->
    ?LOG_ERROR("~p failed to start observability module ~p with arguments:\n  ~p\n",
               [arterial, Module, Options]),
    erlang:raise(E, R, ST)
  end,
  server_loop(resolve_module(Module)).

server_loop(Module) ->
  receive
    {'EXIT', _Pid, Reason} when Reason == normal; Reason == shutdown ->
      stop(Module);
    {'EXIT', _Pid, Reason} ->
      stop(Module),
      erlang:error(Reason);
    _ ->
      server_loop(Module)
  end.

-spec stop(atom()) -> ok.
stop(Module) ->
  (Module =/= nil) andalso Module:stop(),
  persistent_term:erase(?MODULE).

%% Resolve module names to actual implementation modules
resolve_module(Module) ->
  case Module of
    undefined -> nil;
    nil -> nil;
    ok -> nil;  % Handle weird supervisor restart cases
    telemetry -> arterial_observe_telemetry;
    prometheus -> arterial_observe_prometheus;
    arterial_observe_telemetry -> arterial_observe_telemetry;
    arterial_observe_prometheus -> arterial_observe_prometheus;
    Mod when is_atom(Mod) -> Mod;
    _ -> nil  % Invalid input, disable observability
  end.

-doc """
Runs `Fun/0` (expected to return `{Result, StopMetadata}` or `{Result,
ExtraMeasurements, StopMetadata}`, exactly like `telemetry:span/3`),
emitting `[arterial | EventNameSuffix] ++ [start]` before and
`[arterial | EventNameSuffix] ++ [stop]` after (or `++ [exception]` if
`Fun` raises, which is then re-raised unchanged) through the configured
backend. If no backend is configured (`observability` env var unset),
just runs `Fun/0` and returns its result -- no events, no `start`/`stop`
timing overhead.

`EventNameSuffix` is a list, e.g. `[call]` or `[checkout]` -- see this
module's moduledoc for the full event catalog.
""".
-spec span([atom()], map(), fun(() -> {Result, map()} | {Result, map(), map()})) -> Result
  when Result :: term().
span(EventNameSuffix, StartMetadata, Fun) when is_list(EventNameSuffix), is_function(Fun, 0) ->
  case backend() of
    nil ->
      case Fun() of
        {Result, _StopMetadata}                     -> Result;
        {Result, _ExtraMeasurements, _StopMetadata} -> Result
      end;
    Backend ->
      StartTime = erlang:monotonic_time(),
      Backend:event([arterial | EventNameSuffix] ++ [start],
                    #{monotonic_time => StartTime}, StartMetadata),
      try Fun() of
        {Result, StopMetadata} ->
          StopTime = erlang:monotonic_time(),
          Backend:event([arterial | EventNameSuffix] ++ [stop],
                        #{duration => StopTime - StartTime, monotonic_time => StopTime},
                        StopMetadata),
          Result;
        {Result, ExtraMeasurements, StopMetadata} ->
          StopTime = erlang:monotonic_time(),
          Measurements = ExtraMeasurements#{duration => StopTime - StartTime, monotonic_time => StopTime},
          Backend:event([arterial | EventNameSuffix] ++ [stop], Measurements, StopMetadata),
          Result
      catch Class:Reason:Stacktrace ->
        StopTime = erlang:monotonic_time(),
        Backend:event([arterial | EventNameSuffix] ++ [exception],
                      #{duration => StopTime - StartTime, monotonic_time => StopTime},
                      StartMetadata#{kind => Class, reason => Reason}),
        erlang:raise(Class, Reason, Stacktrace)
      end
  end.

-doc """
Whether an observability backend is currently configured. Callers on a
hot path (e.g. `arterial_client:call/3,2`) can check this first to skip
building `span/3`'s metadata map and closure entirely when there's no
backend to receive them -- `span/3` itself only skips the *event
emission* cost in that case, not the construction cost its caller
already paid to produce the arguments.
""".
-spec enabled() -> boolean().
enabled() ->
  backend() =/= nil.

-doc "Equivalent to `event/3` with `Measurements = #{}`.".
-spec event([atom()], map()) -> ok.
event(EventNameSuffix, Metadata) ->
  event(EventNameSuffix, #{}, Metadata).

-doc """
Emits `[arterial | EventNameSuffix]` with the given `Measurements`/
`Metadata` through the configured backend. Use for one-shot events that
aren't a start/stop pair (e.g. `[disconnect]`, `[sweep, stop]`). A no-op
if no backend is configured.
""".
-spec event([atom()], map(), map()) -> ok.
event(EventNameSuffix, Measurements, Metadata) when is_list(EventNameSuffix) ->
  case backend() of
    nil     -> ok;
    Backend -> Backend:event([arterial | EventNameSuffix], Measurements, Metadata)
  end.

%% Cached backend module.
backend() ->
  case persistent_term:get(?MODULE, false) of
    false ->
      Module =
        case application:get_env(arterial, observability, nil) of
          prometheus            -> arterial_observe_prometheus;
          telemetry             -> arterial_observe_telemetry;
          nil                   -> nil;
          undefined             -> nil;
          Mod when is_atom(Mod) -> Mod
        end,
      persistent_term:put(?MODULE, Module),
      Module;
    Module ->
      Module
  end.

