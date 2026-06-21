-module(arterial_observability).

-moduledoc """
Generic, pluggable instrumentation facade. Every instrumentation point in
`arterial` calls `span/3`/`event/2,3` here instead of talking to any
specific metrics library directly, so swapping backends never touches
`arterial_client`/`arterial_connection`/`arterial_sweeper`.

The backend actually receiving events is chosen via the `arterial`
application's `observability` env var:

```erlang
{arterial, [{observability, telemetry}]}    % default; m:arterial_observability_telemetry
{arterial, [{observability, prometheus}]}   % m:arterial_observability_prometheus
{arterial, [{observability, my_app_metrics}]} % your own module, see below
```

`telemetry` and `prometheus` are both strictly optional dependencies of
`arterial` (declared in `rebar.config` but absent from `arterial.app.src`'s
`applications`) -- only the configured backend's dependency is ever
actually started, the same lazy pattern `arterial_socket` uses for `ssl`.

## Writing your own backend

Implement the `arterial_observability` behaviour: a single callback,

```erlang
-callback event(EventNameSuffix :: [atom()], Measurements :: map(), Metadata :: map()) -> ok.
```

`EventNameSuffix` is always `[arterial | _]`-prefixed (e.g.
`[arterial, call, stop]`, `[arterial, disconnect]`) -- see this module's
"Events" section below for the full catalog this facade emits, including
which `Measurements`/`Metadata` keys come with each. Set
`{observability, my_app_metrics}` and arterial calls
`my_app_metrics:event/3` directly; no registration step needed.

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

-callback event(EventNameSuffix :: [atom()], Measurements :: map(), Metadata :: map()) -> ok.

-export([span/3, event/2, event/3]).

-doc """
Runs `Fun/0` (expected to return `{Result, StopMetadata}` or `{Result,
ExtraMeasurements, StopMetadata}`, exactly like `telemetry:span/3`),
emitting `[arterial | EventNameSuffix] ++ [start]` before and
`[arterial | EventNameSuffix] ++ [stop]` after (or `++ [exception]` if
`Fun` raises, which is then re-raised unchanged) through the configured
backend.

`EventNameSuffix` is a list, e.g. `[call]` or `[checkout]` -- see this
module's moduledoc for the full event catalog.
""".
-spec span([atom()], map(), fun(() -> Result)) -> Result
  when Result :: {term(), map()} | {term(), map(), map()}.
span(EventNameSuffix, StartMetadata, Fun) when is_list(EventNameSuffix), is_function(Fun, 0) ->
  Backend = backend(),
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
  end.

-doc "Equivalent to `event/3` with `Measurements = #{}`.".
-spec event([atom()], map()) -> ok.
event(EventNameSuffix, Metadata) ->
  event(EventNameSuffix, #{}, Metadata).

-doc """
Emits `[arterial | EventNameSuffix]` with the given `Measurements`/
`Metadata` through the configured backend. Use for one-shot events that
aren't a start/stop pair (e.g. `[disconnect]`, `[sweep, stop]`).
""".
-spec event([atom()], map(), map()) -> ok.
event(EventNameSuffix, Measurements, Metadata) when is_list(EventNameSuffix) ->
  (backend()):event([arterial | EventNameSuffix], Measurements, Metadata).

%% Cached backend module.
backend() ->
  case persistent_term:get(?MODULE, nil) of
    nil ->
      Module =
        case application:get_env(arterial, observability, telemetry) of
          prometheus -> arterial_observability_prometheus;
          telemetry  -> arterial_observability_telemetry;
          Mod when is_atom(Mod) -> Mod
        end,
      persistent_term:put(?MODULE, Module),
      Module;
    Module ->
      Module
  end.

