-module(arterial_observability_prometheus).

-moduledoc """
`arterial_observability` backend that records straight into Prometheus
metrics -- no dependency on the `telemetry` library at all, only on
`prometheus` itself.

Selected via `{arterial, [{observability, prometheus}]}`. Call
`start/0,1` once at application boot (after configuring whatever exporter
you use, e.g. `prometheus_httpd`/`prometheus_cowboy2`) to declare this
module's metrics; `event/3` (and so every `arterial_observability:span/3`/
`event/2,3` call) lazily starts the `prometheus` application itself the
same way `arterial_socket` lazily starts `ssl`, but metric *declaration*
only happens in `start/0,1` -- call it before traffic starts flowing or
the first few observations may race metric registration.

## Metrics

* `arterial_call_duration_seconds` (histogram; labels `pool`, `result`)
* `arterial_cast_duration_seconds` (histogram; labels `pool`, `result`)
* `arterial_checkout_duration_seconds` (histogram; labels `pool`, `mode`, `outcome`)
* `arterial_connect_duration_seconds` (histogram; labels `pool`, `result`)
* `arterial_disconnects_total` (counter; labels `pool`, `reason`)
* `arterial_sweep_expired_total` (counter; labels `pool`)
* `arterial_exceptions_total` (counter; labels `pool`, `span`) -- incremented
  on any `[arterial, Span, exception]` event (`call`/`cast`/`checkout`/`connect`),
  `span` identifies which one.

All histograms use Prometheus's default bucket boundaries
(`prometheus_histogram`'s `default`); pass your own via `Opts`'s
`buckets` key (applied to every histogram declared here) if the defaults
don't fit your latency profile.

## Examples

```
1> application:set_env(arterial, observability, prometheus).
ok
2> arterial_observability_prometheus:start().
ok
3> arterial_client:call(my_pool, my_request, 5000).
{ok, my_response}
4> prometheus_text_format:format().
%% ... arterial_call_duration_seconds_bucket{pool="my_pool",result="ok",le="0.005"} 1
%% ...
```
""".

-behaviour(arterial_observability).

-export([start/0, start/1, event/3]).

-doc "Equivalent to `start/1` with default histogram buckets.".
-spec start() -> ok.
start() -> start(#{}).

-doc """
Declare this module's Prometheus metrics. Idempotent -- calling it again
just re-declares the same metrics (a no-op per `prometheus_metric`'s own
`declare/1` semantics).

`Opts`:

* `buckets => prometheus_histogram:buckets()` -- override the default
  histogram bucket boundaries for every histogram this module declares.
""".
-spec start(#{buckets => [number(), ...]}) -> ok.
start(Opts) when is_map(Opts) ->
  ok = ensure_started(),

  Buckets = maps:get(buckets, Opts, default),
  HistogramOpts = case Buckets of
    default -> [];
    _       -> [{buckets, Buckets}]
  end,

  declare_histogram(arterial_call_duration_seconds,
    "Duration of arterial_client:call/3 calls, in seconds.",
    [pool, result], HistogramOpts),
  declare_histogram(arterial_cast_duration_seconds,
    "Duration of arterial_client:cast/2 calls, in seconds.",
    [pool, result], HistogramOpts),
  declare_histogram(arterial_checkout_duration_seconds,
    "Duration of arterial connection checkouts, in seconds.",
    [pool, mode, outcome], HistogramOpts),
  declare_histogram(arterial_connect_duration_seconds,
    "Duration of arterial_connection's outbound connect attempts, in seconds.",
    [pool, result], HistogramOpts),

  declare_counter(arterial_disconnects_total,
    "Total number of arterial connection disconnects.",
    [pool, reason]),
  declare_counter(arterial_sweep_expired_total,
    "Total number of in-flight requests evicted by arterial_nif:sweep_timeouts/1.",
    [pool]),
  declare_counter(arterial_exceptions_total,
    "Total number of arterial spans that raised an exception.",
    [pool, span]),
  ok.

-doc false.
-spec event([atom()], map(), map()) -> ok.
event(EventName, Measurements, Metadata) ->
  ok = ensure_started(),
  handle_event(EventName, Measurements, Metadata).

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

ensure_started() ->
  case persistent_term:get(?MODULE, false) of
    true  -> ok;
    false ->
      {ok, _} = application:ensure_all_started(prometheus),
      persistent_term:put(?MODULE, true),
      ok
  end.

declare_histogram(Name, Help, Labels, ExtraOpts) ->
  _ = prometheus_histogram:declare([{name, Name}, {help, Help}, {labels, Labels} | ExtraOpts]),
  ok.

declare_counter(Name, Help, Labels) ->
  _ = prometheus_counter:declare([{name, Name}, {help, Help}, {labels, Labels}]),
  ok.

%% Prometheus convention: durations are seconds (floats), not native time
%% units -- convert once here so every histogram is in the same unit
%% regardless of what arterial_observability:span/3 measured internally.
seconds(DurationNative) ->
  erlang:convert_time_unit(DurationNative, native, microsecond) / 1_000_000.

handle_event([arterial, call, stop], #{duration := D}, #{pool := Pool, result := Result}) ->
  prometheus_histogram:observe(arterial_call_duration_seconds, [Pool, Result], seconds(D));
handle_event([arterial, call, exception], _Measurements, #{pool := Pool}) ->
  prometheus_counter:inc(arterial_exceptions_total, [Pool, call]);

handle_event([arterial, cast, stop], #{duration := D}, #{pool := Pool, result := Result}) ->
  prometheus_histogram:observe(arterial_cast_duration_seconds, [Pool, Result], seconds(D));
handle_event([arterial, cast, exception], _Measurements, #{pool := Pool}) ->
  prometheus_counter:inc(arterial_exceptions_total, [Pool, cast]);

handle_event([arterial, checkout, stop], #{duration := D},
             #{pool := Pool, mode := Mode, outcome := Outcome}) ->
  prometheus_histogram:observe(arterial_checkout_duration_seconds, [Pool, Mode, Outcome], seconds(D));
handle_event([arterial, checkout, exception], _Measurements, #{pool := Pool}) ->
  prometheus_counter:inc(arterial_exceptions_total, [Pool, checkout]);

handle_event([arterial, connect, stop], #{duration := D}, #{pool := Pool, result := Result}) ->
  prometheus_histogram:observe(arterial_connect_duration_seconds, [Pool, Result], seconds(D));
handle_event([arterial, connect, exception], _Measurements, #{pool := Pool}) ->
  prometheus_counter:inc(arterial_exceptions_total, [Pool, connect]);

handle_event([arterial, disconnect], _Measurements, #{pool := Pool, reason := Reason}) ->
  prometheus_counter:inc(arterial_disconnects_total, [Pool, Reason]);

handle_event([arterial, sweep, stop], #{expired_count := Count}, #{pool := Pool}) ->
  prometheus_counter:inc(arterial_sweep_expired_total, [Pool], Count);

%% start/start events: this backend only records durations on stop/exception.
handle_event([arterial, _, start], _Measurements, _Metadata) ->
  ok.
