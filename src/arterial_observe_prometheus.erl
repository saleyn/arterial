-module(arterial_observe_prometheus).

-moduledoc """
`arterial_observe` backend that records straight into Prometheus
metrics -- no dependency on the `telemetry` library at all, only on
`prometheus` itself.

Selected via `{arterial, [{observability, prometheus}]}` (or
`{prometheus, Opts}`, where `Opts` is the map `start/1` takes -- see
below). `arterial_app`'s top-level supervisor calls `start/1` once at
boot (after configuring whatever exporter you use, e.g.
`prometheus_httpd`/`prometheus_cowboy2`) and `stop/0` on shutdown (see
`m:arterial_observe`'s moduledoc for the full lifecycle contract).
`start/1` both starts the `prometheus` application (if it isn't running
already, the same lazy pattern `arterial_socket` uses for `ssl`) and
declares this module's metrics -- do this before traffic starts flowing,
or the first few observations may race metric registration. `stop/0`
only stops the `prometheus` application again if this module is the one
that started it (tracked via `ensure_started/0`'s `internal`/`external`
bookkeeping, so a host application that already runs `prometheus` for
its own purposes never has it pulled out from under it when `arterial`
stops).

## Metrics

* `arterial_call_duration_seconds` (histogram; labels `pool`, `result`)
* `arterial_cast_duration_seconds` (histogram; labels `pool`, `result`)
* `arterial_checkout_duration_seconds` (histogram; labels `pool`, `mode`, `outcome`)
* `arterial_connect_duration_seconds` (histogram; labels `pool`, `result`)
* `arterial_disconnects_total` (counter; labels `pool`, `reason`)
* `arterial_timeouts_total` (counter; labels `pool`, `conn_id`)
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
2> arterial_observe_prometheus:start().
ok
3> arterial_client:call(my_pool, my_request, 5000).
{ok, my_response}
4> prometheus_text_format:format().
%% ... arterial_call_duration_seconds_bucket{pool="my_pool",result="ok",le="0.005"} 1
%% ...
```
""".

-behaviour(arterial_observe).

-export([start/0, start/1, stop/0, event/3]).

-doc """
Equivalent to `start/1` with default histogram buckets. Mainly useful for
calling this module manually (a shell session, a test) outside the
`arterial_observe` supervised lifecycle, which always calls `start/1`.
""".
-spec start() -> ok.
start() -> start(#{}).

-doc """
The `arterial_observe` behaviour's `start/1` callback: starts the
`prometheus` application (see `ensure_started/0`) and declares this
module's Prometheus metrics. Idempotent -- calling it again just
re-declares the same metrics (a no-op per `prometheus_metric`'s own
`declare/1` semantics).

`Opts`:

* `buckets => prometheus_histogram:buckets()` -- override the default
  histogram bucket boundaries for every histogram this module declares.
""".
-spec start(#{buckets => [number(), ...]}) -> ok.
start(Opts) when is_map(Opts) ->
  ensure_started(),

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
  declare_counter(arterial_timeouts_total,
    "Total number of in-flight requests timed out by a connection's arterial_conn_owner.",
    [pool, conn_id]),
  declare_counter(arterial_exceptions_total,
    "Total number of arterial spans that raised an exception.",
    [pool, span]),
  ok.

-doc """
Stops the `prometheus` application, but only if `start/1` was the one
that started it (a host application that already had `prometheus`
running for its own purposes keeps it running). Does not un-declare the
metrics `start/1` registered -- Prometheus has no API for that; they
simply stop receiving observations.
""".
-spec stop() -> ok.
stop() ->
  case ensure_started() of
    internal -> application:stop(prometheus);
    _        -> ok
  end,
  persistent_term:erase(?MODULE),
  ok.

-doc "Records the event into this module's Prometheus metrics; see the moduledoc's Metrics section for the event-to-metric mapping.".
-spec event([atom()], map(), map()) -> ok.
event(EventName, Measurements, Metadata) ->
  handle_event(EventName, Measurements, Metadata).

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

ensure_started() ->
  case persistent_term:get(?MODULE, nil) of
    nil ->
      {ok, Apps} = application:ensure_all_started(prometheus),
      Status = case lists:member(prometheus, Apps) of
        true  -> internal;
        false -> external
      end,
      persistent_term:put(?MODULE, Status),
      Status;
    Cached ->
      Cached
  end.

declare_histogram(Name, Help, Labels, ExtraOpts) ->
  _ = prometheus_histogram:declare([{name, Name}, {help, Help}, {labels, Labels} | ExtraOpts]),
  ok.

declare_counter(Name, Help, Labels) ->
  _ = prometheus_counter:declare([{name, Name}, {help, Help}, {labels, Labels}]),
  ok.

%% Prometheus convention: durations are seconds (floats), not native time
%% units -- convert once here so every histogram is in the same unit
%% regardless of what arterial_observe:span/3 measured internally.
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

handle_event([arterial, timeout], _Measurements, #{pool := Pool, conn_id := ConnID}) ->
  prometheus_counter:inc(arterial_timeouts_total, [Pool, ConnID]);

%% start/start events: this backend only records durations on stop/exception.
handle_event([arterial, _, start], _Measurements, _Metadata) ->
  ok.
