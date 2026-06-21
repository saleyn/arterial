-module(arterial_observability_telemetry).

-moduledoc """
Default `arterial_observability` backend: forwards every event to
`telemetry:execute/3` unchanged, so any standard `telemetry:attach/4`
handler (writing to Prometheus, OpenTelemetry, a custom counter, plain
logging, etc.) works against arterial exactly as it would for any other
`telemetry`-instrumented library.

Selected via `{arterial, [{observability, telemetry}]}` (the default --
no config needed). Lazily starts the `telemetry` application on first
use, the same way `arterial_socket` lazily starts `ssl` only when a
connection actually uses the `ssl` transport.

See `m:arterial_observability` for the full `[arterial | _]` event
catalog this module receives and forwards.
""".

-behaviour(arterial_observability).

-export([event/3]).

-doc false.
-spec event([atom()], map(), map()) -> ok.
event(EventName, Measurements, Metadata) ->
  ensure_started(),
  telemetry:execute(EventName, Measurements, Metadata).

%% `telemetry`'s handler lookup relies on an ETS table created when the
%% `telemetry` application starts; a caller that uses arterial_pool/
%% arterial_nif directly (most eunit tests, and any app that never calls
%% arterial:start/0 or boots `arterial` as a supervised OTP application)
%% never triggers that, so every emit would otherwise silently no-op
%% (with a logged warning) instead of just being a no-op because no
%% handler is attached. persistent_term avoids paying this check's cost
%% (an ETS lookup, since application:ensure_all_started/1 is itself
%% cheap-but-not-free once already started) on every single emit.
ensure_started() ->
  case persistent_term:get(?MODULE, false) of
    true  -> ok;
    false ->
      application:ensure_all_started(telemetry),
      persistent_term:put(?MODULE, true)
  end.
