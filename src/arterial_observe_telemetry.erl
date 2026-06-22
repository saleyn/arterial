-module(arterial_observe_telemetry).

-moduledoc """
Built-in `arterial_observe` backend: forwards every event to
`telemetry:execute/3` unchanged, so any standard `telemetry:attach/4`
handler (writing to Prometheus, OpenTelemetry, a custom counter, plain
logging, etc.) works against arterial exactly as it would for any other
`telemetry`-instrumented library.

Selected via `{arterial, [{observability, telemetry}]}` (or
`{telemetry, Opts}` -- `Opts` is accepted but currently unused; reserved
for future per-backend configuration). `arterial_app`'s top-level
supervisor calls `start/1` once at boot and `stop/0` on shutdown (see
`m:arterial_observe`'s moduledoc for the full lifecycle contract);
`start/1` starts the `telemetry` application (if it isn't running
already), the same lazy pattern `arterial_socket` uses for `ssl`, and
`stop/0` only stops it again if this module is the one that started it
(tracked via `ensure_started/0`'s `internal`/`external` bookkeeping, so a
host application that already runs `telemetry` for its own purposes
never has it pulled out from under it when `arterial` stops).

See `m:arterial_observe` for the full `[arterial | _]` event
catalog this module receives and forwards.
""".

-behaviour(arterial_observe).

-export([start/1, stop/0, event/3]).

-doc """
Starts the `telemetry` application if it isn't already running.
Idempotent. `Opts` is accepted for symmetry with other backends but
currently unused.
""".
-spec start(any()) -> ok.
start(_Opts) ->
  _ = ensure_started(),
  ok.

-doc """
Stops the `telemetry` application, but only if `start/1` was the one
that started it (a host application that already had `telemetry` running
for its own purposes keeps it running).
""".
-spec stop() -> ok.
stop() ->
  case ensure_started() of
    internal -> application:stop(telemetry);
    _        -> ok
  end,
  persistent_term:erase(?MODULE),
  ok.

-doc "Forwards the event to `telemetry:execute/3` unchanged.".
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
%% cheap-but-not-free once already started) on every single emit. The
%% cached value also doubles as start/1 vs. stop/0's internal/external
%% bookkeeping: `internal` means this module's ensure_all_started/1 call
%% actually started telemetry (so stop/0 should stop it again);
%% `external` means it was already running (so stop/0 must leave it alone).
ensure_started() ->
  case persistent_term:get(?MODULE, nil) of
    nil ->
      {ok, Apps} = application:ensure_all_started(telemetry),
      Status = case lists:member(telemetry, Apps) of
        true  -> internal;
        false -> external
      end,
      persistent_term:put(?MODULE, Status),
      Status;
    Cached ->
      Cached
  end.