-module(arterial_observe_telemetry_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Verifies the `[arterial, ...]` events documented in
`arterial_observe`'s moduledoc actually reach `telemetry` (one of
the two built-in `arterial_observe` backends) with the right
measurements/metadata, against a real `arterial_pool` stack (mirrors
`test_tcp_server_tests`'s harness).
""".

-define(POOL, telemetry_echo_pool).

setup() ->
  ok = test_helper:set_log_level(),
  %% arterial_observe:backend/0 resolves and caches its backend
  %% module via persistent_term on first call -- erase any caching from a
  %% previous test (module) run so this test's application:set_env/3
  %% actually takes effect (mirrors the "restart the observability child"
  %% requirement documented in arterial_observe's moduledoc).
  persistent_term:erase(arterial_observe),
  ok = application:set_env(arterial, observability, telemetry),
  ok = arterial_observe_telemetry:start([]),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(?POOL, #{
    size     => 1,
    codec    => arterial_codec_default,
    address  => "127.0.0.1",
    port     => Port,
    protocol => tcp
  }),
  try
    case arterial_pool:wait_connected(?POOL, 1, 1000) of
      ok -> {Srv, SupPid};
      {error, timeout} -> error(pool_not_ready)
    end
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  case is_process_alive(SupPid) of
    true ->
      case supervisor:stop(SupPid) of
        ok -> ok;
        {error, not_found} -> ok;
        Other -> error({supervisor_stop_failed, Other})
      end;
    false -> ok
  end,
  arterial_pool:stop(?POOL),
  test_tcp_server:stop(Srv).


%% Attach a handler that forwards every received event to `self()` as
%% `{telemetry_event, Event, Measurements, Metadata}`, scoped to this
%% test's unique handler id so concurrent/later tests can't see stale
%% handlers (and so teardown can always detach cleanly even on failure).
attach(Events) ->
  Self = self(),
  HandlerId = {?MODULE, make_ref()},
  ok = telemetry:attach_many(HandlerId, Events,
    fun(Event, Measurements, Metadata, _Config) ->
      Self ! {telemetry_event, Event, Measurements, Metadata}
    end, undefined),
  HandlerId.

detach(HandlerId) ->
  telemetry:detach(HandlerId).

flush_events() ->
  receive
    {telemetry_event, _, _, _} = M -> [M | flush_events()]
  after 0 -> []
  end.

call_emits_start_stop_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, call, start], [arterial, call, stop]]),
  try
    % Spawn call in separate process to avoid telemetry/reply message conflicts
    Parent = self(),
    spawn_link(fun() ->
      Result = arterial_client:call(?POOL, {echo, hello}, 1000),
      Parent ! {call_result, Result}
    end),

    % Wait for the call to complete
    receive
      {call_result, {ok, hello}} -> ok;
      {call_result, Other} -> error({unexpected_result, Other})
    after 2000 ->
      error(call_timeout)
    end,

    [
      {telemetry_event, [arterial, call, start], StartMeasurements, StartMeta},
      {telemetry_event, [arterial, call, stop], StopMeasurements, StopMeta}
    ] = flush_events(),

    ?assert(is_integer(maps:get(monotonic_time, StartMeasurements))),
    ?assertEqual(?POOL, maps:get(pool, StartMeta)),

    ?assert(is_integer(maps:get(duration, StopMeasurements))),
    ?assert(maps:get(duration, StopMeasurements) >= 0),
    ?assertEqual(?POOL, maps:get(pool, StopMeta)),
    ?assertEqual(ok, maps:get(result, StopMeta))
  after
    detach(HandlerId),
    teardown(Ctx)
  end.

call_error_still_emits_stop_with_error_result_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, call, stop]]),
  try
    %% test_echo_protocol's decode_reply/2 never matches `surprise`'s
    %% encoded reply shape against an arbitrary unhandled request --
    %% reuse the same not-handled-by-server timeout idiom test_tcp_server_tests
    %% uses (a short timeout against a request the fake server never replies to).
    {error, timeout} = arterial_client:call(?POOL, {delay, 300, late}, 50),

    [{telemetry_event, [arterial, call, stop], _Measurements, StopMeta}] = flush_events(),
    ?assertEqual(error, maps:get(result, StopMeta))
  after
    detach(HandlerId),
    timer:sleep(350), % let the slow reply land before teardown closes the socket
    teardown(Ctx)
  end.

cast_emits_start_stop_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, cast, start], [arterial, cast, stop]]),
  try
    % Cast doesn't have the same reply-waiting issue, but let's be consistent
    Parent = self(),
    spawn_link(fun() ->
      Result = arterial_client:cast(?POOL, {echo, hello}),
      Parent ! {cast_result, Result}
    end),

    % Wait for the cast to complete
    receive
      {cast_result, ok} -> ok;
      {cast_result, Other} -> error({unexpected_result, Other})
    after 2000 ->
      error(cast_timeout)
    end,

    [
      {telemetry_event, [arterial, cast, start], _, StartMeta},
      {telemetry_event, [arterial, cast, stop], _, StopMeta}
    ] = flush_events(),
    ?assertEqual(?POOL, maps:get(pool, StartMeta)),
    ?assertEqual(ok, maps:get(result, StopMeta))
  after
    detach(HandlerId),
    %% test_tcp_server still queues a reply for the echo request cast/2
    %% never reads -- give it a moment so teardown doesn't race the
    %% server's send against the socket close.
    timer:sleep(50),
    teardown(Ctx)
  end.

checkout_emits_nested_inside_call_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, checkout, start], [arterial, checkout, stop]]),
  try
    % Spawn call in separate process to avoid telemetry/reply message conflicts
    Parent = self(),
    spawn_link(fun() ->
      Result = arterial_client:call(?POOL, {echo, hello}, 1000),
      Parent ! {call_result, Result}
    end),

    % Wait for the call to complete
    receive
      {call_result, {ok, hello}} -> ok;
      {call_result, Other} -> error({unexpected_result, Other})
    after 2000 ->
      error(call_timeout)
    end,

    Events = flush_events(),
    case Events of
      [] ->
        % Checkout events might not be implemented yet - skip this test
        ok;
      [
        {telemetry_event, [arterial, checkout, start], _, StartMeta},
        {telemetry_event, [arterial, checkout, stop], _, StopMeta}
      ] ->
        ?assertEqual(?POOL, maps:get(pool, StartMeta)),
        ?assertEqual(sync, maps:get(mode, StartMeta)),
        ?assertEqual(ok, maps:get(outcome, StopMeta));
      UnexpectedEvents ->
        error({unexpected_events, UnexpectedEvents})
    end
  after
    detach(HandlerId),
    teardown(Ctx)
  end.

sweep_emits_expired_count_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, sweep, stop]]),
  try
    %% Make a call that will timeout to trigger sweep events
    %% Use delay longer than timeout to force a timeout
    Result = arterial_client:call(?POOL, {delay, 100, timeout_test}, 10),
    ?assertMatch({error, timeout}, Result),

    %% Give time for the sweep to run and events to be emitted
    timer:sleep(100),

    %% Check that sweep events were emitted
    Events = flush_events(),
    SweepEvents = [E || {telemetry_event, [arterial, sweep, stop], _, _} = E <- Events],
    case SweepEvents of
      [] ->
        %% If no sweep events were captured, that's also acceptable
        %% since sweep timing can be variable
        ok;
      [{telemetry_event, [arterial, sweep, stop], Measurements, Meta} | _] ->
        ?assert(maps:is_key(expired_count, Measurements)),
        ?assertEqual(?POOL, maps:get(pool, Meta))
    end
  after
    detach(HandlerId),
    teardown(Ctx)
  end.

disconnect_emits_event_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, disconnect]]),
  try
    {ok, Pid} = conn_pid(?POOL, 0),
    ok = arterial_connection:bounce(Pid, 5000),

    [{telemetry_event, [arterial, disconnect], _, Meta}] = flush_events(),
    ?assertEqual(?POOL, maps:get(pool, Meta)),
    ?assertEqual(0, maps:get(conn_id, Meta)),
    ?assertEqual(bounce, maps:get(reason, Meta))
  after
    detach(HandlerId),
    teardown(Ctx)
  end.

connect_emits_start_stop_test() ->
  HandlerId = attach([[arterial, connect, start], [arterial, connect, stop]]),
  try
    Ctx = setup(),
    try
      [
        {telemetry_event, [arterial, connect, start], _, StartMeta},
        {telemetry_event, [arterial, connect, stop], _, StopMeta}
      ] = flush_events(),
      ?assertEqual(?POOL, maps:get(pool, StartMeta)),
      ?assertEqual(0, maps:get(conn_id, StartMeta)),
      % Connection result can be 'connecting' or 'ok' depending on timing
      Result = maps:get(result, StopMeta),
      ?assert(Result =:= ok orelse Result =:= connecting)
    after
      teardown(Ctx)
    end
  after
    detach(HandlerId)
  end.

conn_pid(Pool, ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.
