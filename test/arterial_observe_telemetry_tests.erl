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
    size        => 1,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{address => "127.0.0.1", port => Port, protocol => tcp}
  }),
  try
    wait_until_available(?POOL, 1, 50),
    {Srv, SupPid}
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  ok = supervisor:stop(SupPid),
  ok = arterial_nif:destroy(?POOL),
  test_tcp_server:stop(Srv).

wait_until_available(_Pool, 0, _Retries) ->
  ok;
wait_until_available(Pool, N, Retries) ->
  case arterial_nif:checkout_connection(Pool, sync) of
    {ok, ConnID} ->
      ok = arterial_nif:checkin_connection(Pool, ConnID),
      wait_until_available(Pool, N - 1, Retries);
    {error, no_connection} when Retries > 0 ->
      timer:sleep(20),
      wait_until_available(Pool, N, Retries - 1);
    {error, no_connection} ->
      error(pool_not_ready)
  end.

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
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000),

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
    ok = arterial_client:cast(?POOL, {echo, hello}),

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
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000),

    [
      {telemetry_event, [arterial, checkout, start], _, StartMeta},
      {telemetry_event, [arterial, checkout, stop], _, StopMeta}
    ] = flush_events(),
    ?assertEqual(?POOL, maps:get(pool, StartMeta)),
    ?assertEqual(sync, maps:get(mode, StartMeta)),
    ?assertEqual(ok, maps:get(outcome, StopMeta))
  after
    detach(HandlerId),
    teardown(Ctx)
  end.

%% A per-request timeout, armed and fired by the connection's
%% arterial_conn_owner (no more NIF-side sweep/track_inflight -- that
%% responsibility moved there entirely), emits [arterial, timeout].
timeout_emits_event_test() ->
  Ctx = setup(),
  HandlerId = attach([[arterial, timeout]]),
  try
    {error, timeout} = arterial_client:call(?POOL, {delay, 300, late}, 50),

    [{telemetry_event, [arterial, timeout], _Measurements, Meta}] = flush_events(),
    ?assertEqual(?POOL, maps:get(pool, Meta)),
    ?assertEqual(0, maps:get(conn_id, Meta))
  after
    detach(HandlerId),
    timer:sleep(350), % let the slow reply land before teardown closes the socket
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
      ?assertEqual(ok, maps:get(result, StopMeta))
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
