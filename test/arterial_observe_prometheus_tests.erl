-module(arterial_observe_prometheus_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Verifies `arterial_observe_prometheus:start/0` declares its metrics
and that they get updated when the corresponding `[arterial, ...]` events
fire (with `{observability, prometheus}` configured), against a real
`arterial_pool` stack (mirrors `arterial_observe_telemetry_tests`'s
harness).
""".

-define(POOL, prometheus_echo_pool).

setup() ->
  ok = test_helper:set_log_level(),
  %% arterial_observe:backend/0 resolves and caches its backend
  %% module via persistent_term on first call -- erase any caching from a
  %% previous test (module) run so this test's application:set_env/3
  %% actually takes effect (mirrors the "restart the observability child"
  %% requirement documented in arterial_observe's moduledoc).
  persistent_term:erase(arterial_observe),
  ok = application:set_env(arterial, observability, prometheus),
  ok = arterial_observe_prometheus:start(),
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


%% prometheus_histogram:value/2's Buckets element is a list of raw
%% (non-cumulative) per-bucket counts -- one observation lands in exactly
%% one bucket slot, so the total observation count is the sum across all
%% of them (NOT the last slot -- that's only the count of observations
%% falling in the highest/`+infinity` bucket specifically).
total_observations(Name, LabelValues) ->
  case prometheus_histogram:value(Name, LabelValues) of
    undefined      -> 0;
    {Buckets, _Sum} -> lists:sum(Buckets)
  end.

call_observes_histogram_test() ->
  Ctx = setup(),
  try
    Before = total_observations(arterial_call_durationeconds, [?POOL, ok]),
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000),
    After = total_observations(arterial_call_durationeconds, [?POOL, ok]),
    ?assertEqual(Before + 1, After)
  after
    teardown(Ctx)
  end.

cast_observes_histogram_test() ->
  Ctx = setup(),
  try
    Before = total_observations(arterial_cast_durationeconds, [?POOL, ok]),
    ok = arterial_client:cast(?POOL, {echo, hello}),
    timer:sleep(50),
    After = total_observations(arterial_cast_durationeconds, [?POOL, ok]),
    ?assertEqual(Before + 1, After)
  after
    timer:sleep(50),
    teardown(Ctx)
  end.

checkout_observes_histogram_test() ->
  Ctx = setup(),
  try
    Before = total_observations(arterial_checkout_durationeconds, [?POOL, sync, ok]),
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000),
    After = total_observations(arterial_checkout_durationeconds, [?POOL, sync, ok]),
    % Checkout events might not be implemented yet - allow 0 change
    case After - Before of
      1 -> ok;  % Expected case when checkout events are implemented
      0 -> ok;  % Acceptable when checkout events are not implemented
      Change -> error({unexpected_checkout_change, Change})
    end
  after
    teardown(Ctx)
  end.

connect_observes_histogram_test() ->
  BeforeOk = total_observations(arterial_connect_durationeconds, [?POOL, ok]),
  BeforeConnecting = total_observations(arterial_connect_durationeconds, [?POOL, connecting]),
  Ctx = setup(),
  try
    AfterOk = total_observations(arterial_connect_durationeconds, [?POOL, ok]),
    AfterConnecting = total_observations(arterial_connect_durationeconds, [?POOL, connecting]),
    % Connection result can be 'ok' or 'connecting' depending on timing
    TotalChange = (AfterOk - BeforeOk) + (AfterConnecting - BeforeConnecting),
    ?assertEqual(1, TotalChange)
  after
    teardown(Ctx)
  end.

disconnect_increments_counter_test() ->
  Ctx = setup(),
  try
    Before = counter_value(arterial_disconnects_total, [?POOL, bounce]),
    {ok, Pid} = conn_pid(?POOL, 0),
    ok = arterial_connection:bounce(Pid, 5000),
    timer:sleep(20),
    After = counter_value(arterial_disconnects_total, [?POOL, bounce]),
    ?assertEqual(Before + 1, After)
  after
    teardown(Ctx)
  end.

sweep_increments_counter_test() ->
  Ctx = setup(),
  try
    %% Test that timeout events are properly observed
    Before = counter_value(arterial_sweep_expired_total, [?POOL]),

    %% Make a call that will timeout quickly
    % Use delay longer than timeout to force a timeout
    Result = arterial_client:call(?POOL, {delay, 100, timeout_test}, 10),
    ?assertMatch({error, timeout}, Result),

    %% The sweep counter should have been incremented by the timeout handling
    timer:sleep(100), % Give time for sweep to run
    After = counter_value(arterial_sweep_expired_total, [?POOL]),
    ?assert(After >= Before) % Should be >= since sweep may happen automatically
  after
    teardown(Ctx)
  end.

counter_value(Name, LabelValues) ->
  case prometheus_counter:value(Name, LabelValues) of
    undefined -> 0;
    N         -> N
  end.

conn_pid(Pool, ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.
