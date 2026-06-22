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

%% prometheus_histogram:value/2's Buckets element is a list of raw
%% (non-cumulative) per-bucket counts -- one observation lands in exactly
%% one bucket slot, so the total observation count is the sum across all
%% of them (NOT the last slot -- that's only the count of observations
%% falling in the highest/`+infinity` bucket specifically).
%%
%% Called through erlang:apply/3 rather than directly: prometheus_histogram's
%% own -spec for value/2 claims {number(), ...} (matching its sibling
%% counter-style metrics), but its actual implementation
%% (reduce_buckets_counters/1) returns {[number()], number()} -- a spec
%% bug in the dependency itself, confirmed by reading its source, not a
%% bug here. Dialyzer's success typing trusts that wrong spec over the
%% real code, which would otherwise flag this clause (and propagate "no
%% local return" into every caller) as unreachable; apply/3 is opaque to
%% success typing, so it sees this call's result as term() instead.
total_observations(Name, LabelValues) ->
  case erlang:apply(prometheus_histogram, value, [Name, LabelValues]) of
    undefined       -> 0;
    {Buckets, _Sum} -> lists:sum(Buckets)
  end.

call_observes_histogram_test() ->
  Ctx = setup(),
  try
    Before = total_observations(arterial_call_duration_seconds, [?POOL, ok]),
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000),
    After = total_observations(arterial_call_duration_seconds, [?POOL, ok]),
    ?assertEqual(Before + 1, After)
  after
    teardown(Ctx)
  end.

cast_observes_histogram_test() ->
  Ctx = setup(),
  try
    Before = total_observations(arterial_cast_duration_seconds, [?POOL, ok]),
    ok = arterial_client:cast(?POOL, {echo, hello}),
    timer:sleep(50),
    After = total_observations(arterial_cast_duration_seconds, [?POOL, ok]),
    ?assertEqual(Before + 1, After)
  after
    timer:sleep(50),
    teardown(Ctx)
  end.

checkout_observes_histogram_test() ->
  Ctx = setup(),
  try
    Before = total_observations(arterial_checkout_duration_seconds, [?POOL, sync, ok]),
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000),
    After = total_observations(arterial_checkout_duration_seconds, [?POOL, sync, ok]),
    ?assertEqual(Before + 1, After)
  after
    teardown(Ctx)
  end.

connect_observes_histogram_test() ->
  Before = total_observations(arterial_connect_duration_seconds, [?POOL, ok]),
  Ctx = setup(),
  try
    After = total_observations(arterial_connect_duration_seconds, [?POOL, ok]),
    ?assertEqual(Before + 1, After)
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

%% A per-request timeout, armed and fired by the connection's
%% arterial_conn_owner (no more NIF-side sweep/track_inflight -- that
%% responsibility moved there entirely), increments arterial_timeouts_total.
timeout_increments_counter_test() ->
  Ctx = setup(),
  try
    Before = counter_value(arterial_timeouts_total, [?POOL, 0]),
    {error, timeout} = arterial_client:call(?POOL, {delay, 300, late}, 50),
    After = counter_value(arterial_timeouts_total, [?POOL, 0]),
    ?assertEqual(Before + 1, After)
  after
    timer:sleep(350), % let the slow reply land before teardown closes the socket
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
