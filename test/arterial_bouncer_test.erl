-module(arterial_bouncer_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Simple test to verify arterial_bouncer functionality without requiring
a full TCP server setup. Tests the integration, configuration, and
basic behavior of the bouncer component.
""".

%% Test that bouncer is started when bounce_interval_ms is configured
bouncer_integration_test() ->
  ok = test_helper:set_log_level(),

  % Start a pool with bouncer configuration
  {ok, _SupPid} = arterial_pool:start_link(test_bouncer_pool, #{
    size => 2,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 12345,  % Non-existent port, connections will fail but that's ok for this test
    protocol => tcp,
    bounce_interval_ms => 1000,  % Bounce every second
    bounce_drain_timeout_ms => 500
  }),

  try
    % Check that the bouncer child process was started
    Children = supervisor:which_children(arterial_pool:sup_name(test_bouncer_pool)),
    BouncerChild = lists:keyfind(arterial_bouncer, 1, Children),

    % Verify bouncer is running
    ?assertMatch({arterial_bouncer, Pid, _, _} when is_pid(Pid), BouncerChild),
    {_, BouncerPid, _, _} = BouncerChild,
    ?assert(is_process_alive(BouncerPid)),

    % Verify connection workers are started
    ConnChild0 = lists:keyfind({arterial_connection, 0}, 1, Children),
    ConnChild1 = lists:keyfind({arterial_connection, 1}, 1, Children),
    ?assertMatch({{arterial_connection, 0}, Pid0, _, _} when is_pid(Pid0), ConnChild0),
    ?assertMatch({{arterial_connection, 1}, Pid1, _, _} when is_pid(Pid1), ConnChild1),

    ok
  after
    arterial_pool:stop(test_bouncer_pool)
  end.

%% Test that bouncer is NOT started when bounce_interval_ms is undefined
no_bouncer_test() ->
  ok = test_helper:set_log_level(),

  % Start a pool without bouncer configuration
  {ok, _SupPid} = arterial_pool:start_link(test_no_bouncer_pool, #{
    size => 1,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 12346,
    protocol => tcp
    % bounce_interval_ms is undefined by default
  }),

  try
    % Check that no bouncer child process was started
    Children = supervisor:which_children(arterial_pool:sup_name(test_no_bouncer_pool)),
    BouncerChild = lists:keyfind(arterial_bouncer, 1, Children),

    % Verify bouncer is NOT running
    ?assertEqual(false, BouncerChild),

    % But connection worker should still be there
    ConnChild0 = lists:keyfind({arterial_connection, 0}, 1, Children),
    ?assertMatch({{arterial_connection, 0}, Pid0, _, _} when is_pid(Pid0), ConnChild0),

    ok
  after
    arterial_pool:stop(test_no_bouncer_pool)
  end.

%% Test bounce function directly (the connections will fail to connect,
%% but bounce should handle the case gracefully)
direct_bounce_test() ->
  ok = test_helper:set_log_level(),

  {ok, _SupPid} = arterial_pool:start_link(test_direct_bounce_pool, #{
    size => 1,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 12347,
    protocol => tcp
  }),

  try
    % Get the connection worker PID
    Children = supervisor:which_children(arterial_pool:sup_name(test_direct_bounce_pool)),
    {{arterial_connection, 0}, ConnPid, _, _} = lists:keyfind({arterial_connection, 0}, 1, Children),

    % Call bounce directly (should succeed even though connection is not established)
    % The bounce will try to drain but since there's no active connection,
    % it should return quickly
    Result = arterial_connection:bounce(ConnPid, 100),

    % Should succeed or timeout, but not crash
    ?assert(Result =:= ok orelse Result =:= {error, timeout}),

    % Process should still be alive after bounce
    ?assert(is_process_alive(ConnPid)),

    ok
  after
    arterial_pool:stop(test_direct_bounce_pool)
  end.

%% Test that throttling can be configured and works at the NIF level
throttling_configuration_test() ->
  ok = test_helper:set_log_level(),

  % Start a pool with throttling configuration
  {ok, _SupPid} = arterial_pool:start_link(test_throttle_pool, #{
    size => 1,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 12348,
    protocol => tcp,
    throttle => #{rate_per_sec => 10}
  }),

  try
    % Verify throttling state is stored
    ThrottleState = arterial_pool:throttle(test_throttle_pool),
    ?assertMatch({10, 1000}, ThrottleState),

    % The throttling itself will be tested at the NIF level during
    % send_and_release calls, but we can verify the configuration is correct
    ok
  after
    arterial_pool:stop(test_throttle_pool)
  end.