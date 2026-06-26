-module(arterial_throttle_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Simple test to verify C++ NIF-based throttling functionality.
Tests that throttling can be configured and integrated correctly.
""".

%% Test that throttling can be configured
throttling_configuration_test() ->
  ok = test_helper:set_log_level(),

  % Start a pool with throttling configuration
  {ok, _SupPid} = arterial_pool:start_link(test_throttle_pool, #{
    size => 1,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 12348,
    protocol => tcp,
    throttle => #{rate_per_sec => 10, window_msec => 1000}  % 10 requests/sec, 1s window
  }),

  try
    % Verify throttling state is stored
    ThrottleState = arterial_pool:throttle(test_throttle_pool),
    ?assertMatch({10, 1000}, ThrottleState),

    % The throttling logic itself is tested at the NIF level during
    % send_and_release calls, but we can verify the configuration is correct
    ok
  after
    arterial_pool:stop(test_throttle_pool)
  end.

%% Test that pools without throttling work correctly
no_throttling_test() ->
  ok = test_helper:set_log_level(),

  {ok, _SupPid} = arterial_pool:start_link(test_no_throttle_pool, #{
    size => 1,
    codec => arterial_codec_default,
    address => "127.0.0.1",
    port => 12349,
    protocol => tcp
    % No throttle configuration
  }),

  try
    % Verify no throttling state is stored
    ThrottleState = arterial_pool:throttle(test_no_throttle_pool),
    ?assertEqual(undefined, ThrottleState),
    ok
  after
    arterial_pool:stop(test_no_throttle_pool)
  end.