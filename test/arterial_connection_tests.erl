-module(arterial_connection_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Fast, deterministic unit tests for `arterial_connection`'s reconnect-
backoff interval sequence (`recon_state/1`, `backoff_timeout/1`,
exported only under `-ifdef(TEST)`), as opposed to
`test_tcp_server_tests`'s real-socket end-to-end coverage of the
connect/reconnect/address-failover state machine as a whole.
""".

fixed_interval_stays_flat_test() ->
  RS0 = arterial_connection:recon_state(#{reconnect_time => 250}),
  {I1, RS1} = arterial_connection:backoff_timeout(RS0),
  {I2, RS2} = arterial_connection:backoff_timeout(RS1),
  {I3, _}   = arterial_connection:backoff_timeout(RS2),
  ?assertEqual([250, 250, 250], [I1, I2, I3]).

backoff_grows_then_caps_at_max_test() ->
  RS0 = arterial_connection:recon_state(#{reconnect_time => {backoff, 100, 1000}}),
  {Intervals, _} = lists:foldl(
    fun(_, {Acc, RS}) ->
      {I, RS1} = arterial_connection:backoff_timeout(RS),
      {[I | Acc], RS1}
    end,
    {[], RS0},
    lists:seq(1, 10)),
  [First | _] = Sequence = lists:reverse(Intervals),

  %% First retry is always exactly Min (no jitter on the first attempt).
  ?assertEqual(100, First),

  %% Each interval is non-decreasing (jitter never pushes it backwards
  %% past the previous one) and never exceeds Max.
  lists:foldl(fun(I, Prev) ->
    ?assert(I >= Prev),
    ?assert(I =< 1000),
    I
  end, 0, Sequence),

  %% With Min=100 doubling each step, it must reach the Max=1000 cap
  %% well within 10 attempts (100 -> ~200 -> ~400 -> ~800 -> 1000).
  ?assertEqual(1000, lists:last(Sequence)).

backoff_with_infinity_max_grows_unbounded_test() ->
  RS0 = arterial_connection:recon_state(#{reconnect_time => {backoff, 50, infinity}}),
  {I1, RS1} = arterial_connection:backoff_timeout(RS0),
  {I2, RS2} = arterial_connection:backoff_timeout(RS1),
  {I3, _}   = arterial_connection:backoff_timeout(RS2),
  ?assertEqual(50, I1),
  %% Roughly doubling (with jitter) each step, no cap to clamp it.
  ?assert(I2 > I1),
  ?assert(I3 > I2).

%% `reconnect` => false disables the timer entirely (recon_state/1
%% returns undefined; recon_timer/1 on undefined just clears the
%% socket field with no timer -- not exercised here since recon_timer/1
%% isn't exported, but recon_state/1's own contract is).
reconnect_disabled_test() ->
  ?assertEqual(undefined, arterial_connection:recon_state(#{reconnect => false})).

%% Legacy reconnect_time_min/reconnect_time_max options (pre-dating the
%% unified reconnect_time option) still work, folded into the equivalent
%% {backoff, Min, Max}.
legacy_min_max_options_test() ->
  RS0 = arterial_connection:recon_state(#{reconnect_time_min => 100, reconnect_time_max => 1000}),
  {I1, _} = arterial_connection:backoff_timeout(RS0),
  ?assertEqual(100, I1).
