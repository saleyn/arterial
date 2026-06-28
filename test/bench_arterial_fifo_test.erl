-module(bench_arterial_fifo_test).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Basic tests to verify the FIFO benchmark module compiles and works.
""".

%% Test that benchmark options are well-formed
opts_test() ->
  Opts = bench_arterial_fifo:opts(),
  ?assertMatch(#{mode := fifo, pool_size := _, workers := _}, Opts).

%% Test that FIFO-specific options are included
fifo_opts_test() ->
  FIFOOpts = bench_arterial_fifo:fifo_opts(),
  ?assertMatch(#{stripe_pick    := scheduler_id,
                 reserv_timeout := _,
                 req_timeout    := _}, FIFOOpts).

%% Test internal helper functions
percentile_test() ->
  % Test percentile calculation with known data
  Data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  % 50th percentile of 1-10 should be 5 or 6
  P50 = bench_arterial_fifo:percentile(Data, 0.5),
  ?assert(P50 >= 5 andalso P50 =< 6),

  % 90th percentile should be 9 or 10
  P90 = bench_arterial_fifo:percentile(Data, 0.9),
  ?assert(P90 >= 9).

%% Test timing statistics calculation
timing_stats_test() ->
  TimingData = [1000, 2000, 3000, 4000, 5000],  % microseconds
  Stats = bench_arterial_fifo:calculate_timing_stats(TimingData),

  ?assertMatch(#{mean_us := Mean, p95_us := P95} when Mean > 0 andalso P95 > 0, Stats),
  ?assertEqual(3000.0, maps:get(mean_us, Stats)),  % Mean should be 3000
  ?assertEqual(1000, maps:get(min_us, Stats)),
  ?assertEqual(5000, maps:get(max_us, Stats)).

%% Test empty timing data handling
empty_timing_test() ->
  Stats = bench_arterial_fifo:calculate_timing_stats([]),
  ?assertMatch(#{mean_us := +0.0, min_us := 0, max_us := 0}, Stats).

%% Test that shackle availability detection works
shackle_available_test() ->
  Available = bench_arterial_fifo:shackle_available(),
  ?assert(is_boolean(Available)).

%% Test that poolboy availability detection works
poolboy_available_test() ->
  Available = bench_arterial_fifo:poolboy_available(),
  ?assert(is_boolean(Available)).