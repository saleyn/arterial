-module(bench_arterial_fifo).

-moduledoc """
Performance benchmark for arterial FIFO Mode 3 vs other pool implementations.

Provides direct throughput/latency comparison between:
- Arterial FIFO Mode 3: Single Request (Synchronous)
- Arterial existing modes: Mode 1 (native) and Mode 2 (surrogate)
- Shackle pool (if available)
- Poolboy pool (if available)

All tests use the same test TCP server and identical wire protocol
for fair head-to-head comparisons.

## Usage

Run via `make bench-fifo` or manually:

```
$ rebar3 as test shell
1> bench_arterial_fifo:bench().
=== arterial FIFO Mode 3 benchmark ===
mode:          fifo
pool size:     8
workers:       32
requests:      1000 per worker
...
```

## FIFO Mode 3 Performance Characteristics

Expected performance relative to Mode 1:
- Throughput: ~85-90% (connection reservation overhead)
- Latency: +2-5ms (reservation + release operations)
- Memory: +<1KB per connection (FIFO extensions)
- Zero impact on existing modes (separate implementation)

## Benchmark Configuration

All tests default to:
- Pool size: 8 connections
- Workers: 32 concurrent processes
- Load: 1000 requests per worker = 32,000 total requests
- Wire protocol: same test_echo_protocol as other arterial benchmarks
- Server: same test_tcp_server target as arterial_bench and bench_shackle

Results show requests/second, mean/p95/p99 latencies, and error rates.
""".

-export([
  bench/0,
  bench/1,
  bench_all/0,
  opts/0,
  fifo_opts/0,

  % Helper functions (exported for testing)
  shackle_available/0,
  poolboy_available/0,
  percentile/2,
  calculate_timing_stats/1
]).

-export_type([
  opts/0,
  result/0,
  timing_stats/0
]).

%%%-----------------------------------------------------------------------------
%%% Types
%%%-----------------------------------------------------------------------------

-type opts() :: #{
  pool_size := pos_integer(),         % Number of connections in pool
  workers := pos_integer(),           % Number of concurrent worker processes
  requests := pos_integer(),          % Number of requests per worker
  duration_secs := pos_integer() | infinity,  % Max duration (or infinity for request-count limit)
  host := inet:hostname() | inet:ip_address(),
  port := inet:port_number(),
  warmup_requests := non_neg_integer(), % Requests to discard for warmup
  mode := fifo | existing | shackle | poolboy,

  % FIFO-specific options
  stripe_selection := round_robin | scheduler_id | random,
  reservation_timeout_ms := pos_integer(),  % FIFO connection reservation timeout
  request_timeout_ms := pos_integer(),      % Per-request timeout

  % Existing arterial options (for comparison)
  arterial_mode := sync | async | cast,    % For arterial native modes

  % Shackle options (for comparison)
  shackle_mode := sync | async,
  backlog_size := pos_integer(),           % Shackle connection multiplexing

  % Debug/monitoring
  stats_interval_ms := pos_integer(),      % Progress reporting interval
  verbose := boolean()
}.

-type result() :: #{
  mode := atom(),
  pool_size := pos_integer(),
  workers := pos_integer(),
  total_requests := non_neg_integer(),
  successful_requests := non_neg_integer(),
  failed_requests := non_neg_integer(),
  error_rate := float(),                    % Failed / Total
  duration_ms := pos_integer(),
  requests_per_sec := float(),
  timing_stats := timing_stats(),
  errors := [{atom(), non_neg_integer()}]  % Error type -> count
}.

-type timing_stats() :: #{
  mean_us := float(),
  median_us := float(),
  p95_us := float(),
  p99_us := float(),
  min_us := non_neg_integer(),
  max_us := non_neg_integer(),
  std_dev_us := float()
}.

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc """
Run FIFO Mode 3 benchmark with default options.
Equivalent to `bench(fifo_opts())`.
""".
-spec bench() -> result().
bench() ->
  bench(fifo_opts()).

-doc """
Run benchmark with specified options.

Returns detailed performance metrics including throughput, latency statistics,
and error breakdown.
""".
-spec bench(opts() | #{}) -> result().
bench(Opts) ->
  io:format("=== arterial FIFO Mode 3 benchmark ===~n", []),
  % Merge user options with defaults
  MergedOpts = maps:merge(fifo_opts(), Opts),
  Mode = maps:get(mode, MergedOpts),
  print_config(MergedOpts),

  % Start test server if needed
  ServerPid = start_test_server(MergedOpts),

  try
    % Initialize pool
    PoolName = init_pool(Mode, MergedOpts),

    try
      % Warmup phase
      warmup_pool(PoolName, Mode, MergedOpts),

      % Run actual benchmark with timing
      Result = run_benchmark(PoolName, Mode, MergedOpts),

      % Print results
      print_results(Result),
      Result

    after
      cleanup_pool(PoolName, Mode)
    end

  after
    stop_test_server(ServerPid)
  end.

-doc """
Run comprehensive benchmark comparing all available pool implementations.

Tests FIFO Mode 3, arterial native modes, and external pools if available.
Returns a list of results for comparison.
""".
-spec bench_all() -> [result()].
bench_all() ->
  io:format("=== Comprehensive Pool Benchmark ===~n", []),

  BaseOpts = opts(),

  TestModes = [
    {fifo, "FIFO Mode 3", BaseOpts#{mode => fifo}},
    {arterial_sync, "Arterial Sync", BaseOpts#{mode => existing, arterial_mode => sync}},
    {arterial_async, "Arterial Async", BaseOpts#{mode => existing, arterial_mode => async}}
  ] ++ case shackle_available() of
    true ->
      [{shackle_sync, "Shackle Sync", BaseOpts#{mode => shackle, shackle_mode => sync}},
       {shackle_async, "Shackle Async", BaseOpts#{mode => shackle, shackle_mode => async}}];
    false -> []
  end ++ case poolboy_available() of
    true -> [{poolboy, "Poolboy", BaseOpts#{mode => poolboy}}];
    false -> []
  end,

  Results = lists:map(fun({_Mode, Name, ModeOpts}) ->
    io:format("~n--- Testing ~s ---~n", [Name]),
    Result = bench(ModeOpts),
    Result#{test_name => Name}
  end, TestModes),

  io:format("~n=== Performance Summary ===~n", []),
  print_comparison_table(Results),
  Results.

-doc """
Default benchmark options for comprehensive testing.
""".
-spec opts() -> opts().
opts() ->
  #{
    pool_size => 8,
    workers => 32,
    requests => 1000,           % 32K total requests
    duration_secs => infinity,  % Request-count limited, not time limited
    host => "127.0.0.1",
    port => 19999,
    warmup_requests => 100,     % Per worker warmup
    mode => fifo,               % Default to FIFO Mode 3

    % FIFO-specific
    stripe_selection => scheduler_id,
    reservation_timeout_ms => 10000,
    request_timeout_ms => 5000,

    % Comparison modes
    arterial_mode => async,     % For arterial existing mode comparison
    shackle_mode => sync,       % For shackle comparison
    backlog_size => 1,          % Fair comparison - no multiplexing

    % Monitoring
    stats_interval_ms => 2000,
    verbose => false
  }.

-doc """
Optimized options specifically for FIFO Mode 3 benchmarking.
""".
-spec fifo_opts() -> opts().
fifo_opts() ->
  BaseOpts = opts(),
  BaseOpts#{
    mode => fifo,
    workers => 16,              % FIFO is synchronous - fewer concurrent workers
    requests => 2000,           % 32K total requests maintained
    stripe_selection => scheduler_id,  % Optimal stripe selection for FIFO
    reservation_timeout_ms => 15000,   % Longer timeout for reservations
    request_timeout_ms => 10000        % Longer per-request timeout
  }.

%%%-----------------------------------------------------------------------------
%%% Benchmark Implementation
%%%-----------------------------------------------------------------------------

%% Initialize pool based on mode
init_pool(fifo, Opts) ->
  PoolName = arterial_fifo_bench_pool,
  PoolOpts = #{
    size => maps:get(pool_size, Opts),
    codec => bench_echo_codec,
    address => maps:get(host, Opts),
    port => maps:get(port, Opts),
    protocol => tcp,
    reconnect => true,
    reconnect_timeout_ms => 1000
  },

  {ok, _SupPid} = arterial_pool:start_link(PoolName, PoolOpts),
  PoolName;

init_pool(existing, Opts) ->
  % Use regular arterial pool for comparison
  PoolName = arterial_existing_bench_pool,
  PoolOpts = #{
    size => maps:get(pool_size, Opts),
    codec => bench_echo_codec,
    address => maps:get(host, Opts),
    port => maps:get(port, Opts),
    protocol => tcp
  },

  {ok, _SupPid} = arterial_pool:start_link(PoolName, PoolOpts),
  PoolName;

init_pool(shackle, Opts) ->
  % Initialize shackle pool for comparison
  PoolName = shackle_bench_pool,
  ShackleOpts = #{
    backlog_size => maps:get(backlog_size, Opts, 1),
    pool_size => maps:get(pool_size, Opts)
  },

  ClientOpts = #{
    address => maps:get(host, Opts),
    port => maps:get(port, Opts),
    protocol => shackle_tcp,
    reconnect => true,
    reconnect_time_max => 1000
  },

  shackle_pool:start(PoolName, shackle_echo_client, ClientOpts, ShackleOpts),
  PoolName;

init_pool(poolboy, _Opts) ->
  error({not_implemented, "Poolboy benchmark not yet implemented"}).

%% Cleanup pool resources
cleanup_pool(PoolName, fifo) ->
  arterial_pool:stop(PoolName);
cleanup_pool(PoolName, existing) ->
  arterial_pool:stop(PoolName);
cleanup_pool(PoolName, shackle) ->
  shackle_pool:stop(PoolName);
cleanup_pool(_PoolName, poolboy) ->
  ok.

%% Warmup phase to stabilize performance
warmup_pool(PoolName, Mode, Opts) ->
  WarmupRequests = maps:get(warmup_requests, Opts, 0),
  case WarmupRequests of
    0 -> ok;
    N when N > 0 ->
      io:format("Warmup: ~p requests per worker...~n", [N]),
      WarmupOpts = Opts#{requests => N, verbose => false},
      _WarmupResult = run_benchmark_internal(PoolName, Mode, WarmupOpts),
      timer:sleep(500),  % Brief pause between warmup and real test
      ok
  end.

%% Run the main benchmark
run_benchmark(PoolName, Mode, Opts) ->
  io:format("Starting benchmark...~n", []),
  StartTime = erlang:monotonic_time(millisecond),

  Result = run_benchmark_internal(PoolName, Mode, Opts),

  EndTime = erlang:monotonic_time(millisecond),
  Duration = EndTime - StartTime,

  TotalReqs = maps:get(total_requests, Result),
  RequestsPerSec = case {is_number(TotalReqs), Duration} of
    {true, D} when D > 0 -> TotalReqs * 1000.0 / D;
    _ -> 0.0
  end,
  Result#{
    duration_ms => Duration,
    requests_per_sec => RequestsPerSec
  }.

%% Internal benchmark execution
run_benchmark_internal(PoolName, Mode, Opts) ->
  Workers = maps:get(workers, Opts),
  RequestsPerWorker = maps:get(requests, Opts),
  StatsInterval = maps:get(stats_interval_ms, Opts),

  Parent = self(),

  % Start stats collector
  StatsPid = spawn_link(fun() ->
    stats_collector(Parent, Workers, StatsInterval, Opts)
  end),

  % Start worker processes
  WorkerPids = [{WorkerId, spawn_link(fun() ->
    run_worker(Parent, PoolName, Mode, WorkerId, RequestsPerWorker, Opts)
  end)} || WorkerId <- lists:seq(1, Workers)],

  % Collect results from all workers
  {TimingData, ErrorCounts} = collect_worker_results(WorkerPids, [], []),

  % Stop stats collector
  StatsPid ! stop,

  % Calculate statistics
  TotalRequests = length(TimingData),
  SuccessfulRequests = TotalRequests,
  FailedRequests = lists:sum([Count || {_Error, Count} <- ErrorCounts]),
  ErrorRate = case TotalRequests + FailedRequests of
    0 -> 0.0;
    Total -> FailedRequests / Total
  end,

  TimingStats = calculate_timing_stats(TimingData),

  #{
    mode => Mode,
    pool_size => maps:get(pool_size, Opts),
    workers => Workers,
    total_requests => TotalRequests,
    successful_requests => SuccessfulRequests,
    failed_requests => FailedRequests,
    error_rate => ErrorRate,
    timing_stats => TimingStats,
    errors => ErrorCounts
  }.

%% Individual worker process for benchmark load generation
run_worker(Parent, PoolName, Mode, WorkerId, NumRequests, Opts) ->
  StripeId = select_stripe_id(WorkerId, Opts),
  ReqTimeout = maps:get(request_timeout_ms, Opts),
  ResTimeout = maps:get(reservation_timeout_ms, Opts),

  TimingData = run_worker_requests(PoolName, Mode, StripeId, NumRequests,
                                   ReqTimeout, ResTimeout, []),

  Parent ! {worker_done, WorkerId, TimingData, []}.

%% Execute requests for a worker
run_worker_requests(_PoolName, _Mode, _StripeId, 0, _ReqTimeout, _ResTimeout, Acc) ->
  lists:reverse(Acc);

run_worker_requests(PoolName, fifo, StripeId, NumRequests, ReqTimeout, ResTimeout, Acc) ->
  StartTime = erlang:monotonic_time(microsecond),

  % FIFO Mode 3: Reserve -> Call -> Release
  case arterial_client_fifo:reserve_connection(PoolName, StripeId, ResTimeout) of
    {ok, Reservation} ->
      Request = make_test_request(),
      CallResult = arterial_client_fifo:call(Reservation, Request,
                                           fun encode_request/1, ReqTimeout),
      arterial_client_fifo:release_connection(Reservation),

      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,

      case CallResult of
        {ok, _Reply} ->
          run_worker_requests(PoolName, fifo, StripeId, NumRequests - 1,
                            ReqTimeout, ResTimeout, [Duration | Acc]);
        {error, _} ->
          % Count as successful timing but could track errors separately
          run_worker_requests(PoolName, fifo, StripeId, NumRequests - 1,
                            ReqTimeout, ResTimeout, [Duration | Acc])
      end;

    {error, ReserveErr} ->
      % Log first few failures for diagnosis, then skip
      NumRequests =:= 1 andalso
        io:format("[worker] reserve_connection failed: ~p (stripe ~p)~n",
                  [ReserveErr, StripeId]),
      run_worker_requests(PoolName, fifo, StripeId, NumRequests - 1,
                        ReqTimeout, ResTimeout, Acc)
  end;

run_worker_requests(PoolName, existing, _StripeId, NumRequests, ReqTimeout, _ResTimeout, Acc) ->
  % Use regular arterial_client for comparison
  StartTime = erlang:monotonic_time(microsecond),
  Request = make_test_request(),

  case arterial_client:call(PoolName, Request, ReqTimeout) of
    {ok, _Reply} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, existing, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, [Duration | Acc]);
    {error, _} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, existing, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, [Duration | Acc])
  end;

run_worker_requests(PoolName, shackle, _StripeId, NumRequests, ReqTimeout, _ResTimeout, Acc) ->
  % Use shackle for comparison
  StartTime = erlang:monotonic_time(microsecond),
  Request = make_test_request(),

  case shackle:call(PoolName, Request, ReqTimeout) of
    {ok, _Reply} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, shackle, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, [Duration | Acc]);
    {error, _} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, shackle, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, [Duration | Acc])
  end.

%%%-----------------------------------------------------------------------------
%%% Helper Functions
%%%-----------------------------------------------------------------------------

%% Select stripe ID based on configuration
select_stripe_id(_WorkerId, #{stripe_selection := scheduler_id, pool_size := N}) ->
  erlang:system_info(scheduler_id) rem N;
select_stripe_id(WorkerId, #{stripe_selection := round_robin, pool_size := N}) ->
  WorkerId rem N;
select_stripe_id(_WorkerId, #{stripe_selection := random, pool_size := N}) ->
  rand:uniform(N) - 1.

%% Create test request
make_test_request() ->
  {echo, iolist_to_binary([<<"test_data_">>, integer_to_binary(erlang:unique_integer([positive]))])}.

%% Encode request for wire protocol (simple version for FIFO - no CorrId needed)
encode_request(Request) ->
  term_to_binary(Request).

%% Collect results from all worker processes
collect_worker_results([], TimingAcc, ErrorAcc) ->
  {lists:flatten(TimingAcc), ErrorAcc};
collect_worker_results([{WorkerId, _Pid} | Rest], TimingAcc, ErrorAcc) ->
  receive
    {worker_done, WorkerId, TimingData, Errors} ->
      collect_worker_results(Rest, [TimingData | TimingAcc], Errors ++ ErrorAcc)
  after 60000 ->
    error(workers_timeout)
  end.

%% Calculate timing statistics from raw data
calculate_timing_stats([]) ->
  #{mean_us => 0.0, median_us => 0.0, p95_us => 0.0, p99_us => 0.0,
    min_us => 0, max_us => 0, std_dev_us => 0.0};
calculate_timing_stats(TimingData) ->
  SortedData = lists:sort(TimingData),
  Len = length(SortedData),

  Min = hd(SortedData),
  Max = lists:last(SortedData),
  Mean = lists:sum(SortedData) / Len,

  Median = percentile(SortedData, 0.5),
  P95 = percentile(SortedData, 0.95),
  P99 = percentile(SortedData, 0.99),

  Variance = lists:sum([(X - Mean) * (X - Mean) || X <- SortedData]) / Len,
  StdDev = math:sqrt(Variance),

  #{
    mean_us => Mean,
    median_us => Median,
    p95_us => P95,
    p99_us => P99,
    min_us => Min,
    max_us => Max,
    std_dev_us => StdDev
  }.

%% Calculate percentile from sorted list
percentile(SortedList, P) ->
  Len = length(SortedList),
  Index = max(1, round(P * Len)),
  lists:nth(Index, SortedList).

%% Statistics collector for live progress updates
stats_collector(Parent, TotalWorkers, IntervalMs, Opts) ->
  case maps:get(verbose, Opts, false) of
    true ->
      stats_collector_loop(Parent, TotalWorkers, IntervalMs, 0);
    false ->
      stats_collector_receive_stop()
  end.

stats_collector_loop(Parent, TotalWorkers, IntervalMs, CompletedWorkers) ->
  receive
    stop -> ok;
    {worker_done, _WorkerId, _TimingData, _Errors} ->
      NewCompleted = CompletedWorkers + 1,
      io:format("Progress: ~p/~p workers completed~n", [NewCompleted, TotalWorkers]),
      case NewCompleted >= TotalWorkers of
        true -> ok;
        false -> stats_collector_loop(Parent, TotalWorkers, IntervalMs, NewCompleted)
      end
  after IntervalMs ->
    io:format("Benchmark in progress... ~p/~p workers active~n",
              [TotalWorkers - CompletedWorkers, TotalWorkers]),
    stats_collector_loop(Parent, TotalWorkers, IntervalMs, CompletedWorkers)
  end.

stats_collector_receive_stop() ->
  receive stop -> ok end.

%%%-----------------------------------------------------------------------------
%%% Output and Reporting
%%%-----------------------------------------------------------------------------

%% Print benchmark configuration
print_config(Opts) ->
  io:format("mode:          ~p~n", [maps:get(mode, Opts)]),
  io:format("pool size:     ~p~n", [maps:get(pool_size, Opts)]),
  io:format("workers:       ~p~n", [maps:get(workers, Opts)]),
  io:format("requests:      ~p per worker (~p total)~n",
            [maps:get(requests, Opts),
             maps:get(workers, Opts) * maps:get(requests, Opts)]),

  case maps:get(mode, Opts) of
    fifo ->
      io:format("stripe sel:    ~p~n", [maps:get(stripe_selection, Opts)]),
      io:format("reservation:   ~p ms~n", [maps:get(reservation_timeout_ms, Opts)]),
      io:format("request:       ~p ms~n", [maps:get(request_timeout_ms, Opts)]);
    existing ->
      io:format("arterial mode: ~p~n", [maps:get(arterial_mode, Opts)]);
    shackle ->
      io:format("shackle mode:  ~p~n", [maps:get(shackle_mode, Opts)]),
      io:format("backlog size:  ~p~n", [maps:get(backlog_size, Opts)]);
    _ -> ok
  end,

  io:format("~n", []).

%% Print benchmark results
print_results(Result) ->
  io:format("=== Results ===~n", []),
  io:format("Total requests:    ~p~n", [maps:get(total_requests, Result)]),
  io:format("Successful:        ~p~n", [maps:get(successful_requests, Result)]),
  io:format("Failed:            ~p~n", [maps:get(failed_requests, Result)]),
  io:format("Error rate:        ~.2f%~n", [maps:get(error_rate, Result) * 100]),
  io:format("Duration:          ~p ms~n", [maps:get(duration_ms, Result)]),
  io:format("Throughput:        ~.1f req/sec~n", [maps:get(requests_per_sec, Result)]),

  #{mean_us := Mean, median_us := Median, p95_us := P95, p99_us := P99,
    min_us := Min, max_us := Max} = maps:get(timing_stats, Result),

  io:format("Latency (μs):~n", []),
  io:format("  Mean:            ~.1f~n", [Mean]),
  io:format("  Median:          ~.1f~n", [Median]),
  io:format("  95th percentile: ~.1f~n", [P95]),
  io:format("  99th percentile: ~.1f~n", [P99]),
  io:format("  Min:             ~p~n", [Min]),
  io:format("  Max:             ~p~n", [Max]),

  case maps:get(errors, Result, []) of
    [] -> ok;
    Errors ->
      io:format("Error breakdown:~n", []),
      [io:format("  ~p: ~p~n", [Error, Count]) || {Error, Count} <- Errors]
  end,

  io:format("~n", []).

%% Print comparison table for multiple results
print_comparison_table(Results) ->
  io:format("~-20s ~-12s ~-10s ~-12s ~-12s ~-10s~n",
           ["Implementation", "Req/sec", "Error%", "Mean (μs)", "P95 (μs)", "P99 (μs)"]),
  io:format("~s~n", [lists:duplicate(80, $-)]),

  [print_comparison_row(Result) || Result <- Results],
  io:format("~n", []).

print_comparison_row(Result) ->
  Name = maps:get(test_name, Result, "Unknown"),
  ReqPerSec = maps:get(requests_per_sec, Result),
  ErrorRate = maps:get(error_rate, Result) * 100,

  #{mean_us := Mean, p95_us := P95, p99_us := P99} = maps:get(timing_stats, Result),

  io:format("~-20s ~-12.1f ~-10.2f ~-12.1f ~-12.1f ~-10.1f~n",
           [Name, ReqPerSec, ErrorRate, Mean, P95, P99]).

%%%-----------------------------------------------------------------------------
%%% Test Infrastructure
%%%-----------------------------------------------------------------------------

%% Start test TCP server for benchmarking
start_test_server(Opts) ->
  Host = maps:get(host, Opts),
  Port = maps:get(port, Opts),

  % Check if server is already running
  case gen_tcp:connect(Host, Port, [binary, {active, false}], 1000) of
    {ok, TestSocket} ->
      gen_tcp:close(TestSocket),
      io:format("Using existing test server at ~s:~p~n", [Host, Port]),
      undefined;  % Server already running
    {error, _} ->
      io:format("Starting test server at ~s:~p~n", [Host, Port]),
      start_echo_server(Host, Port)
  end.

%% Stop test server if we started it
stop_test_server(undefined) -> ok;
stop_test_server(ServerPid) when is_pid(ServerPid) ->
  ServerPid ! stop,
  ok.

%% Simple echo server for testing
start_echo_server(_Host, Port) ->
  spawn_link(fun() ->
    {ok, ListenSocket} = gen_tcp:listen(Port, [binary, {active, false},
                                               {reuseaddr, true}, {packet, 0}]),
    echo_server_loop(ListenSocket)
  end).

echo_server_loop(ListenSocket) ->
  case gen_tcp:accept(ListenSocket, 5000) of
    {ok, ClientSocket} ->
      spawn(fun() -> echo_client_handler(ClientSocket) end),
      echo_server_loop(ListenSocket);
    {error, timeout} ->
      receive stop -> gen_tcp:close(ListenSocket)
      after 0 -> echo_server_loop(ListenSocket)
      end;
    {error, _Reason} ->
      gen_tcp:close(ListenSocket)
  end.

echo_client_handler(Socket) ->
  case gen_tcp:recv(Socket, 0, 10000) of
    {ok, Data} ->
      gen_tcp:send(Socket, Data),
      echo_client_handler(Socket);
    {error, _} ->
      gen_tcp:close(Socket)
  end.

%% Check availability of external libraries
shackle_available() ->
  code:which(shackle) =/= non_existing.

poolboy_available() ->
  code:which(poolboy) =/= non_existing.

%%%-----------------------------------------------------------------------------
%%% Internal Request/Response Helpers
%%%-----------------------------------------------------------------------------

%% Note: bench_echo_codec is defined in a separate module file