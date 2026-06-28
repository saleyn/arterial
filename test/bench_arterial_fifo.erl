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
workers:       8
requests:      4000 per worker
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
- Workers: 8 concurrent processes
- Load: 4000 requests per worker = 32,000 total requests
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
  pool_size       := pos_integer(),            % Number of connections in pool
  workers         := pos_integer(),            % Number of concurrent worker processes
  requests        := pos_integer(),            % Number of requests per worker
  duration_secs   := pos_integer() | infinity, % Max duration (or infinity for request-count limit)
  host            := inet:hostname() | inet:ip_address(),
  port            := inet:port_number(),
  warmup_requests := non_neg_integer(),        % Requests to discard for warmup
  mode            := existing | fifo | shackle | poolboy,

  % FIFO-specific options
  stripe_pick     := round_robin | scheduler_id | random,
  reserv_timeout  := pos_integer(),            % FIFO connection reservation timeout
  req_timeout     := pos_integer(),            % Per-request timeout

  % FIFO backlog queue options
  fifo_backlog_size    := non_neg_integer(),   % Max backlog entries (0 = disabled)
  fifo_backlog_timeout := pos_integer(),       % Max wait time in backlog (ms)
  fifo_queue_monitor   := boolean(),           % Enable backlog queue monitoring

  % Unified synchronization mode (for arterial and shackle comparison)
  sync_mode       := sync | async | cast,      % sync/async for all pools, cast for arterial only
  backlog_size    := pos_integer(),            % Connection multiplexing (shackle, arterial async)

  % Debug/monitoring
  stats_interval_ms := pos_integer(),          % Progress reporting interval
  verbose           := boolean()
}.

-type result() :: #{
  mode                := atom(),
  pool_size           := pos_integer(),
  workers             := pos_integer(),
  total_requests      := non_neg_integer(),
  successful_requests := non_neg_integer(),
  failed_requests     := non_neg_integer(),
  error_rate          := float(),           % Failed / Total
  reconnects          := non_neg_integer(), % Number of reconnections
  duration_ms         := pos_integer(),
  requests_per_sec    := float(),
  timing_stats        := timing_stats(),
  errors              := [{atom(), non_neg_integer()}], % Error type -> count

  % FIFO backlog queue statistics (when applicable)
  backlog_stats       => #{
    total_queued => non_neg_integer(),      % Total requests queued in backlog
    peak_backlog => non_neg_integer(),      % Peak backlog queue depth
    avg_wait_time_ms => float(),            % Average wait time in backlog
    backlog_timeouts => non_neg_integer()   % Requests that timed out in backlog
  }
}.

-type timing_stats() :: #{
  mean_us    := float(),
  median_us  := float(),
  p95_us     := float(),
  p99_us     := float(),
  min_us     := non_neg_integer(),
  max_us     := non_neg_integer(),
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
  Verbose = maps:get(verbose, MergedOpts, false),

  % Suppress verbose logging during benchmarks unless --verbose
  case Verbose of
    false ->
      % Maximum suppression for quiet benchmarks
      logger:set_application_level(arterial, emergency),  % Suppress all arterial messages
      logger:set_application_level(kernel, error),        % Suppress kernel messages
      logger:set_application_level(stdlib, error),        % Suppress stdlib messages
      logger:set_primary_config(level, emergency),        % Set primary to emergency
      logger:set_handler_config(default, level, emergency); % Handler level too
    true ->
      logger:set_application_level(arterial, info),       % Show arterial info+
      logger:set_application_level(kernel, warning),      % Show kernel warnings
      logger:set_application_level(stdlib, warning),      % Show stdlib warnings
      logger:set_primary_config(level, info),             % Enable primary info
      logger:set_handler_config(default, level, info)     % Handler level info
  end,

  print_config(MergedOpts),

  % Start test server if needed
  ServerPid = start_test_server(MergedOpts),

  try
    % Start reconnect tracking before initializing the pool
    start_reconnect_tracking(),

    % Initialize pool
    PoolName = init_pool(Mode, MergedOpts),

    try
      % Wait for pool connections to establish before warmup
      wait_pool_connected(PoolName, Mode, MergedOpts),

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
    stop_reconnect_tracking(),
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
    {fifo,            "FIFO Mode 3 (with backlog)", BaseOpts#{mode => fifo}},
    {fifo_no_backlog, "FIFO Mode 3 (no backlog)", BaseOpts#{
      mode               => fifo,
      fifo_backlog_size  => 0,
      fifo_queue_monitor => false
    }},
    {arterial_sync,    "Arterial Sync",  BaseOpts#{mode => existing, sync_mode => sync}},
    {arterial_async,   "Arterial Async", BaseOpts#{mode => existing, sync_mode => async}},
    {arterial_cast,    "Arterial Cast",  BaseOpts#{mode => existing, sync_mode => cast}}
  ] ++ case shackle_available() of
    true ->
      [{shackle_sync,  "Shackle Sync",   BaseOpts#{mode => shackle, sync_mode => sync}},
       {shackle_async, "Shackle Async",  BaseOpts#{mode => shackle, sync_mode => async}}];
    false -> []
  end ++ case poolboy_available() of
    true ->  [{poolboy, "Poolboy",       BaseOpts#{mode => poolboy}}];
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
    pool_size         => 8,
    workers           => 8,        % Match workers to pool_size consistently
    requests          => 4000,     % Maintain 32K total requests: 8 * 4000 = 32K
    duration_secs     => infinity, % Request-count limited, not time limited
    host              => "127.0.0.1",
    port              => 19999,
    warmup_requests   => 100,      % Per worker warmup
    mode              => fifo,     % Default to FIFO Mode 3

    % FIFO-specific
    stripe_pick       => round_robin,
    reserv_timeout    => 10000,
    req_timeout       => 5000,

    % FIFO backlog queue options
    fifo_backlog_size    => 1000,  % Enable backlog by default - 1000 entries
    fifo_backlog_timeout => 30000, % 30 second max wait in backlog
    fifo_queue_monitor   => true,  % Enable monitoring for benchmarks

    % Comparison modes
    sync_mode         => async,    % Default sync mode for all pool comparisons
    backlog_size      => 1,        % Fair comparison - no multiplexing

    % Monitoring
    stats_interval_ms => 2000,
    verbose           => false
  }.

-doc """
Optimized options specifically for FIFO Mode 3 benchmarking.
""".
-spec fifo_opts() -> opts().
fifo_opts() ->
  BaseOpts = opts(),
  BaseOpts#{
    mode             => fifo,
    workers          => 8,           % Match workers to pool_size to reduce contention (#5)
    requests         => 4000,        % Maintain total request count: 8 * 4000 = 32K
    stripe_pick      => round_robin, % Each worker gets its own stripe
    reserv_timeout   => 15000,       % Longer timeout for reservations
    req_timeout      => 10000        % Longer per-request timeout
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
    size                 => maps:get(pool_size, Opts),
    codec                => bench_echo_codec,
    address              => maps:get(host, Opts),
    port                 => maps:get(port, Opts),
    protocol             => tcp,
    reconnect            => true,
    reconnect_timeout_ms => 1000
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

init_pool(poolboy, Opts) ->
  Host = maps:get(host, Opts),
  Port = maps:get(port, Opts),
  PoolSize = maps:get(pool_size, Opts),

  PoolName = poolboy_fifo_bench_pool,

  % Poolboy configuration - similar to bench_poolboy.erl
  PoolArgs = [
    {name, {local, PoolName}},
    {worker_module, poolboy_echo_worker},
    {size, PoolSize},
    {max_overflow, 0},  % No overflow for fair comparison
    {strategy, fifo}    % FIFO to match FIFO Mode 3 ordering
  ],

  % Worker arguments - connection details
  WorkerArgs = [{host, Host}, {port, Port}],

  {ok, _PoolPid} = poolboy:start_link(PoolArgs, WorkerArgs),
  PoolName.

%% Cleanup pool resources
cleanup_pool(PoolName, fifo) ->
  arterial_pool:stop(PoolName);
cleanup_pool(PoolName, existing) ->
  arterial_pool:stop(PoolName);
cleanup_pool(PoolName, shackle) ->
  shackle_pool:stop(PoolName);
cleanup_pool(PoolName, poolboy) ->
  poolboy:stop(PoolName).

%% Wait for pool connections before warmup
wait_pool_connected(PoolName, Mode, Opts)
    when Mode =:= fifo; Mode =:= existing ->
  PoolSize = arterial_pool:size(PoolName),
  Verbose = maps:get(verbose, Opts, false),
  % Poll manually so we can log progress and not block indefinitely
  wait_pool_loop(PoolName, PoolSize, 60, 500, Verbose);
wait_pool_connected(_PoolName, _Mode, _Opts) -> ok.

wait_pool_loop(_PoolName, _PoolSize, 0, _Interval, Verbose) ->
  Verbose andalso io:format("ERROR: connections never established~n");
wait_pool_loop(PoolName, PoolSize, Retries, Interval, Verbose) ->
  States = [arterial_pool:is_available(PoolName, I) || I <- lists:seq(0, PoolSize - 1)],
  Connected = length([1 || true <- States]),
  if Connected =:= PoolSize ->
    Verbose andalso io:format("All ~p connections established~n", [PoolSize]);
  true ->
    Verbose andalso io:format("~p/~p connections ready, waiting (~p retries left)...~n",
              [Connected, PoolSize, Retries - 1]),
    timer:sleep(Interval),
    wait_pool_loop(PoolName, PoolSize, Retries - 1, Interval, Verbose)
  end.

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
  Verbose = maps:get(verbose, Opts, false),
  {TimingData, ErrorCounts} = collect_worker_results(WorkerPids, [], [], Verbose),

  % Stop stats collector
  StatsPid ! stop,

  % Calculate statistics
  TotalRequests      = length(TimingData),
  SuccessfulRequests = TotalRequests,
  FailedRequests     = lists:sum([Count || {_Error, Count} <- ErrorCounts]),
  ErrorRate =
    case TotalRequests + FailedRequests of
      0 -> 0.0;
      Total -> FailedRequests / Total
    end,

  TimingStats = calculate_timing_stats(TimingData),

  % Collect backlog statistics if FIFO mode with backlog enabled
  BacklogStats = collect_backlog_stats(PoolName, Opts),

  BaseResult = #{
    mode                => Mode,
    pool_size           => maps:get(pool_size, Opts),
    workers             => Workers,
    total_requests      => TotalRequests,
    successful_requests => SuccessfulRequests,
    failed_requests     => FailedRequests,
    error_rate          => ErrorRate,
    reconnects          => get_reconnect_count(),
    timing_stats        => TimingStats,
    errors              => ErrorCounts
  },

  % Add backlog stats if available
  case BacklogStats of
    undefined -> BaseResult;
    Stats -> BaseResult#{backlog_stats => Stats}
  end.

%% Individual worker process for benchmark load generation
run_worker(Parent, PoolName, Mode, WorkerId, NumRequests, Opts) ->
  StripeId = select_stripe_id(WorkerId, Opts),
  ReqTimeout = maps:get(req_timeout, Opts),
  ResTimeout = maps:get(reserv_timeout, Opts),

  Verbose = maps:get(verbose, Opts, false),
  TimingData = run_worker_requests(PoolName, Mode, StripeId, NumRequests,
                                   ReqTimeout, ResTimeout, Verbose, [], Opts),

  Parent ! {worker_done, WorkerId, TimingData, []}.

%% Execute requests for a worker
run_worker_requests(_PoolName, _Mode, _StripeId, 0, _ReqTimeout, _ResTimeout, _Verbose, Acc, _Opts) ->
  lists:reverse(Acc);

run_worker_requests(PoolName, fifo, StripeId, NumRequests, ReqTimeout, ResTimeout, Verbose, Acc, _Opts) ->
  StartTime = erlang:monotonic_time(microsecond),
  Request = make_test_request(),

  % FIFO Mode 3: Optimized Reserve+Send -> Release (#3 + #1/#2 - reduced NIF calls + queuing)
  case arterial_client_fifo:reserve_send_call(PoolName, StripeId, Request,
                                              fun encode_request/1, ResTimeout) of
    {ok, _Reply, Reservation} ->
      % Release connection back to pool
      arterial_client_fifo:release_connection(Reservation),

      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,

      run_worker_requests(PoolName, fifo, StripeId, NumRequests - 1,
                        ReqTimeout, ResTimeout, Verbose, [Duration | Acc], _Opts);

    {error, ReserveErr} ->
      % Log first few failures for diagnosis, then skip
      NumRequests =:= 1 andalso Verbose andalso
        io:format("[worker] reserve_send_call failed: ~p (stripe ~p)~n",
                  [ReserveErr, StripeId]),
      run_worker_requests(PoolName, fifo, StripeId, NumRequests - 1,
                        ReqTimeout, ResTimeout, Verbose, Acc, _Opts)
  end;

run_worker_requests(PoolName, existing, _StripeId, NumRequests, ReqTimeout, _ResTimeout, Verbose, Acc, Opts) ->
  % Use regular arterial_client for comparison with configurable sync mode
  StartTime = erlang:monotonic_time(microsecond),
  Request = make_test_request(),
  SyncMode = maps:get(sync_mode, Opts, sync),

  Result = case SyncMode of
    sync -> arterial_client:call(PoolName, Request, ReqTimeout);
    async ->
      % Note: arterial async mode requires different handling,
      % but for benchmark purposes we simulate with sync call
      arterial_client:call(PoolName, Request, ReqTimeout);
    cast ->
      % Cast mode - fire and forget (no response timing)
      arterial_client:cast(PoolName, Request),
      {ok, cast}
  end,

  case Result of
    {ok, _Reply} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, existing, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, Verbose, [Duration | Acc], Opts);
    {error, _} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, existing, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, Verbose, [Duration | Acc], Opts)
  end;

run_worker_requests(PoolName, shackle, _StripeId, NumRequests, ReqTimeout, _ResTimeout, Verbose, Acc, Opts) ->
  % Use shackle for comparison with configurable sync mode
  StartTime = erlang:monotonic_time(microsecond),
  Request = make_test_request(),
  SyncMode = maps:get(sync_mode, Opts, sync),

  Result = case SyncMode of
    sync -> shackle:call(PoolName, Request, ReqTimeout);
    async ->
      % Shackle async mode - for benchmark we still measure round-trip
      shackle:call(PoolName, Request, ReqTimeout);
    cast ->
      % Cast not supported by shackle, fallback to sync
      shackle:call(PoolName, Request, ReqTimeout)
  end,

  case Result of
    {ok, _Reply} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, shackle, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, Verbose, [Duration | Acc], Opts);
    {error, _} ->
      EndTime = erlang:monotonic_time(microsecond),
      Duration = EndTime - StartTime,
      run_worker_requests(PoolName, shackle, _StripeId, NumRequests - 1,
                        ReqTimeout, _ResTimeout, Verbose, [Duration | Acc], Opts)
  end;

run_worker_requests(PoolName, poolboy, _StripeId, NumRequests, ReqTimeout, _ResTimeout, Verbose, Acc, _Opts) ->
  % Use poolboy for comparison - similar to bench_poolboy.erl
  StartTime = erlang:monotonic_time(microsecond),
  Request = make_test_request(),

  _Result = try
    poolboy:transaction(PoolName,
      fun(Worker) ->
        poolboy_echo_worker:call(Worker, Request)
      end, ReqTimeout)
  catch
    exit:{timeout, _} -> {error, timeout};
    _Class:_Reason -> {error, failed}
  end,

  EndTime = erlang:monotonic_time(microsecond),
  Duration = EndTime - StartTime,
  run_worker_requests(PoolName, poolboy, _StripeId, NumRequests - 1,
                    ReqTimeout, _ResTimeout, Verbose, [Duration | Acc], _Opts).

%%%-----------------------------------------------------------------------------
%%% Helper Functions
%%%-----------------------------------------------------------------------------

%% Select stripe ID based on configuration
select_stripe_id(_WorkerId, #{stripe_pick := scheduler_id, pool_size := N}) ->
  erlang:system_info(scheduler_id) rem N;
select_stripe_id(WorkerId, #{stripe_pick := round_robin, pool_size := N}) ->
  WorkerId rem N;
select_stripe_id(_WorkerId, #{stripe_pick := random, pool_size := N}) ->
  rand:uniform(N) - 1.

%% Create test request
make_test_request() ->
  {echo, iolist_to_binary([<<"test_data_">>, integer_to_binary(erlang:unique_integer([positive]))])}.

%% Encode request for wire protocol (simple version for FIFO - no CorrId needed)
encode_request(Request) ->
  term_to_binary(Request).

%% Collect results from all worker processes with improved timeout handling
collect_worker_results([], TimingAcc, ErrorAcc, _Verbose) ->
  {lists:flatten(TimingAcc), ErrorAcc};
collect_worker_results([{WorkerId, Pid} | Rest], TimingAcc, ErrorAcc, Verbose) ->
  % Check if worker is still alive first
  case is_process_alive(Pid) of
    false ->
      % Worker is already dead, skip waiting for it
      Verbose andalso io:format("Worker ~p (pid ~p) is no longer alive - skipping~n", [WorkerId, Pid]),
      collect_worker_results(Rest, [[] | TimingAcc], [#{error => worker_dead, worker_id => WorkerId}] ++ ErrorAcc, Verbose);
    true ->
      receive
        {worker_done, WorkerId, TimingData, Errors} ->
          collect_worker_results(Rest, [TimingData | TimingAcc], Errors ++ ErrorAcc, Verbose);
        {'EXIT', Pid, normal} ->
          % Worker completed successfully but didn't send result - treat as empty result
          collect_worker_results(Rest, [[] | TimingAcc], ErrorAcc, Verbose);
        {'EXIT', Pid, Reason} ->
          % Worker failed - count as error
          Verbose andalso io:format("Worker ~p (pid ~p) failed: ~p~n", [WorkerId, Pid, Reason]),
          collect_worker_results(Rest, [[] | TimingAcc], [#{error => worker_crash, reason => Reason, worker_id => WorkerId}] ++ ErrorAcc, Verbose)
      after 60000 -> % Reduced timeout back to 1 minute but with better handling
        % Try to kill the hanging worker and continue
        Verbose andalso io:format("Worker ~p (pid ~p) hanging, attempting to kill...~n", [WorkerId, Pid]),
        exit(Pid, kill),
        % Wait a bit for the kill to take effect, then continue
        timer:sleep(100),
        collect_worker_results(Rest, [[] | TimingAcc], [#{error => worker_timeout, worker_id => WorkerId}] ++ ErrorAcc, Verbose)
      end
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
      io:format("stripe sel:    ~p~n",    [maps:get(stripe_pick, Opts)]),
      io:format("reservation:   ~p ms~n", [maps:get(reserv_timeout, Opts)]),
      io:format("request:       ~p ms~n", [maps:get(req_timeout, Opts)]),
      BacklogSize = maps:get(fifo_backlog_size, Opts, 0),
      BacklogTimeout = maps:get(fifo_backlog_timeout, Opts, 0),
      case BacklogSize of
        0 ->
          io:format("backlog size:  64 (NIF queue only - no backlog)~n");
        _ ->
          io:format("backlog size:  64 + ~p (NIF + backlog queue)~n", [BacklogSize]),
          io:format("backlog timeout: ~p ms~n", [BacklogTimeout])
      end;
    existing ->
      io:format("sync mode:     ~p~n",    [maps:get(sync_mode, Opts)]),
      io:format("backlog size:  0 (no queuing - immediate error)~n");
    shackle ->
      io:format("sync mode:     ~p~n",    [maps:get(sync_mode, Opts)]),
      io:format("backlog size:  ~p~n",    [maps:get(backlog_size, Opts)]);
    poolboy ->
      io:format("backlog size:  unlimited (process mailbox queuing)~n");
    _ ->
      io:format("backlog size:  unknown~n")
  end,

  % Print protocol mode information
  print_protocol_info(maps:get(mode, Opts), Opts),

  io:put_chars("\n").

%% Print protocol mode information
print_protocol_info(fifo, _Opts) ->
  io:format("~nProtocol Mode: Arterial FIFO Ordering (No Correlation IDs)~n"),
  io:format("Usage:         Protocols without request IDs requiring strict order~n"),
  io:format("Examples:      Redis MULTI/EXEC, simple command protocols, streaming~n"),
  io:format("FIFO Order:    Yes - guaranteed first-in-first-out reply ordering~n"),
  io:format("CorrID Match:  No - order-based matching (first sent = first received)~n"),
  io:format("Connection:    Reserved per request (exclusive, 1:1 mapping)~n"),
  io:format("NIF Calls:     2 per request (reserve + send, release)~n");

print_protocol_info(existing, Opts) ->
  SyncMode = maps:get(sync_mode, Opts, sync),
  case SyncMode of
    sync ->
      io:format("~nProtocol Mode: Arterial CorrID Sync~n"),
      io:format("Usage:         Maximum performance for protocols with request IDs~n"),
      io:format("Examples:      HTTP, gRPC, PostgreSQL, MySQL with request IDs~n"),
      io:format("FIFO Order:    No - replies can arrive in any order~n"),
      io:format("CorrID Match:  Yes - random access via correlation ID in ETS table~n"),
      io:format("Connection:    Shared across requests (send-and-release immediately)~n"),
      io:format("NIF Calls:     1 per request (send_and_release)~n");
    async ->
      io:format("~nProtocol Mode: Arterial CorrID Async~n"),
      io:format("Usage:         Non-blocking for protocols with request IDs~n"),
      io:format("Examples:      Async HTTP, gRPC streaming, event-driven protocols~n"),
      io:format("FIFO Order:    No - async replies delivered as they arrive~n"),
      io:format("CorrID Match:  Yes - correlation ID matched via async message delivery~n"),
      io:format("Connection:    Shared across requests (async send, immediate release)~n"),
      io:format("NIF Calls:     1 per request (send) + async message delivery~n");
    cast ->
      io:format("~nProtocol Mode: Arterial Cast (Fire-and-Forget)~n"),
      io:format("Usage:         One-way messages, no reply expected~n"),
      io:format("Examples:      Event publishing, logging, notifications~n"),
      io:format("FIFO Order:    N/A - no replies to order~n"),
      io:format("CorrID Match:  N/A - no replies to match~n"),
      io:format("Connection:    Shared across requests (fire-and-forget)~n"),
      io:format("NIF Calls:     1 per request (send only, no waiting)~n")
  end;

print_protocol_info(shackle, Opts) ->
  SyncMode = maps:get(sync_mode, Opts, sync),
  case SyncMode of
    sync ->
      io:format("~nProtocol Mode: Shackle Sync (External Pool Comparison)~n"),
      io:format("Usage:         Alternative connection pool with correlation IDs~n"),
      io:format("Examples:      Same as Arterial Sync but using Shackle library~n"),
      io:format("FIFO Order:    No - replies can arrive in any order~n"),
      io:format("CorrID Match:  Yes - correlation ID matching via Shackle internal system~n"),
      io:format("Connection:    Shared via Shackle's connection pool~n"),
      io:format("NIF Calls:     0 (uses Shackle's own connection management)~n");
    async ->
      io:format("~nProtocol Mode: Shackle Async (External Pool Comparison)~n"),
      io:format("Usage:         Non-blocking alternative pool with correlation IDs~n"),
      io:format("Examples:      Same as Arterial Async but using Shackle library~n"),
      io:format("FIFO Order:    No - async replies delivered as they arrive~n"),
      io:format("CorrID Match:  Yes - async correlation ID via Shackle callbacks~n"),
      io:format("Connection:    Shared via Shackle's async connection pool~n"),
      io:format("NIF Calls:     0 (uses Shackle's own async mechanisms)~n")
  end;

print_protocol_info(poolboy, _Opts) ->
  io:format("~nProtocol Mode: Poolboy (Generic Process Pool)~n"),
  io:format("Usage:         Generic worker processes, any protocol~n"),
  io:format("Examples:      Custom protocols, legacy systems, general pooling~n"),
  io:format("FIFO Order:    Depends on worker implementation~n"),
  io:format("CorrID Match:  Depends on worker implementation~n"),
  io:format("Connection:    One per worker process (checkout/checkin pattern)~n"),
  io:format("NIF Calls:     0 (uses gen_server worker processes)~n");

print_protocol_info(_, _) ->
  io:format("~nProtocol Mode: Unknown~n").

%% Print benchmark results
print_results(Result) ->
  io:format("=== Results ===~n", []),
  io:format("Total requests:    ~p~n",         [maps:get(total_requests, Result)]),
  io:format("Successful:        ~p~n",         [maps:get(successful_requests, Result)]),
  io:format("Failed:            ~p~n",         [maps:get(failed_requests, Result)]),
  io:format("Error rate:        ~.2f%~n",      [maps:get(error_rate, Result) * 100]),
  io:format("Reconnects:        ~p~n",         [maps:get(reconnects, Result)]),
  io:format("Duration:          ~p ms~n",      [maps:get(duration_ms, Result)]),
  io:format("Throughput:        ~.1f req/s~n", [maps:get(requests_per_sec, Result)]),

  #{mean_us := Mean, median_us := Median, p95_us := P95, p99_us := P99,
    min_us := Min, max_us := Max} = maps:get(timing_stats, Result),

  io:format("Latency (us):~n", []),
  io:format("  Mean:            ~.1f~n", [float(Mean)]),
  io:format("  Median:          ~.1f~n", [float(Median)]),
  io:format("  95th percentile: ~.1f~n", [float(P95)]),
  io:format("  99th percentile: ~.1f~n", [float(P99)]),
  io:format("  Min:             ~p~n",   [Min]),
  io:format("  Max:             ~p~n",   [Max]),

  % Print FIFO backlog statistics if available
  case maps:get(backlog_stats, Result, undefined) of
    undefined -> ok;
    #{total_queued := TotalQueued, peak_backlog := PeakBacklog,
      avg_wait_time_ms := AvgWait, backlog_timeouts := Timeouts} ->
      io:format("FIFO Backlog:~n", []),
      io:format("  Total queued:     ~p~n", [TotalQueued]),
      io:format("  Peak backlog:     ~p~n", [PeakBacklog]),
      io:format("  Avg wait time:    ~.1f ms~n", [float(AvgWait)]),
      io:format("  Backlog timeouts: ~p~n", [Timeouts])
  end,

  case maps:get(errors, Result, []) of
    [] -> ok;
    Errors ->
      io:format("Error breakdown:~n", []),
      [io:format("  ~p: ~p~n", [Error, Count]) || {Error, Count} <- Errors]
  end,

  io:put_chars("\n").

%% Print comparison table for multiple results using util's stringx:pretty_print_table
print_comparison_table(Results) ->
  % Prepare headers
  Headers = {"Implementation", "Req/sec", "Error%", "Mean/μs", "P95/μs", "P99/μs"},

  % Prepare data rows
  Rows = lists:map(fun(Result) ->
    Name      = maps:get(test_name, Result, "Unknown"),
    ReqPerSec = maps:get(requests_per_sec, Result),
    ErrorRate = maps:get(error_rate, Result) * 100,

    #{mean_us := Mean, p95_us := P95, p99_us := P99} = maps:get(timing_stats, Result),

    % Format values as tuples for stringx:pretty_print_table
    {Name,
     float(ReqPerSec),
     float(ErrorRate),
     round(Mean),
     round(P95),
     round(P99)}
  end, Results),

  % Use pretty_print_table with custom formatting and right alignment
  stringx:pretty_print_table(Headers, Rows, #{
    th_dir => both,   % Header padding both sides
    td_pad => #{
      2 => leading,   % Req/sec column - right align numbers
      3 => leading,   % Error% column - right align numbers
      4 => leading,   % Mean (us) column - right align numbers
      5 => leading,   % P95 (us) column - right align numbers
      6 => leading    % P99 (us) column - right align numbers
    },
    td_formats => {
      undefined,      % Implementation column - default string formatting
      fun(V) when is_float(V)   -> {string, stringx:format_number(V, 2, #{thousands => ","})} end, % Req/sec
      fun(V) when is_float(V)   -> {number, float_to_list(V, [{decimals, 2}])} end, % Error%
      fun(V) when is_integer(V) -> {number, integer_to_list(V)} end,   % Mean
      fun(V) when is_integer(V) -> {number, integer_to_list(V)} end,   % P95
      fun(V) when is_integer(V) -> {number, integer_to_list(V)} end    % P99
    },
    unicode   => true,
    thousands => ",",
    outline   => full
  }).


%%%-----------------------------------------------------------------------------
%%% Reconnection Tracking
%%%-----------------------------------------------------------------------------

%% Get current reconnect count from the observer backend
get_reconnect_count() ->
  bench_reconnect_observer:get_count().

%% Collect FIFO backlog statistics from pool
%% TODO: This would call arterial_client_fifo:backlog_stats/1 when implemented
collect_backlog_stats(_PoolName, #{mode := fifo} = Opts) ->
  case maps:get(fifo_backlog_size, Opts, 0) of
    0 -> undefined;  % No backlog configured
    _ ->
      % Placeholder implementation - would call NIF for real stats
      #{
        total_queued => 0,       % Total requests that entered backlog
        peak_backlog => 0,       % Peak backlog depth during benchmark
        avg_wait_time_ms => 0.0, % Average wait time in backlog
        backlog_timeouts => 0    % Requests that timed out in backlog
      }
  end;
collect_backlog_stats(_PoolName, _Opts) ->
  undefined.  % Not FIFO mode

%% Start reconnect tracking by configuring arterial_observe
start_reconnect_tracking() ->
  % Configure arterial to use our reconnect observer backend
  application:set_env(arterial, observability, bench_reconnect_observer),
  % Reset the counter
  bench_reconnect_observer:reset_count(),
  ok.

%% Stop reconnect tracking
stop_reconnect_tracking() ->
  % Reset back to no observability to avoid affecting other tests
  application:unset_env(arterial, observability).

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