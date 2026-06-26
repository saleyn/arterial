-module(bench_compare).

-moduledoc """
Comparison benchmark runner that executes bench_arterial, bench_shackle,
and bench_poolboy sequentially with the same configuration and presents
the results in a comparison table.

This allows for direct performance comparison between arterial's connection
pool implementation and the third-party alternatives (shackle, poolboy).
""".

-export([bench/0, bench/1, help/0, bench_help/1, plot_scaling/0, plot_scaling/1]).

-type opts() :: #{
  pool_size       => pos_integer(),
  workers         => pos_integer(),
  duration        => pos_integer(),
  payload         => binary(),
  external_server => boolean(),
  plot            => boolean(),
  restart_server  => boolean(),
  wait_between    => non_neg_integer()
}.

-type plot_opts() :: #{
  pool_sizes      => [pos_integer()],
  duration        => pos_integer(),
  payload         => binary(),
  external_server => boolean(),
  output_file     => string(),
  restart_server  => boolean(),
  wait_between    => non_neg_integer()
}.

-define(DEFAULT_OPTS, #{
  pool_size       => 8,
  workers         => 8,
  duration        => 5,
  payload         => <<"the quick brown fox jumps over the lazy dog">>,
  external_server => true,
  plot            => false,
  restart_server  => false,  % Use single long-lived server by default
  wait_between    => 1       % 1 second wait between tests by default
}).

-doc "Run comparison benchmark with default options.".
-spec bench() -> ok.
bench() -> bench(?DEFAULT_OPTS).

-doc """
Run comparison benchmark with custom options.
Sequentially runs bench_arterial, bench_shackle, and bench_poolboy
with the same configuration and presents results in a comparison table.
""".
-spec bench(opts()) -> ok.
bench(Opts) ->
  FinalOpts = maps:merge(?DEFAULT_OPTS, Opts),

  case maps:get(plot, FinalOpts, false) of
    true ->
      plot_scaling(FinalOpts);
    false ->
      io:format("~n=== Connection Pool Performance Comparison ===~n"),
      io:format("Configuration: ~p~n~n", [FinalOpts]),

      % Run benchmarks and collect results
      % Remove plotting-specific options that individual benchmarks don't understand
      BenchOpts = maps:without([plot], FinalOpts),
      WaitBetweenS = maps:get(wait_between, FinalOpts, 1),

      ArterialResult = run_benchmark(arterial, bench_arterial, BenchOpts),
      wait_between_tests(WaitBetweenS),

      ShackleResult = run_benchmark(shackle, bench_shackle, BenchOpts),
      wait_between_tests(WaitBetweenS),

      PoolboyResult = run_benchmark(poolboy, bench_poolboy, BenchOpts),

      Results = [ArterialResult, ShackleResult, PoolboyResult],

      % Display results table
      display_results_table(Results),
      ok
  end.

-doc "Display help information.".
-spec help() -> ok.
help() ->
  io:format(
    "bench_compare:bench(Opts) -- Run all pool benchmarks and compare results~n"
    "bench_compare:plot_scaling(Opts) -- Generate throughput scaling plots~n"
    "~n"
    "bench(Opts) - Opts is a map with any of:~n"
    "  pool_size       => pos_integer()  (default: 8)~n"
    "  workers         => pos_integer()  (default: 8)~n"
    "  duration        => pos_integer()  (default: 5) - in seconds~n"
    "  payload         => binary()       (default: fixed test string)~n"
    "  external_server => boolean()      (default: true)~n"
    "  plot            => boolean()      (default: false) - enable scaling plot~n"
    "  restart_server  => boolean()      (default: false) - restart server between tests~n"
    "  wait_between    => non_neg_integer() (default: 1) - seconds wait time between tests~n"
    "~n"
    "plot_scaling(Opts) - Opts is a map with any of:~n"
    "  pool_sizes      => [pos_integer()] (default: [2,4,8,16,32,64])~n"
    "  duration        => pos_integer()   (default: 3) - in seconds~n"
    "  external_server => boolean()       (default: true)~n"
    "  output_file     => string()        (default: \"pool_scaling.png\")~n"
    "  restart_server  => boolean()       (default: false) - restart server between tests~n"
    "  wait_between  => non_neg_integer() (default: 1) - seconds wait time between tests~n"
    "~n"
    "Examples:~n"
    "  1> bench_compare:bench().~n"
    "  2> bench_compare:bench(#{pool_size => 16, duration => 10}).~n"
    "  3> bench_compare:bench(#{plot => true}).~n"
    "  4> bench_compare:plot_scaling().~n"
    "  5> bench_compare:plot_scaling(#{pool_sizes => [4,8,16]}).~n"
    "  6> bench_compare:plot_scaling(#{restart_server => true, wait_between => 2}).~n"
    "  7> bench_compare:bench(#{wait_between => 0}).  % No waits between tests~n"
    "~n"
  ),
  ok.

-doc "Help function for Makefile compatibility.".
-spec bench_help(opts()) -> ok.
bench_help(_Opts) -> help().

-doc "Run scaling plot with default options.".
-spec plot_scaling() -> ok.
plot_scaling() ->
  plot_scaling(#{}).

-doc """
Generate throughput vs pool_size scaling plot.
Runs benchmarks across multiple pool sizes and generates a gnuplot visualization.
""".
-spec plot_scaling(plot_opts()) -> ok.
plot_scaling(Opts) ->
  DefaultPlotOpts = #{
    pool_sizes => [2, 4, 8, 16, 32, 64],
    duration => 3,  % Shorter for scaling tests
    payload => <<"the quick brown fox jumps over the lazy dog">>,
    external_server => true,  % Better for scaling tests
    output_file => "pool_scaling.png",
    restart_server => false,  % Use single long-lived server by default
    wait_between => 1       % 1 second wait between tests by default
  },
  FinalOpts = maps:merge(DefaultPlotOpts, Opts),

  PoolSizes = maps:get(pool_sizes, FinalOpts),
  Duration = maps:get(duration, FinalOpts),
  Payload = maps:get(payload, FinalOpts),
  ExternalServer = maps:get(external_server, FinalOpts),
  OutputFile = maps:get(output_file, FinalOpts),
  RestartServer = maps:get(restart_server, FinalOpts),
  WaitBetweenS = maps:get(wait_between, FinalOpts),

  io:format("~n=== Pool Scaling Analysis ===~n"),
  io:format("Testing pool sizes: ~p~n", [PoolSizes]),
  io:format("Duration per test: ~w seconds~n", [Duration]),
  io:format("External server: ~p~n", [ExternalServer]),
  io:format("Restart server between tests: ~p~n", [RestartServer]),
  io:format("Wait between tests: ~w seconds~n~n", [WaitBetweenS]),

  % Collect results for each pool size
  AllResults = lists:foldl(fun(PoolSize, Acc) ->
    io:format("Testing pool_size=~w...~n", [PoolSize]),

    TestOpts = #{
      pool_size => PoolSize,
      workers => PoolSize,  % Match workers to pool size
      duration => Duration,
      payload => Payload,
      external_server => ExternalServer
    },

    % Run benchmarks with wait times between them
    ArterialResult = run_benchmark(arterial, bench_arterial, TestOpts),
    wait_between_tests(WaitBetweenS),

    ShackleResult = run_benchmark(shackle, bench_shackle, TestOpts),
    wait_between_tests(WaitBetweenS),

    PoolboyResult = run_benchmark(poolboy, bench_poolboy, TestOpts),

    Results = [ArterialResult, ShackleResult, PoolboyResult],

    % Add cleanup wait if restart_server is true (force cleanup)
    if RestartServer -> wait_between_tests(WaitBetweenS);
       true -> ok
    end,

    % Wait between pool sizes (except for the last one)
    LastPoolSize = lists:last(PoolSizes),
    case PoolSize =:= LastPoolSize of
      false -> wait_between_tests(WaitBetweenS);
      true -> ok
    end,

    [{PoolSize, Results} | Acc]
  end, [], PoolSizes),

  % Reverse to maintain original order
  FinalResults = lists:reverse(AllResults),

  % Generate data files and plot
  generate_scaling_plot(FinalResults, OutputFile),
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

-record(bench_result, {
  name        :: atom(),
  throughput  :: float(),
  latency_p50 :: non_neg_integer(),
  latency_p95 :: non_neg_integer(),
  latency_p99 :: non_neg_integer(),
  latency_max :: non_neg_integer(),
  rejected    :: non_neg_integer(),
  error       :: term() | undefined
}).

wait_between_tests(0) -> ok;
wait_between_tests(Seconds) when Seconds > 0 ->
  io:format("Waiting ~w seconds...~n", [Seconds]),
  timer:sleep(Seconds * 1000),
  ok.

run_benchmark(Name, Module, Opts) ->
  io:format("Running ~s benchmark...~n", [Name]),
  try
    % Remove bench_compare-specific options that benchmarks don't understand
    CleanOpts = maps:without([restart_server, wait_between], Opts),
    % Capture the output to parse results
    {Result, Output} = capture_benchmark_output(Module, CleanOpts),
    case Result of
      ok ->
        ParsedResult = parse_benchmark_output(Name, Output),
        io:format("~s: ~.1f req/s~n", [Name, ParsedResult#bench_result.throughput]),
        ParsedResult;
      Error ->
        io:format("~s: FAILED (~p)~n", [Name, Error]),
        #bench_result{name = Name, error = Error, throughput = 0.0,
                      latency_p50 = 0, latency_p95 = 0, latency_p99 = 0,
                      latency_max = 0, rejected = 0}
    end
  catch
    Class:Reason:Stack ->
      CrashError = {Class, Reason, Stack},
      io:format("~s: CRASHED (~p)~n", [Name, CrashError]),
      #bench_result{name = Name, error = CrashError, throughput = 0.0,
                    latency_p50 = 0, latency_p95 = 0, latency_p99 = 0,
                    latency_max = 0, rejected = 0}
  end.

capture_benchmark_output(Module, Opts) ->
  % Create a temporary file to capture benchmark output
  TempFile = "/tmp/bench_output_" ++ integer_to_list(rand:uniform(1000000)),
  {ok, Device} = file:open(TempFile, [write]),

  % Redirect stdout to the temporary file
  OldGroupLeader = group_leader(),
  group_leader(Device, self()),

  try
    Result = Module:bench(Opts),
    file:close(Device),
    {ok, Output} = file:read_file(TempFile),
    {Result, binary_to_list(Output)}
  after
    group_leader(OldGroupLeader, self()),
    file:delete(TempFile)
  end.

parse_benchmark_output(Name, Output) ->
  % Parse throughput (req/s)
  Throughput = case re:run(Output, "throughput:\\s+([0-9.]+)\\s+req/s",
                          [{capture, [1], list}]) of
    {match, [ThroughputStr]} -> list_to_float(ThroughputStr);
    nomatch -> 0.0
  end,

  % Parse latency percentiles
  {P50, P95, P99, Max} = case re:run(Output,
      "latency \\(µs\\): p50=([0-9]+) p95=([0-9]+) p99=([0-9]+) max=([0-9]+)",
      [{capture, [1,2,3,4], list}]) of
    {match, [P50Str, P95Str, P99Str, MaxStr]} ->
      {list_to_integer(P50Str), list_to_integer(P95Str),
       list_to_integer(P99Str), list_to_integer(MaxStr)};
    nomatch -> {0, 0, 0, 0}
  end,

  % Parse rejected requests
  Rejected = case re:run(Output, "rejected:\\s+([0-9]+)",
                        [{capture, [1], list}]) of
    {match, [RejectedStr]} -> list_to_integer(RejectedStr);
    nomatch -> 0
  end,

  #bench_result{
    name = Name,
    throughput = Throughput,
    latency_p50 = P50,
    latency_p95 = P95,
    latency_p99 = P99,
    latency_max = Max,
    rejected = Rejected,
    error = undefined
  }.

display_results_table(Results) ->
  io:format("~n=== Performance Comparison Results ===~n"),
  io:format("┌─────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┬──────────┐~n"),
  io:format("│ Library     │ Throughput  │   P50   │   P95   │   P99   │   Max   │ Rejected │~n"),
  io:format("│             │   (req/s)   │  (µs)   │  (µs)   │  (µs)   │  (µs)   │          │~n"),
  io:format("├─────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┼──────────┤~n"),

  ValidResults = [R || R <- Results, R#bench_result.error =:= undefined],
  SortedResults = lists:sort(fun(A, B) ->
    A#bench_result.throughput > B#bench_result.throughput
  end, ValidResults),

  lists:foreach(fun(Result) ->
    case Result#bench_result.error of
      undefined ->
        io:format("│ ~-11s │ ~11.1f │ ~7w │ ~7w │ ~7w │ ~7w │ ~8w │~n", [
          Result#bench_result.name,
          Result#bench_result.throughput,
          Result#bench_result.latency_p50,
          Result#bench_result.latency_p95,
          Result#bench_result.latency_p99,
          Result#bench_result.latency_max,
          Result#bench_result.rejected
        ]);
      _Error ->
        io:format("│ ~-11s │    FAILED   │  ERROR  │  ERROR  │  ERROR  │  ERROR  │  ERROR   │~n", [
          Result#bench_result.name
        ])
    end
  end, Results),

  io:format("└─────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┴──────────┘~n"),

  % Add performance ranking if we have valid results
  case SortedResults of
    [] ->
      io:format("~nNo successful benchmark runs to rank.~n");
    [Best | _] ->
      io:format("~nPerformance Ranking (by throughput):~n"),
      lists:foldl(fun(Result, Rank) ->
        Ratio = if Best#bench_result.throughput > 0 ->
                     Result#bench_result.throughput / Best#bench_result.throughput;
                   true -> 0.0
                end,
        io:format("  ~w. ~-8s: ~9.1f req/s (~5.1f% of best)~n", [
          Rank, Result#bench_result.name,
          Result#bench_result.throughput, Ratio * 100
        ]),
        Rank + 1
      end, 1, SortedResults),
      io:format("~n")
  end.

generate_scaling_plot(AllResults, OutputFile) ->
  % Extract data for each library
  ArterialData = extract_library_data(AllResults, arterial),
  ShackleData = extract_library_data(AllResults, shackle),
  PoolboyData = extract_library_data(AllResults, poolboy),

  % Write data files
  ArterialFile = "/tmp/arterial_scaling.dat",
  ShackleFile = "/tmp/shackle_scaling.dat",
  PoolboyFile = "/tmp/poolboy_scaling.dat",

  write_data_file(ArterialFile, ArterialData),
  write_data_file(ShackleFile, ShackleData),
  write_data_file(PoolboyFile, PoolboyData),

  % Generate gnuplot script
  PlotScript = "/tmp/scaling_plot.gnuplot",
  write_gnuplot_script(PlotScript, OutputFile, ArterialFile, ShackleFile, PoolboyFile),

  % Execute gnuplot
  case os:cmd("which gnuplot") of
    "" ->
      io:format("~nWarning: gnuplot not found. Data files saved:~n"),
      io:format("  Arterial: ~s~n", [ArterialFile]),
      io:format("  Shackle:  ~s~n", [ShackleFile]),
      io:format("  Poolboy:  ~s~n", [PoolboyFile]),
      display_scaling_table(AllResults);
    _GnuplotPath ->
      Command = "GNUTERM=png gnuplot " ++ PlotScript,
      io:format("Executing: ~s~n", [Command]),
      case os:cmd(Command) of
        "" ->
          io:format("Plot generated: ~s~n", [OutputFile]),
          display_scaling_table(AllResults);
        Error ->
          io:format("Gnuplot error: ~s~n", [Error]),
          io:format("~nTry: sudo pacman -Syu && sudo pacman -Rns gnuplot && sudo pacman -S gnuplot~n"),
          display_scaling_table(AllResults)
      end
  end,

  % Clean up temporary files
  file:delete(ArterialFile),
  file:delete(ShackleFile),
  file:delete(PoolboyFile),
  file:delete(PlotScript).

extract_library_data(AllResults, Library) ->
  lists:filtermap(fun({PoolSize, Results}) ->
    case lists:keyfind(Library, #bench_result.name, Results) of
      #bench_result{error = undefined, throughput = Throughput} ->
        {true, {PoolSize, Throughput}};
      _ ->
        false
    end
  end, AllResults).

write_data_file(Filename, Data) ->
  {ok, File} = file:open(Filename, [write]),
  lists:foreach(fun({PoolSize, Throughput}) ->
    io:format(File, "~w ~.1f~n", [PoolSize, Throughput])
  end, Data),
  file:close(File).

write_gnuplot_script(ScriptFile, OutputFile, ArterialFile, ShackleFile, PoolboyFile) ->
  {ok, File} = file:open(ScriptFile, [write]),
  Script = io_lib:format(
    "# Force non-interactive mode to avoid wxWidgets issues~n"
    "set terminal png size 600,400~n"
    "set output '~s'~n"
    "set title 'Connection Pool Throughput Scaling'~n"
    "set xlabel 'Pool Size'~n"
    "set ylabel 'Throughput (req/s)'~n"
    "set grid~n"
    "set key top left~n"
    "set logscale x 2~n"
    "set xtics (2,4,8,16,32,64)~n"
    "set style line 1 lc rgb '#d62728' lt 1 lw 2 pt 7 ps 1.2~n"
    "set style line 2 lc rgb '#2ca02c' lt 1 lw 2 pt 5 ps 1.2~n"
    "set style line 3 lc rgb '#1f77b4' lt 1 lw 2 pt 9 ps 1.2~n"
    "plot '~s' using 1:2 with linespoints ls 1 title 'Arterial', \\~n"
    "     '~s' using 1:2 with linespoints ls 2 title 'Shackle', \\~n"
    "     '~s' using 1:2 with linespoints ls 3 title 'Poolboy'~n",
    [OutputFile, ArterialFile, ShackleFile, PoolboyFile]
  ),
  io:format(File, "~s", [Script]),
  file:close(File).

display_scaling_table(AllResults) ->
  io:format("~n=== Scaling Results Table ===~n"),
  io:format("┌───────────┬─────────────┬─────────────┬─────────────┐~n"),
  io:format("│ Pool Size │   Arterial  │   Shackle   │   Poolboy   │~n"),
  io:format("│           │   (req/s)   │   (req/s)   │   (req/s)   │~n"),
  io:format("├───────────┼─────────────┼─────────────┼─────────────┤~n"),

  lists:foreach(fun({PoolSize, Results}) ->
    ArterialTput = get_throughput(Results, arterial),
    ShackleTput = get_throughput(Results, shackle),
    PoolboyTput = get_throughput(Results, poolboy),

    io:format("│ ~9w │ ~11s │ ~11s │ ~11s │~n", [
      PoolSize,
      format_throughput(ArterialTput),
      format_throughput(ShackleTput),
      format_throughput(PoolboyTput)
    ])
  end, AllResults),

  io:format("└───────────┴─────────────┴─────────────┴─────────────┘~n").

get_throughput(Results, Library) ->
  case lists:keyfind(Library, #bench_result.name, Results) of
    #bench_result{error = undefined, throughput = Throughput} ->
      Throughput;
    _ ->
      0.0
  end.

format_throughput(Throughput) when Throughput =:= +0.0; Throughput =:= -0.0 ->
  "    FAILED ";
format_throughput(Throughput) ->
  io_lib:format("~10.1f ", [Throughput]).