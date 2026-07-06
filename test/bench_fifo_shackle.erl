-module(bench_fifo_shackle).

-moduledoc """
Shackle benchmark in FIFO Mode 3 configuration: `pool_size` connections,
`pool_size` workers, `backlog_size => 1` — one in-flight request per
connection per worker, each worker dedicated to one connection.

This is the direct apples-to-apples comparison point for
`bench_arterial_fifo`: arterial FIFO Mode 3 reserves a connection, sends
one request, waits for the reply, then releases; shackle with
`backlog_size => 1` and `pool_size` workers achieves the same exclusivity
through its own pool dispatch (one outstanding request per connection).

Both benchmarks drive the same `test_tcp_server` over loopback TCP with
the same wire framing (`test_echo_protocol:frame/2`/`unframe/1`) so the
numbers are directly comparable.

Run via `make bench-fifo-shackle` or:

```
$ rebar3 as test shell
1> bench_fifo_shackle:bench().
2> bench_fifo_shackle:bench(#{pool_size => 16, workers => 16}).
```
""".

-export([bench/0, bench/1, bench_help/1, help/0]).

-define(POOL, fifo_shackle_bench_pool).

-type opts() :: #{
  pool_size      => pos_integer(),
  workers        => pos_integer(),
  duration       => pos_integer(),
  requests       => pos_integer() | undefined,
  payload        => binary(),
  external_server => boolean()
}.

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc "Equivalent to `bench/1` with the defaults shown in the moduledoc.".
-spec bench() -> ok.
bench() -> bench(#{}).

-doc false.
bench_help(_) -> help().

-doc """
Print supported options.
""".
-spec help() -> ok.
help() ->
  io:format(
    "bench_fifo_shackle:bench(Opts) -- Opts is a map with any of:~n"
    "~n"
    "  pool_size     => pos_integer()  (default: 8)~n"
    "                   Number of shackle connections (= number of workers).~n"
    "  workers       => pos_integer()  (default: pool_size)~n"
    "                   Number of concurrent benchmark workers.~n"
    "  duration      => pos_integer()  (default: 5000)~n"
    "                   Benchmark duration in milliseconds.~n"
    "  payload       => binary()  (default: 32-byte binary)~n"
    "                   Request payload echoed to the server.~n"
    "  external_server => boolean()  (default: false)~n"
    "                   Use an already-running server instead of~n"
    "                   starting one.~n"
    "~n"
    "Examples:~n"
    "  bench_fifo_shackle:bench().~n"
    "  bench_fifo_shackle:bench(#{pool_size => 16}).~n"
  ).

-doc """
Run the FIFO-equivalent shackle benchmark with the given options.

Workers: `pool_size` (one per connection, mirroring FIFO Mode 3).
Backlog: 1 per connection (one in-flight request at a time, like FIFO).
""".
-spec bench(opts()) -> ok.
bench(Opts) ->
  KnownKeys = [pool_size, workers, duration, requests,
               payload, external_server],
  UnknownKeys = maps:keys(maps:without(KnownKeys, Opts)),
  case UnknownKeys of
    [] ->
      ok;
    _ ->
      error({unrecognized_bench_opts, UnknownKeys})
  end,

  PoolSize    = maps:get(pool_size, Opts, 8),
  Workers     = maps:get(workers, Opts, PoolSize),
  Duration    = maps:get(duration, Opts, 5000),
  Requests    = maps:get(requests, Opts, undefined),
  Payload     = maps:get(payload, Opts, {echo, bench_payload}),
  ExtServer   = maps:get(external_server, Opts, false),

  io:format("=== shackle FIFO-equivalent benchmark ===~n"),
  io:format("pool size:     ~p~n", [PoolSize]),
  io:format("workers:       ~p~n", [Workers]),
  case Requests of
    undefined -> io:format("duration:      ~p ms~n", [Duration]);
    N         ->
      io:format("requests:      ~p per worker (~p total)~n",
                [N, N * Workers])
  end,
  io:format("backlog:       1 (one in-flight per connection, like FIFO)~n"),

  ok = ensure_shackle_started(),
  Srv = setup(PoolSize, ExtServer),

  try
    {T, Rejected} = run(Workers, Duration, Requests, Payload),
    TotalReqs  = length(T),
    DurationMs = case Requests of
      undefined -> Duration;
      _         -> lists:sum(T) div 1000
    end,
    ActualMs = max(1, DurationMs),
    Throughput = TotalReqs * 1000 / ActualMs,

    Sorted = lists:sort(T),
    P50  = percentile(Sorted, 0.50),
    P95  = percentile(Sorted, 0.95),
    P99  = percentile(Sorted, 0.99),
    Max  = case Sorted of [] -> 0; _ -> lists:last(Sorted) end,

    io:format("duration:      ~p ms (target), ~p ms (actual)~n",
              [Duration, ActualMs]),
    io:format("requests:      ~p~n", [TotalReqs]),
    io:format("rejected:      ~p (no_server/timeout)~n", [Rejected]),
    io:format("throughput:    ~.1f req/s~n", [float(Throughput)]),
    io:format("latency (µs): p50=~p p95=~p p99=~p max=~p~n",
              [P50, P95, P99, Max])
  after
    teardown(Srv)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

ensure_shackle_started() ->
  case application:ensure_all_started(shackle) of
    {ok,         _} -> ok;
    {error, Reason} -> error({shackle_start_failed, Reason})
  end.

setup(PoolSize, ExternalServer) ->
  Srv = bench_external_server:start(ExternalServer),
  Port = bench_external_server:port(Srv),
  ok = shackle_pool:start(?POOL, shackle_echo_client, [
    {address, "127.0.0.1"},
    {port, Port},
    {reconnect, true},
    %% shackle_server resets to {active,true} without binary after setup;
    %% shackle_echo_client:setup/2 re-applies binary to compensate.
    {socket_options, [{mode, binary}, {active, true}, {packet, 0}]}
  ], [
    {pool_size, PoolSize},
    {pool_strategy, round_robin},
    %% One in-flight request per connection — equivalent to FIFO Mode 3
    {backlog_size, 1}
  ]),
  wait_until_available(PoolSize, 200),
  Srv.

teardown(Srv) ->
  ok = shackle_pool:stop(?POOL),
  bench_external_server:stop(Srv).

wait_until_available(_N, 0) ->
  error(pool_not_ready);
wait_until_available(N, Retries) ->
  case shackle:call(?POOL, {echo, ready}, 200) of
    ready -> ok;
    _     ->
      timer:sleep(20),
      wait_until_available(N, Retries - 1)
  end.

run(Workers, Duration, Requests, Payload) ->
  Parent   = self(),
  Deadline = erlang:monotonic_time(millisecond) + Duration,
  Pids = [spawn_link(fun() ->
    Result = worker_loop(?POOL, Deadline, Requests, Payload, [], 0),
    Parent ! {done, self(), Result}
  end) || _ <- lists:seq(1, Workers)],
  collect(Pids, [], 0).

collect([], Acc, Rej) ->
  {lists:flatten(Acc), Rej};
collect([Pid | Rest], Acc, Rej) ->
  receive
    {done, Pid, {Timings, R}} ->
      collect(Rest, [Timings | Acc], Rej + R)
  after 60000 ->
    error(workers_timeout)
  end.

worker_loop(_Pool, _Deadline, 0, _Payload, Timings, Rej) ->
  {lists:reverse(Timings), Rej};
worker_loop(Pool, Deadline, Remaining, Payload, Timings, Rej) ->
  Now = erlang:monotonic_time(millisecond),
  Done = Remaining =:= undefined andalso Now >= Deadline,
  if Done ->
    {lists:reverse(Timings), Rej};
  true ->
    T0 = erlang:monotonic_time(microsecond),
    {NewTimings, NewRej} =
      case shackle:call(Pool, Payload, 5000) of
        {error, Reason}
            when Reason =:= no_server; Reason =:= timeout ->
          {Timings, Rej + 1};
        _ ->
          T1 = erlang:monotonic_time(microsecond),
          {[T1 - T0 | Timings], Rej}
      end,
    Next = case Remaining of undefined -> undefined; N -> N - 1 end,
    worker_loop(Pool, Deadline, Next, Payload, NewTimings, NewRej)
  end.

percentile([], _) -> 0;
percentile(Sorted, P) ->
  Len = length(Sorted),
  lists:nth(max(1, round(P * Len)), Sorted).
