-module(poolboy_bench).

-moduledoc """
Throughput/latency micro-benchmark for `poolboy`
(https://github.com/devinus/poolboy), driven against the same real
`test_tcp_server` over loopback TCP, the same wire framing
(`test_echo_protocol:frame/2`/`unframe/1`, via `poolboy_echo_worker`),
and the same workload shape as `arterial_bench`/`shackle_bench` -- so
all three numbers are directly comparable head-to-head rather than
measuring different fixtures.

poolboy has no protocol/wire awareness of its own (unlike shackle) and
no NIF-backed connection pool (unlike arterial) -- it just hands out a
`pid()` from `poolboy:checkout/3` for the caller to do whatever it
wants with. Each `poolboy_echo_worker` here owns one persistent TCP
connection and serves at most one in-flight request at a time (see that
module's moduledoc), so the structural shape is: worker process blocks
on `gen_server:call/3` into the pool worker, which itself blocks on
`gen_tcp:send/2` + `gen_tcp:recv/2`. That's the architecture comparable
to `arterial_bench`'s `sync` mode (caller's own process pays the socket
I/O cost directly, no separate async dispatcher in the loop) -- pairing
this against arterial's `async` mode (its own default, the fair
head-to-head against `shackle_bench`) would compare two different
architectures, not just two pool implementations.

There is only one `mode` here (poolboy doesn't offer a cast/receive
split the way shackle or arterial's async driver do), and no backlog
concept either -- each worker's underlying socket serves one request at
a time, same forced default as `arterial_bench`/`shackle_bench`.

Requires the `poolboy` dependency, only pulled in under the `test`
profile (see `rebar.config`) -- not part of `arterial`'s runtime deps.

Not part of the regular `rebar3 eunit` run (no `_test`/`_test_` exported
functions) -- run it via `make bench-poolboy` (see the top-level
Makefile) or explicitly from a shell:

```
$ rebar3 as test shell
1> poolboy_bench:bench().
=== poolboy benchmark ===
pool size:     8
max overflow:  0
strategy:      fifo
workers:       8
duration:      5000 ms (target), 5006 ms (actual)
requests:      98213
rejected:      0 (checkout timeouts, retried inline)
throughput:    19613.4 req/s
latency (µs): p50=398 p95=712 p99=940 max=3105
ok
```

Tune the load shape via `bench/1`; see `help/0` for the full option list.
Options mirror `arterial_bench:opts/0`/`shackle_bench:opts/0` wherever
the same concept applies (`pool_size`, `workers`, `duration_s`,
`payload`), so `make bench`, `make bench-shackle`, and `make
bench-poolboy` results line up directly:

```
$ make bench           BENCH_OPTS='pool_size=16 duration_s=10'
$ make bench-shackle   BENCH_OPTS='pool_size=16 duration_s=10'
$ make bench-poolboy   BENCH_OPTS='pool_size=16 duration_s=10'
```
""".

-export([bench/0, bench/1, help/0, bench_help/1]).

-type opts() :: #{
  pool_size    => pos_integer(),
  pool_strategy => fifo | lifo,
  workers      => pos_integer(),
  duration_s   => pos_integer(),
  payload      => binary(),
  external_server => boolean()
}.

-define(POOL, poolboy_bench_pool).

-doc "Equivalent to `bench/1` with the defaults shown in the moduledoc.".
-spec bench() -> ok.
bench() -> bench(#{}).

-doc false.
bench_help(_) -> help().

-doc """
Print the supported `t:opts/0` keys, their defaults, and how to pass them
-- from a shell (`poolboy_bench:help().`) or via `make bench-poolboy
BENCH_OPTS='key=value, ...'` (or `BENCH_OPTS='#{...}'` for values that
need real Erlang syntax).
""".
-spec help() -> ok.
help() ->
  io:format(
    "poolboy_bench:bench(Opts) -- Opts is a map with any of:~n"
    "~n"
    "  pool_size     => pos_integer()  (default: 8)~n"
    "                   Number of pooled connections to test_tcp_server.~n"
    "  pool_strategy => fifo | lifo  (default: fifo)~n"
    "                   poolboy's worker checkin/checkout order -- fifo is~n"
    "                   the closest match to arterial's default fifo pool~n"
    "                   order, for a fair comparison.~n"
    "  workers       => pos_integer()  (default: same as pool_size)~n"
    "                   Concurrent callers.~n"
    "  duration_s    => pos_integer()  (default: 5)~n"
    "                   How long to hammer the pool, in seconds.~n"
    "  payload       => binary()  (default: a short fixed binary)~n"
    "                   The {echo, Payload} request body sent on every call.~n"
    "  external_server => boolean()  (default: false)~n"
    "                   false: test_tcp_server runs in this VM, same as~n"
    "                   calling it directly. true: runs it as a standalone~n"
    "                   child erl node instead -- see~n"
    "                   bench_external_server's moduledoc for why (keeps~n"
    "                   test_tcp_server's per-request spawn/1 out of a~n"
    "                   perf profile taken on this VM).~n"
    "~n"
    "Passing an unrecognized option key raises~n"
    "{unrecognized_bench_opts, Keys} instead of silently ignoring it.~n"
    "~n"
    "Examples:~n"
    "  1> poolboy_bench:bench().~n"
    "  2> poolboy_bench:bench(#{pool_size => 16, duration_s => 10}).~n"
    "  3> poolboy_bench:bench(#{pool_strategy => lifo}).~n"
    "~n"
    "  $ make bench-poolboy~n"
  ),
  ok.

-doc """
Run the benchmark with `Opts` (see `t:opts/0`), print a summary, and
return `ok`. Starts and tears down its own `test_tcp_server` and
`poolboy` pool -- safe to call repeatedly from a live shell.
""".
-spec bench(opts()) -> ok.
bench(Opts) ->
  PoolSize0 = maps:get(pool_size, Opts, 8),
  Defaults = #{
    pool_size     => PoolSize0,
    pool_strategy => fifo,
    %% One worker per connection (every connection kept saturated, none
    %% idle), same default rationale as arterial_bench:bench/1.
    workers       => PoolSize0,
    duration_s    => 5,
    payload       => <<"the quick brown fox jumps over the lazy dog">>,
    external_server => false
  },
  case maps:keys(maps:without(maps:keys(Defaults), Opts)) of
    [] -> ok;
    UnknownKeys -> error({unrecognized_bench_opts, UnknownKeys})
  end,
  #{
    pool_size     := PoolSize,
    pool_strategy := PoolStrategy,
    workers       := Workers,
    duration_s    := DurationS,
    payload       := Payload,
    external_server := ExternalServer
  } = maps:merge(Defaults, Opts),

  {Srv, PoolPid} = setup(PoolSize, PoolStrategy, ExternalServer),
  try
    DurationMs = DurationS * 1000,
    Parent = self(),
    StartAt = erlang:monotonic_time(millisecond),
    Deadline = StartAt + DurationMs,

    WorkerPids = [
      spawn_link(fun() -> worker_loop(Parent, Deadline, Payload, []) end)
      || _ <- lists:seq(1, Workers)
    ],

    {AllLatenciesUs, Rejected} = collect(WorkerPids, [], 0),
    ElapsedMs = erlang:monotonic_time(millisecond) - StartAt,

    report(PoolSize, PoolStrategy, Workers, DurationMs, ElapsedMs, AllLatenciesUs, Rejected)
  after
    teardown(Srv, PoolPid)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

setup(PoolSize, PoolStrategy, ExternalServer) ->
  Srv = bench_external_server:start(ExternalServer),
  Port = bench_external_server:port(Srv),
  PoolArgs = [
    {name, {local, ?POOL}},
    {worker_module, poolboy_echo_worker},
    {size, PoolSize},
    {max_overflow, 0},
    {strategy, PoolStrategy}
  ],
  WorkerArgs = [{address, "127.0.0.1"}, {port, Port}],
  {ok, PoolPid} = poolboy:start_link(PoolArgs, WorkerArgs),
  {Srv, PoolPid}.

teardown(Srv, PoolPid) ->
  ok = poolboy:stop(PoolPid),
  bench_external_server:stop(Srv).

%% Mirrors arterial_bench:worker_loop/5 and shackle_bench:worker_loop/5:
%% hammer requests back-to-back until Deadline, recording wall-clock
%% latency (microseconds) of every successful call. poolboy:checkout/3
%% raising on a `full`/timeout (no worker free within ?TIMEOUT and
%% max_overflow exhausted) is the same transient, load-dependent outcome
%% the other two benches treat as "rejected, retry inline" rather than a
%% bug -- but unlike arterial/shackle, poolboy with max_overflow => 0 and
%% workers =< pool_size never actually hits this path (every worker
%% checks itself back in before looping), so Rejected stays 0 unless the
%% caller deliberately oversubscribes workers > pool_size.
worker_loop(Parent, Deadline, Payload, Acc) ->
  worker_loop(Parent, Deadline, Payload, Acc, 0).

worker_loop(Parent, Deadline, Payload, Acc, Rejected) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {self(), done, Acc, Rejected};
    false ->
      T0 = erlang:monotonic_time(microsecond),
      case do_request(Payload) of
        {ok, Payload} ->
          T1 = erlang:monotonic_time(microsecond),
          worker_loop(Parent, Deadline, Payload, [T1 - T0 | Acc], Rejected);
        {error, Reason} when Reason =:= full; Reason =:= timeout ->
          erlang:yield(),
          worker_loop(Parent, Deadline, Payload, Acc, Rejected + 1);
        Other ->
          Parent ! {self(), done, Acc, Rejected},
          error({unexpected_reply, Other})
      end
  end.

do_request(Payload) ->
  try
    poolboy:transaction(?POOL, fun(Worker) -> poolboy_echo_worker:call(Worker, Payload) end, 5000)
  catch
    exit:{timeout, _} -> {error, timeout}
  end.

collect([], Acc, RejectedAcc) ->
  {lists:append(Acc), RejectedAcc};
collect([Pid | Rest], Acc, RejectedAcc) ->
  receive
    {Pid, done, Latencies, Rejected} -> collect(Rest, [Latencies | Acc], RejectedAcc + Rejected)
  after 60000 ->
    error({worker_timeout, Pid})
  end.

report(PoolSize, PoolStrategy, Workers, DurationMs, ElapsedMs, LatenciesUs, Rejected) ->
  N = length(LatenciesUs),
  Sorted = lists:sort(LatenciesUs),
  ThroughputPerSec = N * 1000 / ElapsedMs,

  io:format("=== poolboy benchmark ===~n"),
  io:format("pool size:     ~p~n", [PoolSize]),
  io:format("max overflow:  0~n"),
  io:format("strategy:      ~p~n", [PoolStrategy]),
  io:format("workers:       ~p~n", [Workers]),
  io:format("duration:      ~p ms (target), ~p ms (actual)~n", [DurationMs, ElapsedMs]),
  io:format("requests:      ~p~n", [N]),
  io:format("rejected:      ~p (checkout timeouts, retried inline)~n", [Rejected]),
  io:format("throughput:    ~.1f req/s~n", [ThroughputPerSec]),
  case N of
    0 ->
      io:format("latency (µs): n/a (no completed requests)~n");
    _ ->
      io:format("latency (µs): p50=~p p95=~p p99=~p max=~p~n", [
        percentile(Sorted, 0.50),
        percentile(Sorted, 0.95),
        percentile(Sorted, 0.99),
        lists:last(Sorted)
      ])
  end,
  ok.

percentile(Sorted, P) ->
  N = length(Sorted),
  Idx = max(1, min(N, ceil(P * N))),
  lists:nth(Idx, Sorted).
