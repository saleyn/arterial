-module(arterial_bench).

-moduledoc """
Throughput/latency micro-benchmark for `arterial`, driven against a real
`test_tcp_server` over loopback TCP using the same
`test_echo_protocol`/`test_echo_client` pair as `test_tcp_server_tests`.

Every worker calls `arterial_client:call/3` directly -- there is no
separate "async dispatcher" mode anymore (an earlier version of this
benchmark routed an `async` mode through `arterial_async_driver`, a
small fixed pool of dispatcher processes, to avoid blocking N worker
processes directly on socket I/O; `call/3` itself now has that same
shape unconditionally, since the connection's `arterial_conn_owner`
process -- not the caller -- owns the socket and does the actual I/O;
see that module's moduledoc).

Because the owner serializes and demuxes replies itself, `backlog > 1`
is now safe to multiplex: several concurrent callers can be checked out
onto the same connection at once (each via its own `call/3`, each
talking to the same owner), unlike before this redesign. Set `backlog`
(default `1`, matching `arterial_pool`'s own default) to study
multiplexing on its own terms -- pair it with `workers > pool_size` to
actually exercise it (`workers =< pool_size * backlog` keeps every
connection's backlog from ever rejecting a checkout purely from
capacity).

Not part of the regular `rebar3 eunit` run (no `_test`/`_test_` exported
functions) -- run it via `make bench` (see the top-level Makefile's
`bench` target) or explicitly from a shell:

```
$ rebar3 as test shell
1> arterial_bench:bench().
=== arterial benchmark ===
pool size:    8
backlog:      1
order:        fifo
workers:      8
duration:     5000 ms (target), 5007 ms (actual)
requests:     106292
rejected:     0 (no_connection, retried inline)
throughput:   21228.7 req/s
latency (µs): p50=329 p95=664 p99=891 max=4466
ok
```

Tune the load shape via `bench/1`; see `help/0` for the full option list:

```
1> arterial_bench:bench(#{pool_size => 16, duration_s => 10}).
2> arterial_bench:bench(#{backlog => 4, workers => 32}).
```

From the shell, `BENCH_OPTS` takes a plain comma-separated `key=value`
list (the Makefile converts it to a map literal); for values that need
real Erlang syntax (e.g. a binary), pass a map literal directly --
detected by a leading `#{` and passed through as-is:

```
$ make bench BENCH_OPTS='pool_size=16 duration_s=10'
$ make bench BENCH_OPTS='#{pool_size => 16, payload => <<"hi">>}'
```
""".

-export([bench/0, bench/1, help/0, bench_help/1]).

-type opts() :: #{
  pool_size  => pos_integer(),
  backlog    => pos_integer(),
  workers    => pos_integer(),
  duration_s => pos_integer(),
  payload    => binary(),
  fifo       => boolean(),
  backoff    => arterial_connection:reconnect_time()
}.

-define(POOL, arterial_bench_pool).

-doc "Equivalent to `bench/1` with the defaults shown in the moduledoc.".
-spec bench() -> ok.
bench() -> bench(#{}).

-doc false.
bench_help(_) -> help().

-doc """
Print the supported `t:opts/0` keys, their defaults, and how to pass them
-- from a shell (`arterial_bench:help().`) or via `make bench
BENCH_OPTS='key=value, ...'` (or `BENCH_OPTS='#{...}'` for values that
need real Erlang syntax).
""".
-spec help() -> ok.
help() ->
  io:format(
    "arterial_bench:bench(Opts) -- Opts is a map with any of:~n"
    "~n"
    "  pool_size  => pos_integer()  (default: 8)~n"
    "                Number of pooled connections to test_tcp_server.~n"
    "  backlog    => pos_integer()  (default: 1)~n"
    "                Max in-flight requests per connection. Each~n"
    "                connection's arterial_conn_owner process serializes~n"
    "                and demuxes its own socket I/O, so backlog > 1 safely~n"
    "                multiplexes several concurrent callers onto one~n"
    "                connection -- raise this together with workers to~n"
    "                study multiplexing.~n"
    "  fifo       => boolean()  (default: true)~n"
    "                Reply demux mode passed to arterial_pool -- true~n"
    "                assumes replies arrive in send order (no wire-level~n"
    "                id needed); test_echo_protocol supports both.~n"
    "  workers    => pos_integer()  (default: same as pool_size)~n"
    "                Concurrent callers. workers > pool_size * backlog~n"
    "                oversubscribes (excess workers spin on~n"
    "                {error, no_connection} instead of measuring~n"
    "                sustained throughput) -- raise this deliberately to~n"
    "                study that case, or together with backlog to~n"
    "                exercise multiplexing instead.~n"
    "  duration_s => pos_integer()  (default: 5)~n"
    "                How long to hammer the pool, in seconds.~n"
    "  payload    => binary()  (default: a short fixed binary)~n"
    "                The {echo, Payload} request body sent on every call.~n"
    "  backoff    => arterial_connection:reconnect_time()~n"
    "                (default: not set, library default applies)~n"
    "                Passed straight through to client_opts as~n"
    "                reconnect_time -- either a fixed non_neg_integer()~n"
    "                delay, or {backoff, Min, Max} for exponential~n"
    "                backoff between reconnect attempts.~n"
    "~n"
    "Examples:~n"
    "  1> arterial_bench:bench().~n"
    "  2> arterial_bench:bench(#{pool_size => 16, duration_s => 10}).~n"
    "  3> arterial_bench:bench(#{backlog => 4, workers => 32}).~n"
    "  4> arterial_bench:bench(#{backoff => {backoff, 100, 5000}}).~n"
    "~n"
    "  $ make bench~n"
    "  $ make bench-shackle BENCH_OPTS='pool_size=16 duration_s=10'~n"
    "  $ make bench-poolboy BENCH_OPTS='pool_size=16 duration_s=10'~n"
    "  $ make bench BENCH_OPTS='pool_size=16 duration_s=10'~n"
    "  $ make bench BENCH_OPTS='#{pool_size => 16, duration_s => 10}'~n"
  ),
  ok.

-doc """
Run the benchmark with `Opts` (see `t:opts/0`), print a summary, and
return `ok`. Starts and tears down its own `test_tcp_server` and
`arterial_pool` -- safe to call repeatedly from a live shell.
""".
-spec bench(opts()) -> ok.
bench(Opts) ->
  PoolSize0 = maps:get(pool_size, Opts, 8),
  Defaults = #{
    pool_size  => PoolSize0,
    backlog    => 1,
    fifo       => true,
    %% No queue-when-busy by default, so workers > pool_size * backlog
    %% just oversubscribes -- excess workers busy-spin on
    %% {error, no_connection} instead of measuring real throughput.
    %% Default to one worker per connection (every connection kept
    %% saturated, none idle) unless the caller deliberately wants to
    %% study oversubscription/multiplexing by passing `workers` or
    %% `backlog` explicitly.
    workers    => PoolSize0,
    duration_s => 5,
    payload    => <<"the quick brown fox jumps over the lazy dog">>
  },
  #{
    pool_size  := PoolSize,
    backlog    := Backlog,
    workers    := Workers,
    duration_s := DurationS,
    payload    := Payload,
    fifo       := Fifo
  } = maps:merge(Defaults, Opts),

  {Srv, SupPid} = setup(PoolSize, Backlog, Fifo, maps:get(backoff, Opts, undefined)),
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

    report(PoolSize, Backlog, Workers, DurationMs, ElapsedMs, AllLatenciesUs, Rejected, Fifo)
  after
    teardown({Srv, SupPid})
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

setup(PoolSize, Backlog, Fifo, Backoff) ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  ClientOpts0 = #{address => "127.0.0.1", port => Port, protocol => tcp},
  ClientOpts = case Backoff of
    undefined -> ClientOpts0;
    _         -> ClientOpts0#{reconnect_time => Backoff}
  end,
  {ok, SupPid} = arterial_pool:start_link(?POOL, #{
    size        => PoolSize,
    backlog     => Backlog,
    fifo        => Fifo,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => ClientOpts
  }),
  try
    wait_until_available(PoolSize, 200),
    {Srv, SupPid}
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  ok = supervisor:stop(SupPid),
  try arterial_nif:destroy(?POOL) catch _:_ -> ok end,
  test_tcp_server:stop(Srv).

%% Block until all `N` connections have (re)connected: check them all out
%% at once, then immediately check them back in, so no reservation is
%% stranded.
wait_until_available(N, Retries) ->
  case checkout_n(N, []) of
    {ok, ConnIDs} ->
      lists:foreach(fun(ConnID) -> ok = arterial_nif:checkin_connection(?POOL, ConnID) end, ConnIDs);
    {error, no_connection} when Retries > 0 ->
      timer:sleep(20),
      wait_until_available(N, Retries - 1);
    {error, no_connection} ->
      error(pool_not_ready)
  end.

checkout_n(0, Acc) ->
  {ok, Acc};
checkout_n(N, Acc) ->
  case arterial_nif:checkout_connection(?POOL, sync) of
    {ok, ConnID} ->
      checkout_n(N - 1, [ConnID | Acc]);
    {error, no_connection} = Error ->
      lists:foreach(fun(ConnID) -> ok = arterial_nif:checkin_connection(?POOL, ConnID) end, Acc),
      Error
  end.

%% Each worker hammers requests back-to-back until Deadline, recording
%% the wall-clock latency (microseconds) of every successful request.
%% {error, no_connection} is a transient, load-dependent outcome (every
%% connection momentarily at backlog capacity under enough concurrent
%% workers) rather than a bug -- it's counted separately and retried
%% immediately instead of aborting the worker. Any other error is
%% unexpected and aborts the loop, surfaced via the worker's exit reason.
worker_loop(Parent, Deadline, Payload, Acc) ->
  worker_loop(Parent, Deadline, Payload, Acc, 0).

worker_loop(Parent, Deadline, Payload, Acc, Rejected) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {self(), done, Acc, Rejected};
    false ->
      T0 = erlang:monotonic_time(microsecond),
      case arterial_client:call(?POOL, {echo, Payload}, 5000) of
        {ok, Payload} ->
          T1 = erlang:monotonic_time(microsecond),
          worker_loop(Parent, Deadline, Payload, [T1 - T0 | Acc], Rejected);
        {error, no_connection} ->
          %% Yield instead of busy-spinning: with enough concurrent
          %% workers, retrying instantly on every rejection starves the
          %% schedulers that the actual in-flight requests need to
          %% complete, which paradoxically makes rejections *more*
          %% likely rather than just wasting CPU.
          erlang:yield(),
          worker_loop(Parent, Deadline, Payload, Acc, Rejected + 1);
        Other ->
          Parent ! {self(), done, Acc, Rejected},
          error({unexpected_reply, Other})
      end
  end.

collect([], Acc, RejectedAcc) ->
  {lists:append(Acc), RejectedAcc};
collect([Pid | Rest], Acc, RejectedAcc) ->
  receive
    {Pid, done, Latencies, Rejected} -> collect(Rest, [Latencies | Acc], RejectedAcc + Rejected)
  after 60000 ->
    error({worker_timeout, Pid})
  end.

report(PoolSize, Backlog, Workers, DurationMs, ElapsedMs, LatenciesUs, Rejected, Fifo) ->
  N = length(LatenciesUs),
  Sorted = lists:sort(LatenciesUs),
  ThroughputPerSec = N * 1000 / ElapsedMs,

  io:format("=== arterial benchmark ===~n"),
  io:format("pool size:    ~p~n", [PoolSize]),
  io:format("backlog:      ~p~n", [Backlog]),
  io:format("order:        ~s~n", [case Fifo of true -> "fifo"; false -> "lifo" end]),
  io:format("workers:      ~p~n", [Workers]),
  io:format("duration:     ~p ms (target), ~p ms (actual)~n", [DurationMs, ElapsedMs]),
  io:format("requests:     ~p~n", [N]),
  io:format("rejected:     ~p (no_connection, retried inline)~n", [Rejected]),
  io:format("throughput:   ~.1f req/s~n", [ThroughputPerSec]),
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
