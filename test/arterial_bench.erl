-module(arterial_bench).

-moduledoc """
Throughput/latency micro-benchmark for `arterial`, driven against a real
`test_tcp_server` over loopback TCP using the same
`test_echo_protocol`/`test_echo_client` pair as `test_tcp_server_tests`.

Two `mode`s, both with zero request multiplexing (at most one in-flight
request per connection at a time -- see `t:opts/0`'s `mode` doc):

- `async` (default): each worker process casts through a small, fixed
  pool of `arterial_async_driver` dispatcher processes (one per
  connection) instead of blocking on the socket itself -- see that
  module's doc for why this is "non-blocking dispatch", not wire-level
  pipelining (arterial's public API has no way to hold a connection
  checked out across more than one in-flight request). This is the mode
  comparable to `shackle_bench`: `shackle:call/3` always hands the
  request off to a `shackle_server` process internally, even in
  shackle's own default mode -- `arterial_client:call/3`'s `sync` mode
  below has no such dispatcher in the loop, so benchmarking it against
  shackle's *any* mode compares two different architectures, not just
  two pool implementations.
- `sync`: each worker process blocks directly on
  `arterial_client:call/3`, owning a connection's socket for the whole
  request/reply round trip -- no dispatcher process involved at all.
  Useful to see the ceiling when the caller pays its own socket I/O
  cost, but not the fair comparison point against `shackle_bench`.

Not part of the regular `rebar3 eunit` run (no `_test`/`_test_` exported
functions) -- run it via `make bench` (see the top-level Makefile's
`bench` target) or explicitly from a shell:

```
$ rebar3 as test shell
1> arterial_bench:bench().
=== arterial benchmark ===
mode:         async
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
2> arterial_bench:bench(#{mode => sync}).
```

From the shell, `BENCH_OPTS` takes a plain comma-separated `key=value`
list (the Makefile converts it to a map literal); for values that need
real Erlang syntax (e.g. a binary), pass a map literal directly --
detected by a leading `#{` and passed through as-is:

```
$ make bench BENCH_OPTS='pool_size=16, duration_s=10'
$ make bench BENCH_OPTS='#{pool_size => 16, payload => <<"hi">>}'
```
""".

-export([bench/0, bench/1, help/0, bench_help/1]).

-type opts() :: #{
  mode       => sync | async,
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
    "  mode       => sync | async  (default: async)~n"
    "                async: each worker casts through a small, fixed pool~n"
    "                of arterial_async_driver dispatcher processes (one~n"
    "                per connection) instead of blocking on the socket~n"
    "                itself. This is the mode comparable to shackle_bench~n"
    "                -- shackle:call/3 always dispatches to a~n"
    "                shackle_server process internally, so arterial's~n"
    "                sync mode (below) would compare a different~n"
    "                architecture, not just a different pool.~n"
    "                sync: each worker blocks directly on~n"
    "                arterial_client:call/3, owning a connection's socket~n"
    "                for the whole request/reply round trip -- no~n"
    "                dispatcher process involved.~n"
    "                Still at most one in-flight request per connection~n"
    "                at a time either way -- arterial's public API has no~n"
    "                way to hold a connection checked out across more~n"
    "                than one in-flight request, so async here is~n"
    "                non-blocking dispatch, not wire-level pipelining.~n"
    "  pool_size  => pos_integer()  (default: 8)~n"
    "                Number of pooled connections to test_tcp_server.~n"
    "  workers    => pos_integer()  (default: same as pool_size)~n"
    "                Concurrent callers. Neither mode queues when every~n"
    "                connection is busy, so workers > pool_size just~n"
    "                oversubscribes (excess workers spin on~n"
    "                {error, no_connection} instead of measuring~n"
    "                sustained throughput) -- only raise this above~n"
    "                pool_size deliberately, to study that case.~n"
    "  backlog    => pos_integer()  (default: 1, NOT independently~n"
    "                configurable here -- both modes own a connection's~n"
    "                socket exclusively for one request at a time, so~n"
    "                backlog > 1 would let two concurrent callers race~n"
    "                reading replies off the same socket).~n"
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
    "  3> arterial_bench:bench(#{mode => sync}).~n"
    "  4> arterial_bench:bench(#{backoff => {backoff, 100, 5000}}).~n"
    "~n"
    "  $ make bench~n"
    "  $ make bench-shackle BENCH_OPTS='pool_size=16, duration_s=10'~n"
    "  $ make bench BENCH_OPTS='pool_size=16, duration_s=10'~n"
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
    %% async is the architecture comparable to shackle_bench -- see this
    %% module's moduledoc/help() for why sync isn't a fair head-to-head.
    mode       => async,
    pool_size  => PoolSize0,
    fifo       => true,
    %% backlog > 1 lets multiple in-flight requests pipeline on the same
    %% connection, but neither mode here can safely share one socket
    %% that way: sync's call/3 owns a connection's socket directly for
    %% blocking send/recv, and async's arterial_async_driver serves one
    %% request to completion before picking up the next (see that
    %% module's doc -- arterial's public API has no way to hold a
    %% connection checked out across more than one in-flight request).
    %% Two concurrent callers checked out onto the *same* connection
    %% (legal once backlog > 1 says it has room) would race reading
    %% replies off that one socket. Keep backlog at 1 so each connection
    %% serves at most one caller at a time, same as arterial_pool's own
    %% default.
    backlog    => 1,
    %% No queue-when-busy in either mode, so workers > pool_size just
    %% oversubscribes -- excess workers busy-spin on {error, no_connection}
    %% instead of measuring real throughput. Default to one worker per
    %% connection (every connection kept saturated, none idle) unless the
    %% caller deliberately wants to study oversubscription by passing
    %% `workers` explicitly.
    workers    => PoolSize0,
    duration_s => 5,
    payload    => <<"the quick brown fox jumps over the lazy dog">>
  },
  #{
    mode       := Mode,
    pool_size  := PoolSize,
    backlog    := Backlog,
    workers    := Workers,
    duration_s := DurationS,
    payload    := Payload,
    fifo       := Fifo
  } = maps:merge(Defaults, Opts),

  {Srv, SupPid} = setup(PoolSize, Backlog, Fifo, Mode, maps:get(backoff, Opts, undefined)),
  try
    DurationMs = DurationS * 1000,
    Parent = self(),
    StartAt = erlang:monotonic_time(millisecond),
    Deadline = StartAt + DurationMs,

    WorkerPids = [
      spawn_link(fun() -> worker_loop(Mode, Parent, Deadline, Payload, []) end)
      || _ <- lists:seq(1, Workers)
    ],

    {AllLatenciesUs, Rejected} = collect(WorkerPids, [], 0),
    ElapsedMs = erlang:monotonic_time(millisecond) - StartAt,

    report(Mode, PoolSize, Backlog, Workers, DurationMs, ElapsedMs, AllLatenciesUs, Rejected, Fifo)
  after
    teardown({Srv, SupPid}, Mode)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

setup(PoolSize, Backlog, Fifo, Mode, Backoff) ->
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
    Mode =:= async andalso arterial_async_driver:start_link(?POOL, PoolSize, test_echo_client),
    {Srv, SupPid}
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}, Mode),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}, Mode) ->
  Mode =:= async andalso arterial_async_driver:stop(?POOL),
  ok = supervisor:stop(SupPid),
  try arterial_nif:destroy(?POOL) catch _:_ -> ok end,
  test_tcp_server:stop(Srv).

%% Block until all `N` connections have (re)connected, the same way
%% test_tcp_server_tests:wait_until_available/3 does: check them all out
%% at once, then immediately check them back in (with their real ReqIDs,
%% per checkin_connection/4's contract) so no backlog slot is stranded.
wait_until_available(N, Retries) ->
  case checkout_n(N, []) of
    {ok, Reservations} ->
      lists:foreach(
        fun({ConnID, ReqIDs}) -> ok = arterial_nif:checkin_connection(?POOL, ConnID, ReqIDs, <<>>) end,
        Reservations);
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
    {ok, #{conn_id := ConnID, req_ids := ReqIDs}} ->
      checkout_n(N - 1, [{ConnID, ReqIDs} | Acc]);
    {error, no_connection} = Error ->
      lists:foreach(
        fun({ConnID, ReqIDs}) -> ok = arterial_nif:checkin_connection(?POOL, ConnID, ReqIDs, <<>>) end,
        Acc),
      Error
  end.

%% Each worker hammers requests back-to-back until Deadline, recording
%% the wall-clock latency (microseconds) of every successful request.
%% sync calls arterial_client:call/3 directly; async casts through
%% arterial_async_driver:request/3 instead -- same workload, same
%% {ok, Payload} | {error, no_connection} contract either way.
%% {error, no_connection} is a transient, load-dependent outcome (every
%% connection momentarily busy/throttled under enough concurrent
%% workers) rather than a bug -- it's counted separately and retried
%% immediately instead of aborting the worker. Any other error is
%% unexpected and aborts the loop, surfaced via the worker's exit reason.
worker_loop(Mode, Parent, Deadline, Payload, Acc) ->
  worker_loop(Mode, Parent, Deadline, Payload, Acc, 0).

worker_loop(Mode, Parent, Deadline, Payload, Acc, Rejected) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {self(), done, Acc, Rejected};
    false ->
      T0 = erlang:monotonic_time(microsecond),
      case do_request(Mode, Payload) of
        {ok, Payload} ->
          T1 = erlang:monotonic_time(microsecond),
          worker_loop(Mode, Parent, Deadline, Payload, [T1 - T0 | Acc], Rejected);
        {error, no_connection} ->
          %% Yield instead of busy-spinning: with enough concurrent
          %% workers, retrying instantly on every rejection starves the
          %% schedulers that the actual in-flight requests need to
          %% complete, which paradoxically makes rejections *more*
          %% likely rather than just wasting CPU.
          erlang:yield(),
          worker_loop(Mode, Parent, Deadline, Payload, Acc, Rejected + 1);
        Other ->
          Parent ! {self(), done, Acc, Rejected},
          error({unexpected_reply, Other})
      end
  end.

do_request(sync, Payload) ->
  arterial_client:call(?POOL, {echo, Payload}, 5000);
do_request(async, Payload) ->
  arterial_async_driver:request(?POOL, {echo, Payload}, 5000).

collect([], Acc, RejectedAcc) ->
  {lists:append(Acc), RejectedAcc};
collect([Pid | Rest], Acc, RejectedAcc) ->
  receive
    {Pid, done, Latencies, Rejected} -> collect(Rest, [Latencies | Acc], RejectedAcc + Rejected)
  after 60000 ->
    error({worker_timeout, Pid})
  end.

report(Mode, PoolSize, Backlog, Workers, DurationMs, ElapsedMs, LatenciesUs, Rejected, Fifo) ->
  N = length(LatenciesUs),
  Sorted = lists:sort(LatenciesUs),
  ThroughputPerSec = N * 1000 / ElapsedMs,

  io:format("=== arterial benchmark ===~n"),
  io:format("mode:         ~p~n", [Mode]),
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
