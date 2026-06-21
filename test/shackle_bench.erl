-module(shackle_bench).

-moduledoc """
Throughput/latency micro-benchmark for `shackle`
(https://github.com/lpgauth/shackle), driven against the same real
`test_tcp_server` over loopback TCP, the same wire framing
(`test_echo_protocol:frame/2`/`unframe/1`, via `shackle_echo_client`),
and the same workload shape as `arterial_bench` -- so the two numbers
are directly comparable head-to-head rather than measuring different
fixtures.

Two `mode`s, mirroring `arterial_bench:opts/0`'s `mode`, both forcing
`backlog_size => 1` (see `t:opts/0`'s doc) so neither side multiplexes
multiple in-flight requests onto one connection -- shackle's wire
protocol and `shackle_server` *can* do that safely (each request tagged
with its own `external_request_id`, demuxed off one persistent socket),
but arterial's can't (see `arterial_async_driver`'s moduledoc), so it's
disabled here for a fair head-to-head:

- `sync` (default): each worker blocks directly on `shackle:call/3`
  (itself just `cast/3` + `receive_response/1`).
- `async`: each worker calls `shackle:cast/4` then its own
  `receive_response/2` -- structurally the same non-blocking-dispatch
  shape as arterial_bench's async mode (the request is hand-delivered to
  a `shackle_server` process instead of blocking the caller's own
  process on the socket), but functionally near-identical to `call/3`
  here since `backlog_size => 1` still limits each connection to one
  outstanding request at a time either way.

Requires the `shackle` dependency, only pulled in under the `test`
profile (see `rebar.config`) -- not part of `arterial`'s runtime deps.

Not part of the regular `rebar3 eunit` run (no `_test`/`_test_` exported
functions) -- run it via `make bench-shackle` (see the top-level
Makefile) or explicitly from a shell:

```
$ rebar3 as test shell
1> shackle_bench:bench().
=== shackle benchmark ===
mode:          sync
pool size:     8
pool strategy: round_robin
workers:       8
duration:      5000 ms (target), 5007 ms (actual)
requests:      150218
rejected:      0 (no_server/timeout, retried inline)
throughput:    30041.3 req/s
latency (µs): p50=234 p95=450 p99=626 max=2103
ok
```

Tune the load shape via `bench/1`; see `help/0` for the full option list.
Options mirror `arterial_bench:opts/0` wherever the same concept applies
(`mode`, `pool_size`, `workers`, `duration_s`, `payload`), so `make bench`
and `make bench-shackle` results line up directly:

```
$ make bench          BENCH_OPTS='pool_size=16, duration_s=10'
$ make bench-shackle  BENCH_OPTS='pool_size=16, duration_s=10'
```
""".

-export([bench/0, bench/1, help/0, bench_help/1]).

-type opts() :: #{
  mode          => sync | async,
  pool_size     => pos_integer(),
  pool_strategy => round_robin | random,
  max_retries   => non_neg_integer(),
  workers       => pos_integer(),
  duration_s    => pos_integer(),
  payload       => binary()
}.

-define(POOL, shackle_bench_pool).

-doc "Equivalent to `bench/1` with the defaults shown in the moduledoc.".
-spec bench() -> ok.
bench() -> bench(#{}).

-doc false.
bench_help(_) -> help().

-doc """
Print the supported `t:opts/0` keys, their defaults, and how to pass them
-- from a shell (`shackle_bench:help().`) or via `make bench-shackle
BENCH_OPTS='key=value, ...'` (or `BENCH_OPTS='#{...}'` for values that
need real Erlang syntax).
""".
-spec help() -> ok.
help() ->
  io:format(
    "shackle_bench:bench(Opts) -- Opts is a map with any of:~n"
    "~n"
    "  mode          => sync | async  (default: sync)~n"
    "                   sync: each worker blocks directly on~n"
    "                   shackle:call/3. async: each worker calls~n"
    "                   shackle:cast/4 then its own receive_response/2.~n"
    "                   Both modes force backlog_size => 1 (see the~n"
    "                   moduledoc) so neither side multiplexes more than~n"
    "                   one in-flight request per connection, for a fair~n"
    "                   comparison against arterial_bench's own~n"
    "                   one-in-flight-per-connection constraint.~n"
    "  pool_size     => pos_integer()  (default: 8)~n"
    "                   Number of pooled connections to test_tcp_server.~n"
    "  pool_strategy => round_robin | random  (default: round_robin)~n"
    "                   shackle_pool's connection selection strategy --~n"
    "                   round_robin is the closest match to arterial's~n"
    "                   default fifo pool order, for a fair comparison.~n"
    "  max_retries   => non_neg_integer()  (default: 0)~n"
    "                   shackle_pool's retry count when a server is~n"
    "                   disabled/backlogged before giving up with~n"
    "                   {error, no_server} -- kept at 0 to match~n"
    "                   arterial_bench's own no-retry-inside-the-library~n"
    "                   behavior (retries happen in worker_loop/5 here,~n"
    "                   same as arterial_bench's worker_loop/5).~n"
    "  workers       => pos_integer()  (default: same as pool_size)~n"
    "                   Concurrent callers.~n"
    "  duration_s    => pos_integer()  (default: 5)~n"
    "                   How long to hammer the pool, in seconds.~n"
    "  payload       => binary()  (default: a short fixed binary)~n"
    "                   The {echo, Payload} request body sent on every call.~n"
    "~n"
    "Examples:~n"
    "  1> shackle_bench:bench().~n"
    "  2> shackle_bench:bench(#{pool_size => 16, duration_s => 10}).~n"
    "  3> shackle_bench:bench(#{mode => async}).~n"
    "~n"
    "  $ make bench-shackle~n"
    "  $ make bench-shackle BENCH_OPTS='pool_size=16, duration_s=10'~n"
  ),
  ok.

-doc """
Run the benchmark with `Opts` (see `t:opts/0`), print a summary, and
return `ok`. Starts and tears down its own `test_tcp_server` and
`shackle_pool` -- safe to call repeatedly from a live shell.
""".
-spec bench(opts()) -> ok.
bench(Opts) ->
  PoolSize0 = maps:get(pool_size, Opts, 8),
  #{
    mode          := Mode,
    pool_size     := PoolSize,
    pool_strategy := PoolStrategy,
    max_retries   := MaxRetries,
    workers       := Workers,
    duration_s    := DurationS,
    payload       := Payload
  } = maps:merge(#{
    mode          => sync,
    pool_size     => PoolSize0,
    pool_strategy => round_robin,
    max_retries   => 0,
    %% One worker per connection (every connection kept saturated, none
    %% idle), same default rationale as arterial_bench:bench/1.
    workers       => PoolSize0,
    duration_s    => 5,
    payload       => <<"the quick brown fox jumps over the lazy dog">>
  }, Opts),

  ok = ensure_shackle_started(),
  Srv = setup(PoolSize, PoolStrategy, MaxRetries),
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

    report(Mode, PoolSize, PoolStrategy, Workers, DurationMs, ElapsedMs, AllLatenciesUs, Rejected)
  after
    teardown(Srv)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

ensure_shackle_started() ->
  case application:ensure_all_started(shackle) of
    {ok, _} -> ok;
    {error, Reason} -> error({shackle_start_failed, Reason})
  end.

setup(PoolSize, PoolStrategy, MaxRetries) ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  ok = shackle_pool:start(?POOL, shackle_echo_client, [
    {address, "127.0.0.1"},
    {port, Port},
    {reconnect, true},
    %% shackle's own socket_options default is [] -- gen_tcp then
    %% defaults to list-mode {active,true} delivery, but
    %% shackle_server:handle_msg_data/4 (and this client's handle_data/2)
    %% expect a binary. Without this, every reply crashes the connection
    %% worker with badarg in erlang:size/1.
    {socket_options, [binary]}
  ], [
    {pool_size, PoolSize},
    {pool_strategy, PoolStrategy},
    {max_retries, MaxRetries},
    %% Force exactly one in-flight request per connection, same as
    %% arterial_bench's hardcoded backlog => 1 -- shackle's wire protocol
    %% and shackle_server can safely multiplex many in-flight requests
    %% per connection (each tagged with its own external_request_id),
    %% but arterial's can't (see arterial_async_driver's moduledoc), so
    %% this is disabled here for a fair head-to-head comparison rather
    %% than measuring shackle's pipelining advantage.
    {backlog_size, 1}
  ]),
  wait_until_available(PoolSize, 200),
  Srv.

teardown(Srv) ->
  ok = shackle_pool:stop(?POOL),
  test_tcp_server:stop(Srv).

%% shackle has no synchronous "checkout" primitive to probe readiness
%% with (unlike arterial_nif:checkout_connection/2) -- the closest
%% equivalent is a real, successful call/3 round-trip, retried until one
%% succeeds (each connection dials in asynchronously after
%% shackle_pool:start/4 returns).
wait_until_available(_PoolSize, 0) ->
  error(pool_not_ready);
wait_until_available(PoolSize, Retries) ->
  %% Unlike arterial_client:call/3, shackle:call/3 returns the decoded
  %% reply term directly (not {ok, Reply}) -- handle_data/2's {ReqID,
  %% Reply} pairs are unwrapped to just Reply by shackle:receive_response/1.
  case shackle:call(?POOL, {echo, ready}, 200) of
    ready -> ok;
    _ ->
      timer:sleep(20),
      wait_until_available(PoolSize, Retries - 1)
  end.

%% Mirrors arterial_bench:worker_loop/6: hammer requests back-to-back
%% until Deadline, recording wall-clock latency (microseconds) of every
%% successful call. sync calls shackle:call/3 directly; async calls
%% shackle:cast/4 then its own receive_response/2 -- same {error,
%% no_server | timeout} transient, load-dependent outcome either way,
%% which arterial_bench treats the same: "rejected, retry inline"
%% rather than a bug.
worker_loop(Mode, Parent, Deadline, Payload, Acc) ->
  worker_loop(Mode, Parent, Deadline, Payload, Acc, 0).

worker_loop(Mode, Parent, Deadline, Payload, Acc, Rejected) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {self(), done, Acc, Rejected};
    false ->
      T0 = erlang:monotonic_time(microsecond),
      %% Both return the decoded reply term directly (see
      %% wait_until_available/2's comment), not {ok, Reply}.
      case do_request(Mode, Payload) of
        Payload ->
          T1 = erlang:monotonic_time(microsecond),
          worker_loop(Mode, Parent, Deadline, Payload, [T1 - T0 | Acc], Rejected);
        {error, Reason} when Reason =:= no_server; Reason =:= timeout ->
          erlang:yield(),
          worker_loop(Mode, Parent, Deadline, Payload, Acc, Rejected + 1);
        Other ->
          Parent ! {self(), done, Acc, Rejected},
          error({unexpected_reply, Other})
      end
  end.

do_request(sync, Payload) ->
  shackle:call(?POOL, {echo, Payload}, 5000);
do_request(async, Payload) ->
  case shackle:cast(?POOL, {echo, Payload}, self(), 5000) of
    {ok, RequestId} -> shackle:receive_response(RequestId, 5000);
    {error, Reason} -> {error, Reason}
  end.

collect([], Acc, RejectedAcc) ->
  {lists:append(Acc), RejectedAcc};
collect([Pid | Rest], Acc, RejectedAcc) ->
  receive
    {Pid, done, Latencies, Rejected} -> collect(Rest, [Latencies | Acc], RejectedAcc + Rejected)
  after 60000 ->
    error({worker_timeout, Pid})
  end.

report(Mode, PoolSize, PoolStrategy, Workers, DurationMs, ElapsedMs, LatenciesUs, Rejected) ->
  N = length(LatenciesUs),
  Sorted = lists:sort(LatenciesUs),
  ThroughputPerSec = N * 1000 / ElapsedMs,

  io:format("=== shackle benchmark ===~n"),
  io:format("mode:          ~p~n", [Mode]),
  io:format("pool size:     ~p~n", [PoolSize]),
  io:format("pool strategy: ~p~n", [PoolStrategy]),
  io:format("workers:       ~p~n", [Workers]),
  io:format("duration:      ~p ms (target), ~p ms (actual)~n", [DurationMs, ElapsedMs]),
  io:format("requests:      ~p~n", [N]),
  io:format("rejected:      ~p (no_server/timeout, retried inline)~n", [Rejected]),
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
