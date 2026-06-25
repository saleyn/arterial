-module(arterial_bench).

-moduledoc """
Throughput/latency micro-benchmark for `arterial_pool`/`arterial_client`
(the NIF-resident-I/O backend, see `arterial_pool`'s moduledoc), driven
against a real `test_tcp_server` over loopback TCP.

`test_tcp_server` already speaks `<<ReqID:32, Len:32, Payload/binary>>`
with `Payload = term_to_binary(Term)` (see `test_echo_protocol`) -- the
exact same framing `arterial_codec2_default` uses with `ReqID` repurposed
as the correlation id -- so this benchmark needs no protocol glue of its
own, unlike `arterial_bench` (which pairs `test_tcp_server` with
`test_echo_client`/`arterial_async_driver` to get a comparable dispatch
shape out of the original backend).

There is no `mode` option here (compare `arterial_bench:bench/1`'s
`sync`/`async`): `arterial_client:call/2,3` already writes inline in
the caller's own process and waits for its reply via a plain `receive`,
with no dispatcher process and no per-connection checkout/backlog to
configure -- this backend only has the one shape. `backlog`/`fifo` don't
apply either, for the same reason (see `arterial_pool`'s moduledoc on
why this backend has no FIFO/backlog matching mode at all).

Not part of the regular `rebar3 eunit` run (no `_test`/`_test_` exported
functions) -- run it via `make bench2` (see the top-level Makefile) or
explicitly from a shell:

```
$ rebar3 as test shell
1> arterial_bench:bench().
=== arterial_pool benchmark ===
pool size:    8
workers:      8
duration:     5000 ms (target), 5006 ms (actual)
requests:     142318
rejected:     0 (no_connection, retried inline)
throughput:   28415.3 req/s
latency (µs): p50=241 p95=512 p99=703 max=3102
ok
```

Tune the load shape via `bench/1`; see `help/0` for the full option list:

```
1> arterial_bench:bench(#{pool_size => 16, duration_s => 10}).
2> arterial_bench:bench(#{throttle => #{rate => 1000, burst => 100}}).
```

From the shell, `BENCH_OPTS` takes a plain comma-separated `key=value`
list (the Makefile converts it to a map literal); for values that need
real Erlang syntax (e.g. a binary or nested map), pass a map literal
directly -- detected by a leading `#{` and passed through as-is:

```
$ make bench2 BENCH_OPTS='pool_size=16 duration_s=10'
$ make bench2 BENCH_OPTS='#{pool_size => 16, payload => <<"hi">>}'
```
""".

-export([bench/0, bench/1, help/0, bench_help/1]).

-type opts() :: #{
  pool_size       => pos_integer(),
  workers         => pos_integer(),
  duration_s      => pos_integer(),
  payload         => binary(),
  backoff         => arterial_connection:reconnect_time(),
  throttle        => undefined | arterial_pool:throttle_opts(),
  external_server => boolean()
}.

-define(POOL, arterial_bench_pool).

-doc "Equivalent to `bench/1` with the defaults shown in the moduledoc.".
-spec bench() -> ok.
bench() -> bench(#{}).

-doc false.
bench_help(_) -> help().

-doc """
Print the supported `t:opts/0` keys, their defaults, and how to pass them
-- from a shell (`arterial_bench:help().`) or via `make bench2
BENCH_OPTS='key=value, ...'` (or `BENCH_OPTS='#{...}'` for values that
need real Erlang syntax).
""".
-spec help() -> ok.
help() ->
  io:format(
    "arterial_bench:bench(Opts) -- Opts is a map with any of:~n"
    "~n"
    "  pool_size  => pos_integer()  (default: 8)~n"
    "                Number of pooled connections to test_tcp_server~n"
    "                (also the number of arterial_nif stripes, one~n"
    "                connection per stripe -- see arterial_pool's~n"
    "                moduledoc).~n"
    "  workers    => pos_integer()  (default: same as pool_size)~n"
    "                Concurrent callers. There is no queue-when-busy~n"
    "                here, so workers > pool_size just oversubscribes~n"
    "                (excess workers retry inline on {error,~n"
    "                no_connection} instead of measuring sustained~n"
    "                throughput) -- only raise this above pool_size~n"
    "                deliberately, to study that case.~n"
    "  duration_s => pos_integer()  (default: 5)~n"
    "                How long to hammer the pool, in seconds.~n"
    "  payload    => binary()  (default: a short fixed binary)~n"
    "                The {echo, Payload} request body sent on every call.~n"
    "  backoff    => arterial_connection:reconnect_time()~n"
    "                (default: not set, library default applies)~n"
    "                Passed straight through as reconnect_time -- either~n"
    "                a fixed non_neg_integer() delay, or {backoff, Min,~n"
    "                Max} for exponential backoff between reconnect~n"
    "                attempts.~n"
    "  throttle   => undefined | #{rate := pos_integer(), burst := pos_integer()}~n"
    "                (default: undefined, disabled)~n"
    "                Per-connection token-bucket rate limit -- see~n"
    "                arterial_throttle2. Set this to see its effect on~n"
    "                throughput/rejection rate under load.~n"
    "  external_server => boolean()  (default: false)~n"
    "                false: test_tcp_server runs in this VM, same as~n"
    "                calling it directly. true: runs it as a standalone~n"
    "                child erl node instead -- see bench_external_server's~n"
    "                moduledoc for why (keeps test_tcp_server's~n"
    "                per-request spawn/1 out of a perf profile taken on~n"
    "                this VM).~n"
    "~n"
    "Examples:~n"
    "  1> arterial_bench:bench().~n"
    "  2> arterial_bench:bench(#{pool_size => 16, duration_s => 10}).~n"
    "  3> arterial_bench:bench(#{throttle => #{rate => 1000, burst => 100}}).~n"
    "  4> arterial_bench:bench(#{backoff => {backoff, 100, 5000}}).~n"
    "~n"
    "  $ make bench2~n"
    "  $ make bench BENCH_OPTS='pool_size=16 duration_s=10'~n"
    "  $ make bench2 BENCH_OPTS='pool_size=16 duration_s=10'~n"
    "  $ make bench2 BENCH_OPTS='#{pool_size => 16, duration_s => 10}'~n"
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
    %% No queue-when-busy, so workers > pool_size just oversubscribes --
    %% excess workers retry inline on {error, no_connection} instead of
    %% measuring real throughput. Default to one worker per connection.
    workers    => PoolSize0,
    duration_s => 5,
    payload    => <<"the quick brown fox jumps over the lazy dog">>,
    throttle   => undefined,
    external_server => false
  },
  #{
    pool_size  := PoolSize,
    workers    := Workers,
    duration_s := DurationS,
    payload    := Payload,
    throttle   := Throttle,
    external_server := ExternalServer
  } = maps:merge(Defaults, Opts),

  Srv = setup(PoolSize, Throttle, maps:get(backoff, Opts, undefined), ExternalServer),
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

    report(PoolSize, Workers, DurationMs, ElapsedMs, AllLatenciesUs, Rejected)
  after
    teardown(Srv)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

setup(PoolSize, Throttle, Backoff, ExternalServer) ->
  Srv = bench_external_server:start(ExternalServer),
  Port = bench_external_server:port(Srv),
  PoolOpts0 = #{
    size       => PoolSize,
    codec      => arterial_codec2_default,
    address    => "127.0.0.1",
    port       => Port,
    throttle   => Throttle
  },
  PoolOpts = case Backoff of
    undefined -> PoolOpts0;
    _         -> PoolOpts0#{reconnect_time => Backoff}
  end,
  try
    {ok, _SupPid} = arterial_pool:start_link(?POOL, PoolOpts),
    wait_until_available(PoolSize, 200),
    Srv
  catch
    Class:Reason:Stack ->
      teardown(Srv),
      erlang:raise(Class, Reason, Stack)
  end.

teardown(Srv) ->
  arterial_pool:stop(?POOL),
  bench_external_server:stop(Srv).

wait_until_available(Size, Retries) ->
  AllUp = lists:all(fun(ConnID) -> arterial_pool:is_available(?POOL, ConnID) end,
                     lists:seq(0, Size - 1)),
  case AllUp of
    true ->
      ok;
    false when Retries > 0 ->
      timer:sleep(20),
      wait_until_available(Size, Retries - 1);
    false ->
      error(pool_not_ready)
  end.

%% Each worker hammers requests back-to-back until Deadline, recording
%% the wall-clock latency (microseconds) of every successful request.
%% {error, no_connection} (every connection momentarily unavailable/
%% throttled under enough concurrent workers) is a transient,
%% load-dependent outcome rather than a bug -- counted separately and
%% retried immediately instead of aborting the worker. Any other error
%% is unexpected and aborts the loop, surfaced via the worker's exit
%% reason.
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
          %% Yield instead of busy-spinning -- see arterial_bench's
          %% identical comment on why this matters under contention.
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

report(PoolSize, Workers, DurationMs, ElapsedMs, LatenciesUs, Rejected) ->
  N = length(LatenciesUs),
  Sorted = lists:sort(LatenciesUs),
  ThroughputPerSec = N * 1000 / ElapsedMs,

  io:format("=== arterial_pool benchmark ===~n"),
  io:format("pool size:    ~p~n", [PoolSize]),
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
