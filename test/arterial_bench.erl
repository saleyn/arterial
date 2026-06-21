-module(arterial_bench).

-moduledoc """
Throughput/latency micro-benchmark for the synchronous `arterial_client:call/3`
path, driven against a real `test_tcp_server` over loopback TCP using the
same `test_echo_protocol`/`test_echo_client` pair as
`test_tcp_server_tests`.

Not part of the regular `rebar3 eunit` run (no `_test`/`_test_` exported
functions) -- run it via `make bench` (see the top-level Makefile's
`bench` target) or explicitly from a shell:

```
$ rebar3 as test shell
1> arterial_bench:bench().
=== arterial benchmark ===
pool size:    8
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
```
""".

-export([bench/0, bench/1, help/0, bench_help/1]).

-type opts() :: #{
  pool_size  => pos_integer(),
  backlog    => pos_integer(),
  workers    => pos_integer(),
  duration_s => pos_integer(),
  payload    => binary(),
  fifo       => boolean()
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
BENCH_OPTS='#{...}'`.
""".
-spec help() -> ok.
help() ->
  io:format(
    "arterial_bench:bench(Opts) -- Opts is a map with any of:~n"
    "~n"
    "  pool_size  => pos_integer()  (default: 8)~n"
    "                Number of pooled connections to test_tcp_server.~n"
    "  workers    => pos_integer()  (default: same as pool_size)~n"
    "                Concurrent call/3 callers. call/3 has no~n"
    "                queue-when-busy path, so workers > pool_size just~n"
    "                oversubscribes (excess workers spin on~n"
    "                {error, no_connection} instead of measuring~n"
    "                sustained throughput) -- only raise this above~n"
    "                pool_size deliberately, to study that case.~n"
    "  backlog    => pos_integer()  (default: 1, NOT independently~n"
    "                configurable here -- call/3 owns a connection's~n"
    "                socket exclusively for the whole request, so~n"
    "                backlog > 1 would let two concurrent callers race~n"
    "                reading replies off the same socket).~n"
    "  duration_s => pos_integer()  (default: 5)~n"
    "                How long to hammer the pool, in seconds.~n"
    "  payload    => binary()  (default: a short fixed binary)~n"
    "                The {echo, Payload} request body sent on every call.~n"
    "  fifo       => boolean()  (default: true)~n"
    "                Connection selection order on checkout: true picks~n"
    "                the least-recently-used connection (round-robins~n"
    "                load evenly); false picks the most-recently-used one~n"
    "                (favors keeping a hot connection's socket warm, at~n"
    "                the cost of leaving others idle under partial load).~n"
    "~n"
    "Examples:~n"
    "  1> arterial_bench:bench().~n"
    "  2> arterial_bench:bench(#{pool_size => 16, duration_s => 10}).~n"
    "~n"
    "  $ make bench~n"
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
  #{
    pool_size  := PoolSize,
    backlog    := Backlog,
    workers    := Workers,
    duration_s := DurationS,
    payload    := Payload,
    fifo       := Fifo
  } = maps:merge(#{
    pool_size  => PoolSize0,
    fifo       => true,
    %% backlog > 1 lets multiple in-flight requests pipeline on the same
    %% connection, but only the asynchronous path (checkout_async/3 +
    %% handle_request/2/handle_data/2, serialized through
    %% arterial_connection's own mailbox) can safely share one socket
    %% that way. The synchronous call/3 path used here checks out and
    %% owns the whole connection's socket directly for blocking
    %% send/recv -- two concurrent call/3 callers checked out onto the
    %% *same* connection (legal once backlog > 1 says it has room) would
    %% race reading each other's replies off that one socket. Keep
    %% backlog at 1 so each connection serves at most one call/3 caller
    %% at a time, same as arterial_pool's own default.
    backlog    => 1,
    %% No queue-when-busy here (call/3 doesn't use the wait-list), so
    %% workers > pool_size just oversubscribes -- excess workers busy-spin
    %% on {error, no_connection} instead of measuring real throughput.
    %% Default to one worker per connection (every connection kept
    %% saturated, none idle) unless the caller deliberately wants to
    %% study oversubscription by passing `workers` explicitly.
    workers    => PoolSize0,
    duration_s => 5,
    payload    => <<"the quick brown fox jumps over the lazy dog">>
  }, Opts),

  {Srv, SupPid} = setup(PoolSize, Backlog, Fifo),
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

    report(PoolSize, Workers, DurationMs, ElapsedMs, AllLatenciesUs, Rejected, Fifo)
  after
    teardown({Srv, SupPid})
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

setup(PoolSize, Backlog, Fifo) ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(?POOL, #{
    size        => PoolSize,
    backlog     => Backlog,
    fifo        => Fifo,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{address => "127.0.0.1", port => Port, protocol => tcp}
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

%% Each worker hammers call/3 back-to-back until Deadline, recording the
%% wall-clock latency (microseconds) of every successful call.
%% {error, no_connection} is a transient, load-dependent outcome (every
%% connection momentarily busy/throttled under enough concurrent
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

report(PoolSize, Workers, DurationMs, ElapsedMs, LatenciesUs, Rejected, Fifo) ->
  N = length(LatenciesUs),
  Sorted = lists:sort(LatenciesUs),
  ThroughputPerSec = N * 1000 / ElapsedMs,

  io:format("=== arterial benchmark ===~n"),
  io:format("pool size:    ~p~n", [PoolSize]),
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
