-module(test_tcp_server_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
End-to-end example: a real TCP server (`test_tcp_server`) driven through
the full `arterial_pool` stack (supervisor, `arterial_connection` with
its connect/reconnect state machine, `test_echo_client`, and
`test_echo_protocol` for wire framing), exercised via the synchronous
`arterial_client:call/3` API.

Contrast with `arterial_nif_tests`'s `protocol_sync_call_test`/
`protocol_async_checkout_test`, which use `test_protocol`'s in-process
fake transport (no real socket) to isolate NIF-level checkout/checkin
behavior; this module instead proves the whole stack -- real listening
socket, real `gen_tcp`-style connect, real bytes on the wire -- works
together.
""".

%% If anything after arterial_pool:start_link/2 fails (most likely
%% wait_until_available/3 exhausting its retries), this runs *before* any
%% test body's own try/after -- so without cleaning up here, the pool's
%% registered supervisor name, its buffer ETS table, and the listening
%% test_tcp_server would all leak past this test, making every subsequent
%% setup/0 in the suite fail with {already_started, _}.
setup() -> setup(2).

setup(Size) ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(tcp_echo_pool, #{
    size        => Size,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{address => "127.0.0.1", port => Port, protocol => tcp}
  }),
  try
    %% Give the pool's connections a moment to dial in before the test
    %% issues its first call/3.
    wait_until_available(tcp_echo_pool, Size, 50),
    {Srv, SupPid}
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  ok = supervisor:stop(SupPid),
  ok = arterial_nif:destroy(tcp_echo_pool),
  test_tcp_server:stop(Srv).

%% Block until all `N' connections of `Pool' have (re)connected, by
%% checking out all of them at once (so they must all be simultaneously
%% available) and then checking each back in with its reserved ReqID, so
%% as not to (per checkin_connection/2's contract) permanently strand
%% that backlog slot.
wait_until_available(Pool, N, Retries) ->
  case checkout_n(Pool, N, []) of
    {ok, Reservations} ->
      lists:foreach(
        fun({ConnID, ReqIDs}) -> ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>) end,
        Reservations);
    {error, no_connection} when Retries > 0 ->
      timer:sleep(20),
      wait_until_available(Pool, N, Retries - 1);
    {error, no_connection} ->
      error(pool_not_ready)
  end.

checkout_n(_Pool, 0, Acc) ->
  {ok, Acc};
checkout_n(Pool, N, Acc) ->
  case arterial_nif:checkout_connection(Pool, sync) of
    {ok, #{conn_id := ConnID, req_ids := ReqIDs}} ->
      checkout_n(Pool, N - 1, [{ConnID, ReqIDs} | Acc]);
    {error, no_connection} = Error ->
      lists:foreach(
        fun({ConnID, ReqIDs}) -> ok = arterial_nif:checkin_connection(Pool, ConnID, ReqIDs, <<>>) end,
        Acc),
      Error
  end.

tcp_echo_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, hello} = arterial_client:call(tcp_echo_pool, {echo, hello}, 1000)
  after
    teardown({Srv, SupPid})
  end.

tcp_upcase_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, <<"ARTERIAL">>} =
      arterial_client:call(tcp_echo_pool, {upcase, <<"arterial">>}, 1000)
  after
    teardown({Srv, SupPid})
  end.

tcp_delayed_reply_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, slow} = arterial_client:call(tcp_echo_pool, {delay, 50, slow}, 1000)
  after
    teardown({Srv, SupPid})
  end.

tcp_unknown_request_test() ->
  {Srv, SupPid} = setup(),
  try
    {ok, {error, unknown_request}} =
      arterial_client:call(tcp_echo_pool, surprise, 1000)
  after
    teardown({Srv, SupPid})
  end.

%% Two connections in the pool means two concurrent calls are each
%% served by their own socket without blocking on one another.
tcp_concurrent_calls_test() ->
  {Srv, SupPid} = setup(),
  try
    Parent = self(),
    lists:foreach(fun(N) ->
      spawn(fun() ->
        Result = arterial_client:call(tcp_echo_pool, {delay, 30, N}, 1000),
        Parent ! {result, N, Result}
      end)
    end, [1, 2]),
    {ok, 1} = collect_result(1),
    {ok, 2} = collect_result(2)
  after
    teardown({Srv, SupPid})
  end.

collect_result(N) ->
  receive {result, N, Result} -> Result
  after 1000 -> error(timeout)
  end.

%% A reply that doesn't arrive before the caller's Timeout surfaces as a
%% plain {error, timeout}, not a crash or hang.
tcp_call_timeout_test() ->
  {Srv, SupPid} = setup(),
  try
    {error, timeout} =
      arterial_client:call(tcp_echo_pool, {delay, 200, late}, 50)
  after
    %% The server's delayed reply for the timed-out request is still
    %% in flight; give it time to land before tearing down the (now
    %% single-connection) pool so it doesn't log a connection reset.
    timer:sleep(250),
    teardown({Srv, SupPid})
  end.

%% arterial_connection:bounce/2 must wait for an in-flight request to
%% finish (not abandon it) before disconnecting, and the connection must
%% come back available afterward. Uses a single-connection pool so the
%% slow call is unambiguously on connection 0 (no FIFO-selection guessing).
tcp_bounce_waits_for_drain_test() ->
  {Srv, SupPid} = setup(1),
  try
    Parent = self(),
    %% Tie up the pool's one connection for 150ms with a slow request,
    %% then bounce it immediately -- bounce/2 must block until that
    %% request's reply lands (the backlog drains) before disconnecting.
    spawn(fun() -> Parent ! {slow_result, arterial_client:call(tcp_echo_pool, {delay, 150, slow}, 1000)} end),
    timer:sleep(20), % give the slow call time to actually check out conn 0

    {ok, Pid} = conn_pid(tcp_echo_pool, 0),
    BounceStart = erlang:monotonic_time(millisecond),
    ok = arterial_connection:bounce(Pid, 1000),
    BounceMs = erlang:monotonic_time(millisecond) - BounceStart,

    %% The bounce must not have returned before the slow call's ~150ms
    %% reply landed -- proves it waited for drain rather than abandoning
    %% the in-flight request.
    true = BounceMs >= 100,

    {slow_result, {ok, slow}} = receive Msg -> Msg after 1000 -> error(timeout) end,

    %% Connection 0 must be usable again after the bounce's reconnect.
    {ok, hello} = arterial_client:call(tcp_echo_pool, {echo, hello}, 1000)
  after
    teardown({Srv, SupPid})
  end.

%% A request that never gets a reply must not block the bounce forever --
%% past DrainTimeoutMs, bounce/2 forces the disconnect/reconnect anyway
%% and reports {error, timeout}.
tcp_bounce_drain_timeout_test() ->
  {Srv, SupPid} = setup(1),
  try
    spawn(fun() -> arterial_client:call(tcp_echo_pool, {delay, 5000, never_seen}, 6000) end),
    timer:sleep(20),

    {ok, Pid} = conn_pid(tcp_echo_pool, 0),
    {error, timeout} = arterial_connection:bounce(Pid, 100),

    %% Forced past the stuck request: the connection must still come back
    %% usable once it reconnects.
    {ok, hello} = arterial_client:call(tcp_echo_pool, {echo, hello}, 1000)
  after
    teardown({Srv, SupPid})
  end.

conn_pid(Pool, ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.

%% With `addresses => [Dead, Live]` (sharing one port), every reconnect
%% must skip the first (unreachable) address and fall through to the
%% second without waiting out a backoff interval in between -- proving
%% failover tries the whole list in order on a single reconnect attempt,
%% not just eventually after several backed-off retries.
tcp_multi_address_failover_test() ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  %% "Dead" address: a real listening socket on the *same* port, bound to
  %% a different loopback address, stopped immediately so it's a
  %% deterministic connection-refused target (vs. a firewall/timeout-
  %% dependent unroutable address).
  {ok, DeadSrv} = test_tcp_server:start(Port, {127, 0, 0, 2}),
  ok = test_tcp_server:stop(DeadSrv),

  {ok, SupPid} = arterial_pool:start_link(tcp_echo_pool, #{
    size        => 1,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{
      addresses => ["127.0.0.2", "127.0.0.1"],
      port      => Port,
      protocol  => tcp
    }
  }),
  try
    wait_until_available(tcp_echo_pool, 1, 100),
    {ok, hello} = arterial_client:call(tcp_echo_pool, {echo, hello}, 1000)
  after
    supervisor:stop(SupPid),
    arterial_nif:destroy(tcp_echo_pool),
    test_tcp_server:stop(Srv)
  end.

%% Map-entry addresses (`#{address => IP, port => Port}`) let each entry
%% carry its own port, independent of the connection's shared `port` --
%% needed to test against multiple independent server instances on
%% localhost rather than just multiple addresses sharing one port.
tcp_multi_address_per_entry_port_test() ->
  %% A dead entry, deliberately on its own port (no top-level `port`
  %% option set at all -- proves the map entries are fully self-contained
  %% and don't need a fallback).
  {ok, DeadSrv} = test_tcp_server:start(0),
  DeadPort = test_tcp_server:port(DeadSrv),
  ok = test_tcp_server:stop(DeadSrv),

  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),

  {ok, SupPid} = arterial_pool:start_link(tcp_echo_pool, #{
    size        => 1,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{
      addresses => [
        #{address => "127.0.0.1", port => DeadPort},
        #{address => "127.0.0.1", port => Port}
      ],
      protocol => tcp
    }
  }),
  try
    wait_until_available(tcp_echo_pool, 1, 100),
    {ok, hello} = arterial_client:call(tcp_echo_pool, {echo, hello}, 1000)
  after
    supervisor:stop(SupPid),
    arterial_nif:destroy(tcp_echo_pool),
    test_tcp_server:stop(Srv)
  end.
