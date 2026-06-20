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

setup() ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(tcp_echo_pool, #{
    size        => 2,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{address => "127.0.0.1", port => Port, protocol => tcp}
  }),
  %% Give the pool's connections a moment to dial in before the test
  %% issues its first call/3.
  wait_until_available(tcp_echo_pool, 2, 50),
  {Srv, SupPid}.

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
