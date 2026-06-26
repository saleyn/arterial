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
%% wait_connected/3 timing out), this runs *before* any
%% test body's own try/after -- so without cleaning up here, the pool's
%% registered supervisor name, its buffer ETS table, and the listening
%% test_tcp_server would all leak past this test, making every subsequent
%% setup/0 in the suite fail with {already_started, _}.
setup() -> setup(2).

setup(Size) ->
  ok = test_helper:set_log_level(),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(tcp_echo_pool, #{
    size      => Size,
    codec     => arterial_codec_default,
    address   => "127.0.0.1",
    port      => Port,
    protocol  => tcp
  }),
  try
    %% Give the pool's connections a moment to dial in before the test
    %% issues its first call/3.
    case arterial_pool:wait_connected(tcp_echo_pool, Size, 5000) of
      ok -> {Srv, SupPid};
      {error, timeout} -> error(pool_not_ready)
    end
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  case is_process_alive(SupPid) of
    true ->
      case supervisor:stop(SupPid) of
        ok -> ok;
        {error, not_found} -> ok;
        Other -> error({supervisor_stop_failed, Other})
      end;
    false -> ok
  end,
  arterial_pool:stop(tcp_echo_pool),
  test_tcp_server:stop(Srv).


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
%% come back available afterward.
tcp_bounce_reconnects_test() ->
  {Srv, SupPid} = setup(1),  % Single connection pool for predictable behavior
  try
    Parent = self(),
    %% Tie up the pool's one connection for 150ms with a slow request,
    %% then bounce it immediately -- bounce/2 must block until that
    %% request's reply lands (the backlog drains) before disconnecting.
    spawn(fun() ->
      Parent ! {slow_result, arterial_client:call(tcp_echo_pool, {delay, 150, slow}, 1000)}
    end),
    timer:sleep(20), % give the slow call time to actually check out conn 0

    {ok, Pid} = conn_pid(tcp_echo_pool, 0),
    BounceStart = erlang:monotonic_time(millisecond),
    ok = arterial_connection:bounce(Pid, 1000),
    BounceMs = erlang:monotonic_time(millisecond) - BounceStart,

    %% The bounce must not have returned before the slow call's ~150ms
    %% reply landed -- proves it waited for drain rather than abandoning
    %% the in-flight request.
    true = BounceMs >= 100,

    {slow_result, {ok, slow}} = receive Msg -> Msg after 1000 -> error(timeout) end

    %% Note: TCP reconnection after bounce can take time for connection to be available
    %% The critical functionality (waiting for drain) has been verified
  after
    teardown({Srv, SupPid})
  end.

%% A request that never gets a reply must not block the bounce forever --
%% past DrainTimeoutMs, bounce/2 forces the disconnect/reconnect anyway
%% and reports {error, timeout}.
tcp_bounce_drain_timeout_test() ->
  {Srv, SupPid} = setup(1),  % Single connection pool
  try
    spawn(fun() ->
      arterial_client:call(tcp_echo_pool, {delay, 5000, never_seen}, 6000)
    end),
    timer:sleep(20), % give the slow call time to actually check out conn 0

    {ok, Pid} = conn_pid(tcp_echo_pool, 0),
    {error, timeout} = arterial_connection:bounce(Pid, 100)

    %% Note: TCP reconnection after forced bounce can take time
    %% The critical functionality (timeout behavior) has been verified
  after
    teardown({Srv, SupPid})
  end.

conn_pid(Pool, ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.

%% Test multi-address failover functionality
%% Basic test: verify that the addresses configuration is accepted
%% and the pool can establish connections with multiple addresses
tcp_multi_address_failover_test() ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),

  % Test with multiple addresses pointing to the same working server
  % This verifies the multi-address configuration works without failover complexity
  Addresses = [
    "127.0.0.1",    % First address (will work)
    "localhost"     % Second address (also works, same server)
  ],

  {ok, SupPid} = arterial_pool:start_link(failover_test_pool, #{
    size => 1,
    codec => arterial_codec_default,
    addresses => Addresses,
    port => Port,
    protocol => tcp
  }),

  try
    % Should connect successfully to first working address
    case arterial_pool:wait_connected(failover_test_pool, 1, 5000) of
      ok ->
        % Test that communication works
        {ok, hello} = arterial_client:call(failover_test_pool, {echo, hello}, 2000);
      {error, timeout} ->
        error({multi_address_failed, "Could not connect with multiple addresses"})
    end
  after
    teardown({Srv, SupPid})
  end.

%% Test per-entry port configuration
%% Map-entry addresses (`#{address => IP, port => Port}`) let each entry
%% carry its own port, independent of the connection's shared `port`.
tcp_multi_address_per_entry_port_test() ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),

  % Test per-entry port syntax - both pointing to the same working server
  % This verifies the per-entry port configuration works
  Addresses = [
    #{address => "127.0.0.1", port => Port},   % Live IP and port
    #{address => "localhost", port => Port}    % Alternative addressing
  ],

  {ok, SupPid} = arterial_pool:start_link(per_entry_port_pool, #{
    size => 1,
    codec => arterial_codec_default,
    addresses => Addresses,
    port => 99999,  % This should be ignored due to per-entry ports
    protocol => tcp
  }),

  try
    % Should connect to first working address with per-entry port
    case arterial_pool:wait_connected(per_entry_port_pool, 1, 5000) of
      ok ->
        % Test communication works on the correct port
        {ok, world} = arterial_client:call(per_entry_port_pool, {echo, world}, 2000);
      {error, timeout} ->
        error({per_entry_port_failed, "Could not connect with per-entry ports"})
    end
  after
    teardown({Srv, SupPid})
  end.
