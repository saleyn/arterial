-module(test_udp_server_tests).
-include_lib("eunit/include/eunit.hrl").


-moduledoc """
End-to-end example: a real UDP server (`test_udp_server`) driven via
`arterial_client:call/3` over an actual loopback UDP socket.

Since UDP is connectionless, this test manually creates and connects a UDP
socket, then registers it directly with the NIF pool using the new
`register_socket/4` API. This bypasses `arterial_connection`/`arterial_pool`
which are designed for connection-oriented protocols.
""".

setup() ->
  ok = test_helper:set_log_level(),
  {ok, Srv} = test_udp_server:start(0),
  Port = test_udp_server:port(Srv),

  % Use the normal arterial_pool approach - UDP is supported in the NIF
  % Generate unique pool name to avoid conflicts between tests
  PoolName = list_to_atom("udp_test_" ++ integer_to_list(erlang:unique_integer([positive]))),
  {ok, SupPid} = arterial_pool:start_link(PoolName, #{
    size      => 1,
    codec     => arterial_codec_default,
    address   => "127.0.0.1",
    port      => Port,
    protocol  => udp
  }),

  % Wait for UDP connection to be established
  timer:sleep(200),
  {Srv, SupPid, PoolName}.

teardown({Srv, _SupPid, PoolName}) ->
  arterial_pool:stop(PoolName),
  test_udp_server:stop(Srv).


udp_echo_test() ->
  {Srv, SupPid, PoolName} = setup(),
  try
    {ok, hello} = arterial_client:call(PoolName, {echo, hello}, 1000)
  after
    teardown({Srv, SupPid, PoolName})
  end.

udp_upcase_test() ->
  {Srv, SupPid, PoolName} = setup(),
  try
    {ok, <<"ARTERIAL">>} =
      arterial_client:call(PoolName, {upcase, <<"arterial">>}, 1000)
  after
    teardown({Srv, SupPid, PoolName})
  end.

udp_delayed_reply_test() ->
  {Srv, SupPid, PoolName} = setup(),
  try
    {ok, slow} = arterial_client:call(PoolName, {delay, 50, slow}, 1000)
  after
    teardown({Srv, SupPid, PoolName})
  end.

udp_unknown_request_test() ->
  {Srv, SupPid, PoolName} = setup(),
  try
    {ok, {error, unknown_request}} =
      arterial_client:call(PoolName, surprise, 1000)
  after
    teardown({Srv, SupPid, PoolName})
  end.

%% Sequential calls on the same UDP socket each get their own reply
%% matched by wire-level request id, just like the TCP byte-stream case.
udp_sequential_calls_test() ->
  {Srv, SupPid, PoolName} = setup(),
  try
    {ok, 1} = arterial_client:call(PoolName, {echo, 1}, 1000),
    {ok, 2} = arterial_client:call(PoolName, {echo, 2}, 1000),
    {ok, 3} = arterial_client:call(PoolName, {echo, 3}, 1000)
  after
    teardown({Srv, SupPid, PoolName})
  end.

%% A reply that doesn't arrive before the caller's Timeout surfaces as a
%% plain {error, timeout}, not a crash or hang.
udp_call_timeout_test() ->
  {Srv, SupPid, PoolName} = setup(),
  try
    {error, timeout} =
      arterial_client:call(PoolName, {delay, 200, late}, 50)
  after
    %% Let the server's delayed datagram land before closing the socket.
    timer:sleep(250),
    teardown({Srv, SupPid, PoolName})
  end.
