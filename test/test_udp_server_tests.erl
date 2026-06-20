-module(test_udp_server_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
End-to-end example: a real UDP server (`test_udp_server`) driven via
`arterial_client:call/3` over an actual loopback UDP socket.

`arterial_socket:connect/5`'s `udp` clause only binds a local socket (UDP
has no connection handshake), so it doesn't associate a default peer for
plain `send/2`/`recv/2` -- and `arterial_connection`'s `client_opts` has
no option to request that. This test therefore connects the UDP socket
itself (`socket:connect/3`, which *does* support associating a default
peer for a `dgram` socket) and registers it directly with the pool via
`arterial_nif:set_socket/3` + `make_available/2`, bypassing
`arterial_connection`/`arterial_pool` -- the same pattern
`arterial_nif_tests` uses for its NIF-level tests, just with a real
socket instead of the atom `dummy_socket`.
""".

setup() ->
  {ok, Srv} = test_udp_server:start(0),
  Port = test_udp_server:port(Srv),
  {ok, Sock} = socket:open(inet, dgram, udp),
  ok = socket:connect(Sock, #{family => inet, addr => {127,0,0,1}, port => Port}),
  ok = arterial_nif:create(udp_echo_pool, 1, 1, true, test_echo_protocol),
  true = arterial_nif:set_socket(udp_echo_pool, 0, Sock),
  true = arterial_nif:make_available(udp_echo_pool, 0),
  {Srv, Sock}.

teardown({Srv, Sock}) ->
  ok = arterial_nif:destroy(udp_echo_pool),
  socket:close(Sock),
  test_udp_server:stop(Srv).

udp_echo_test() ->
  Ctx = setup(),
  try
    {ok, hello} = arterial_client:call(udp_echo_pool, {echo, hello}, 1000)
  after
    teardown(Ctx)
  end.

udp_upcase_test() ->
  Ctx = setup(),
  try
    {ok, <<"ARTERIAL">>} =
      arterial_client:call(udp_echo_pool, {upcase, <<"arterial">>}, 1000)
  after
    teardown(Ctx)
  end.

udp_delayed_reply_test() ->
  Ctx = setup(),
  try
    {ok, slow} = arterial_client:call(udp_echo_pool, {delay, 50, slow}, 1000)
  after
    teardown(Ctx)
  end.

udp_unknown_request_test() ->
  Ctx = setup(),
  try
    {ok, {error, unknown_request}} =
      arterial_client:call(udp_echo_pool, surprise, 1000)
  after
    teardown(Ctx)
  end.

%% Sequential calls on the same UDP socket each get their own reply
%% matched by wire-level request id, just like the TCP byte-stream case.
udp_sequential_calls_test() ->
  Ctx = setup(),
  try
    {ok, 1} = arterial_client:call(udp_echo_pool, {echo, 1}, 1000),
    {ok, 2} = arterial_client:call(udp_echo_pool, {echo, 2}, 1000),
    {ok, 3} = arterial_client:call(udp_echo_pool, {echo, 3}, 1000)
  after
    teardown(Ctx)
  end.

%% A reply that doesn't arrive before the caller's Timeout surfaces as a
%% plain {error, timeout}, not a crash or hang.
udp_call_timeout_test() ->
  Ctx = setup(),
  try
    {error, timeout} =
      arterial_client:call(udp_echo_pool, {delay, 200, late}, 50)
  after
    %% Let the server's delayed datagram land before closing the socket.
    timer:sleep(250),
    teardown(Ctx)
  end.
