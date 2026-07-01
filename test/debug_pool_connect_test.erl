%% Debug test for pool-style connection
-module(debug_pool_connect_test).
-include_lib("eunit/include/eunit.hrl").

debug_pool_connect_test() ->
  application:ensure_all_started(arterial),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),

  {ok, SupPid} = arterial_pool:start_link(debug_pool, #{
    size     => 2,
    codec    => arterial_codec_default,
    address  => "127.0.0.1",
    port     => Port,
    protocol => tcp
  }),

  io:format("Waiting for 2 connections...~n"),
  timer:sleep(2000),
  io:format("Slot 0 available: ~p~n", [arterial_nif:is_slot_available(arterial_pool:pool_ref(debug_pool), 0, 0)]),
  io:format("Slot 1 available: ~p~n", [arterial_nif:is_slot_available(arterial_pool:pool_ref(debug_pool), 1, 0)]),
  Result = arterial_pool:wait_connected(debug_pool, 2, 10000),
  io:format("wait_connected result: ~p~n", [Result]),

  ?assertEqual(ok, Result),

  supervisor:stop(SupPid),
  arterial_pool:stop(debug_pool),
  test_tcp_server:stop(Srv),
  application:stop(arterial).