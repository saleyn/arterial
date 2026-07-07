-module(debug_test).
-include_lib("eunit/include/eunit.hrl").
-define(POOL, debug_echo_pool7).

debug_call_test_() ->
  {timeout, 30, fun debug_call_test/0}.

debug_call_test() ->
  ok = test_helper:set_log_level(),
  persistent_term:erase(arterial_observe),
  ok = application:set_env(arterial, observability, undefined),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(?POOL, #{
    size => 1, codec => arterial_codec_default,
    address => "127.0.0.1", port => Port, protocol => tcp
  }),
  ok = arterial_pool:wait_connected(?POOL, 1, 2000),
  timer:sleep(200),

  PoolRef = arterial_pool:pool_ref(?POOL),
  Children = supervisor:which_children(arterial_pool:sup_name(?POOL)),
  {_, ConnPid, _, _} = lists:keyfind({arterial_connection, 0}, 1, Children),

  % Register corr with CONN PID (not self()) to see if reactor routes to conn pid
  CorrId = 88888,
  Codec = arterial_pool:codec(?POOL),
  Data = iolist_to_binary(Codec:encode_request(CorrId, {echo, conn_test})),
  Deadline = os:system_time(microsecond) + 5000000,
  ok = arterial_nif:register_corr(PoolRef, 0, CorrId, ConnPid, 0, Deadline),
  {ok, _} = arterial_nif:send_and_release(PoolRef, 0, [Data]),
  timer:sleep(500),
  supervisor:stop(SupPid),
  test_tcp_server:stop(Srv).
