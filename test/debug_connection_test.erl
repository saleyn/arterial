%% Quick test to debug connection establishment
-module(debug_connection_test).
-include_lib("eunit/include/eunit.hrl").

debug_connect_to_local_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 4),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),

  io:format("Connecting to 127.0.0.1:~p~n", [Port]),

  Result = arterial_nif:connect_proto_with_opts(
    PoolRef, 0, {127,0,0,1}, Port, 5000, tcp, true, self(), []),

  io:format("Connect result: ~p~n", [Result]),

  case Result of
    {ok, connecting, SlotId} ->
      io:format("Waiting for write or connect_result event...~n"),
      receive
        {arterial_event, StripeId, MsgSlotId, write} ->
          io:format("Got write event for ~p/~p, calling handle_writable~n",
                    [StripeId, MsgSlotId]),
          Ret = arterial_nif:handle_writable(PoolRef, StripeId, MsgSlotId),
          io:format("handle_writable returned: ~p~n", [Ret]),
          receive
            Msg2 ->
              io:format("Got follow-up message: ~p~n", [Msg2])
          after 500 ->
            io:format("No follow-up message~n")
          end;
        {arterial_event, _, MsgSlotId, connect_result, ok} ->
          io:format("Got direct connect_result ok for slot ~p~n", [MsgSlotId]);
        Other ->
          io:format("Got unexpected: ~p~n", [Other])
      after 2000 ->
        io:format("TIMEOUT: No write event received~n")
      end,
      ?assert(SlotId =:= 0);
    Other ->
      io:format("Unexpected initial result: ~p~n", [Other]),
      ?assert(false)
  end,
  test_tcp_server:stop(Srv).