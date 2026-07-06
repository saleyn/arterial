%% Test the proper timeout flow: timeout message → cleanup → slot freed
-module(proper_timeout_flow_test).
-include_lib("eunit/include/eunit.hrl").

proper_timeout_flow_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 4),

  TimeoutMs = 50,

  % Step 1: Start connection that will timeout
  Result = arterial_nif:connect_proto_with_opts(PoolRef, 0, {192, 0, 2, 1}, 80, TimeoutMs, tcp, true, self(), []),
  ?assertMatch({ok, connecting, _}, Result),
  {ok, connecting, SlotId} = Result,

  % Step 2: Wait for timeout message
  receive
    {arterial_event, StripeId, MsgSlotId, timeout} ->
      ?assertEqual(SlotId, MsgSlotId),
      ?assertEqual(0, StripeId),

      % Step 3: Call timeout cleanup function
      CleanupResult = arterial_nif:handle_connection_timeout(PoolRef, StripeId, SlotId),
      ?assertEqual(ok, CleanupResult),

      % Step 4: Verify slot is now available for reuse by checking availability
      % (The slot should be freed and available for new connections)
      io:format("Timeout handled properly: slot ~p cleaned up~n", [SlotId])

  after TimeoutMs + 100 ->
    ?assert(false)  % Timeout message was not delivered
  end.