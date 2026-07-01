%% Test to verify timeout messages are now being delivered correctly
-module(timeout_message_test).
-include_lib("eunit/include/eunit.hrl").

timeout_message_delivered_test() ->
  application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(1, 4),

  TimeoutMs = 50,  % Very short timeout

  % Use non-routable IP to guarantee timeout
  Result = arterial_nif:connect_proto_with_opts(PoolRef, 0, {192, 0, 2, 1}, 80, TimeoutMs, tcp, true, self(), []),

  ?assertMatch({ok, connecting, _}, Result),
  {ok, connecting, SlotId} = Result,

  % Now we should receive the timeout message directly to our test process
  receive
    {arterial_event, StripeId, MsgSlotId, timeout} ->
      ?assertEqual(SlotId, MsgSlotId),
      ?assertEqual(0, StripeId),  % We used stripe 0
      ok
  after TimeoutMs + 100 ->
    ?assert(false)  % Timeout message was not delivered
  end.