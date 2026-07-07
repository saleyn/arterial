-module(s4_only_test).
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 5000).

s4_test_() -> {timeout, 20, fun do_s4/0}.

do_s4() ->
  application:ensure_all_started(arterial),
  {ok, SPort, ServerState} = nif_echo_server:start(),
  {ok, PoolRef} = arterial_nif:init_pool(1, 1),
  Parent = self(),
  spawn(fun() ->
    _SlotId = connect_slot(PoolRef, 0, {127,0,0,1}, SPort),
    {ok, _} = arterial_nif:send_and_release(PoolRef, 0, [<<0:32/big, 16#ABCDABCD:32>>]),
    receive _ -> ok
    after ?TIMEOUT -> error(recv_timeout)
    end,
    Parent ! worker_done
  end),
  receive worker_done -> ok after 8000 -> ?assert(false) end,
  nif_echo_server:stop(ServerState).

connect_slot(Pool, Idx, Addr, Port) ->
  R = arterial_nif:connect_proto_with_opts(Pool, Idx, Addr, Port, 3000, tcp, true, self(), []),
  case R of
    {ok, connecting, SlotId} ->
      receive
        {arterial_event, Idx, SlotId, connect_result, ok} -> SlotId;
        {arterial_event, Idx, SlotId, connect_result, _}  -> error(connect_failed);
        {arterial_event, Idx, SlotId, timeout}            -> error(timeout)
      after 3000 -> error(timeout)
      end;
    {ok, SlotId} -> SlotId
  end.
