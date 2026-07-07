-module(s4_multi_test).
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 5000).
-define(CONNS, 4).
-define(REQS, 100).

s4_multi_test_() -> {timeout, 30, fun do_s4_multi/0}.

do_s4_multi() ->
  application:ensure_all_started(arterial),
  {ok, SPort, ServerState} = nif_echo_server:start(),
  {ok, PoolRef} = arterial_nif:init_pool(?CONNS, 1),
  Parent = self(),
  Pids = [spawn(fun() ->
    try
      SlotId = connect_slot(PoolRef, Idx, {127,0,0,1}, SPort),
      ok = send_loop(PoolRef, Idx, SlotId, 0, ?REQS)
    catch E:R -> io:format(standard_error, "worker ~p error: ~p:~p~n", [Idx, E, R])
    end,
    Parent ! {done, self()}
  end) || Idx <- lists:seq(0, ?CONNS-1)],
  [receive {done, P} -> ok after 10000 -> error(worker_timeout) end || P <- Pids],
  nif_echo_server:stop(ServerState),
  ?assert(true).

send_loop(_Pool, _Idx, _Slot, _Seq, 0) -> ok;
send_loop(Pool, Idx, Slot, Seq, Rem) ->
  Req = <<Seq:32/big, 16#ABCDABCD:32>>,
  {ok, _} = arterial_nif:send_and_release(Pool, Idx, [Req]),
  receive
    {arterial_event, Idx, Slot, read, <<Seq:32/big, _:32>>} -> ok;
    {arterial_event, Idx, Slot, read, _Other} -> ok;
    {arterial_event, Idx, Slot, closed} -> error(closed)
  after ?TIMEOUT ->
    error({recv_timeout, Idx, Seq, Rem})
  end,
  send_loop(Pool, Idx, Slot, Seq+1, Rem-1).

connect_slot(Pool, Idx, Addr, Port) ->
  R = arterial_nif:connect_proto_with_opts(Pool, Idx, Addr, Port, 3000, tcp, true, self(), []),
  case R of
    {ok, connecting, S} ->
      receive
        {arterial_event, Idx, S, connect_result, ok} -> S;
        {arterial_event, Idx, S, connect_result, _}  -> error(connect_failed);
        {arterial_event, Idx, S, timeout}            -> error(connect_timeout)
      after 3000 -> error(connect_timeout)
      end;
    {ok, S} -> S
  end.
