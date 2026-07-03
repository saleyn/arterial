-module(s4_only_test).
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 5000).

s4_test_() -> {timeout, 20, fun s4_test/0}.

s4_test() ->
  application:ensure_all_started(arterial),
  Bin = find_bench(),
  Port = erlang:open_port({spawn_executable, Bin}, [binary, {args, ["server"]}, use_stdio, stderr_to_stdout]),
  receive {Port, {data, D}} ->
    [_, PS] = string:split(string:trim(binary_to_list(D)), " "),
    SPort = list_to_integer(PS),
    io:format(standard_error, "server port: ~p~n", [SPort]),
    {ok, PoolRef} = arterial_nif:init_pool(1, 1),
    Parent = self(),
    spawn(fun() ->
      SlotId = connect_slot(PoolRef, 0, {127,0,0,1}, SPort),
      io:format(standard_error, "connected slotid=~p~n", [SlotId]),
      {ok, _} = arterial_nif:send_and_release(PoolRef, 0, [<<0:32/big, 16#ABCDABCD:32>>]),
      io:format(standard_error, "sent~n", []),
      receive Msg -> io:format(standard_error, "reply: ~p~n", [Msg])
      after ?TIMEOUT -> io:format(standard_error, "RECV TIMEOUT~n", [])
      end,
      Parent ! worker_done
    end),
    receive worker_done -> ok after 8000 -> ?assert(false) end,
    Port ! {self(), close}
  after 3000 -> ?assert(false)
  end.

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

find_bench() ->
  Beam = filename:absname(code:which(s4_only_test)),
  Dir  = filename:dirname(Beam),
  filename:join([Dir, "reactor_bench"]).
