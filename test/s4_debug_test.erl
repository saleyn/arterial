-module(s4_debug_test).
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 2000).
-define(CONNS, 8).
-define(REQS, 50).

s4_debug_test() ->
  application:ensure_all_started(arterial),
  Bin = find_bench(),
  Port = erlang:open_port({spawn_executable, Bin}, [binary, {args, ["server"]}, use_stdio, stderr_to_stdout]),
  receive {Port, {data, D}} ->
    [_, PS] = string:split(string:trim(binary_to_list(D)), " "),
    SPort = list_to_integer(PS),
    {ok, PoolRef} = arterial_nif:init_pool(?CONNS, 1),
    Parent = self(),
    T0 = erlang:monotonic_time(millisecond),
    Pids = [spawn(fun() ->
      try
        SlotId = connect_slot(PoolRef, Idx, {127,0,0,1}, SPort),
        ok = send_loop(PoolRef, Idx, SlotId, 0, ?REQS, <<>>)
      catch E:R:ST ->
        io:format(standard_error, "worker ~p: ~p:~p~n~p~n", [Idx,E,R,ST])
      end,
      Parent ! {done, self()}
    end) || Idx <- lists:seq(0, ?CONNS-1)],
    Results = [receive {done, P} -> ok after 10000 ->
      io:format(standard_error, "WORKER TIMEOUT pid=~p~n", [P]), timeout
    end || P <- Pids],
    T1 = erlang:monotonic_time(millisecond),
    Errs = length([X || X <- Results, X =:= timeout]),
    Port ! {self(), close},
    ?assertEqual(0, Errs)
  after 3000 -> ?assert(false)
  end.

send_loop(_P, _I, _S, _Seq, 0, _Buf) -> ok;
send_loop(Pool, Idx, Slot, Seq, Rem, Buf) ->
  Req = <<Seq:32/big, 16#ABCDABCD:32>>,
  {ok, _} = arterial_nif:send_and_release(Pool, Idx, [Req]),
  case recv_frame(Idx, Slot, Buf) of
    {ok, <<Seq:32/big, _:32>>, Rest} ->
      send_loop(Pool, Idx, Slot, Seq+1, Rem-1, Rest);
    {ok, Frame, Rest} ->
      io:format(standard_error, "seq mismatch idx=~p exp=~p got=~p~n", [Idx, Seq, Frame]),
      send_loop(Pool, Idx, Slot, Seq+1, Rem-1, Rest);
    {error, Reason} ->
      erlang:error({recv_failed, Idx, Seq, Reason})
  end.

recv_frame(_Idx, _Slot, Buf) when byte_size(Buf) >= 8 ->
  <<Frame:8/binary, Rest/binary>> = Buf,
  {ok, Frame, Rest};
recv_frame(Idx, Slot, Buf) ->
  receive
    {arterial_event, Idx, Slot, read, <<>>} ->
      recv_frame(Idx, Slot, Buf);
    {arterial_event, Idx, Slot, read, More} ->
      recv_frame(Idx, Slot, <<Buf/binary, More/binary>>);
    {arterial_event, Idx, Slot, closed} ->
      {error, closed}
  after ?TIMEOUT ->
    io:format(standard_error, "recv_frame timeout idx=~p slot=~p buf_sz=~p~n",
              [Idx, Slot, byte_size(Buf)]),
    {error, timeout}
  end.

connect_slot(Pool, Idx, Addr, Port) ->
  R = arterial_nif:connect_proto_with_opts(Pool, Idx, Addr, Port, 2000, tcp, true, self(), []),
  case R of
    {ok, connecting, S} ->
      receive
        {arterial_event, Idx, S, connect_result, ok} -> S;
        {arterial_event, Idx, S, connect_result, _}  -> error(connect_failed);
        {arterial_event, Idx, S, timeout}            -> error(connect_timeout)
      after 2000 -> error(connect_timeout)
      end;
    {ok, S} -> S
  end.

find_bench() ->
  Beam = filename:absname(code:which(?MODULE)),
  filename:join([filename:dirname(Beam), "reactor_bench"]).
