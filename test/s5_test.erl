-module(s5_test).
-include_lib("eunit/include/eunit.hrl").
-define(TIMEOUT, 3000).

s5_test() ->
  application:ensure_all_started(arterial),
  %% Start NIF server
  {ok, SPort, ServerState} = start_nif_server(),
  io:format(standard_error, "NIF server port: ~p~n", [SPort]),
  timer:sleep(50),
  %% Connect NIF client
  {ok, CPool} = arterial_nif:init_pool(1, 1),
  SlotId = connect(CPool, 0, {127,0,0,1}, SPort),
  io:format(standard_error, "client slot: ~p~n", [SlotId]),
  %% Ping-pong
  {ok, _} = arterial_nif:send_and_release(CPool, 0, [<<0:32/big, 0:32>>]),
  receive
    {arterial_event, 0, SlotId, read, Bin} ->
      io:format(standard_error, "reply: ~p~n", [Bin]),
      ?assert(byte_size(Bin) >= 8)
  after ?TIMEOUT ->
    io:format(standard_error, "TIMEOUT~n", []),
    ?assert(false)
  end,
  stop_nif_server(ServerState).

start_nif_server() ->
  {ok, PoolRef} = arterial_nif:init_pool(256, 1),
  {ok, {ListenFd, Port}} = arterial_nif:reactor_listen(PoolRef, 0),
  Self = self(),
  Pid = spawn(fun() ->
    ok = arterial_nif:reactor_accept(PoolRef, ListenFd, self()),
    Self ! ready,
    accept_loop(PoolRef, ListenFd, 0)
  end),
  receive ready -> ok after 2000 -> error(start_timeout) end,
  {ok, Port, {Pid, ListenFd, PoolRef}}.

stop_nif_server({Pid, ListenFd, PoolRef}) ->
  exit(Pid, kill),
  arterial_nif:reactor_close_fd(PoolRef, ListenFd).

accept_loop(PoolRef, ListenFd, StripeId) ->
  receive
    {arterial_accept, ListenFd, ClientFd, _IP, _Port} ->
      %% Spawn worker first so owner_pid = worker (reactor sends events to it).
      Acc = self(),
      spawn(fun() ->
        case arterial_nif:reactor_register_client(PoolRef, StripeId, ClientFd, self()) of
          {ok, SlotId} ->
            Acc ! {registered, StripeId, SlotId},
            echo_worker(PoolRef, StripeId, SlotId);
          {error, _} ->
            arterial_nif:reactor_close_fd(PoolRef, ClientFd)
        end
      end),
      receive {registered, StripeId, _} -> ok after 1000 -> ok end,
      accept_loop(PoolRef, ListenFd, StripeId + 1)
  after 5000 -> ok
  end.

echo_worker(PoolRef, StripeId, SlotId) ->
  receive
    {arterial_event, StripeId, SlotId, read, Bin} when byte_size(Bin) > 0 ->
      arterial_nif:send_and_release(PoolRef, StripeId, [Bin]),
      echo_worker(PoolRef, StripeId, SlotId);
    {arterial_event, StripeId, SlotId, read, <<>>} ->
      echo_worker(PoolRef, StripeId, SlotId);
    {arterial_event, StripeId, SlotId, closed} -> ok;
    _ -> echo_worker(PoolRef, StripeId, SlotId)
  after 5000 -> ok
  end.

connect(Pool, Idx, Addr, Port) ->
  R = arterial_nif:connect_proto_with_opts(Pool, Idx, Addr, Port, 2000, tcp, true, self(), []),
  case R of
    {ok, connecting, S} ->
      receive
        {arterial_event, Idx, S, connect_result, ok} -> S;
        {arterial_event, Idx, S, connect_result, _}  -> error(connect_failed)
      after 2000 -> error(connect_timeout)
      end;
    {ok, S} -> S
  end.
