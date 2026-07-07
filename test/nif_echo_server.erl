-module(nif_echo_server).
-export([start/0, port/1, stop/1]).

%% Start a NIF-based echo server.  Returns {ok, Port, State}.
start() ->
  {ok, PoolRef} = arterial_nif:init_pool(256, 1),
  {ok, {ListenFd, SPort}} = arterial_nif:reactor_listen(PoolRef, 0),
  Self = self(),
  Pid = spawn(fun() ->
    ok = arterial_nif:reactor_accept(PoolRef, ListenFd, self()),
    Self ! ready,
    accept_loop(PoolRef, ListenFd, 0)
  end),
  receive ready -> ok after 2000 -> error(start_timeout) end,
  {ok, SPort, {Pid, ListenFd, PoolRef}}.

port({_Pid, _ListenFd, _PoolRef} = _State) ->
  error(use_start_return_value).

stop({Pid, ListenFd, PoolRef}) ->
  exit(Pid, kill),
  arterial_nif:reactor_close_fd(PoolRef, ListenFd).

accept_loop(PoolRef, ListenFd, StripeId) ->
  receive
    {arterial_accept, ListenFd, ClientFd, _IP, _Port} ->
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
  after 10000 -> ok
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
  after 10000 -> ok
  end.
