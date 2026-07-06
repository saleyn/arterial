%% vim:ts=2:sw=2:et
%% Poolboy worker for reactor_bench's echo protocol:
%%   <<Seq:32/big, Payload/binary>>
%% Each worker owns one persistent gen_tcp socket, serves one request at a
%% time via gen_server:call/3.
-module(reactor_bench_poolboy_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-export([start_link/1, call/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(st, {sock :: gen_tcp:socket(), msg_size :: pos_integer()}).

-spec start_link(proplists:proplist()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

%% Send one framed request, block for the matching echo reply.
-spec call(pid(), non_neg_integer(), binary()) -> {ok, binary()} | {error, term()}.
call(Worker, Seq, Payload) ->
  gen_server:call(Worker, {echo, Seq, Payload}, 5000).

init(Args) ->
  Address = proplists:get_value(address, Args, "127.0.0.1"),
  Port    = proplists:get_value(port, Args),
  MsgSize = proplists:get_value(msg_size, Args, 8),
  case gen_tcp:connect(Address, Port,
                       [binary, {active, false}, {packet, raw}, {nodelay, true}]) of
    {ok, Sock} -> {ok, #st{sock = Sock, msg_size = MsgSize}};
    {error, Reason} -> {stop, Reason}
  end.

handle_call({echo, Seq, Payload}, _From, #st{sock = Sock, msg_size = MsgSize} = St) ->
  Frame = <<Seq:32/big, Payload/binary>>,
  Reply =
    case gen_tcp:send(Sock, Frame) of
      ok    -> recv_reply(Sock, MsgSize, <<>>);
      Error -> Error
    end,
  {reply, Reply, St}.

handle_cast(_Msg, St) -> {noreply, St}.
handle_info(_Info, St) -> {noreply, St}.
terminate(_Reason, #st{sock = Sock}) -> gen_tcp:close(Sock), ok.
code_change(_OldVsn, St, _Extra) -> {ok, St}.

recv_reply(_Sock, MsgSize, Buf) when byte_size(Buf) >= MsgSize ->
  <<Frame:MsgSize/binary, _/binary>> = Buf,
  {ok, Frame};
recv_reply(Sock, MsgSize, Buf) ->
  case gen_tcp:recv(Sock, 0, 5000) of
    {ok, Data} -> recv_reply(Sock, MsgSize, <<Buf/binary, Data/binary>>);
    Error      -> Error
  end.
