%% vim:ts=2:sw=2:et
%% Simple shackle_client for reactor_bench's echo protocol:
%%   <<Seq:32/big, Payload/binary>>
%% The server echoes the exact bytes back.  shackle tags each request with
%% its own external_request_id so multiple in-flight requests on one socket
%% are demuxed correctly; we set backlog_size => 1 to keep one request per
%% connection, matching the other reactor_bench clients.
-module(reactor_bench_shackle_client).
-behaviour(shackle_client).

-export([init/1, setup/2, handle_request/2, handle_data/2, terminate/1]).

-record(st, {msg_size = 8 :: pos_integer(), buf = <<>> :: binary()}).

init(Options) ->
  MsgSize = proplists:get_value(msg_size, Options, 8),
  {ok, #st{msg_size = MsgSize}}.

setup(Socket, State) ->
  inet:setopts(Socket, [binary, {nodelay, true}]),
  {ok, State}.

%% Request is {Seq :: non_neg_integer(), Payload :: binary()}.
%% We use Seq as the shackle external_request_id so the demux is exact.
handle_request({Seq, Payload}, #st{} = State) ->
  Frame = <<Seq:32/big, Payload/binary>>,
  {ok, Seq, Frame, State}.

handle_data(Data, #st{msg_size = MsgSize, buf = Buf} = State) ->
  All = <<Buf/binary, Data/binary>>,
  decode_all(All, MsgSize, [], State).

decode_all(Buf, MsgSize, Acc, State) when byte_size(Buf) >= MsgSize ->
  <<Frame:MsgSize/binary, Rest/binary>> = Buf,
  <<Seq:32/big, _/binary>> = Frame,
  decode_all(Rest, MsgSize, [{Seq, Frame} | Acc], State);
decode_all(Buf, _MsgSize, Acc, State) ->
  {ok, lists:reverse(Acc), State#st{buf = Buf}}.

terminate(_State) -> ok.
