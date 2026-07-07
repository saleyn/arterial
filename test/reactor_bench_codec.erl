%% vim:ts=2:sw=2:et
%% Codec for reactor_bench's fixed-size echo protocol:
%%   <<CorrId:32/big, Payload:(MsgSize-4)/binary>>
%%
%% The server echoes the frame verbatim.  CorrId doubles as the arterial
%% correlation id.  Because the protocol has no length prefix the frame
%% size is stored in a persistent_term before the pool is started and
%% read back on every decode call.
-module(reactor_bench_codec).
-behaviour(arterial_codec).
-export([encode_request/2, decode/1]).
-export([set_msg_size/1]).

-define(PT_KEY, {?MODULE, msg_size}).

-spec set_msg_size(pos_integer()) -> ok.
set_msg_size(MsgSize) ->
  persistent_term:put(?PT_KEY, MsgSize).

-spec encode_request(non_neg_integer(), binary()) -> iodata().
encode_request(CorrId, Payload) ->
  <<CorrId:32/big, Payload/binary>>.

-spec decode(binary()) ->
  {ok, non_neg_integer(), binary(), binary()} | more | {error, term()}.
decode(Buf) ->
  MsgSize = persistent_term:get(?PT_KEY),
  case byte_size(Buf) >= MsgSize of
    false ->
      more;
    true ->
      <<Frame:MsgSize/binary, Rest/binary>> = Buf,
      <<CorrId:32/big, _/binary>> = Frame,
      {ok, CorrId, Frame, Rest}
  end.
