-module(test_echo_client).

-moduledoc """
Minimal `arterial_client` callback module paired with
`test_echo_protocol`, used by the real-socket TCP/UDP end-to-end test
suites (`test_tcp_server_tests`, `test_udp_server_tests`).

No handshake is needed against `test_tcp_server`/`test_udp_server`, so
`init/1` and `setup/2` just carry the request counter used to mint
wire-level request ids for the asynchronous path
(`handle_request/2`/`handle_data/2`); the synchronous `arterial_client:call/3`
path used by the tests below doesn't go through these two at all.
""".

-export([init/1, setup/2, handle_request/2, handle_data/2, terminate/2]).

-record(st, {next_id = 0 :: non_neg_integer(), buf = <<>> :: binary()}).

init(_Options) ->
  {ok, #st{}}.

setup(_Socket, State) ->
  {ok, State}.

handle_request(Request, #st{next_id = ReqID} = State) ->
  {ok, Data} = test_echo_protocol:encode_request(ReqID, Request, infinity),
  {ok, [ReqID], Data, State#st{next_id = ReqID + 1}}.

handle_data(Data, #st{buf = Buf} = State) ->
  decode_all(<<Buf/binary, Data/binary>>, [], State).

decode_all(Buffer, Acc, State) ->
  case test_echo_protocol:unframe(Buffer) of
    {ok, _ReqID, Payload, Rest} ->
      decode_all(Rest, [binary_to_term(Payload) | Acc], State);
    more ->
      {ok, lists:reverse(Acc), State#st{buf = Buffer}}
  end.

terminate(_Reason, _State) -> ok.
