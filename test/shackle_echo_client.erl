-module(shackle_echo_client).
-behaviour(shackle_client).

-moduledoc """
Minimal `shackle_client` callback module paired with the built-in
`shackle_tcp` protocol, used by `shackle_bench` to drive the same
`test_tcp_server`/wire framing as `arterial_bench`'s `test_echo_client`
(reusing `test_echo_protocol:frame/2`/`unframe/1` directly) -- so the two
benchmarks measure each library's own dispatch overhead against an
identical server and identical bytes on the wire, not a difference in
test fixtures.
""".

-export([init/1, setup/2, handle_request/2, handle_data/2, terminate/1]).

-record(st, {next_id = 0 :: non_neg_integer(), buf = <<>> :: binary()}).

init(_Options) ->
  {ok, #st{}}.

setup(_Socket, State) ->
  {ok, State}.

handle_request(Request, #st{next_id = ReqID} = State) ->
  Data = test_echo_protocol:frame(ReqID, term_to_binary(Request)),
  {ok, ReqID, Data, State#st{next_id = ReqID + 1}}.

handle_data(Data, #st{buf = Buf} = State) ->
  %% OTP 29 compatibility: ensure Data is binary
  DataBin = case is_binary(Data) of
    true -> Data;
    false -> iolist_to_binary(Data)
  end,
  decode_all(<<Buf/binary, DataBin/binary>>, [], State).

decode_all(Buffer, Acc, State) ->
  case test_echo_protocol:unframe(Buffer) of
    {ok, ReqID, Payload, Rest} ->
      decode_all(Rest, [{ReqID, binary_to_term(Payload)} | Acc], State);
    more ->
      {ok, lists:reverse(Acc), State#st{buf = Buffer}}
  end.

terminate(_State) -> ok.
