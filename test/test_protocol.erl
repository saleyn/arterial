 -module(test_protocol).
 -behaviour(arterial_protocol).

 -export([connect/3, close/1, send/2, recv/2, setopts/2,
          encode_request/3, decode_reply/2]).

%% Simple in-test protocol implementation used by unit tests.
%% The socket value used is the tuple {test_server, Pid} where Pid
%% is the server process. send/2 forwards the binary to the server;
%% recv/2 waits for a {reply, Binary} message in the caller mailbox.

connect(_Addr, _Port, _Opts) ->
  {ok, undefined}.

close(_Sock) -> ok.

send({test_server, Pid}, Data) when is_pid(Pid) ->
  Pid ! {request, self(), Data},
  ok;
send(_Sock, _Data) -> {error, bad_socket}.

recv(_Sock, Timeout) when is_integer(Timeout) ->
  receive
    {reply, Bin} -> {ok, Bin}
  after Timeout -> {error, timeout}
  end.

setopts(_Sock, _Opts) -> ok.

encode_request(ReqID, Request, _Timeout) ->
  {ok, term_to_binary({ReqID, Request})}.

decode_reply(ReqID, Buffer) when is_binary(Buffer) ->
  try
    case binary_to_term(Buffer) of
      {ReqID1, Reply} when ReqID1 =:= ReqID -> {ok, Reply, <<>>};
      _ -> {more, Buffer}
    end
  catch _:_ -> {more, Buffer}
  end.
