-module(ssl_echo_protocol).
-behaviour(arterial_protocol).

-moduledoc """
The `ssl` counterpart of `test_echo_protocol`, used by
`test_ssl_server_tests`. Identical wire framing and request language --
the only difference is `send/2`/`recv/2` call `ssl:send/2`/`ssl:recv/3`
instead of `socket:send/2`/`socket:recv/3`, since `arterial_socket`'s
`ssl` transport hands back an `ssl:sslsocket()`, not a `socket:socket()`
(see that module's moduledoc for why this needs OTP 28+).

`connect/3`/`close/1` are unused stubs for the same reason as
`test_echo_protocol`'s: the real socket is opened (and TLS-upgraded) by
`arterial_connection` via `arterial_socket:connect/6`, before this module
ever sees it.
""".

-export([connect/3, close/1, send/2, recv/2, setopts/2,
         encode_request/3, decode_reply/2]).

connect(_Address, _Port, _Opts) -> {error, not_used}.
close(_Sock) -> ok.

send(Sock, Data) ->
  case ssl:send(Sock, Data) of
    ok -> ok;
    {error, _} = Error -> Error
  end.

recv(Sock, Timeout) ->
  case ssl:recv(Sock, 0, Timeout) of
    {ok, Bin}           -> {ok, Bin};
    {error, _} = Error  -> Error
  end.

setopts(_Sock, _Opts) -> ok.

encode_request(ReqID, Term, _Timeout) ->
  {ok, test_echo_protocol:frame(ReqID, term_to_binary(Term))}.

decode_reply(ReqID, Buffer) ->
  case test_echo_protocol:unframe(Buffer) of
    {ok, ReqID, Payload, Rest} ->
      {ok, binary_to_term(Payload), Rest};
    {ok, _OtherReqID, _Payload, _Rest} ->
      {more, Buffer};
    more ->
      {more, Buffer}
  end.
