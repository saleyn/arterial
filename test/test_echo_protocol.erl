-module(test_echo_protocol).
-behaviour(arterial_protocol).

-moduledoc """
A minimal real-socket protocol used by the TCP/UDP end-to-end test
suites (`test_tcp_server_tests`, `test_udp_server_tests`).

The actual socket is opened by `arterial_connection` itself (via
`arterial_socket:connect/5`, selected by the pool's `client_opts`
`protocol => tcp | udp` option) before this module ever sees it, so
`connect/3`/`close/1` below are unused stubs required by the
`arterial_protocol` behaviour -- this module only frames bytes and
does raw send/recv on the socket it's given.

Each request/reply is framed as

```
<<ReqID:32, Len:32, Payload:Len/binary>>
```

This framing works unmodified over both `tcp` (a byte stream, so
`decode_reply/2` buffers across `recv/2` calls until a full frame
arrives) and `udp` (each `recv/2` call already returns exactly one
datagram, i.e. one whole frame).
""".

-export([connect/3, close/1, send/2, recv/2, setopts/2,
         encode_request/3, decode_reply/2]).
-export([frame/2, unframe/1]).

connect(_Address, _Port, _Opts) -> {error, not_used}.
close(_Sock) -> ok.

send(Sock, Data) ->
  case socket:send(Sock, Data) of
    ok -> ok;
    {error, _} = Error -> Error
  end.

recv(Sock, Timeout) ->
  case socket:recv(Sock, 0, Timeout) of
    {ok, Bin}           -> {ok, Bin};
    {error, _} = Error  -> Error
  end.

setopts(_Sock, _Opts) -> ok.

-doc "Frame `Payload' (owned by `ReqID') as `<<ReqID:32, Len:32, Payload/binary>>'.".
-spec frame(non_neg_integer(), binary()) -> binary().
frame(ReqID, Payload) when is_binary(Payload) ->
  <<ReqID:32, (byte_size(Payload)):32, Payload/binary>>.

-doc "Try to split one complete frame off the front of `Buffer'.".
-spec unframe(binary()) ->
  {ok, ReqID::non_neg_integer(), Payload::binary(), Rest::binary()} | more.
unframe(<<ReqID:32, Len:32, Rest/binary>>) when byte_size(Rest) >= Len ->
  <<Payload:Len/binary, Tail/binary>> = Rest,
  {ok, ReqID, Payload, Tail};
unframe(_Buffer) ->
  more.

encode_request(ReqID, Term, _Timeout) ->
  {ok, frame(ReqID, term_to_binary(Term))}.

decode_reply(ReqID, Buffer) ->
  case unframe(Buffer) of
    {ok, ReqID, Payload, Rest} ->
      {ok, binary_to_term(Payload), Rest};
    {ok, _OtherReqID, _Payload, _Rest} ->
      %% Out-of-order reply for a different in-flight request: keep it
      %% buffered (this simple example pool always uses backlog=1, so
      %% this case shouldn't normally happen in the tests below).
      {more, Buffer};
    more ->
      {more, Buffer}
  end.
