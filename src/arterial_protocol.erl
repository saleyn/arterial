-module(arterial_protocol).

-moduledoc """
Behaviour implemented by wire-transport/codec modules: connection
setup/teardown, raw I/O, and per-request encode/decode.

Paired with an `arterial_client` callback module and passed to
`arterial_pool:start_link/2` via the `protocol` option. `arterial_client`
owns the request/reply lifecycle; `arterial_protocol` owns the bytes:
how to open/close the transport, send/receive raw data on it, and
encode/decode individual requests and replies.

See `test/test_protocol.erl` in the `arterial` source tree for a minimal
example implementation used by the test suite, or `test/ssl_echo_protocol.erl`
for an `ssl`-transport example (`send/2`/`recv/2` over `ssl:send/2`/
`ssl:recv/3` instead of `socket:send/2`/`socket:recv/3` -- see
`m:arterial_socket`'s moduledoc for why `ssl` needs OTP 28+).
""".

-doc """
Open a transport connection to `Address`:`Port` with the given socket
options. Called by `arterial_connection` on (re)connect, before
`c:arterial_client:setup/2`.
""".
-callback connect(arterial:inet_address(), arterial:inet_port(),
                  [arterial_pool:sockopt()]) ->
  {ok, arterial:socket()} | {error, atom()}.

-doc "Close `Socket`. Called by `arterial_connection` on disconnect.".
-callback close(arterial:socket()) -> ok.

-doc "Write `iodata()` to `Socket`.".
-callback send(arterial:socket(), iodata()) -> ok | {error, atom()}.

-doc """
Read the next available chunk of bytes from `Socket`, waiting up to
`Timeout` milliseconds (or indefinitely if `infinity`).
""".
-callback recv(arterial:socket(), Timeout::non_neg_integer()) ->
  {ok, binary()} | {error, any()}.

-doc "Apply socket options to `Socket` after it's connected.".
-callback setopts(arterial:socket(), [arterial_pool:sockopt()]) ->
  ok | {error, atom()}.

-doc """
Encode `Request` (owned by `ReqID`) into wire bytes to be sent via
`send/2`. Used by the synchronous `arterial_client:call/3` path.
""".
-callback encode_request(ReqID::arterial:request_id(), Request::term(),
                          Timeout::non_neg_integer()) ->
  {ok, iodata()} | {error, term()}.

-doc """
Try to decode the reply to `ReqID` out of the front of `Buffer`
(the unconsumed bytes accumulated so far for this connection).
""".
-callback decode_reply(ReqID::arterial:request_id(), Buffer::binary()) ->
  {ok, arterial:response(), Rest::binary()} |
  {more, Buffer::binary()} |
  {error, term()}.
