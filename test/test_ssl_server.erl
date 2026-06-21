-module(test_ssl_server).

-moduledoc """
A real TLS server used by `test_ssl_server_tests` to exercise
`arterial_pool`/`arterial_client:call/3` end-to-end over an actual
loopback `ssl` socket, mirroring `test_tcp_server` (same
`test_echo_protocol`-style framing/request language, decoded via
`ssl_echo_protocol`'s `frame/2`/`unframe/1`).

Requires OTP 28+ (see `arterial_socket`'s moduledoc) -- generates its own
self-signed RSA test certificate at `start/1` time via
`public_key:pkix_test_data/1`, no external cert files needed.

## Examples

```
1> {ok, Srv} = test_ssl_server:start(0).
{ok,<0.123.0>}
2> Port = test_ssl_server:port(Srv).
54321
3> test_ssl_server:stop(Srv).
ok
```
""".

-export([start/1, port/1, stop/1]).

-doc "Start listening on `Port` (`0` picks a free ephemeral port), bound to `127.0.0.1`.".
-spec start(arterial:inet_port()) -> {ok, pid()}.
start(Port) ->
  application:ensure_all_started(ssl),
  Parent = self(),
  Pid = spawn_link(fun() -> init(Parent, Port) end),
  receive
    {Pid, ready, _ListenPort} -> {ok, Pid}
  after 5000 ->
    error(ssl_server_start_timeout)
  end.

-doc "Return the port `Srv` is actually listening on.".
-spec port(pid()) -> arterial:inet_port().
port(Srv) ->
  Srv ! {self(), get_port},
  receive {Srv, port, Port} -> Port end.

-doc "Stop the server and close every connection it accepted.".
-spec stop(pid()) -> ok.
stop(Srv) ->
  Srv ! {self(), stop},
  receive {Srv, stopped} -> ok end.

init(Parent, Port) ->
  Key = {rsa, 2048, 65537},
  #{server_config := ServerConf} = public_key:pkix_test_data(#{
    server_chain => #{root => [{key, Key}], intermediates => [], peer => [{key, Key}]},
    client_chain => #{root => [{key, Key}], intermediates => [], peer => [{key, Key}]}
  }),
  {ok, LSock} = ssl:listen(Port, ServerConf ++ [
    {active, false}, {mode, binary}, {reuseaddr, true}, {backlog, 128}
  ]),
  {ok, {_, ListenPort}} = ssl:sockname(LSock),
  Parent ! {self(), ready, ListenPort},
  accept_loop(LSock, ListenPort, []).

accept_loop(LSock, ListenPort, Conns) ->
  Self = self(),
  AcceptorPid = spawn(fun() ->
    case ssl:transport_accept(LSock) of
      {ok, TSock} ->
        case ssl:handshake(TSock) of
          {ok, ASock} ->
            ConnPid = spawn(fun() -> conn_recv_loop(ASock, <<>>) end),
            ok = ssl:controlling_process(ASock, ConnPid),
            Self ! {self(), accepted, ConnPid};
          {error, Reason} ->
            Self ! {self(), accept_error, Reason}
        end;
      {error, Reason} ->
        Self ! {self(), accept_error, Reason}
    end
  end),
  loop(LSock, ListenPort, AcceptorPid, Conns).

loop(LSock, ListenPort, AcceptorPid, Conns) ->
  receive
    {AcceptorPid, accepted, ConnPid} ->
      accept_loop(LSock, ListenPort, [ConnPid | Conns]);
    {AcceptorPid, accept_error, _Reason} ->
      accept_loop(LSock, ListenPort, Conns);
    {From, get_port} ->
      From ! {self(), port, ListenPort},
      loop(LSock, ListenPort, AcceptorPid, Conns);
    {From, stop} ->
      ssl:close(LSock),
      lists:foreach(fun(C) -> exit(C, kill) end, Conns),
      From ! {self(), stopped}
  end.

conn_recv_loop(Sock, Buf) ->
  case ssl:recv(Sock, 0, infinity) of
    {ok, Data} ->
      Buf1 = handle_frames(Sock, <<Buf/binary, Data/binary>>),
      conn_recv_loop(Sock, Buf1);
    {error, _} ->
      ssl:close(Sock)
  end.

handle_frames(Sock, Buf) ->
  case test_echo_protocol:unframe(Buf) of
    {ok, ReqID, Payload, Rest} ->
      spawn(fun() -> respond(Sock, ReqID, binary_to_term(Payload)) end),
      handle_frames(Sock, Rest);
    more ->
      Buf
  end.

respond(Sock, ReqID, {echo, Term}) ->
  reply(Sock, ReqID, Term);
respond(Sock, ReqID, {upcase, Bin}) when is_binary(Bin) ->
  reply(Sock, ReqID, string:uppercase(Bin));
respond(Sock, ReqID, {delay, Ms, Term}) ->
  timer:sleep(Ms),
  reply(Sock, ReqID, Term);
respond(Sock, ReqID, _Other) ->
  reply(Sock, ReqID, {error, unknown_request}).

reply(Sock, ReqID, Term) ->
  Data = test_echo_protocol:frame(ReqID, term_to_binary(Term)),
  ssl:send(Sock, Data).
