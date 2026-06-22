-module(test_tcp_server).

-moduledoc """
A real TCP server used by `test_tcp_server_tests` to exercise
`arterial_pool`/`arterial_client:call/3` end-to-end over an actual
loopback socket (as opposed to `test_protocol`'s in-process fake used by
`arterial_nif_tests`).

Listens on `Port` (use `0` to let the OS pick a free port, then read it
back via `port/1`), accepts connections, and for each
`test_echo_protocol`-framed request applies a tiny request language:

- `{echo, Term}`        -> replies with `Term`
- `{upcase, Binary}`    -> replies with `string:uppercase(Binary)`
- `{delay, Ms, Term}`   -> replies with `Term` after sleeping `Ms`
- `{noreply, Term}`     -> processed, but never replies (the only shape
  here safe to send via `arterial_client:cast/2`'s one-way contract)
- anything else         -> replies with `{error, unknown_request}`

## Examples

```
1> {ok, Srv} = test_tcp_server:start(0).
{ok,<0.123.0>}
2> Port = test_tcp_server:port(Srv).
54321
3> test_tcp_server:stop(Srv).
ok
```
""".

-export([start/1, start/2, port/1, stop/1]).

-doc "Equivalent to `start/2` bound to `127.0.0.1`.".
-spec start(arterial:inet_port()) -> {ok, pid()}.
start(Port) -> start(Port, {127, 0, 0, 1}).

-doc """
Start listening on `Port` (`0` picks a free ephemeral port), bound to
`Address`.
""".
-spec start(arterial:inet_port(), inet:ip_address()) -> {ok, pid()}.
start(Port, Address) ->
  Parent = self(),
  Pid = spawn_link(fun() -> init(Parent, Port, Address) end),
  receive
    {Pid, ready, _ListenPort} -> {ok, Pid}
  after 5000 ->
    error(tcp_server_start_timeout)
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

init(Parent, Port, Address) ->
  {ok, LSock} = socket:open(inet, stream, tcp),
  ok = socket:setopt(LSock, socket, reuseaddr, true),
  ok = socket:bind(LSock, #{family => inet, addr => Address, port => Port}),
  %% Default backlog is small on some platforms; a pool with many
  %% connections all dialing in at once (e.g. arterial_bench) can exceed
  %% it, leaving extra SYNs queued at the kernel level until the single
  %% accept-at-a-time loop below catches up.
  ok = socket:listen(LSock, 128),
  {ok, #{port := ListenPort}} = socket:sockname(LSock),
  Parent ! {self(), ready, ListenPort},
  accept_loop(LSock, ListenPort, []).

accept_loop(LSock, ListenPort, Conns) ->
  Self = self(),
  AcceptorPid = spawn(fun() ->
    case socket:accept(LSock) of
      {ok, ASock} ->
        %% `socket' ties a socket's lifetime to its owning process: the
        %% process that calls accept/1 becomes ASock's owner, so it must
        %% hand ownership to the long-lived connection process *before*
        %% this one-shot acceptor exits, or ASock gets closed right away.
        ConnPid = spawn(fun() -> conn_recv_loop(ASock, <<>>) end),
        ok = socket:setopt(ASock, otp, controlling_process, ConnPid),
        Self ! {self(), accepted, ConnPid};
      {error, Reason} ->
        %% loop/4 only re-arms the next acceptor upon {accepted,...} --
        %% an accept error (can happen transiently under a burst of
        %% near-simultaneous connection attempts) must still notify the
        %% loop, or every connection still queued in the kernel's accept
        %% backlog is silently never accepted, hanging those clients'
        %% connect/5 calls forever instead of just failing this one.
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
      socket:close(LSock),
      lists:foreach(fun(C) -> exit(C, kill) end, Conns),
      From ! {self(), stopped}
  end.

conn_recv_loop(Sock, Buf) ->
  case socket:recv(Sock, 0, infinity) of
    {ok, Data} ->
      Buf1 = handle_frames(Sock, <<Buf/binary, Data/binary>>),
      conn_recv_loop(Sock, Buf1);
    {error, _} ->
      socket:close(Sock)
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
respond(_Sock, _ReqID, {noreply, _Term}) ->
  %% Genuinely one-way: processed, but no reply frame is ever written --
  %% the only request shape this server supports that's actually safe to
  %% send via arterial_client:cast/2 (mode (e)'s contract requires the
  %% protocol to never reply; every other request shape here always
  %% does, cast or not, since the server has no idea which API the client
  %% used to send it).
  ok;
respond(Sock, ReqID, _Other) ->
  reply(Sock, ReqID, {error, unknown_request}).

reply(Sock, ReqID, Term) ->
  Data = test_echo_protocol:frame(ReqID, term_to_binary(Term)),
  socket:send(Sock, Data).
