-module(test_udp_server).

-moduledoc """
A real UDP server used by `test_udp_server_tests` to exercise
`arterial_client:call/3` end-to-end over an actual loopback UDP socket.

Speaks the same `test_echo_protocol` framing and request language as
`test_tcp_server` (`{echo, Term}`, `{upcase, Binary}`,
`{delay, Ms, Term}`), but reply-to-sender is per-datagram (`socket:recvfrom/2`
+ `socket:sendto/3`) since UDP has no per-client connection state.

## Examples

```
1> {ok, Srv} = test_udp_server:start(0).
{ok,<0.123.0>}
2> Port = test_udp_server:port(Srv).
54321
3> test_udp_server:stop(Srv).
ok
```
""".

-export([start/1, port/1, stop/1]).

-doc "Start listening on `Port` (`0` picks a free ephemeral port).".
-spec start(arterial:inet_port()) -> {ok, pid()}.
start(Port) ->
  Parent = self(),
  Pid = spawn_link(fun() -> init(Parent, Port) end),
  receive
    {Pid, ready, _ListenPort} -> {ok, Pid}
  after 5000 ->
    error(udp_server_start_timeout)
  end.

-doc "Return the port `Srv` is actually listening on.".
-spec port(pid()) -> arterial:inet_port().
port(Srv) ->
  Srv ! {self(), get_port},
  receive {Srv, port, Port} -> Port end.

-doc "Stop the server.".
-spec stop(pid()) -> ok.
stop(Srv) ->
  Srv ! {self(), stop},
  receive {Srv, stopped} -> ok end.

init(Parent, Port) ->
  {ok, Sock} = socket:open(inet, dgram, udp),
  ok = socket:setopt(Sock, socket, reuseaddr, true),
  ok = socket:bind(Sock, #{family => inet, addr => {127,0,0,1}, port => Port}),
  {ok, #{port := ListenPort}} = socket:sockname(Sock),
  Parent ! {self(), ready, ListenPort},
  recv_loop(Sock, ListenPort, <<>>).

recv_loop(Sock, ListenPort, Buf) ->
  Self = self(),
  ReceiverPid = spawn(fun() ->
    case socket:recvfrom(Sock, 0, infinity) of
      {ok, {Peer, Data}} -> Self ! {self(), datagram, Peer, Data};
      {error, _}         -> ok
    end
  end),
  wait(Sock, ListenPort, ReceiverPid, Buf).

wait(Sock, ListenPort, ReceiverPid, Buf) ->
  receive
    {ReceiverPid, datagram, Peer, Data} ->
      handle_frames(Sock, Peer, Data),
      recv_loop(Sock, ListenPort, Buf);
    {From, get_port} ->
      From ! {self(), port, ListenPort},
      wait(Sock, ListenPort, ReceiverPid, Buf);
    {From, stop} ->
      socket:close(Sock),
      From ! {self(), stopped}
  end.

handle_frames(Sock, Peer, Data) ->
  case test_echo_protocol:unframe(Data) of
    {ok, ReqID, Payload, <<>>} ->
      spawn(fun() -> respond(Sock, Peer, ReqID, binary_to_term(Payload)) end);
    _ ->
      %% Each UDP datagram carries exactly one frame; anything else is
      %% a malformed/truncated packet and is silently dropped.
      ok
  end.

respond(Sock, Peer, ReqID, {echo, Term}) ->
  reply(Sock, Peer, ReqID, Term);
respond(Sock, Peer, ReqID, {upcase, Bin}) when is_binary(Bin) ->
  reply(Sock, Peer, ReqID, string:uppercase(Bin));
respond(Sock, Peer, ReqID, {delay, Ms, Term}) ->
  timer:sleep(Ms),
  reply(Sock, Peer, ReqID, Term);
respond(Sock, Peer, ReqID, _Other) ->
  reply(Sock, Peer, ReqID, {error, unknown_request}).

reply(Sock, Peer, ReqID, Term) ->
  Data = test_echo_protocol:frame(ReqID, term_to_binary(Term)),
  socket:sendto(Sock, Data, Peer).
