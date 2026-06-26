-module(poolboy_echo_worker).
-behaviour(gen_server).
-behaviour(poolboy_worker).

-moduledoc """
Minimal `poolboy_worker` paired with a raw `gen_tcp` socket, used by
`poolboy_bench` to drive the same `test_tcp_server`/wire framing
(reusing `test_echo_protocol:frame/2`/`unframe/1` directly) as
`arterial_bench` and `shackle_bench` -- so all three benchmarks measure
each pool's own dispatch overhead against an identical server and
identical bytes on the wire, not a difference in test fixtures.

Unlike `shackle_client` (an async, request-tagged protocol callback) or
arterial's own NIF-backed connection (a lock-free pool of sockets),
poolboy has no protocol/wire awareness at all -- it just hands out a
worker `pid()` and the caller talks to it however it likes. So each
worker here owns one persistent TCP connection and serves one
`{echo, Payload}` `gen_call` at a time: send the framed request, block
on `gen_tcp:recv/2` for the matching framed reply. That makes a poolboy
checkout + this worker's `call/2` the structural equivalent of
arterial's `sync` mode (the caller's own process blocks on socket I/O,
no separate dispatcher process in the loop) -- see `poolboy_bench`'s
moduledoc for why that's the fair comparison point.
""".

-export([start_link/1, call/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
          code_change/3]).

-record(st, {sock :: gen_tcp:socket(), next_id = 0 :: non_neg_integer()}).

-spec start_link(proplists:proplist()) -> {ok, pid()} | {error, term()}.
start_link(Args) ->
  gen_server:start_link(?MODULE, Args, []).

-doc "Send `{echo, Payload}` to `Worker` and block for its decoded reply.".
-spec call(pid(), term()) -> {ok, term()} | {error, term()}.
call(Worker, Payload) ->
  gen_server:call(Worker, {echo, Payload}, 5000).

init(Args) ->
  Address = proplists:get_value(address, Args, "127.0.0.1"),
  Port    = proplists:get_value(port, Args),
  case gen_tcp:connect(Address, Port, [binary, {active, false}, {packet, raw}]) of
    {ok, Sock} -> {ok, #st{sock = Sock}};
    {error, Reason} -> {stop, Reason}
  end.

handle_call({echo, Payload}, _From, #st{sock = Sock, next_id = ReqID} = State) ->
  Frame = test_echo_protocol:frame(ReqID, term_to_binary({echo, Payload})),
  Reply =
    case gen_tcp:send(Sock, Frame) of
      ok -> recv_reply(Sock, ReqID, <<>>);
      {error, _} = Error -> Error
    end,
  {reply, Reply, State#st{next_id = ReqID + 1}}.

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, #st{sock = Sock}) ->
  gen_tcp:close(Sock),
  ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

recv_reply(Sock, ReqID, Buf) ->
  case test_echo_protocol:unframe(Buf) of
    {ok, ReqID, Payload, _Rest} ->
      {ok, binary_to_term(Payload)};
    more ->
      case gen_tcp:recv(Sock, 0, 5000) of
        {ok, Data} -> recv_reply(Sock, ReqID, <<Buf/binary, Data/binary>>);
        {error, _} = Error -> Error
      end
  end.
