-module(bench_external_server_main).

-moduledoc """
Entry point for the child `erl` node `bench_external_server:start/1`
spawns when `External = true` -- started via `-eval
"bench_external_server_main:run()."` on a node that shares the parent's
code path (see `bench_external_server`'s moduledoc for why).
""".

-export([run/0]).

-doc """
Starts `test_tcp_server` on an OS-assigned loopback port, prints
`"PORT <N>"` on stdout for the parent to read, then blocks until the
parent closes the pipe (read as `eof` on stdin) before stopping the
server and halting.
""".
-spec run() -> no_return().
run() ->
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  io:format("PORT ~b~n", [Port]),
  wait_for_eof(),
  test_tcp_server:stop(Srv),
  halt(0).

wait_for_eof() ->
  case io:get_line('') of
    eof -> ok;
    {error, _} -> ok;
    _Line -> wait_for_eof()
  end.