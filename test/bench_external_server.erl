-module(bench_external_server).

-moduledoc """
Runs `test_tcp_server` either in-process (the status quo: a plain
`spawn_link`'d process inside the caller's own VM) or out-of-process, as
a standalone child `erl` node reached over loopback TCP.

`test_tcp_server:handle_frames/2` spawns a new Erlang process for every
inbound request (see its moduledoc/source) -- cheap individually, but at
benchmark throughput (tens of thousands of req/s) the resulting
process-table/allocator churn shows up as VM-wide noise in a `perf`
profile of the *same* VM the client/NIF code under test is running in,
alongside whatever is actually being profiled. Running the server as a
separate OS process moves that noise out of the profiled VM entirely.

Used by `arterial_bench`, `arterial_bench2`, `shackle_bench`, and
`poolboy_bench` via their `external_server` opt -- see each module's
`help/0`. Defaults to `false` (in-process) everywhere; only flip it on
when profiling the client side in isolation.
""".

-export([start/1, port/1, stop/1]).

-type handle() :: {local, pid()} | {external, {port(), inet:port_number()}}.

-doc """
`External = false` starts `test_tcp_server` in-process, equivalent to
calling it directly. `External = true` spawns it as a standalone child
`erl` node instead (sharing this node's code path), listening on an
OS-assigned loopback port.
""".
-spec start(boolean()) -> handle().
start(false) ->
  {ok, Srv} = test_tcp_server:start(0),
  {local, Srv};
start(true) ->
  {external, start_external()}.

-doc "Return the port `Handle` is listening on.".
-spec port(handle()) -> inet:port_number().
port({local, Srv}) -> test_tcp_server:port(Srv);
port({external, {_ErlPort, ListenPort}}) -> ListenPort.

-doc "Stop the server started by `start/1` and release its handle.".
-spec stop(handle()) -> ok.
stop({local, Srv}) ->
  test_tcp_server:stop(Srv);
stop({external, {ErlPort, _ListenPort}}) ->
  %% Closing the port closes the child's stdin pipe; its
  %% bench_external_server_main:wait_for_eof/0 loop reads that as `eof`
  %% and uses it to stop test_tcp_server and halt cleanly on its own.
  try erlang:port_close(ErlPort) catch _:_ -> ok end,
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

start_external() ->
  ErlPath = case os:find_executable("erl") of
    false -> error(erl_not_found);
    Path  -> Path
  end,
  %% Mirror this node's own code path so the child can load
  %% test_tcp_server/bench_external_server_main without depending on
  %% which Makefile target (or shell) launched the parent.
  PaArgs = lists:flatmap(fun(Dir) -> ["-pa", filename:absname(Dir)] end, code:get_path()),
  %% -noshell only -- NOT -noinput, which makes the runtime never touch
  %% stdin at all and would turn every read in bench_external_server_main
  %% into an immediate `eof`, short-circuiting our shutdown signal before
  %% stop/1 ever calls port_close/1.
  Args = ["-noshell" | PaArgs] ++ ["-eval", "bench_external_server_main:run()."],
  ErlPort = open_port({spawn_executable, ErlPath}, [
    {args, Args}, {line, 4096}, exit_status, binary, stderr_to_stdout
  ]),
  ListenPort = wait_for_listen_port(ErlPort),
  {ErlPort, ListenPort}.

wait_for_listen_port(ErlPort) ->
  receive
    {ErlPort, {data, {eol, <<"PORT ", Rest/binary>>}}} ->
      binary_to_integer(Rest);
    {ErlPort, {data, {eol, _OtherLine}}} ->
      wait_for_listen_port(ErlPort);
    {ErlPort, {exit_status, Status}} ->
      error({external_server_exited, Status})
  after 5000 ->
    error(external_server_start_timeout)
  end.
