%% vim:ts=2:sw=2:et
%% -----------------------------------------------------------------------------
%% reactor_bench.erl — seven-scenario throughput & latency benchmark
%%
%% Protocol: <<Seq:32/big, Payload:(MsgSize-4)/binary>> — configurable size
%%
%% Scenarios
%% ---------
%%   1. C++ server (Reactor)   <->  C++ client (Reactor)
%%   2. C++ server (Reactor)   <->  Erl client (gen_tcp)
%%   3. Erl server (gen_tcp)   <->  Erl client (gen_tcp)
%%   4. C++ server (Reactor)   <->  Erl client (Reactor NIF)
%%   5. Erl server (Reactor NIF) <-> C++ client (Reactor)
%%   6. Erl server (gen_tcp)   <->  C++ client (Reactor)
%%   7. Erl server (Reactor NIF) <-> Erl client (Reactor NIF)
%%
%% Usage:
%%   CONNS=N      - Number of connections
%%   REQS=N       - Number of requests
%%   MSIZE=N      - Message size
%%   STRIPES=N    - Client stripe count (0 = one per connection); server uses 2× this
%%   REVERSE      - Reverse the order of tests
%%   TEST=N,M...  - Execute only test(s): N, M, ...
%%   TEST=N-M     - Execute only test(s): N through M
%%
%%   rebar3 eunit --module=reactor_bench
%%   make -C c_src bench CONNS=8 REQS=50000
%% -----------------------------------------------------------------------------
-module(reactor_bench).
-include_lib("eunit/include/eunit.hrl").
-export([run/0, run_with/2, run_with/3, main/1]).
-export([erl_nif_client_run/4, erl_nif_client_run/5]).
-export([arterial_client_run/4]).

-define(CONNS,    8).
-define(REQS,     2_000).
-define(MSG_SIZE, 8).
-define(TIMEOUT,  5_000).
-define(STRIPES,  0).   %% 0 = one stripe per connection

%% EUnit smoke-test: one message size, fewer reqs, 15s hard limit.
%% For the full multi-size benchmark run:  make -C c_src bench  (no time limit).
reactor_bench_test_() ->
  Conns = list_to_integer(os:getenv("CONNS", integer_to_list(?CONNS))),
  Reqs  = list_to_integer(os:getenv("REQS",  integer_to_list(40))),
  MSize = list_to_integer(os:getenv("MSIZE", integer_to_list(?MSG_SIZE))),
  %% Each scenario runs warmup + bench, each up to DurationMs.
  %% DurationMs = max(400ms, Reqs * 2ms).  15 scenarios + overhead → 2×.
  NScenarios  = 15,
  DurationMs  = max(400, Reqs * 2),
  TimeoutSecs = max(30, (NScenarios * DurationMs * 2) div 1000 + 30),
  {timeout, TimeoutSecs, fun() -> run_with(Conns, Reqs, MSize) end}.

run() ->
  Conns  = list_to_integer(os:getenv("CONNS", integer_to_list(?CONNS))),
  Reqs   = list_to_integer(os:getenv("REQS",  integer_to_list(?REQS))),
  MSizes =
    case os:getenv("MSIZE") of
      false -> [8, 256, 1024];
      Val   -> [list_to_integer(Val)]
    end,
  [run_with(Conns, Reqs, MsgSize) || MsgSize <- MSizes],
  ok.

run_with(NConns, NReqs) -> run_with(NConns, NReqs, ?MSG_SIZE).

%% Escript entry point — used when spawned as a standalone OS process.
%%   reactor_bench gentcp <MsgSize>   — run gen_tcp echo server
%%   reactor_bench nif                — run Reactor NIF echo server
main(["gentcp", MSizeStr]) ->
  MsgSize = list_to_integer(MSizeStr),
  {ok, LSock} = socket:open(inet, stream, tcp),
  ok = socket:setopt(LSock, socket, reuseaddr, true),
  ok = socket:setopt(LSock, tcp,    nodelay,  true),
  ok = socket:bind(LSock, #{family => inet, port => 0, addr => {0,0,0,0}}),
  ok = socket:listen(LSock),
  {ok, #{port := Port}} = socket:sockname(LSock),
  io:format("READY ~w~n", [Port]),
  spawn(fun() -> sock_accept_loop(LSock, MsgSize) end),
  timer:sleep(infinity);
main(["nif"]) ->
  NStripes = list_to_integer(os:getenv("STRIPES", "0")),
  %% Default 0 → use 256 stripes so the pool comfortably handles multiple
  %% successive connection waves (warmup + measurement) without wrapping.
  NStripes1 = if NStripes =< 0 -> 256; true -> NStripes end,
  main(["nif", integer_to_list(NStripes1)]);
main(["nif", NStripesStr]) ->
  NStripes = list_to_integer(NStripesStr),
  {ok, _} = application:ensure_all_started(arterial),
  {ok, PoolRef} = arterial_nif:init_pool(NStripes, 1),
  {ok, {ListenFd, Port}} = arterial_nif:reactor_listen(PoolRef, 0),
  ok = arterial_nif:reactor_accept(PoolRef, ListenFd, self()),
  io:format("READY ~w~n", [Port]),
  nif_accept_loop(PoolRef, ListenFd, NStripes, 0);
main(_) ->
  io:format(standard_error, "usage: reactor_bench (gentcp <MsgSize> | nif)~n", []),
  erlang:halt(1).

run_with(NConns, NReqs, MsgSize) ->
  NStripesEnv = list_to_integer(os:getenv("STRIPES", integer_to_list(?STRIPES))),
  NStripes    = if NStripesEnv =< 0 -> NConns; true -> NStripesEnv end,
  Total    = NConns * NReqs,
  WarmReqs = min(NReqs, max(20, NReqs div 10)),
  #{version      := GitVsn,
    optimization := Opt,
    app_version  := TagVsn,
    backend      := Backend,
    pgo          := PGO} = arterial_nif:info(),
  io:format(standard_error, "~n~n~s~n", [bar(123)]),
  io:format(standard_error,
    "Arterial Reactor -- Seven-Scenario Benchmark (~p -O~p~s v~s (~s))~n",
    [Backend, Opt, if PGO -> " PGO"; true -> "" end, TagVsn, GitVsn]),
  io:format(standard_error,
            "Conns: ~p Stripes: ~p Reqs/conn: ~p  Msg: ~p bytes  Total: ~p~n~n",
            [NConns, NStripes, NReqs, MsgSize, Total]),
  io:format(standard_error, "~-50s  ~10s  ~8s  ~8s  ~8s  ~8s  ~8s  ~8s~n",
            ["Scenario","req/s","MB/s","mean µs","p50 µs","p99 µs","p999 µs","err #/%"]),
  io:format(standard_error, "~s~n", [bar(123)]),

  Scenarios = [
    {"C++ server (Reactor) <-> C++ client",
      fun cpp_server_start/0,
      fun cpp_server_stop/1,
      fun cpp_client_run/4},
    {"C++ server (Reactor) <-> Erl client (gen_tcp)",
      fun cpp_server_start/0,
      fun cpp_server_stop/1,
      fun erl_gentcp_client_run/4},
    {"C++ server (Reactor) <-> Erl client (arterial)",
      fun cpp_server_start/0,
      fun cpp_server_stop/1,
      fun(P, C, R, M) -> arterial_client_run(P, C, R, M) end},
    {"C++ server (Reactor) <-> Shackle client",
      fun cpp_server_start/0,
      fun cpp_server_stop/1,
      fun shackle_client_run/4},
    {"C++ server (Reactor) <-> Poolboy client",
      fun cpp_server_start/0,
      fun cpp_server_stop/1,
      fun poolboy_client_run/4},
    "\n",
    {"Erl server (Reactor) <-> C++ client",
      fun() -> erl_nif_server_start(NStripes) end,
      fun erl_nif_server_stop/1,
      fun cpp_client_run/4},
    {"Erl server (Reactor) <-> Erl client (gen_tcp)",
      fun() -> erl_nif_server_start(NStripes) end,
      fun erl_nif_server_stop/1,
      fun erl_gentcp_client_run/4},
    {"Erl server (Reactor) <-> Erl client (arterial)",
      fun() -> erl_nif_server_start(NStripes) end,
      fun erl_nif_server_stop/1,
      fun(P, C, R, M) -> arterial_client_run(P, C, R, M) end},
    {"Erl server (Reactor) <-> Shackle client",
      fun() -> erl_nif_server_start(NStripes) end,
      fun erl_nif_server_stop/1,
      fun shackle_client_run/4},
    {"Erl server (Reactor) <-> Poolboy client",
      fun() -> erl_nif_server_start(NStripes) end,
      fun erl_nif_server_stop/1,
      fun poolboy_client_run/4},
    "\n",
    {"Erl server (gen_tcp) <-> C++ client",
      fun() -> erl_gentcp_server_start(MsgSize) end,
      fun erl_gentcp_server_stop/1,
      fun cpp_client_run/4},
    {"Erl server (gen_tcp) <-> Erl client (gen_tcp)",
      fun() -> erl_gentcp_server_start(MsgSize) end,
      fun erl_gentcp_server_stop/1,
      fun erl_gentcp_client_run/4},
    {"Erl server (gen_tcp) <-> Erl client (arterial)",
      fun() -> erl_gentcp_server_start(MsgSize) end,
      fun erl_gentcp_server_stop/1,
      fun(P, C, R, M) -> arterial_client_run(P, C, R, M) end},
    {"Erl server (gen_tcp) <-> Shackle client",
      fun() -> erl_gentcp_server_start(MsgSize) end,
      fun erl_gentcp_server_stop/1,
      fun shackle_client_run/4},
    {"Erl server (gen_tcp) <-> Poolboy client",
      fun() -> erl_gentcp_server_start(MsgSize) end,
      fun erl_gentcp_server_stop/1,
      fun poolboy_client_run/4}
  ],

  Tests =
    case os:getenv("TEST") of
      false -> lists:seq(1, length(Scenarios));
      Val   ->
        case string:split(Val, "-") of
          [From, To] -> lists:seq(list_to_integer(From), list_to_integer(To));
          [_]        -> [list_to_integer(I) || I <- string:split(Val, ",", all)]
        end
    end,

  OrderedTests =
    case os:getenv("REVERSE") of
      false -> Scenarios;
      _     -> lists:reverse(Scenarios)
    end,

  lists:foldl(fun
    (S, I) when is_list(S) ->
      io:put_chars(standard_error, S),
      I;
    ({Label, Start, Stop, Client}, I) ->
      case lists:member(I, Tests) of
        true ->
          {ok, P, S} = Start(),
          timer:sleep(10),
          Client(P, NConns, WarmReqs, MsgSize),
          R = Client(P, NConns, NReqs, MsgSize),
          Stop(S),
          print_row(Label, I, R);
        false ->
          ok
      end,
      I + 1
  end, 1, OrderedTests),

  io:format(standard_error, "~s~n~n", [bar(123)]).

%% ============================================================
%% C++ echo server (reactor_bench binary, "server" mode)
%% ============================================================

cpp_server_start() ->
  Bin  = bench_bin(),
  Port = erlang:open_port(
    {spawn_executable, Bin},
    [binary, {args, ["server"]}, use_stdio, stderr_to_stdout]),
  receive
    {Port, {data, Data}} ->
      case re:run(Data, <<"READY (\\d+)">>,
                  [{capture, all_but_first, list}]) of
        {match, [P]} -> {ok, list_to_integer(P), Port};
        _            -> erlang:error({cpp_server_start_failed, Data})
      end
  after 5000 ->
    erlang:error(cpp_server_start_timeout)
  end.

cpp_server_stop(P) ->
  P ! {self(), close},
  receive {P, closed} -> ok after 1000 -> ok end.

%% ============================================================
%% Erlang server as a separate OS process
%% ============================================================

erl_server_start(Mode, ExtraArgs) ->
  ErlExe   = filename:join([code:root_dir(), "bin", "erl"]),
  %% Collect all ebin dirs on the current code path so the child VM has
  %% the same libraries available (arterial_nif, shackle deps, etc.).
  LibPaths = lists:flatmap(fun(D) -> ["-pa", D] end, code:get_path()),
  CallArgs = case ExtraArgs of
    [] -> io_lib:format("[\"~s\"]", [Mode]);
    _  -> io_lib:format("[\"~s\", \"~s\"]", [Mode, ExtraArgs])
  end,
  Eval = lists:flatten(
    io_lib:format("~s:main(~s), halt()", [?MODULE, CallArgs])),
  Args = LibPaths ++ ["-noshell", "-eval", Eval],
  Port = erlang:open_port(
    {spawn_executable, ErlExe},
    [binary, {args, Args}, use_stdio, stderr_to_stdout]),
  receive
    {Port, {data, Data}} ->
      case re:run(Data, <<"READY (\\d+)">>,
                  [{capture, all_but_first, list}]) of
        {match, [P]} -> {ok, list_to_integer(P), Port};
        _            -> erlang:error({erl_server_start_failed, Mode, Data})
      end
  after 10000 ->
    erlang:error({erl_server_start_timeout, Mode})
  end.

erl_server_stop(Port) ->
  Port ! {self(), close},
  receive {Port, closed} -> ok after 2000 -> ok end.

bench_bin() ->
  %% The binary is built to priv/reactor_bench.  Walk up from the beam path
  %% to find a priv/ directory containing it.
  BeamPath = filename:absname(code:which(?MODULE)),
  find_bench_bin(filename:dirname(BeamPath)).

find_bench_bin(Dir) ->
  %% Check <dir>/priv/reactor_bench and <dir>/reactor_bench (fallback).
  InPriv = filename:join([Dir, "priv", "reactor_bench"]),
  Direct = filename:join([Dir, "reactor_bench"]),
  case filelib:is_regular(InPriv) of
    true  -> InPriv;
    false ->
      case filelib:is_regular(Direct) of
        true  -> Direct;
        false ->
          Parent = filename:dirname(Dir),
          case Parent =:= Dir of
            true  -> erlang:error({bench_bin_not_found, Dir});
            false -> find_bench_bin(Parent)
          end
      end
  end.

%% ============================================================
%% C++ client (reactor_bench binary, "client" mode)
%% ============================================================

cpp_client_run(ServerPort, Conns, Reqs, MsgSize) ->
  Bin  = bench_bin(),
  Args = string:join(
    ["client",
     integer_to_list(ServerPort),
     integer_to_list(Conns),
     integer_to_list(Reqs),
     integer_to_list(MsgSize)], " "),
  T0  = erlang:monotonic_time(microsecond),
  Out = os:cmd(Bin ++ " " ++ Args),
  T1  = erlang:monotonic_time(microsecond),
  parse_cpp_line(Out, T1 - T0, Conns * Reqs, MsgSize).

parse_cpp_line(Out, ElapsedUs, Total, MsgSize) ->
  F = fun(Key) ->
        Pat = Key ++ "([0-9.eE+-]+)",
        case re:run(Out, Pat, [{capture, all_but_first, list}]) of
          {match, [V]} ->
            try list_to_float(V)
            catch _:_ ->
              try float(list_to_integer(V)) catch _:_ -> 0.0 end
            end;
          _ -> 0.0
        end
      end,
  TotalMB = Total * MsgSize * 2 / (1024 * 1024),
  #{total      => Total,
    elapsed_us => ElapsedUs,
    rps        => F("rps="),
    throughput => case F("throughput_mbs=") of V when V > 0.0 -> V; _ -> TotalMB / (ElapsedUs / 1_000_000) end,
    lat_mean   => F("lat_mean="),
    lat_p50    => F("lat_p50="),
    lat_p99    => F("lat_p99="),
    lat_p999   => F("lat_p999="),
    errors     => round(F("errors=")),
    error_rate => F("error_rate=")}.

%% ============================================================
%% Erlang gen_tcp echo server
%% ============================================================


erl_gentcp_server_start(MsgSize) ->
  erl_server_start("gentcp", integer_to_list(MsgSize)).

erl_gentcp_server_stop(Port) ->
  erl_server_stop(Port).

%% Accept loop using socket module with nowait.
%% Each accepted connection is handed to a multiplexer process that drives
%% N connections from a single receive loop using per-socket select messages.
%%
%% One multiplexer process per CPU scheduler, round-robined across accepts.
%% Each multiplexer holds a map of Socket → AccumBuffer and re-arms read
%% selects after each wakeup.  This avoids one-process-per-connection
%% overhead while staying within the BEAM process model.
sock_accept_loop(LSock, MsgSize) ->
  NMux  = erlang:system_info(schedulers_online),
  Muxes = [spawn(fun() -> sock_mux_loop(MsgSize, #{}) end) || _ <- lists:seq(1, NMux)],
  sock_accept_loop(LSock, MsgSize, Muxes, 0).

sock_accept_loop(LSock, MsgSize, Muxes, Idx) ->
  case socket:accept(LSock, nowait) of
    {ok, Conn} ->
      ok = socket:setopt(Conn, tcp, nodelay, true),
      Mux = lists:nth((Idx rem length(Muxes)) + 1, Muxes),
      Mux ! {add, Conn},
      sock_accept_loop(LSock, MsgSize, Muxes, Idx + 1);
    {select, {select_info, accept, Ref}} ->
      receive
        {'$socket', LSock, select, Ref} ->
          sock_accept_loop(LSock, MsgSize, Muxes, Idx)
      after 5000 -> ok
      end;
    _ -> ok
  end.

%% Multiplexer: manages a set of connections.
%% Conns :: #{Socket => AccumBuf}
sock_mux_loop(MsgSize, Conns) ->
  receive
    {add, Sock} ->
      NewConns = sock_do_recv(Sock, {[], 0}, MsgSize, Conns),
      sock_mux_loop(MsgSize, NewConns);
    {'$socket', Sock, select, _Ref} ->
      case maps:find(Sock, Conns) of
        {ok, Acc} ->
          NewConns = sock_do_recv(Sock, Acc, MsgSize, Conns),
          sock_mux_loop(MsgSize, NewConns);
        error ->
          sock_mux_loop(MsgSize, Conns)
      end
  end.

%% Read all currently-available data from Sock, echo complete frames,
%% then re-arm or remove from the map.
%% Acc = {[Binary], TotalBytes} — a reversed list of received chunks.
%% We only flatten (iolist_to_binary) when TotalBytes >= MsgSize.
sock_do_recv(Sock, {Chunks, BufLen}, MsgSize, Conns) ->
  case socket:recv(Sock, 0, nowait) of
    {ok, Data} ->
      Len2 = BufLen + byte_size(Data),
      case Len2 >= MsgSize of
        true ->
          Flat = iolist_to_binary(lists:reverse([Data | Chunks])),
          case sock_mux_flush(Sock, MsgSize, Flat) of
            closed  -> maps:remove(Sock, Conns);
            Rest    -> sock_do_recv(Sock, {[Rest], byte_size(Rest)}, MsgSize, Conns)
          end;
        false ->
          sock_do_recv(Sock, {[Data | Chunks], Len2}, MsgSize, Conns)
      end;
    {select, _} ->
      Conns#{Sock => {Chunks, BufLen}};
    _ ->
      socket:close(Sock),
      maps:remove(Sock, Conns)
  end.

sock_mux_flush(Sock, MsgSize, Buf) when byte_size(Buf) >= MsgSize ->
  <<Frame:MsgSize/binary, Rest/binary>> = Buf,
  case socket:send(Sock, Frame) of
    ok   -> sock_mux_flush(Sock, MsgSize, Rest);
    _Err -> socket:close(Sock), closed
  end;
sock_mux_flush(_Sock, _MsgSize, Buf) ->
  Buf.

%% ============================================================
%% Erlang Reactor NIF echo server
%%
%% Each accepted connection is registered with the arterial NIF pool.
%% When the reactor delivers {arterial_event, StripeId, SlotId, read, Bin},
%% the server echoes Bin back via send_and_release/3.
%% ============================================================

%% ============================================================
%% Erlang Reactor NIF echo server (pure NIF, no OTP socket)
%%
%% Uses reactor_listen/reactor_accept to avoid any OTP socket involvement.
%% When a client connects, the reactor sends {arterial_accept, LFd, CFd, IP, Port}
%% to the accept loop process.  The accept loop registers CFd with the pool
%% via reactor_register_client, then spawns a per-connection echo worker that
%% receives {arterial_event, StripeId, SlotId, read, Bin} and echoes via
%% send_and_release.
%% ============================================================

erl_nif_server_start(NStripes) ->
  %% Allocate twice as many stripes as the client uses so the accept loop can
  %% keep advancing without wrapping into still-active warmup connections.
  SrvStripes = max(NStripes * 2, 64),
  erl_server_start("nif", integer_to_list(SrvStripes)).

erl_nif_server_stop(Port) ->
  erl_server_stop(Port).

%% Accept loop: receives {arterial_accept, _, ClientFd, IP, Port} from the reactor.
%% Each worker spawns immediately and registers itself; no blocking wait.
%% StripeId wraps around modulo NStripes so the pool can handle many successive
%% connection waves (e.g. warmup + measurement runs) without exhausting stripes.
nif_accept_loop(PoolRef, ListenFd, NStripes, StripeId) ->
  receive
    {arterial_accept, ListenFd, ClientFd, _IP, _Port} ->
      SId = StripeId,
      spawn(fun() ->
        case arterial_nif:reactor_register_client(PoolRef, SId, ClientFd, self()) of
          {ok, SlotId} -> nif_echo_worker(PoolRef, SId, SlotId);
          {error, _}   -> arterial_nif:reactor_close_fd(PoolRef, ClientFd)
        end
      end),
      nif_accept_loop(PoolRef, ListenFd, NStripes, (StripeId + 1) rem NStripes)
  after 15000 ->
    ok
  end.

%% Echo worker: receives data and echoes it back.
nif_echo_worker(PoolRef, StripeId, SlotId) ->
  receive
    {arterial_event, StripeId, SlotId, read, Bin} when byte_size(Bin) > 0 ->
      arterial_nif:send_on_slot(PoolRef, StripeId, SlotId, [Bin]),
      nif_echo_worker(PoolRef, StripeId, SlotId);
    {arterial_event, StripeId, SlotId, read, <<>>} ->
      %% Empty read (transitional) — ignore.
      nif_echo_worker(PoolRef, StripeId, SlotId);
    {arterial_event, StripeId, SlotId, closed} ->
      ok;
    {arterial_event, StripeId, SlotId, _Other} ->
      nif_echo_worker(PoolRef, StripeId, SlotId)
  after ?TIMEOUT ->
    ok
  end.

%% ============================================================
%% Erlang gen_tcp client (one process per connection, sequential)
%% ============================================================

erl_gentcp_client_run(ServerPort, NConns, NReqs, MsgSize) ->
  Total  = NConns * NReqs,
  Parent = self(),
  LatStore = atomics:new(Total + 1, []),
  ErrCount = atomics:new(1, []),
  T0 = erlang:monotonic_time(microsecond),
  Pids = [begin
    Offset = Idx * NReqs,
    spawn(fun() ->
      try gentcp_conn_worker(ServerPort, NReqs, Offset, LatStore, ErrCount, MsgSize)
      catch _:_ -> atomics:add(ErrCount, 1, NReqs)
      end,
      Parent ! {done, self()}
    end)
  end || Idx <- lists:seq(0, NConns-1)],
  [receive {done, P} -> ok end || P <- Pids],
  T1      = erlang:monotonic_time(microsecond),
  make_result(T0, T1, NReqs, LatStore, ErrCount, Total, MsgSize).

gentcp_conn_worker(Port, NReqs, Offset, LatStore, Errs, MsgSize) ->
  {ok, Sock} = gen_tcp:connect("127.0.0.1", Port,
    [binary, {active, false}, {nodelay, true}, {packet, raw}], ?TIMEOUT),
  gentcp_send_loop(Sock, 0, NReqs, Offset, LatStore, Errs, MsgSize),
  gen_tcp:close(Sock).

gentcp_send_loop(_Sock, _Seq, 0, _Offset, _LS, _Errs, _MS) -> ok;
gentcp_send_loop(Sock, Seq, Rem, Offset, LS, Errs, MsgSize) ->
  PSize = MsgSize - 4,
  Req = <<Seq:32/big, 0:(PSize*8)>>,
  T0  = erlang:monotonic_time(microsecond),
  ok  = gen_tcp:send(Sock, Req),
  Idx = Offset + Rem,
  case gen_tcp:recv(Sock, MsgSize, ?TIMEOUT) of
    {ok, <<Seq:32/big, _/binary>>} ->
      atomics:put(LS, Idx, erlang:monotonic_time(microsecond) - T0);
    {ok, _} ->
      atomics:add(Errs, 1, 1);
    {error, _} ->
      atomics:add(Errs, 1, 1)
  end,
  gentcp_send_loop(Sock, Seq+1, Rem-1, Offset, LS, Errs, MsgSize).

%% ============================================================
%% Erlang Reactor NIF client
%%
%% Uses connect_proto_with_opts + send_and_release.
%% Data arrives as {arterial_event, StripeId, SlotId, read, Bin}.
%% ============================================================

erl_nif_client_run(ServerPort, NConns, NReqs, MsgSize) ->
  erl_nif_client_run(ServerPort, NConns, NReqs, MsgSize, NConns).

erl_nif_client_run(ServerPort, NConns, NReqs, MsgSize, NStripes) ->
  {ok, PoolRef} = arterial_nif:init_pool(NStripes, NConns div NStripes + 1),
  Addr     = {127, 0, 0, 1},
  Total    = NConns * NReqs,
  Parent   = self(),
  LatStore = atomics:new(Total + 1, []),
  ErrCount = atomics:new(1, []),

  T0 = erlang:monotonic_time(microsecond),

  %% Each worker connects in its own process so owner_pid = worker pid.
  %% This ensures {arterial_event, ..., read, Bin} messages arrive at the
  %% process that is blocking in receive waiting for them.
  Pids = [begin
    Offset   = Idx * NReqs,
    StripeId = Idx rem NStripes,
    spawn(fun() ->
      try
        SlotId = nif_connect_slot(PoolRef, StripeId, Addr, ServerPort),
        nif_client_worker(PoolRef, StripeId, SlotId, NReqs, Offset, LatStore, ErrCount, MsgSize),
        arterial_nif:close_slot(PoolRef, StripeId, SlotId)
      catch _:_ -> atomics:add(ErrCount, 1, NReqs)
      end,
      Parent ! {done, self()}
    end)
  end || Idx <- lists:seq(0, NConns-1)],

  [receive {done, P} -> ok end || P <- Pids],

  T1 = erlang:monotonic_time(microsecond),
  make_result(T0, T1, NReqs, LatStore, ErrCount, Total, MsgSize).

nif_connect_slot(PoolRef, StripeId, Addr, Port) ->
  Result = arterial_nif:connect_proto_with_opts(
             PoolRef, StripeId, Addr, Port, ?TIMEOUT,
             tcp, true, self(), []),
  case Result of
    {ok, connecting, SlotId} ->
      receive
        {arterial_event, StripeId, SlotId, connect_result, ok} ->
          SlotId;
        {arterial_event, StripeId, SlotId, connect_result, _} ->
          erlang:error({connect_failed, StripeId});
        {arterial_event, StripeId, SlotId, timeout} ->
          erlang:error({connect_timeout, StripeId})
      after ?TIMEOUT ->
        erlang:error({connect_timeout, StripeId})
      end;
    {ok, SlotId} ->
      %% Connection completed immediately (common on loopback).
      SlotId;
    {error, Reason} ->
      erlang:error({connect_error, StripeId, Reason})
  end.

nif_client_worker(PoolRef, StripeId, SlotId, NReqs, Offset, LatStore, Errs, MsgSize) ->
  nif_send_loop(PoolRef, StripeId, SlotId, 0, NReqs, Offset, LatStore, Errs, <<>>, MsgSize).

%% NIF client send loop.
%% The reactor may deliver empty binaries (during SLOT_CONNECTING transitions)
%% or multiple replies bundled in one binary (because handle_readable drains
%% all available bytes in one call).  We buffer bytes and consume 8-byte frames.
nif_send_loop(_PoolRef, _StripeId, _SlotId, _Seq, 0, _Offset, _LS, _Errs, _Buf, _MS) -> ok;
nif_send_loop(PoolRef, StripeId, SlotId, Seq, Rem, Offset, LS, Errs, Buf, MsgSize) ->
  PSize = MsgSize - 4,
  Req = <<Seq:32/big, 0:(PSize*8)>>,
  T0  = erlang:monotonic_time(microsecond),
  ok = arterial_nif:send_on_slot(PoolRef, StripeId, SlotId, [Req]),
  Idx     = Offset + Rem,
  case nif_await_frame(StripeId, SlotId, Buf, T0, MsgSize) of
    {ok, <<Seq:32/big, _/binary>>, Rest, LatUs} ->
      atomics:put(LS, Idx, LatUs),
      nif_send_loop(PoolRef, StripeId, SlotId, Seq+1, Rem-1, Offset, LS, Errs, Rest, MsgSize);
    {ok, _OtherSeq, Rest, _} ->
      atomics:add(Errs, 1, 1),
      nif_send_loop(PoolRef, StripeId, SlotId, Seq+1, Rem-1, Offset, LS, Errs, Rest, MsgSize);
    {error, closed} ->
      atomics:add(Errs, 1, Rem);
    {error, timeout} ->
      atomics:add(Errs, 1, 1),
      nif_send_loop(PoolRef, StripeId, SlotId, Seq+1, Rem-1, Offset, LS, Errs, <<>>, MsgSize)
  end.

%% Accumulate bytes from {arterial_event, ..., read, Bin} messages until
%% we have at least 8 bytes, then return the first frame plus leftover.
nif_await_frame(StripeId, SlotId, Buf, T0, MsgSize) ->
  nif_await_frame(StripeId, SlotId, Buf, T0, MsgSize, ?TIMEOUT).

nif_await_frame(_StripeId, _SlotId, Buf, T0, MsgSize, _Deadline) when byte_size(Buf) >= MsgSize ->
  <<Frame:MsgSize/binary, Rest/binary>> = Buf,
  LatUs = erlang:monotonic_time(microsecond) - T0,
  {ok, Frame, Rest, LatUs};
nif_await_frame(StripeId, SlotId, Buf, T0, MsgSize, _Deadline) ->
  receive
    {arterial_event, StripeId, SlotId, read, <<>>} ->
      nif_await_frame(StripeId, SlotId, Buf, T0, MsgSize, _Deadline);
    {arterial_event, StripeId, SlotId, read, More} ->
      nif_await_frame(StripeId, SlotId, <<Buf/binary, More/binary>>, T0, MsgSize, _Deadline);
    {arterial_event, StripeId, SlotId, closed} ->
      {error, closed}
  after ?TIMEOUT ->
    {error, timeout}
  end.

%% ============================================================
%% arterial_client client
%%
%% Uses arterial_pool + arterial_client:call/3 with reactor_bench_codec.
%% N worker processes each loop calling arterial_client:call/3 until the
%% duration elapses, matching shackle_client_run's structure exactly.
%% ============================================================

-define(ARTERIAL_POOL, reactor_bench_arterial_pool).

arterial_client_run(ServerPort, NConns, NReqs, MsgSize) ->
  DurationMs = max(400, NReqs * 2),
  Payload    = binary:copy(<<0>>, MsgSize - 4),
  Pool       = ?ARTERIAL_POOL,
  reactor_bench_codec:set_msg_size(MsgSize),
  arterial_pool:stop(Pool),
  {ok, _} = application:ensure_all_started(arterial),
  {ok, _} = arterial_pool:start_link(Pool, #{
    size               => NConns,
    codec              => reactor_bench_codec,
    address            => "127.0.0.1",
    port               => ServerPort,
    default_timeout_ms => ?TIMEOUT
  }),
  ok = arterial_pool:wait_connected(Pool, all, 8000),
  try
    arterial_bench_run(Pool, NConns, DurationMs, Payload, MsgSize)
  after
    arterial_pool:stop(Pool)
  end.

arterial_bench_run(Pool, NWorkers, DurationMs, Payload, MsgSize) ->
  Parent   = self(),
  Deadline = erlang:monotonic_time(millisecond) + DurationMs,
  T0       = erlang:monotonic_time(microsecond),
  Pids = [spawn(fun() ->
    arterial_worker_loop(Pool, Parent, Deadline, Payload, [], 0)
  end) || _ <- lists:seq(1, NWorkers)],
  WaitMs  = DurationMs + ?TIMEOUT + 1000,
  Results = [receive {arterial_done, P, Lats, Errs} -> {Lats, Errs}
             after WaitMs -> exit({arterial_worker_timeout, P})
             end || P <- Pids],
  T1 = erlang:monotonic_time(microsecond),
  AllLats   = lists:sort(lists:append([L || {L, _} <- Results])),
  TotalErrs = lists:sum([E || {_, E} <- Results]),
  Total     = length(AllLats) + TotalErrs,
  ElapsedUs = T1 - T0,
  TotalMB   = Total * MsgSize * 2 / (1024 * 1024),
  #{total      => Total,
    elapsed_us => ElapsedUs,
    rps        => Total / (ElapsedUs / 1_000_000),
    throughput => TotalMB / (ElapsedUs / 1_000_000),
    lat_mean   => mean(AllLats),
    lat_p50    => pct(AllLats, 0.50),
    lat_p99    => pct(AllLats, 0.99),
    lat_p999   => pct(AllLats, 0.999),
    errors     => TotalErrs,
    error_rate => TotalErrs / max(1, Total) * 100.0}.

arterial_worker_loop(Pool, Parent, Deadline, Payload, Lats, Errs) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {arterial_done, self(), Lats, Errs};
    false ->
      T0 = erlang:monotonic_time(microsecond),
      case arterial_client:call(Pool, Payload, ?TIMEOUT) of
        {ok, _Reply} ->
          T1 = erlang:monotonic_time(microsecond),
          arterial_worker_loop(Pool, Parent, Deadline, Payload,
                               [T1 - T0 | Lats], Errs);
        {error, _} ->
          arterial_worker_loop(Pool, Parent, Deadline, Payload,
                               Lats, Errs + 1)
      end
  end.

%% ============================================================
%% Shackle client
%%
%% Uses reactor_bench_shackle_client with the same <<Seq:32/big, Payload>>
%% framing.  Duration is derived from NReqs so the run time is comparable
%% to the other clients at the same NReqs setting.
%% ============================================================

-define(SHACKLE_POOL, reactor_bench_shackle_pool).

shackle_client_run(ServerPort, NConns, NReqs, MsgSize) ->
  DurationMs = max(400, NReqs * 2),
  Payload = binary:copy(<<0>>, MsgSize - 4),
  Pool = reactor_bench_shackle_pool,
  %% Stop previous pool if running, start the shackle app if not yet started.
  %% We avoid application:stop/start because shackle_app:stop/1 unconditionally
  %% emits an error_logger report on every shutdown (library bug).
  try shackle_pool:stop(Pool) catch _:_ -> ok end,
  ok = ensure_app_started(shackle),
  ok = shackle_pool:start(Pool, reactor_bench_shackle_client,
    [{address, "127.0.0.1"}, {port, ServerPort},
     {socket_options, [binary, {nodelay, true}, {active, true}, {packet, 0}]},
     {reconnect, true}, {reconnect_time_min, 10},
     {init_options, [{msg_size, MsgSize}]}],
    [{pool_size, NConns}, {pool_strategy, round_robin},
     {max_retries, NConns - 1}, {backlog_size, 1}]),
  true = shackle_pool:wait_until_any_available(Pool, 8000),
  try
    shackle_bench_run(Pool, NConns, DurationMs, Payload, MsgSize)
  after
    ok % pool stays running; next call will stop/restart shackle cleanly
  end.

shackle_bench_run(Pool, NWorkers, DurationMs, Payload, MsgSize) ->
  Parent   = self(),
  Deadline = erlang:monotonic_time(millisecond) + DurationMs,
  T0       = erlang:monotonic_time(microsecond),
  Pids = [spawn(fun() ->
    shackle_worker_loop(Pool, Parent, Deadline, Payload, MsgSize, [], 0)
  end) || _ <- lists:seq(1, NWorkers)],
  WaitMs  = DurationMs + ?TIMEOUT + 1000,
  Results = [receive {shackle_done, P, Lats, Errs} -> {Lats, Errs}
             after WaitMs -> exit({shackle_worker_timeout, P})
             end || P <- Pids],
  T1 = erlang:monotonic_time(microsecond),
  AllLats = lists:sort(lists:append([L || {L, _} <- Results])),
  TotalErrs = lists:sum([E || {_, E} <- Results]),
  Total = length(AllLats) + TotalErrs,
  ElapsedUs = T1 - T0,
  TotalMB = Total * MsgSize * 2 / (1024 * 1024),
  #{total      => Total,
    elapsed_us => ElapsedUs,
    rps        => Total / (ElapsedUs / 1_000_000),
    throughput => TotalMB / (ElapsedUs / 1_000_000),
    lat_mean   => mean(AllLats),
    lat_p50    => pct(AllLats, 0.50),
    lat_p99    => pct(AllLats, 0.99),
    lat_p999   => pct(AllLats, 0.999),
    errors     => TotalErrs,
    error_rate => TotalErrs / max(1, Total) * 100.0}.

shackle_worker_loop(Pool, Parent, Deadline, Payload, MsgSize, Lats, Errs) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {shackle_done, self(), Lats, Errs};
    false ->
      Seq = length(Lats) + Errs,
      T0  = erlang:monotonic_time(microsecond),
      case shackle:call(Pool, {Seq, Payload}, ?TIMEOUT) of
        Frame when is_binary(Frame) ->
          T1 = erlang:monotonic_time(microsecond),
          shackle_worker_loop(Pool, Parent, Deadline, Payload, MsgSize,
                              [T1 - T0 | Lats], Errs);
        _ ->
          shackle_worker_loop(Pool, Parent, Deadline, Payload, MsgSize,
                              Lats, Errs + 1)
      end
  end.

ensure_app_started(App) ->
  case application:ensure_all_started(App) of
    {ok, _}         -> ok;
    {error, Reason} -> erlang:error({app_start_failed, App, Reason})
  end.

%% ============================================================
%% Poolboy client
%%
%% Uses reactor_bench_poolboy_worker with the same <<Seq:32/big, Payload>>
%% framing.  Duration-based, same derivation as shackle_client_run.
%% ============================================================

-define(POOLBOY_POOL, reactor_bench_poolboy_pool).

poolboy_client_run(ServerPort, NConns, NReqs, MsgSize) ->
  DurationMs = max(400, NReqs * 2),
  Payload = binary:copy(<<0>>, MsgSize - 4),
  WorkerArgs = [{address, "127.0.0.1"}, {port, ServerPort}, {msg_size, MsgSize}],
  PoolArgs = [{name, {local, ?POOLBOY_POOL}},
              {worker_module, reactor_bench_poolboy_worker},
              {size, NConns}, {max_overflow, 0}, {strategy, fifo}],
  %% Unlink so poolboy doesn't drag the test process down on stop.
  {ok, PoolPid} = poolboy:start(PoolArgs, WorkerArgs),
  try
    poolboy_bench_run(?POOLBOY_POOL, NConns, DurationMs, Payload, MsgSize)
  after
    poolboy:stop(PoolPid),
    %% Wait until the name is unregistered before returning, so the
    %% warmup and bench calls don't race on the same local name.
    poolboy_wait_stopped(?POOLBOY_POOL, 100)
  end.

poolboy_wait_stopped(_Name, 0) -> ok;
poolboy_wait_stopped(Name, N) ->
  case whereis(Name) of
    undefined -> ok;
    _         -> timer:sleep(5), poolboy_wait_stopped(Name, N - 1)
  end.

poolboy_bench_run(Pool, NWorkers, DurationMs, Payload, MsgSize) ->
  Parent   = self(),
  Deadline = erlang:monotonic_time(millisecond) + DurationMs,
  T0       = erlang:monotonic_time(microsecond),
  Pids = [spawn(fun() ->
    poolboy_worker_loop(Pool, Parent, Deadline, Payload, MsgSize, [], 0)
  end) || _ <- lists:seq(1, NWorkers)],
  WaitMs  = DurationMs + ?TIMEOUT + 1000,
  Results = [receive {poolboy_done, P, Lats, Errs} -> {Lats, Errs}
             after WaitMs -> exit({poolboy_worker_timeout, P})
             end || P <- Pids],
  T1 = erlang:monotonic_time(microsecond),
  AllLats = lists:sort(lists:append([L || {L, _} <- Results])),
  TotalErrs = lists:sum([E || {_, E} <- Results]),
  Total = length(AllLats) + TotalErrs,
  ElapsedUs = T1 - T0,
  TotalMB = Total * MsgSize * 2 / (1024 * 1024),
  #{total      => Total,
    elapsed_us => ElapsedUs,
    rps        => Total / (ElapsedUs / 1_000_000),
    throughput => TotalMB / (ElapsedUs / 1_000_000),
    lat_mean   => mean(AllLats),
    lat_p50    => pct(AllLats, 0.50),
    lat_p99    => pct(AllLats, 0.99),
    lat_p999   => pct(AllLats, 0.999),
    errors     => TotalErrs,
    error_rate => TotalErrs / max(1, Total) * 100.0}.

poolboy_worker_loop(Pool, Parent, Deadline, Payload, MsgSize, Lats, Errs) ->
  case erlang:monotonic_time(millisecond) >= Deadline of
    true ->
      Parent ! {poolboy_done, self(), Lats, Errs};
    false ->
      Seq = length(Lats) + Errs,
      T0  = erlang:monotonic_time(microsecond),
      Result =
        try poolboy:transaction(Pool,
              fun(W) -> reactor_bench_poolboy_worker:call(W, Seq, Payload) end,
              ?TIMEOUT)
        catch exit:{timeout, _} -> {error, timeout}
        end,
      case Result of
        {ok, _Frame} ->
          T1 = erlang:monotonic_time(microsecond),
          poolboy_worker_loop(Pool, Parent, Deadline, Payload, MsgSize,
                              [T1 - T0 | Lats], Errs);
        {error, _} ->
          poolboy_worker_loop(Pool, Parent, Deadline, Payload, MsgSize,
                              Lats, Errs + 1)
      end
  end.

%% ============================================================
%% Shared result builder
%% ============================================================

make_result(T0, T1, _NReqs, LatStore, ErrCount, Total, MsgSize) ->
  Elapsed = T1 - T0,
  Lats = lists:sort(
    [atomics:get(LatStore, I) || I <- lists:seq(1, Total),
     atomics:get(LatStore, I) > 0]),
  Errs    = atomics:get(ErrCount, 1),
  TotalMB = Total * MsgSize * 2 / (1024 * 1024),
  #{total      => Total,
    elapsed_us => Elapsed,
    rps        => Total / (Elapsed / 1_000_000),
    throughput => TotalMB / (Elapsed / 1_000_000),
    lat_mean   => mean(Lats),
    lat_p50    => pct(Lats, 0.50),
    lat_p99    => pct(Lats, 0.99),
    lat_p999   => pct(Lats, 0.999),
    errors     => Errs,
    error_rate => Errs / max(1, Total) * 100.0}.

%% ============================================================
%% Statistics
%% ============================================================

mean([])  -> 0.0;
mean(Xs)  -> lists:sum(Xs) / length(Xs).

pct([], _) -> 0.0;
pct(S, P)  ->
  N = length(S),
  I = max(1, min(N, round(P * N))),
  lists:nth(I, S).

%% ============================================================
%% Output
%% ============================================================

bar(N) -> lists:duplicate(N, $-).

print_row(Label, I,
  #{rps        := RPS,
    throughput := MB,
    lat_mean   := Mean,
    lat_p50    := P50,
    lat_p99    := P99,
    lat_p999   := P999,
    errors     := Err,
    error_rate := ErrPct}) ->
  F   = fun(V, Dec) -> string:pad(float_to_list(float(V), [{decimals, Dec}]), 8, leading) end,
  F10 = fun(V) -> string:pad(float_to_list(float(V), [{decimals, 0}]), 10, leading) end,
  ErrS = if
    Err < 1000 -> integer_to_list(Err);
    ErrPct < 3 -> F(ErrPct, 4) ++ "%";
    true       -> F(ErrPct, 2) ++ "%"
  end,
  io:format(standard_error,
    "~2w: ~-47s  ~s  ~s  ~s  ~s  ~s  ~s ~9s~n",
    [I, Label, F10(RPS), F(MB,2), F(Mean,1), F(P50,1), F(P99,1), F(P999,1), ErrS]).
