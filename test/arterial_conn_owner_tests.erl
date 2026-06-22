-module(arterial_conn_owner_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
End-to-end coverage of `arterial_conn_owner`'s multiplexing, disconnect,
and crash-resilience behavior, against a real `test_tcp_server` (same
fixtures as `test_tcp_server_tests`).

The core scenario these tests guard is the bug this module's
introduction fixed: with `backlog > 1`, several callers can legitimately
hold concurrent reservations on the same connection, and previously each
caller did raw socket I/O directly -- two concurrent `recv`s racing for
whichever bytes arrived next, with the "loser" hanging forever. Now every
send/recv/decode for a connection is serialized through its owner
process, so concurrent callers on the same connection must each get back
exactly their own reply, never one another's.
""".

-define(POOL, conn_owner_test_pool).

setup(Size, Backlog) ->
  setup(Size, Backlog, true).

setup(Size, Backlog, Fifo) ->
  ok = test_helper:set_log_level(),
  {ok, Srv} = test_tcp_server:start(0),
  Port = test_tcp_server:port(Srv),
  {ok, SupPid} = arterial_pool:start_link(?POOL, #{
    size        => Size,
    backlog     => Backlog,
    fifo        => Fifo,
    protocol    => test_echo_protocol,
    client      => test_echo_client,
    client_opts => #{address => "127.0.0.1", port => Port, protocol => tcp}
  }),
  try
    wait_until_available(Size, 100),
    {Srv, SupPid}
  catch
    Class:Reason:Stack ->
      teardown({Srv, SupPid}),
      erlang:raise(Class, Reason, Stack)
  end.

teardown({Srv, SupPid}) ->
  ok = supervisor:stop(SupPid),
  ok = arterial_nif:destroy(?POOL),
  test_tcp_server:stop(Srv).

wait_until_available(N, Retries) ->
  case checkout_n(N, []) of
    {ok, ConnIDs} ->
      lists:foreach(fun(ConnID) -> ok = arterial_nif:checkin_connection(?POOL, ConnID) end, ConnIDs);
    {error, no_connection} when Retries > 0 ->
      timer:sleep(20),
      wait_until_available(N, Retries - 1);
    {error, no_connection} ->
      error(pool_not_ready)
  end.

checkout_n(0, Acc) ->
  {ok, Acc};
checkout_n(N, Acc) ->
  case arterial_nif:checkout_connection(?POOL, sync) of
    {ok, ConnID} ->
      checkout_n(N - 1, [ConnID | Acc]);
    {error, no_connection} = Error ->
      lists:foreach(fun(ConnID) -> ok = arterial_nif:checkin_connection(?POOL, ConnID) end, Acc),
      Error
  end.

%% Core repro: one connection, backlog=4, exactly 4 concurrent callers
%% each with a unique payload -- nobody hangs, and everybody gets back
%% exactly their own payload (the exact misattribution failure mode this
%% module fixes). Deliberately workers =:= backlog (not oversubscribed):
%% `max_waiters` defaults to 0, so excess workers would just see
%% {error, no_connection} immediately, which is a capacity question, not
%% a demux-correctness one -- kept separate from this test.
multiplexed_concurrent_calls_get_own_replies_test() ->
  Backlog = 4,
  {Srv, SupPid} = setup(1, Backlog),
  try
    Parent = self(),
    Pids = [
      spawn(fun() ->
        Payload = {worker, N},
        Result = arterial_client:call(?POOL, {delay, 20, Payload}, 2000),
        Parent ! {self(), Result, Payload}
      end)
      || N <- lists:seq(1, Backlog)
    ],
    Results = [collect_own(Pid) || Pid <- Pids],
    ?assert(lists:all(fun(R) -> R =:= ok end, Results))
  after
    teardown({Srv, SupPid})
  end.

collect_own(Pid) ->
  receive
    {Pid, {ok, Payload}, Payload} -> ok;
    {Pid, Other, Payload}         -> {mismatch, Payload, Other}
  after 3000 ->
    {timeout, Pid}
  end.

%% A connection disconnecting mid-flight must promptly fail every pending
%% caller (some flavor of {error, _}) rather than leaving them hanging
%% until their own per-request timeout fires.
disconnect_flushes_pending_callers_test() ->
  {Srv, SupPid} = setup(1, 4),
  try
    Parent = self(),
    Pids = [
      spawn(fun() -> Parent ! {self(), arterial_client:call(?POOL, {delay, 500, N}, 5000)} end)
      || N <- lists:seq(1, 3)
    ],
    timer:sleep(50), % let the calls actually land on the connection first

    {ok, ConnPid} = conn_pid(0),

    %% Force a disconnect (not a clean bounce) by killing the connection
    %% process directly -- exit(_, kill) bypasses terminate/2 entirely, so
    %% arterial_socket:close/2 never runs, but the OTP `socket` module
    %% ties the underlying fd's lifetime to the process that opened it
    %% (arterial_connection itself), so it's closed by the BEAM regardless
    %% -- the owner's next recv attempt sees {error, closed} and must
    %% still flush its pending callers promptly instead of waiting out
    %% their 5s timeouts.
    StartAt = erlang:monotonic_time(millisecond),
    {Results, ElapsedMs} = with_console_silenced(fun() ->
      exit(ConnPid, kill),
      R = [receive {P, Reply} -> Reply after 2000 -> error({worker_timeout, P}) end || P <- Pids],
      Ms = erlang:monotonic_time(millisecond) - StartAt,

      %% Let the killed connection's supervisor-driven restart/reconnect
      %% settle before teardown -- otherwise supervisor:stop/1 can race a
      %% reconnect attempt still in flight, which is harmless on its own
      %% but leaves no guarantee the next test's setup/0 starts from a
      %% fully quiesced registered-name/ETS/socket state. Also keeps the
      %% supervisor's own (expected) child_terminated report inside the
      %% silenced window, since it's logged asynchronously, slightly
      %% after exit/2 returns.
      ok = wait_until_available(1, 100),
      {R, Ms}
    end),

    ?assert(lists:all(fun(R) -> element(1, R) =:= error end, Results)),
    %% Flushed promptly, not via the 500ms per-request timeout fallback.
    ?assert(ElapsedMs < 400)
  after
    teardown({Srv, SupPid})
  end.

%% cast/2 (fire-and-forget) and call/3 mixed on the same multiplexed
%% connection: the cast's write must not corrupt the demux state used by
%% concurrent call/3 replies. Uses {noreply, _} (test_tcp_server's only
%% genuinely one-way request shape) for the cast -- {echo, _} would
%% violate cast/2's own one-way contract here, since the server always
%% replies to it regardless of which API the client used to send it,
%% desyncing FIFO mode's reply-order assumption for the very next call/3.
cast_mixed_with_concurrent_call_test() ->
  {Srv, SupPid} = setup(1, 4),
  try
    Parent = self(),
    spawn(fun() -> Parent ! {cast_done, arterial_client:cast(?POOL, {noreply, ignored})} end),
    spawn(fun() -> Parent ! {call_done, arterial_client:call(?POOL, {echo, hello}, 2000)} end),

    {cast_done, ok} = receive Msg1 = {cast_done, _} -> Msg1 after 2000 -> error(timeout) end,
    {call_done, {ok, hello}} = receive Msg2 = {call_done, _} -> Msg2 after 2000 -> error(timeout) end
  after
    teardown({Srv, SupPid})
  end.

%% Killing the owner process mid-flight must fail the in-flight caller
%% fast (not hang), and the connection must come back fully usable once
%% the supervisor restarts the owner and arterial_connection republishes
%% the live socket to it. Relies on `monitor_owner_calls => true` (the
%% default -- see `arterial_pool:options/0`), which `setup/2,3` doesn't
%% override.
owner_crash_resilience_test() ->
  {Srv, SupPid} = setup(1, 1),
  try
    Parent = self(),
    spawn(fun() -> Parent ! {result, arterial_client:call(?POOL, {delay, 300, slow}, 5000)} end),
    timer:sleep(50),

    OwnerPid = whereis(arterial_conn_owner:reg_name(?POOL, 0)),
    true = is_pid(OwnerPid),
    %% Keeps the supervisor's own (expected) child_terminated report
    %% inside the silenced window, since it's logged asynchronously,
    %% slightly after exit/2 returns.
    with_console_silenced(fun() ->
      exit(OwnerPid, kill),
      {result, {error, _}} = receive Msg -> Msg after 2000 -> error(timeout) end,

      %% Wait for the supervisor to restart the owner and for
      %% arterial_connection to republish the socket to it.
      ok = wait_until_owner_usable(100)
    end),
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 2000)
  after
    teardown({Srv, SupPid})
  end.

wait_until_owner_usable(0) ->
  error(owner_not_restarted);
wait_until_owner_usable(Retries) ->
  case arterial_client:call(?POOL, {echo, probe}, 500) of
    {ok, probe} -> ok;
    _           ->
      timer:sleep(20),
      wait_until_owner_usable(Retries - 1)
  end.

%% A caller that dies while its request is still in-flight (never reaches
%% its own checkin/after block) must not leave a dangling pending entry
%% in the owner -- the owner's own erlang:monitor on the caller (see
%% arterial_conn_owner's handle_info({'DOWN',...})) drops it immediately.
%% Observed indirectly: the connection must still accept and correctly
%% answer a fresh, unrelated call right away, proving the dead caller's
%% backlog slot (NIF-side) and pending entry (owner-side) were both
%% released rather than stranded until a multi-second timeout.
caller_death_releases_pending_entry_test() ->
  {Srv, SupPid} = setup(1, 1),
  try
    Parent = self(),
    DyingPid = spawn(fun() ->
      Parent ! ready,
      receive die -> ok end,
      %% Crash immediately after sending, before any reply/timeout can
      %% land -- the owner's monitor on this pid must fire and clean up.
      exit(self(), kill)
    end),
    receive ready -> ok end,

    %% Drive the dying process's own send_recv call so the pending entry
    %% is tracked against *its* pid, then kill it before the (slow)
    %% server reply arrives.
    CallerCallPid = spawn(fun() ->
      arterial_client:call(?POOL, {delay, 1000, will_be_abandoned}, 5000)
    end),
    timer:sleep(50),
    exit(CallerCallPid, kill),
    DyingPid ! die,

    %% Give the owner's {'DOWN',...} handler a moment to run, then prove
    %% the connection is immediately usable again (backlog=1, so if the
    %% dead caller's reservation/pending entry weren't released, this
    %% would block until the original 5000ms timeout).
    timer:sleep(100),
    {ok, hello} = arterial_client:call(?POOL, {echo, hello}, 1000)
  after
    teardown({Srv, SupPid})
  end.

conn_pid(ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(?POOL)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.

%% Tests that deliberately exit(_, kill) a supervised child to exercise
%% crash recovery trigger an expected `child_terminated`/`reason: killed`
%% supervisor report -- logged at `error` level, so test_helper's
%% set_log_level/0 (which only raises the *primary* level) doesn't touch
%% it. Temporarily raise the default console handler's own level instead,
%% scoped tightly around the kill, so an unrelated real error logged
%% outside that window still prints normally.
with_console_silenced(Fun) ->
  ok = logger:set_handler_config(default, level, critical),
  try
    Fun()
  after
    logger:set_handler_config(default, level, all)
  end.
