-module(arterial_nif_tests).
-include_lib("eunit/include/eunit.hrl").

%% Exercises the in-flight async request registry: tracking, timeout
%% notification, untracking on checkin, and (critically) that a timed-out
%% request's backlog slot is released so the connection becomes reusable
%% instead of leaking capacity.

setup(Pool, Backlog, Fifo, FixedTtlUs) ->
  setup(Pool, 1, Backlog, Fifo, FixedTtlUs, 0).

setup(Pool, Size, Backlog, Fifo, FixedTtlUs, MaxWaiters) ->
  ok = arterial_nif:create(Pool, Size, Backlog, Fifo, dummy_protocol, FixedTtlUs, MaxWaiters),
  lists:foreach(
    fun(ConnID) ->
      true = arterial_nif:set_socket(Pool, ConnID, dummy_socket),
      true = arterial_nif:make_available(Pool, ConnID)
    end,
    lists:seq(0, Size - 1)),
  Pool.


teardown(Pool) ->
  ok = arterial_nif:destroy(Pool),
  %% Guard against cross-test mailbox contamination: any stray message
  %% for this pool (e.g. a {arterial_ready,...}/{arterial_timeout,...}/
  %% {arterial_disconnected,...} the test forgot to assert on) must not
  %% leak into the next test.
  Leftover = flush_timeouts(Pool) ++ flush_ready(Pool) ++ flush_disconnected(Pool),
  [] = Leftover.

%% Test helpers for protocol-based tests that use `test_protocol`.
setup_protocol(Pool, ServerPid, Size) ->
  ok = arterial_nif:create(Pool, Size, 1, true, test_protocol, 0, 4),
  lists:foreach(
    fun(ConnID) ->
      true = arterial_nif:set_socket(Pool, ConnID, {test_server, ServerPid}),
      true = arterial_nif:make_available(Pool, ConnID)
    end,
    lists:seq(0, Size - 1)),
  Pool.

flush_timeouts(Pool) ->
  receive
    {arterial_timeout, Pool, _} = M -> [M | flush_timeouts(Pool)]
  after 0 -> []
  end.

flush_ready(Pool) ->
  receive
    {arterial_ready, Pool, _, _, _, _} = M -> [M | flush_ready(Pool)]
  after 0 -> []
  end.

flush_disconnected(Pool) ->
  receive
    {arterial_disconnected, Pool, _} = M -> [M | flush_disconnected(Pool)]
  after 0 -> []
  end.

timeout_fires_test() ->
  Pool = setup(arterial_timeout_fires, 1, true, 0),
  try
    {ok, #{conn_id := ConnID, req_ids := [ReqID]}} =
      arterial_nif:checkout_connection(Pool, async),
    ok = arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), 0),
    timer:sleep(1),
    {ok, 1} = arterial_nif:sweep_timeouts(Pool),
    [{arterial_timeout, Pool, ReqID}] = flush_timeouts(Pool)
  after
    teardown(Pool)
  end.

checkin_before_timeout_untracks_test() ->
  Pool = setup(arterial_checkin_untracks, 1, true, 0),
  try
    {ok, #{conn_id := ConnID, req_ids := [ReqID]}} =
      arterial_nif:checkout_connection(Pool, async),
    ok = arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), 1000000),
    ok = arterial_nif:checkin_connection(Pool, ConnID, [ReqID], <<>>),
    {ok, 0} = arterial_nif:sweep_timeouts(Pool),
    [] = flush_timeouts(Pool)
  after
    teardown(Pool)
  end.

fixed_ttl_pool_mode_test() ->
  Pool = setup(arterial_fixed_ttl, 1, true, 1),
  try
    {ok, #{conn_id := ConnID, req_ids := [ReqID]}} =
      arterial_nif:checkout_connection(Pool, async),
    %% TtlUs argument is ignored in fixed-TTL mode; the pool's 1us TTL wins.
    ok = arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), 1000000),
    timer:sleep(1),
    {ok, 1} = arterial_nif:sweep_timeouts(Pool),
    [{arterial_timeout, Pool, ReqID}] = flush_timeouts(Pool)
  after
    teardown(Pool)
  end.

%% Regression test: repeated timeouts on a single-slot backlog must not
%% leak the slot. Before the fix, SweepTimeouts() released the backlog
%% slot but never returned the connection itself to the pool's available
%% list, so the second checkout_connection/2 call would fail forever.
no_backlog_leak_on_repeated_timeout_test() ->
  Pool = setup(arterial_no_leak, 1, true, 0),
  try
    lists:foreach(
      fun(_) ->
        {ok, #{conn_id := ConnID, req_ids := [ReqID]}} =
          arterial_nif:checkout_connection(Pool, async),
        ok = arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), 0),
        timer:sleep(1),
        {ok, 1} = arterial_nif:sweep_timeouts(Pool),
        flush_timeouts(Pool),
        ok = arterial_nif:checkin_connection(Pool, ConnID, [ReqID], <<>>)
      end,
      lists:seq(1, 200))
  after
    teardown(Pool)
  end.

%%%-----------------------------------------------------------------------------
%%% Queue-when-busy (checkout_async/3 + wait-list) tests
%%%-----------------------------------------------------------------------------

checkout_async_immediate_success_test() ->
  Pool = setup(arterial_async_immediate, 1, 1, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    true = is_integer(ReqID),
    [] = flush_ready(Pool),
    [] = flush_timeouts(Pool),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID], <<>>)
  after
    teardown(Pool)
  end.

%% With max_waiters=0 (the default), a busy pool must fail immediately
%% instead of queuing -- same observable behavior as before this feature
%% existed.
checkout_async_disabled_rejects_immediately_test() ->
  Pool = setup(arterial_async_disabled, 1, 1, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    {error, no_connection} = arterial_nif:checkout_async(Pool, self(), 1000000),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>)
  after
    teardown(Pool)
  end.

%% A queued waiter is serviced (gets {arterial_ready,...}) as soon as the
%% busy connection is checked back in.
checkout_async_queued_then_serviced_by_checkin_test() ->
  Pool = setup(arterial_async_queued_checkin, 1, 1, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    {queued, WaiterID} = arterial_nif:checkout_async(Pool, self(), 1000000),
    true = is_integer(WaiterID),
    [] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),

    [{arterial_ready, Pool, ReqID2, 0, dummy_socket, [ReqID2]}] = flush_ready(Pool),
    [] = flush_timeouts(Pool),

    %% the serviced waiter behaves like any other checked-out request
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>)
  after
    teardown(Pool)
  end.

%% A queued waiter is serviced when capacity frees up via sweep_timeouts/1
%% releasing a *different*, timed-out in-flight request's backlog slot.
checkout_async_queued_then_serviced_by_sweep_test() ->
  Pool = setup(arterial_async_queued_sweep, 1, 1, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 0), % expires almost immediately
    {queued, _WaiterID} = arterial_nif:checkout_async(Pool, self(), 1000000),
    timer:sleep(1),

    {ok, 1} = arterial_nif:sweep_timeouts(Pool),

    [{arterial_timeout, Pool, ReqID1}] = flush_timeouts(Pool),
    [{arterial_ready, Pool, ReqID2, 0, dummy_socket, [ReqID2]}] = flush_ready(Pool),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>)
  after
    teardown(Pool)
  end.

%% A queued waiter that's never serviced expires and is removed from the
%% wait-list (does not linger / get serviced after the fact).
checkout_async_queued_then_times_out_test() ->
  Pool = setup(arterial_async_queued_timeout, 1, 1, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    {queued, WaiterID} = arterial_nif:checkout_async(Pool, self(), 0), % expires almost immediately
    timer:sleep(1),

    {ok, 0} = arterial_nif:sweep_timeouts(Pool), % no in-flight requests timed out
    [{arterial_timeout, Pool, WaiterID}] = flush_timeouts(Pool),
    [] = flush_ready(Pool),

    %% releasing the original connection must NOT retroactively service
    %% the already-expired waiter
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),
    [] = flush_ready(Pool)
  after
    teardown(Pool)
  end.

%% When the wait-list is full, further queue attempts are rejected
%% immediately rather than growing unboundedly.
checkout_async_wait_list_full_rejects_test() ->
  Pool = setup(arterial_async_full, 1, 1, true, 0, 2),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    {queued, _W2} = arterial_nif:checkout_async(Pool, self(), 1000000),
    {queued, _W3} = arterial_nif:checkout_async(Pool, self(), 1000000),
    {error, no_connection} = arterial_nif:checkout_async(Pool, self(), 1000000),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),
    [{arterial_ready, Pool, ReqID2, 0, dummy_socket, [ReqID2]}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>),
    [{arterial_ready, Pool, ReqID3, 0, dummy_socket, [ReqID3]}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID3], <<>>)
  after
    teardown(Pool)
  end.

%% Multiple waiters are serviced in FIFO order (oldest queued first).
checkout_async_fifo_order_test() ->
  Pool = setup(arterial_async_fifo, 1, 1, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    {queued, W2} = arterial_nif:checkout_async(Pool, self(), 1000000),
    {queued, W3} = arterial_nif:checkout_async(Pool, self(), 1000000),
    true = W2 < W3,

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),
    [{arterial_ready, Pool, ReqID2, 0, dummy_socket, [ReqID2]}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>),
    [{arterial_ready, Pool, ReqID3, 0, dummy_socket, [ReqID3]}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID3], <<>>)
  after
    teardown(Pool)
  end.

%% Mode (d): a single connection with backlog>1 accepts multiple
%% concurrently outstanding requests (multiplexing) via checkout_connection/3's
%% Samples argument, reserving several backlog slots on one connection in a
%% single call -- this is what backlog>1 buys over backlog=1, and is only
%% safe when the server preserves send order in its replies (see
%% docs/client-guide.md's "Surrogate request IDs" and README.md section
%% 2.1's FIFO-ordering warning). FIFOBackLog::CheckIn ignores the ReqID
%% argument it's given and always pops the head, so this test also pins
%% that down: checking in with the *wrong* (non-head) ReqID still resolves
%% the actual head request, never the one named.
fifo_backlog_multiplexing_test() ->
  Pool = setup(arterial_fifo_multiplex, 1, 3, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1, ReqID2, ReqID3]}} =
      arterial_nif:checkout_connection(Pool, sync, 3),
    true = ReqID1 =/= ReqID2,
    true = ReqID2 =/= ReqID3,

    %% Backlog capacity (3) is now fully reserved AND the connection itself
    %% is checked out of the pool's available ring: a 2nd checkout (even
    %% for just 1 sample) must fail rather than multiplex past capacity or
    %% land on the same connection concurrently.
    {error, no_connection} = arterial_nif:checkout_connection(Pool, sync),
    false = arterial_nif:connection_drained(Pool, 0),

    %% Check in using ReqID3 (the *last* one checked out, not the head) --
    %% FIFOBackLog::CheckIn ignores the argument and pops the head (ReqID1)
    %% regardless of which id is named, since the only contract this mode
    %% offers is "replies arrive in send order". This also re-adds the
    %% connection to the pool's available ring (checkin_nif always does,
    %% independent of how many of the original Samples are still
    %% outstanding), so a fresh 1-sample checkout below succeeds again.
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID3], <<>>),
    false = arterial_nif:connection_drained(Pool, 0),

    %% Capacity freed by the checkin above is reusable immediately.
    {ok, #{conn_id := 0, req_ids := [ReqID4]}} =
      arterial_nif:checkout_connection(Pool, sync),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),
    false = arterial_nif:connection_drained(Pool, 0),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID4], <<>>),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.

%% Mode (d) also works through the async/queue-when-busy path: a single
%% checkout_async/4 call with Samples=2 reserves 2 backlog slots on one
%% connection in one shot, exactly like checkout_connection/3.
fifo_backlog_multiplexing_async_test() ->
  Pool = setup(arterial_fifo_multiplex_async, 1, 2, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1, ReqID2]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000, 2),
    true = ReqID1 =/= ReqID2,

    %% A 3rd-sample request can't fit on the busy connection and there's
    %% nowhere else to put it (pool size 1), so it queues instead of
    %% failing immediately (MaxWaiters=4 here, unlike the sync test above).
    {queued, _WaiterID} = arterial_nif:checkout_async(Pool, self(), 1000000, 1),
    [] = flush_ready(Pool),

    %% Checking in just one of the two multiplexed slots frees the
    %% connection (checkin_nif unconditionally re-adds it to the pool's
    %% available ring) and lets the queued waiter's 1-sample request
    %% through immediately.
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),
    [{arterial_ready, Pool, ReqID3, 0, dummy_socket, [ReqID3]}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID3], <<>>),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.

%% Mode (f-2): a cumulative ack (checkin_up_to/3) releases every
%% outstanding FIFO slot up to and including the named req_id in one
%% call, in send order, even though each slot may belong to a different
%% owning process (each reserved by its own checkout_async/4 call).
checkin_up_to_releases_fifo_span_test() ->
  Pool = setup(arterial_checkin_up_to, 1, 3, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1, ReqID2, ReqID3]}} =
      arterial_nif:checkout_connection(Pool, sync, 3),
    ok = arterial_nif:track_inflight(Pool, 0, ReqID1, self(), 1000000),
    ok = arterial_nif:track_inflight(Pool, 0, ReqID2, self(), 1000000),
    ok = arterial_nif:track_inflight(Pool, 0, ReqID3, self(), 1000000),

    %% Ack only through ReqID2 -- ReqID3 must remain outstanding.
    {ok, Released} = arterial_nif:checkin_up_to(Pool, 0, ReqID2),
    [ReqID1, ReqID2] = Released,
    false = arterial_nif:connection_drained(Pool, 0),

    %% The freed capacity (2 of 3 slots) is immediately reusable.
    {ok, #{conn_id := 0, req_ids := [ReqID4, ReqID5]}} =
      arterial_nif:checkout_connection(Pool, sync, 2),

    %% Ack the rest (ReqID3, then the two fresh ones) to fully drain.
    {ok, [ReqID3]} = arterial_nif:checkin_up_to(Pool, 0, ReqID3),
    {ok, [ReqID4, ReqID5]} = arterial_nif:checkin_up_to(Pool, 0, ReqID5),
    true = arterial_nif:connection_drained(Pool, 0),

    %% No {arterial_disconnected,...} (that's connection_down/2's job, not
    %% checkin_up_to/3's) and no leftover {arterial_timeout,...} either.
    [] = flush_disconnected(Pool),
    [] = flush_timeouts(Pool)
  after
    teardown(Pool)
  end.

%% An ack for an id that was never outstanding (already checked in, or
%% never reserved) is a no-op: {ok, []}, nothing released, no crash.
checkin_up_to_noop_for_unknown_id_test() ->
  Pool = setup(arterial_checkin_up_to_unknown, 1, 2, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_connection(Pool, sync),

    {ok, []} = arterial_nif:checkin_up_to(Pool, 0, ReqID1 + 1000),
    false = arterial_nif:connection_drained(Pool, 0),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>)
  after
    teardown(Pool)
  end.

%% checkin_up_to/3 against a random-access (fifo => false) backlog is a
%% no-op: there's no FIFO order to bulk-collapse, since each request
%% there is already individually id-matched.
checkin_up_to_noop_for_random_access_backlog_test() ->
  Pool = setup(arterial_checkin_up_to_ra, 1, 2, false, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_connection(Pool, sync),

    {ok, []} = arterial_nif:checkin_up_to(Pool, 0, ReqID1),
    false = arterial_nif:connection_drained(Pool, 0),

    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>)
  after
    teardown(Pool)
  end.

%% Disconnect notification: connection_down/2 must tell every owner of an
%% in-flight (track_inflight/5-registered) request on a connection that it
%% died, one {arterial_disconnected, Pool, ReqID} message per request, then
%% free the backlog slot(s) and make the connection available again.
connection_down_notifies_inflight_test() ->
  Pool = setup(arterial_conn_down_notify, 1, 2, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1, ReqID2]}} =
      arterial_nif:checkout_connection(Pool, sync, 2),
    ok = arterial_nif:track_inflight(Pool, 0, ReqID1, self(), 1000000),
    ok = arterial_nif:track_inflight(Pool, 0, ReqID2, self(), 1000000),

    ok = arterial_nif:connection_down(Pool, 0),

    Got = lists:sort(flush_disconnected(Pool)),
    Want = lists:sort([
      {arterial_disconnected, Pool, ReqID1},
      {arterial_disconnected, Pool, ReqID2}
    ]),
    Want = Got,

    %% Both backlog slots are released and the connection is reusable.
    true = arterial_nif:connection_drained(Pool, 0),
    {ok, #{conn_id := 0, req_ids := [_ReqID3]}} =
      arterial_nif:checkout_connection(Pool, sync)
  after
    teardown(Pool)
  end.

%% A connection with no outstanding (tracked) in-flight requests is a
%% no-op: no message, and the connection's drained/available state is
%% unaffected.
connection_down_noop_when_idle_test() ->
  Pool = setup(arterial_conn_down_idle, 1, true, 0),
  try
    ok = arterial_nif:connection_down(Pool, 0),
    [] = flush_disconnected(Pool),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.

%% A synchronous checkout (never registered via track_inflight/5) is not
%% covered by connection_down/2 -- it's released (so the connection is
%% reusable) but no {arterial_disconnected,...} message is sent, since the
%% synchronous caller is expected to learn about the dead connection from
%% its own recv/2 call instead.
connection_down_skips_untracked_test() ->
  Pool = setup(arterial_conn_down_untracked, 1, true, 0),
  try
    {ok, #{conn_id := 0, req_ids := [_ReqID]}} =
      arterial_nif:checkout_connection(Pool, sync),

    ok = arterial_nif:connection_down(Pool, 0),

    [] = flush_disconnected(Pool),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.

%% checkout_async/3 also works with multiple real connections: queuing
%% only happens once ALL connections are busy.
checkout_async_multi_connection_test() ->
  Pool = setup(arterial_async_multi, 2, 1, true, 0, 4),
  try
    {ok, #{conn_id := ConnA, req_ids := [ReqA]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    {ok, #{conn_id := ConnB, req_ids := [ReqB]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    true = ConnA =/= ConnB,

    {queued, _W3} = arterial_nif:checkout_async(Pool, self(), 1000000),
    [] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, ConnA, [ReqA], <<>>),
    [{arterial_ready, Pool, ReqC, ConnA, dummy_socket, [ReqC]}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, ConnB, [ReqB], <<>>),
    ok = arterial_nif:checkin_connection(Pool, ConnA, [ReqC], <<>>)
  after
    teardown(Pool)
  end.

%%-----------------------------------------------------------------------------
%% Protocol tests (sync + async) using `test_protocol` and an in-process
%% test server.
%%-----------------------------------------------------------------------------

protocol_server_loop() ->
  protocol_server_loop(undefined).

%% `Observer`, if set, is sent every {cast_received, Term} the fake server
%% decodes -- used by protocol_cast_test/0 to confirm a send-and-forget
%% request actually reached the "wire" with no reply ever sent back
%% (mode (e); see arterial_client:cast/2).
protocol_server_loop(Observer) ->
  receive
    {request, From, Bin} ->
      %% decode the request; term is {ReqID, Payload}
      try
        case binary_to_term(Bin) of
          {ReqID, {sync, Reply}} ->
            From ! {reply, term_to_binary({ReqID, Reply})};
          {ReqID, {async, DelayMs, Reply}} ->
            spawn(fun() -> timer:sleep(DelayMs), From ! {reply, term_to_binary({ReqID, Reply})} end);
          {_ReqID, {cast, Term}} when Observer =/= undefined ->
            %% Send-and-forget: no reply, ever -- just prove the server saw it.
            Observer ! {cast_received, Term};
          _ ->
            ok
        end
      catch _:E:ST ->
        io:format("Server error ~p:\n  ~p\n", [E, ST])
      end,
      protocol_server_loop(Observer);
    {set_observer, Pid} ->
      protocol_server_loop(Pid);
    stop -> ok
  end.

protocol_sync_call_test() ->
  Server = spawn(fun protocol_server_loop/0),
  Pool = setup_protocol(protocol_sync_pool, Server, 1),
  try
    {ok, Resp} = arterial_client:call(Pool, {sync, hello}, 1000),
    ?assertEqual(hello, Resp)
  after
    Server ! stop,
    teardown(Pool)
  end.

%% Mode (e), send-and-forget: cast/2 hands the request to the transport
%% and returns immediately, with no reply ever expected or waited for.
%% The fake server below never sends anything back for a {cast, Term}
%% request; it just notifies the test process it was received, proving
%% the bytes reached the wire without cast/2 blocking on (or needing) a
%% response.
protocol_cast_test() ->
  Server = spawn(fun protocol_server_loop/0),
  Server ! {set_observer, self()},
  Pool =
    try
      setup_protocol(protocol_cast_pool, Server, 1)
    catch _:E:ST -> {error, E, ST}
    end,
  try
    case Pool of
      {error, R, STrace} -> erlang:raise(error, R, STrace);
      _                  -> ok
    end,

    ok = arterial_client:cast(Pool, {cast, hello}),
    receive
      {cast_received, hello} -> ok
    after 200 -> ?assert(false, cast_never_reached_server)
    end,

    %% The connection must be immediately reusable -- cast/2 doesn't hold
    %% the backlog slot, unlike call/3's hold-until-reply.
    {ok, again} = arterial_client:call(Pool, {sync, again}, 1000)
  after
    Server ! stop,
    teardown(Pool)
  end.

protocol_async_checkout_test() ->
  Server = spawn(fun protocol_server_loop/0),
  Pool = setup_protocol(protocol_async_pool, Server, 1),
  try
    {ok, #{conn_id := ConnID, req_ids := [ReqID]}} =
      arterial_nif:checkout_connection(Pool, async),

    %% Track inflight and simulate sending the request to the server
    ok = arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), 1000),
    {ok, Data} = test_protocol:encode_request(ReqID, {async, 10, pong}, 1000),
    test_protocol:send({test_server, Server}, Data),

    %% Queue a waiter (no connection available) so checkin will service it.
    {queued, _WaiterID} = arterial_nif:checkout_async(Pool, self(), 1000),

    %% After server replies it will deliver a {reply,...} to the caller of recv;
    %% emulate connection worker receiving it by forcing checkin (as other
    %% tests do) so the waiters get notified.
    receive
      {reply, Bin} -> ok = arterial_nif:checkin_connection(Pool, ConnID, [ReqID], Bin)
    after 200 -> ?assert(false, no_reply)
    end,

    [{arterial_ready, Pool, ReqID2, ConnID2, _, [ReqID2]}] = flush_ready(Pool),
    ?assert(is_integer(ReqID2)),
    ?assertEqual(ConnID, ConnID2)
  after
    Server ! stop,
    teardown(Pool)
  end.

%%-----------------------------------------------------------------------------
%% Process-death tests: a checked-out connection must not be permanently
%% stranded if the process that checked it out dies before checking it
%% back in. See ConnectionPool::OnProcessDown() / MonitorOwner() in
%% c_src/arterial.hpp -- driven here via the Erlang-level resource `down`
%% callback wired up in arterial.cpp's on_pool_down().
%%-----------------------------------------------------------------------------

%% Spawn a process that checks out a connection (sync or async, depending
%% on `CheckoutFun`), reports the checkout result back to us, then dies
%% on command -- without ever checking the connection back in itself.
spawn_checkout_then_die(CheckoutFun) ->
  Parent = self(),
  Pid = spawn(fun() ->
    Result = CheckoutFun(),
    Parent ! {self(), checked_out, Result},
    receive die -> ok end
  end),
  Result = receive {Pid, checked_out, R} -> R after 1000 -> error(no_checkout) end,
  Mon = monitor(process, Pid),
  Pid ! die,
  receive {'DOWN', Mon, process, Pid, _} -> ok after 1000 -> error(no_down) end,
  Result.

%% A connection checked out synchronously (checkout_connection/2) by a
%% process that then dies without checking in must become available
%% again, with its backlog slot released, exactly as if checkin_connection/4
%% had been called for it.
sync_checkout_released_on_death_test() ->
  Pool = setup(arterial_sync_death, 1, 1, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [_ReqID]}} =
      spawn_checkout_then_die(fun() -> arterial_nif:checkout_connection(Pool, sync) end),

    %% Give the resource `down` callback a moment to run (it fires from
    %% a different scheduler thread than this test process).
    timer:sleep(50),

    %% The connection must be checked-out-able again: if its backlog slot
    %% (capacity 1) had leaked, this would fail with {error, no_connection}.
    {ok, #{conn_id := 0, req_ids := [ReqID2]}} =
      arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>)
  after
    teardown(Pool)
  end.

%% Same as above, but for the asynchronous immediate-success path
%% (checkout_async/3 returning {ok, _} directly, no queuing involved).
async_checkout_released_on_death_test() ->
  Pool = setup(arterial_async_death, 1, 1, true, 0, 0),
  try
    {ok, #{conn_id := 0, req_ids := [_ReqID]}} =
      spawn_checkout_then_die(fun() ->
        Self = self(),
        arterial_nif:checkout_async(Pool, Self, 1000000)
      end),

    timer:sleep(50),

    {ok, #{conn_id := 0, req_ids := [ReqID2]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>)
  after
    teardown(Pool)
  end.

%% A *queued* waiter (checkout_async/3 returning {queued, _}) that dies
%% before ever being serviced must not be handed a connection later, and
%% must not strand the capacity that frees up for it.
queued_waiter_death_does_not_strand_capacity_test() ->
  Pool = setup(arterial_queued_death, 1, 1, true, 0, 4),
  try
    {ok, #{conn_id := 0, req_ids := [ReqID1]}} =
      arterial_nif:checkout_async(Pool, self(), 1000000),

    Parent = self(),
    WaiterPid = spawn(fun() ->
      {queued, WaiterID} = arterial_nif:checkout_async(Pool, self(), 1000000),
      Parent ! {self(), queued, WaiterID},
      receive die -> ok end
    end),
    _WaiterID = receive {WaiterPid, queued, W} -> W after 1000 -> error(no_queue) end,

    Mon = monitor(process, WaiterPid),
    WaiterPid ! die,
    receive {'DOWN', Mon, process, WaiterPid, _} -> ok after 1000 -> error(no_down) end,
    timer:sleep(50),

    %% Freeing the original connection drains the wait-list and services
    %% the queued entry (its waiter_id is still in m_waiting -- only the
    %% waiter process itself died, the queue entry is untouched), sending
    %% {arterial_ready,...} to the now-dead pid. That send simply goes
    %% nowhere (and the dead pid is monitored, so its reservation is
    %% released again right after); what this test cares about is that
    %% the connection doesn't end up permanently stuck either way.
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID1], <<>>),
    flush_ready(Pool),
    timer:sleep(50),

    %% The connection must still be obtainable: if checking in on behalf
    %% of a dead waiter had stranded it, this would fail.
    {ok, #{conn_id := 0, req_ids := [ReqID2]}} =
      arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>)
  after
    teardown(Pool)
  end.

%% Regression test: a request that's released by sweep_timeouts/1 (its
%% TTL expired) must have its per-pid owner bookkeeping cleared too --
%% otherwise, if the same pid later dies, OnProcessDown() would try to
%% check the same (already-recycled) backlog slot back in a second time,
%% corrupting pool state instead of being a no-op.
death_after_sweep_timeout_does_not_double_checkin_test() ->
  Pool = setup(arterial_sweep_then_death, 1, 1, true, 0, 0),
  try
    Parent = self(),
    Pid = spawn(fun() ->
      {ok, #{conn_id := ConnID, req_ids := [ReqID]}} =
        arterial_nif:checkout_connection(Pool, async),
      %% track_inflight/5's Pid must be the checkout owner (self()) --
      %% forward the timeout to Parent ourselves instead of registering
      %% Parent directly, since Pid is about to die.
      ok = arterial_nif:track_inflight(Pool, ConnID, ReqID, self(), 0),
      Parent ! {self(), tracked},
      receive
        {arterial_timeout, _, _} = M -> Parent ! M
      after 1000 -> ok
      end,
      receive die -> ok end
    end),
    receive {Pid, tracked} -> ok after 1000 -> error(no_track) end,

    timer:sleep(1),
    {ok, 1} = arterial_nif:sweep_timeouts(Pool),
    [{arterial_timeout, arterial_sweep_then_death, _ReqID}] =
      receive {arterial_timeout, _, _} = M0 -> [M0] after 1000 -> [] end,

    %% The slot swept above is already free; a second, unrelated checkout
    %% claims it before Pid dies, so if death wrongly checked it in again
    %% it would corrupt *this* checkout's reservation, not just a free slot.
    {ok, #{conn_id := 0, req_ids := [ReqID2]}} =
      arterial_nif:checkout_connection(Pool, sync),

    Mon = monitor(process, Pid),
    Pid ! die,
    receive {'DOWN', Mon, process, Pid, _} -> ok after 1000 -> error(no_down) end,
    timer:sleep(50),

    %% The live checkout from above must be unaffected by Pid's death.
    ok = arterial_nif:checkin_connection(Pool, 0, [ReqID2], <<>>),
    [] = flush_ready(Pool),
    [] = flush_timeouts(Pool)
  after
    teardown(Pool)
  end.
