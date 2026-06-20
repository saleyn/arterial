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
  %% for this pool (e.g. a {arterial_ready,...}/{arterial_timeout,...}
  %% the test forgot to assert on) must not leak into the next test.
  Leftover = flush_timeouts(Pool) ++ flush_ready(Pool),
  [] = Leftover.

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
        flush_timeouts(Pool)
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
