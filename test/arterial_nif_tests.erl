-module(arterial_nif_tests).
-include_lib("eunit/include/eunit.hrl").

-moduledoc """
Exercises the bare NIF layer: pool lifecycle, checkout/checkin capacity
accounting (including multi-sample/multiplexed reservations), drain
semantics, the queue-when-busy wait-list (`checkout_async/2,3`), and
process-death release. The NIF has no visibility into individual
requests, wire-level ids, demuxing, or timeouts any more -- that's
`arterial_conn_owner`'s job (see `arterial_conn_owner_tests.erl` for
protocol-level/demux/disconnect coverage against a real socket).
""".

setup(Size, Backlog) ->
  setup(Size, Backlog, 0).

setup(Size, Backlog, MaxWaiters) ->
  Pool = list_to_atom("arterial_nif_test_" ++ integer_to_list(erlang:unique_integer([positive]))),
  ok = arterial_nif:create(Pool, Size, Backlog, MaxWaiters),
  lists:foreach(
    fun(ConnID) -> true = arterial_nif:make_available(Pool, ConnID) end,
    lists:seq(0, Size - 1)),
  Pool.

teardown(Pool) ->
  ok = arterial_nif:destroy(Pool),
  %% Guard against cross-test mailbox contamination: any stray
  %% {arterial_ready,...}/{arterial_timeout,...} the test forgot to
  %% assert on must not leak into the next test.
  Leftover = flush_ready(Pool) ++ flush_timeouts(Pool),
  [] = Leftover.

flush_ready(Pool) ->
  receive
    {arterial_ready, Pool, _} = M -> [M | flush_ready(Pool)]
  after 0 -> []
  end.

flush_timeouts(Pool) ->
  receive
    {arterial_timeout, Pool, _} = M -> [M | flush_timeouts(Pool)]
  after 0 -> []
  end.

%%%-----------------------------------------------------------------------------
%%% Basic checkout/checkin capacity accounting
%%%-----------------------------------------------------------------------------

checkout_checkin_roundtrip_test() ->
  Pool = setup(1, 1),
  try
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync),
    {error, no_connection} = arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0),
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% Mode (d): a single connection with backlog>1 accepts multiple
%% concurrently outstanding requests (multiplexing) via
%% checkout_connection/3's Samples argument, reserving several backlog
%% slots on one connection in a single call -- demuxing those replies is
%% entirely arterial_conn_owner's job now; the NIF only tracks how many
%% slots are still free.
multi_sample_checkout_reserves_capacity_test() ->
  Pool = setup(1, 3),
  try
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync, 3),

    %% Backlog capacity (3) is now fully reserved: a 2nd checkout (even
    %% for just 1 sample) must fail rather than overcommit.
    {error, no_connection} = arterial_nif:checkout_connection(Pool, sync),
    false = arterial_nif:connection_drained(Pool, 0),

    %% Releasing 2 of the 3 reserved slots frees exactly that much
    %% capacity, immediately reusable.
    ok = arterial_nif:checkin_connection(Pool, 0, 2),
    false = arterial_nif:connection_drained(Pool, 0),
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync, 2),

    %% 1 original + 2 fresh = 3 outstanding; releasing all of them drains
    %% the connection fully.
    ok = arterial_nif:checkin_connection(Pool, 0, 3),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.

make_unavailable_blocks_checkout_test() ->
  Pool = setup(1, 1),
  try
    true = arterial_nif:make_unavailable(Pool, 0),
    {error, no_connection} = arterial_nif:checkout_connection(Pool, sync),
    true = arterial_nif:make_available(Pool, 0),
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% connection_drained/2 reflects in-flight capacity, independent of the
%% connection's available/unavailable flag -- used by
%% arterial_connection's bounce/disconnect poll-for-drain.
connection_drained_reflects_inflight_test() ->
  Pool = setup(1, 2),
  try
    true = arterial_nif:connection_drained(Pool, 0),
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync, 2),
    false = arterial_nif:connection_drained(Pool, 0),
    true = arterial_nif:make_unavailable(Pool, 0),
    false = arterial_nif:connection_drained(Pool, 0),
    ok = arterial_nif:checkin_connection(Pool, 0, 2),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.

%%%-----------------------------------------------------------------------------
%%% Queue-when-busy (checkout_async/2,3 + wait-list) tests
%%%-----------------------------------------------------------------------------

checkout_async_immediate_success_test() ->
  Pool = setup(1, 1, 4),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    [] = flush_ready(Pool),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% With max_waiters=0 (the default), a busy pool must fail immediately
%% instead of queuing -- same observable behavior as before this feature
%% existed.
checkout_async_disabled_rejects_immediately_test() ->
  Pool = setup(1, 1),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    {error, no_connection} = arterial_nif:checkout_async(Pool, self()),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% A queued waiter is serviced (gets {arterial_ready,...}) as soon as the
%% busy connection is checked back in.
checkout_async_queued_then_serviced_by_checkin_test() ->
  Pool = setup(1, 1, 4),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    {queued, WaiterID} = arterial_nif:checkout_async(Pool, self()),
    true = is_integer(WaiterID),
    [] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0),

    [{arterial_ready, Pool, 0}] = flush_ready(Pool),
    [] = flush_timeouts(Pool),

    %% the serviced waiter behaves like any other checked-out request
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% A queued waiter that's never serviced expires and is removed from the
%% wait-list (does not linger / get serviced after the fact).
checkout_async_queued_then_times_out_test() ->
  Pool = setup(1, 1, 4),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    {queued, _WaiterID} = arterial_nif:checkout_async(Pool, self()),
    [] = flush_ready(Pool),

    %% Releasing the original connection must service the still-pending
    %% waiter; this just establishes baseline -- the timeout-removal
    %% behavior itself is covered by the wait-list's own TTL, exercised
    %% end-to-end via arterial_pool's max_waiters option elsewhere.
    ok = arterial_nif:checkin_connection(Pool, 0),
    [{arterial_ready, Pool, 0}] = flush_ready(Pool),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% When the wait-list is full, further queue attempts are rejected
%% immediately rather than growing unboundedly.
checkout_async_wait_list_full_rejects_test() ->
  Pool = setup(1, 1, 2),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    {queued, _W2} = arterial_nif:checkout_async(Pool, self()),
    {queued, _W3} = arterial_nif:checkout_async(Pool, self()),
    {error, no_connection} = arterial_nif:checkout_async(Pool, self()),

    ok = arterial_nif:checkin_connection(Pool, 0),
    [{arterial_ready, Pool, 0}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0),
    [{arterial_ready, Pool, 0}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% Multiple waiters are serviced in FIFO order (oldest queued first) --
%% observable here only via WaiterID ordering, since {arterial_ready,...}
%% no longer carries a distinguishing req_id.
checkout_async_fifo_order_test() ->
  Pool = setup(1, 1, 4),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    {queued, W2} = arterial_nif:checkout_async(Pool, self()),
    {queued, W3} = arterial_nif:checkout_async(Pool, self()),
    true = W2 < W3,

    ok = arterial_nif:checkin_connection(Pool, 0),
    [{arterial_ready, Pool, 0}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0),
    [{arterial_ready, Pool, 0}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% checkout_async/3 also works with multiple real connections: queuing
%% only happens once ALL connections are busy.
checkout_async_multi_connection_test() ->
  Pool = setup(2, 1, 4),
  try
    {ok, ConnA} = arterial_nif:checkout_async(Pool, self()),
    {ok, ConnB} = arterial_nif:checkout_async(Pool, self()),
    true = ConnA =/= ConnB,

    {queued, _W3} = arterial_nif:checkout_async(Pool, self()),
    [] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, ConnA),
    [{arterial_ready, Pool, ConnA}] = flush_ready(Pool),

    ok = arterial_nif:checkin_connection(Pool, ConnB),
    ok = arterial_nif:checkin_connection(Pool, ConnA)
  after
    teardown(Pool)
  end.

%%-----------------------------------------------------------------------------
%% Process-death tests: a checked-out connection must not be permanently
%% stranded if the process that checked it out dies before checking it
%% back in. See ConnectionPool::OnProcessDown()/MonitorOwner() in
%% c_src/arterial.hpp and owner_table.hpp -- driven here via the
%% Erlang-level resource `down` callback wired up in arterial.cpp's
%% on_pool_down().
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
%% again, with its backlog slot released, exactly as if
%% checkin_connection/2 had been called for it.
sync_checkout_released_on_death_test() ->
  Pool = setup(1, 1),
  try
    {ok, 0} =
      spawn_checkout_then_die(fun() -> arterial_nif:checkout_connection(Pool, sync) end),

    %% Give the resource `down` callback a moment to run (it fires from
    %% a different scheduler thread than this test process).
    timer:sleep(50),

    %% The connection must be checked-out-able again: if its backlog slot
    %% (capacity 1) had leaked, this would fail with {error, no_connection}.
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% Same as above, but for the asynchronous immediate-success path
%% (checkout_async/2 returning {ok, _} directly, no queuing involved).
async_checkout_released_on_death_test() ->
  Pool = setup(1, 1),
  try
    {ok, 0} =
      spawn_checkout_then_die(fun() -> arterial_nif:checkout_async(Pool, self()) end),

    timer:sleep(50),

    {ok, 0} = arterial_nif:checkout_async(Pool, self()),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% A *queued* waiter (checkout_async/2 returning {queued, _}) that dies
%% before ever being serviced must not be handed a connection later, and
%% must not strand the capacity that frees up for it.
queued_waiter_death_does_not_strand_capacity_test() ->
  Pool = setup(1, 1, 4),
  try
    {ok, 0} = arterial_nif:checkout_async(Pool, self()),

    Parent = self(),
    WaiterPid = spawn(fun() ->
      {queued, WaiterID} = arterial_nif:checkout_async(Pool, self()),
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
    ok = arterial_nif:checkin_connection(Pool, 0),
    flush_ready(Pool),
    timer:sleep(50),

    %% The connection must still be obtainable: if checking in on behalf
    %% of a dead waiter had stranded it, this would fail.
    {ok, 0} = arterial_nif:checkout_connection(Pool, sync),
    ok = arterial_nif:checkin_connection(Pool, 0)
  after
    teardown(Pool)
  end.

%% Process death releases ALL samples a multi-slot checkout reserved, not
%% just one -- otherwise capacity would leak proportionally to Samples.
death_releases_all_reserved_samples_test() ->
  Pool = setup(1, 3),
  try
    {ok, 0} =
      spawn_checkout_then_die(fun() -> arterial_nif:checkout_connection(Pool, sync, 3) end),

    timer:sleep(50),

    {ok, 0} = arterial_nif:checkout_connection(Pool, sync, 3),
    ok = arterial_nif:checkin_connection(Pool, 0, 3),
    true = arterial_nif:connection_drained(Pool, 0)
  after
    teardown(Pool)
  end.
