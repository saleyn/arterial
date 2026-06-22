-module(arterial_conn_owner).

-moduledoc """
Per-connection `gen_server`: the ONLY process that ever does raw socket
I/O for one pool connection slot. One instance per `{Pool, ConnID}`,
started by `arterial_pool`'s supervisor alongside the matching
`arterial_connection` worker (which owns connect/reconnect/backoff
lifecycle, and hands the live socket to this process via `set_socket/4`
once connected).

## Why this exists

`arterial_nif`'s checkout/checkin only reserves backlog *capacity* (a
plain atomic counter per connection, see `c_src/connection.hpp`) -- it has
no visibility into individual requests, wire-level ids, demuxing, or
timeouts. Before this module existed, the *caller's own process* did raw
socket I/O directly once it held a reservation. That was safe at
`backlog => 1` (only one process ever held a connection's only
reservation), but broke at `backlog > 1`: multiple different processes
could legitimately hold concurrent reservations on the same connection
(the whole point of multiplexing), and then each independently called
`recv` on the *same shared socket* with no coordination -- two concurrent
`recv`s race for whichever bytes arrive next, and the "loser" hangs
forever.

This module is the fix, modeled on shackle's own architecture (one
process per connection, owning the socket, demuxing replies into the
right caller's mailbox): every `send`/`recv`/decode for a connection goes
through its owner, serialized by the owner's own mailbox. Wire-level
request ids are minted here (a plain per-connection counter) rather than
by the NIF, since this is the only thing that ever needs them.

## Non-blocking I/O, dispatched by transport

The owner must never block waiting for a reply: with `backlog > 1`, it
has to keep accepting new `send_recv` calls (and sending them) while
still waiting on replies to earlier ones, so a blocking `recv` inside
`handle_call` would stall every other in-flight request on the same
connection. `c:arterial_protocol:recv/2`'s callback contract is
blocking-only, so it's never called from the multiplexed path at all --
the owner instead reads directly off the socket, using whichever
non-blocking primitive its transport (`tcp`/`udp`/`ssl`, the
`client_opts` `protocol` key passed to `set_socket/4`) actually provides:

- `tcp`/`udp` (an OTP `socket` module handle): `socket:recv(Sock, 0,
  nowait)`, the same pattern already validated by
  `test/arterial_async_driver.erl` -- either returns already-buffered
  data immediately, or `{select, SelectInfo}`, in which case a
  `{'$socket', Sock, select, Ref}` message arrives in `handle_info` once
  data is ready.
- `ssl` (an `ssl:sslsocket()`): has no `nowait`/select equivalent at all
  (`ssl:recv/2,3` is always blocking) -- instead the owner puts the
  socket in `{active, N}` mode (`ssl:setopts/2`) once connected, so data
  arrives as `{ssl, Sock, Data}` messages directly; `N` re-arms on each
  `{ssl_passive, Sock}` (bounds how many messages can queue in the
  owner's own mailbox before `ssl` pauses delivery, rather than letting
  an unexpectedly fast peer grow it unboundedly).

## Demux modes

Whether a connection's pool was started with `fifo => true` or `false`
(see `arterial_pool:options/0`) selects which of two pending-request
structures this owner keeps -- not two different architectures, just two
data structures for the same loop shape (send -> track pending -> on
data, decode+demux -> reply):

- `fifo => true`: a `queue:queue/0` of pending entries in send order.
  `c:arterial_protocol:decode_reply/2` is always called with the *head*
  entry's id (the protocol has no wire-level id of its own; replies are
  assumed to arrive in the same order requests were sent).
- `fifo => false`: a `map()` of `ReqID => Entry`. `decode_reply/2` is
  called with whichever id is due to be decoded next -- in practice this
  still decodes one at a time in send order today (the underlying decode
  loop doesn't yet support a single buffer chunk containing multiple
  distinct replies needing separately-targeted ids), but the map storage
  means a future out-of-order decode strategy wouldn't need a data
  structure change here, just a different choice of which id to pass to
  `decode_reply/2`.

## Timeouts and disconnect

Every pending entry gets its own `erlang:send_after`-armed deadline (no
NIF-side TTL registry anymore -- that responsibility moved here
entirely). On the socket reporting closed/error, or on `clear_socket/2`
(called by `arterial_connection:disconnect/2`), every still-pending
caller is replied to immediately with `{error, disconnected}` --
covering synchronous and asynchronous callers alike (an improvement over
the old NIF-side `connection_down/2`, which only ever covered requests
explicitly registered via `track_inflight/5`).

Each pending entry also carries an `erlang:monitor(process, CallerPid)`
(`CallerPid` taken straight from the `{send_recv, Ref, CallerPid, ...}`
message `send_recv/4` sends) so a caller
that dies mid-flight -- before a reply or timeout reaches it -- gets its
pending entry (and timer) dropped immediately rather than left to expire
naturally. This is a tidiness improvement, not a correctness one: the
NIF-side backlog reservation that same dead pid holds is released
independently by `owner_table.hpp`'s own `enif_monitor_process`/
`OnProcessDown` (the erlsem-style pattern, see
`c_src/owner_table.hpp`) regardless of whether this module's monitor
ever fires.
""".

-behaviour(gen_server).

-export([start_link/4]).
-export([set_socket/4, clear_socket/2]).
-export([send_recv/4, send/3]).
-export([reg_name/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-record(pending, {
  caller    :: pid(),
  ref       :: reference(),
  timer_ref :: reference(),
  mon_ref   :: reference()
}).

-record(state, {
  pool        :: arterial_pool:name(),
  conn_id     :: non_neg_integer(),
  protocol    :: module(),
  fifo        :: boolean(),
  transport   :: undefined | tcp | udp | ssl,
  sock        :: undefined | arterial:socket(),
  select_ref  :: undefined | reference(),
  active_n    :: pos_integer(),
  buf         :: binary(),
  next_id     :: non_neg_integer(),
  %% fifo => true:  queue:queue(non_neg_integer())   (pending req ids, send order)
  %% fifo => false: #{non_neg_integer() => #pending{}}
  fifo_q      :: queue:queue(non_neg_integer()),
  pending     :: #{non_neg_integer() => #pending{}}
}).

-define(ACTIVE_N, 16).

-doc "Registered name for `Pool`'s connection `ConnID` owner process.".
-spec reg_name(arterial_pool:name(), non_neg_integer()) -> atom().
reg_name(Pool, ConnID) ->
  list_to_atom(atom_to_list(?MODULE) ++ "_" ++ atom_to_list(Pool) ++ "_" ++ integer_to_list(ConnID)).

-doc """
Start the owner process for `Pool`'s connection `ConnID`, registered
locally under `reg_name(Pool, ConnID)`. `Protocol` is the pool's
`c:arterial_protocol` codec module; `Fifo` selects the demux mode (see
moduledoc).
""".
-spec start_link(arterial_pool:name(), non_neg_integer(), module(), boolean()) -> {ok, pid()}.
start_link(Pool, ConnID, Protocol, Fifo) ->
  gen_server:start_link({local, reg_name(Pool, ConnID)}, ?MODULE,
                         [Pool, ConnID, Protocol, Fifo], []).

-doc """
Publish the freshly (re)connected `Socket` to `Pool`'s connection `ConnID`
owner, and arm this connection's non-blocking read strategy for
`Transport` (`tcp`/`udp` -- a `socket` module handle, read via
`socket:recv(...,nowait)` -- or `ssl`, put into `{active, N}` mode).
Called by `arterial_connection:client_init/1` right after the socket
connects, before the connection is made available for checkout.
""".
-spec set_socket(arterial_pool:name(), non_neg_integer(), arterial:socket(),
                  tcp | udp | ssl) -> ok.
set_socket(Pool, ConnID, Socket, Transport) ->
  gen_server:call(reg_name(Pool, ConnID), {set_socket, Socket, Transport}).

-doc """
Tell `Pool`'s connection `ConnID` owner its socket is going away: fails
every pending request with `{error, disconnected}` and clears its state.
Called by `arterial_connection:disconnect/2`, after the connection has
been marked unavailable and drained, but before the socket is actually
closed.

Tolerates the owner being transiently unregistered (a `noproc`, e.g. it
crashed independently and `arterial_pool`'s supervisor hasn't restarted
it yet) as already-satisfied: a freshly (re)started owner has no socket
to clear in the first place.
""".
-spec clear_socket(arterial_pool:name(), non_neg_integer()) -> ok.
clear_socket(Pool, ConnID) ->
  try
    gen_server:call(reg_name(Pool, ConnID), clear_socket)
  catch
    exit:{noproc, _} -> ok
  end.

-doc """
Send `Request` on `Pool`'s connection `ConnID` and block for its decoded
reply (or `Timeout` milliseconds, whichever comes first).

Deliberately NOT a `gen_server:call/3`: this is the single hottest path
in the whole library, called once per `arterial_client:call/3`, and
`gen_server:call/3` pays for `$gen_call`/`gen_server:reply/2` message
wrapping on every single request -- overhead that has nothing to do with
this module's actual job (demuxing replies safely once `backlog > 1`).
Instead, mirrors `shackle`'s own hot-path shape (see
`shackle:call/3`/`shackle_server`): a bare `!` send carrying a fresh
`Ref` and `self()`, and a bare `receive` keyed on that `Ref` -- the owner
replies the same way (see `reply_one/3`), with no `gen_server`
call/reply wrapping involved at all. `clear_socket/2`, `set_socket/4`,
and `send/3` stay on `gen_server:call/2,3` since none of them are
hot-path (each fires at most once per (re)connect, or once per
fire-and-forget request, neither of which approaches `send_recv/4`'s
volume).

Also arms a `monitor(process, Owner)` for the duration of the call, so
the owner crashing mid-flight fails this caller promptly (typically
within however long the supervisor takes to restart it) instead of
silently sitting out the full `Timeout`. This was made configurable
(`monitor_owner_calls => false`) in an earlier version of this function,
to shave the `erlang:monitor/2`/`erlang:demonitor/2` overhead `eprof`
measured on a backlog=1 benchmark -- reverted: `false` made ANY
retry-loop caller dangerous, not just slower. Once `whereis/1` above
resolves to a (possibly about-to-crash) owner pid, an unmonitored
`send_recv` has no way to distinguish "the owner is alive and working on
it" from "the owner just died and nothing will ever reply" until
`Timeout + 5000` elapses -- so a caller that retries on failure (a normal
pattern, e.g. this module's own test suite's `wait_until_owner_usable/1`
needed reworking around exactly this once `false` was in play) could
turn a brief, expected owner-restart window into a multi-second stall
per attempt instead of a fast, cheap failure. The monitor's setup/
teardown cost is real but small next to that risk; keep it unconditional.
`clear_socket/2`, `set_socket/4`, and `send/3` stay on `gen_server:call/2,3`
since none of them are hot-path (each fires at most once per (re)connect,
or once per fire-and-forget request, neither of which approaches
`send_recv/4`'s volume).
""".
-spec send_recv(arterial_pool:name(), non_neg_integer(), term(), timeout()) ->
  {ok, term()} | {error, term()}.
send_recv(Pool, ConnID, Request, Timeout) ->
  case whereis(reg_name(Pool, ConnID)) of
    undefined ->
      {error, disconnected};
    Owner ->
      Ref = make_ref(),
      MonRef = erlang:monitor(process, Owner),
      Owner ! {send_recv, Ref, self(), Request, Timeout},
      Reply = receive
        {Ref, Result} -> Result;
        {'DOWN', MonRef, process, Owner, Reason} -> {error, Reason}
      after Timeout + 5000 ->
        {error, timeout}
      end,
      erlang:demonitor(MonRef, [flush]),
      Reply
  end.

-doc """
Send `Request` on `Pool`'s connection `ConnID` without waiting for (or
expecting) any reply -- mode (e), send-and-forget protocols. Still routed
through the owner (rather than the caller writing directly) so the write
itself is serialized against any other in-flight `send_recv`/`send` on
the same socket.
""".
-spec send(arterial_pool:name(), non_neg_integer(), term()) -> ok | {error, term()}.
send(Pool, ConnID, Request) ->
  gen_server:call(reg_name(Pool, ConnID), {send, Request}).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Pool, ConnID, Protocol, Fifo]) ->
  {ok, #state{
    pool = Pool, conn_id = ConnID, protocol = Protocol, fifo = Fifo,
    active_n = ?ACTIVE_N, buf = <<>>, next_id = 0,
    fifo_q = queue:new(), pending = #{}
  }}.

-doc false.
handle_call({set_socket, Socket, Transport}, _From, State) ->
  arm_transport(Transport, Socket),
  %% select_ref must be reset along with sock/buf: it names a
  %% socket:recv(...,nowait) select operation armed against the
  %% *previous* socket (see maybe_arm_recv/1) -- maybe_arm_recv/1 treats
  %% any non-undefined select_ref as "a recv is already in flight,
  %% nothing to do", so a stale ref surviving this swap would permanently
  %% stop it from ever arming a real recv against the new socket, even
  %% though the '$socket' select message it's waiting for can now never
  %% arrive (it named the old, now-discarded socket).
  {reply, ok, State#state{sock = Socket, transport = Transport, buf = <<>>, select_ref = undefined}};

handle_call(clear_socket, _From, State) ->
  State1 = fail_all_pending(disconnected, State),
  {reply, ok, State1#state{sock = undefined, transport = undefined, buf = <<>>, select_ref = undefined}};

handle_call({send, _Request}, _From, #state{sock = undefined} = State) ->
  {reply, {error, disconnected}, State};

handle_call({send, Request}, _From, #state{
  protocol = Proto, sock = Socket, next_id = ReqID
} = State) ->
  Reply = case Proto:encode_request(ReqID, Request, infinity) of
    {ok, Data} -> Proto:send(Socket, Data);
    {error, _} = Error -> Error
  end,
  {reply, Reply, State#state{next_id = ReqID + 1}}.

-doc false.
handle_cast(_Msg, State) ->
  {noreply, State}.

%% Hot path -- see send_recv/4's moduledoc for why this is a raw message
%% instead of a handle_call/3 clause.
handle_info({send_recv, Ref, CallerPid, _Request, _Timeout}, #state{sock = undefined} = State) ->
  CallerPid ! {Ref, {error, disconnected}},
  {noreply, State};

handle_info({send_recv, Ref, CallerPid, Request, Timeout}, #state{
  protocol = Proto, sock = Socket, next_id = ReqID
} = State) ->
  case Proto:encode_request(ReqID, Request, Timeout) of
    {ok, Data} ->
      case Proto:send(Socket, Data) of
        ok ->
          TimerRef = erlang:send_after(Timeout, self(), {timeout, ReqID}),
          MonRef = erlang:monitor(process, CallerPid),
          Pending = #pending{caller = CallerPid, ref = Ref, timer_ref = TimerRef, mon_ref = MonRef},
          State1 = track_pending(ReqID, Pending, State),
          State2 = pump_decode(State1#state{next_id = ReqID + 1}),
          {noreply, State2};
        {error, _} = Error ->
          CallerPid ! {Ref, Error},
          {noreply, State#state{next_id = ReqID + 1}}
      end;
    {error, _} = Error ->
      CallerPid ! {Ref, Error},
      {noreply, State}
  end;

%% tcp/udp ('socket' module): a previously armed socket:recv(...,nowait)
%% fired. Re-read (won't block, the select already confirmed readability)
%% and decode as much as is now available.
handle_info({'$socket', Socket, select, Ref}, #state{sock = Socket, select_ref = Ref} = State) ->
  case socket:recv(Socket, 0, nowait) of
    {ok, Data} ->
      {noreply, pump_decode(State#state{buf = <<(State#state.buf)/binary, Data/binary>>, select_ref = undefined})};
    {select, {select_info, recv, Ref1}} ->
      {noreply, State#state{select_ref = Ref1}};
    {error, _} = Error ->
      {noreply, fail_all_pending(Error, State#state{select_ref = undefined})}
  end;
handle_info({'$socket', _Socket, select, _Ref}, State) ->
  {noreply, State};
handle_info({'$socket', Socket, abort, {Ref, Reason}}, #state{sock = Socket, select_ref = Ref} = State) ->
  {noreply, fail_all_pending(Reason, State#state{select_ref = undefined})};
handle_info({'$socket', _Socket, abort, _}, State) ->
  {noreply, State};

%% ssl, {active, N} mode.
handle_info({ssl, Socket, Data}, #state{sock = Socket} = State) ->
  {noreply, pump_decode(State#state{buf = <<(State#state.buf)/binary, Data/binary>>})};
handle_info({ssl_passive, Socket}, #state{sock = Socket, active_n = N} = State) ->
  ssl:setopts(Socket, [{active, N}]),
  {noreply, State};
handle_info({ssl_closed, Socket}, #state{sock = Socket} = State) ->
  {noreply, fail_all_pending(disconnected, State)};
handle_info({ssl_error, Socket, Reason}, #state{sock = Socket} = State) ->
  {noreply, fail_all_pending(Reason, State)};

handle_info({timeout, ReqID}, #state{pool = Pool, conn_id = ConnID} = State) ->
  case take_pending(ReqID, State) of
    {undefined, State1} ->
      {noreply, State1};
    {#pending{caller = CallerPid, ref = Ref}, State1} ->
      CallerPid ! {Ref, {error, timeout}},
      arterial_observe:event([timeout], #{pool => Pool, conn_id => ConnID}),
      {noreply, State1}
  end;

%% The original checkout caller died while its request was still
%% in-flight (it never got the chance to run its own checkin/after
%% block) -- drop its pending entry now instead of carrying a dead
%% From/timer until the per-request deadline eventually fires.
handle_info({'DOWN', MonRef, process, _Pid, _Reason}, State) ->
  {noreply, drop_pending_by_mon(MonRef, State)};

handle_info(_Msg, State) ->
  {noreply, State}.

-doc false.
terminate(_Reason, State) ->
  fail_all_pending(owner_terminated, State),
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

%% tcp/udp arm their first socket:recv(...,nowait) eagerly (handled inline
%% in pump_decode/1, since it's only useful once there's a pending
%% request to decode against); ssl needs {active, N} armed once up front,
%% since data can arrive as a message at any time regardless of whether
%% anything is pending yet.
arm_transport(ssl, Socket) ->
  %% The socket was opened by arterial_connection (it calls
  %% arterial_socket:connect/6, which underneath is the process that
  %% calls ssl:connect/3) -- ssl ties {active,N}-mode message delivery to
  %% whichever process is the socket's "controlling process", which
  %% defaults to whoever connected it. Without this transfer, every
  %% {ssl, Socket, Data}/{ssl_passive, Socket}/{ssl_closed, Socket}
  %% message would land in arterial_connection's mailbox -- which never
  %% reads them -- instead of here, and every request would silently
  %% time out waiting for a reply that already arrived, just to the
  %% wrong process.
  ok = ssl:controlling_process(Socket, self()),
  ok = ssl:setopts(Socket, [{active, ?ACTIVE_N}]);
arm_transport(_Transport, _Socket) ->
  ok.

track_pending(ReqID, Pending, #state{fifo = true, fifo_q = Q, pending = Map} = State) ->
  State#state{fifo_q = queue:in(ReqID, Q), pending = Map#{ReqID => Pending}};
track_pending(ReqID, Pending, #state{fifo = false, pending = Map} = State) ->
  State#state{pending = Map#{ReqID => Pending}}.

take_pending(ReqID, #state{pending = Map} = State) ->
  case maps:take(ReqID, Map) of
    {Pending, Map1} ->
      cancel_timer(Pending#pending.timer_ref),
      cancel_mon(Pending#pending.mon_ref),
      {Pending, drop_from_fifo(ReqID, State#state{pending = Map1})};
    error ->
      {undefined, State}
  end.

%% Counterpart to take_pending/2 for the {'DOWN',...} path: same cleanup,
%% but found by monitor ref (the only handle handle_info/2 has) rather
%% than by ReqID, and no reply is sent -- the caller that would have
%% received it is already dead.
drop_pending_by_mon(MonRef, #state{pending = Map} = State) ->
  case maps:filter(fun(_ReqID, P) -> P#pending.mon_ref =:= MonRef end, Map) of
    Found when map_size(Found) =:= 0 ->
      State;
    Found ->
      maps:fold(fun(ReqID, Pending, StateAcc) ->
        cancel_timer(Pending#pending.timer_ref),
        drop_from_fifo(ReqID, StateAcc#state{pending = maps:remove(ReqID, StateAcc#state.pending)})
      end, State, Found)
  end.

drop_from_fifo(_ReqID, #state{fifo = false} = State) ->
  State;
drop_from_fifo(ReqID, #state{fifo = true, fifo_q = Q} = State) ->
  State#state{fifo_q = queue:filter(fun(Id) -> Id =/= ReqID end, Q)}.

cancel_timer(TimerRef) -> erlang:cancel_timer(TimerRef), ok.

cancel_mon(MonRef) -> erlang:demonitor(MonRef, [flush]), ok.

%% Decode as many complete replies as the current buffer holds, replying
%% to each pending caller in turn. For tcp/udp, also (re-)arms the next
%% socket:recv(...,nowait) once nothing more can be decoded from what's
%% buffered and at least one request is still pending -- ssl needs no
%% equivalent re-arm here, {active, N} already delivers the next chunk as
%% a message on its own.
pump_decode(#state{pending = Pending} = State) when map_size(Pending) =:= 0 ->
  State;
pump_decode(State) ->
  case decode_one(State#state.buf, State) of
    {ok, ReqID, Result, Rest} ->
      pump_decode(reply_one(ReqID, {ok, Result}, State#state{buf = Rest}));
    {error, ReqID, Reason, Rest} ->
      pump_decode(reply_one(ReqID, {error, Reason}, State#state{buf = Rest}));
    {more, Buf1} ->
      maybe_arm_recv(State#state{buf = Buf1})
  end.

maybe_arm_recv(#state{transport = ssl} = State) ->
  State; % {active, N} already delivers the next chunk on its own
maybe_arm_recv(#state{select_ref = Ref} = State) when Ref =/= undefined ->
  State; % a socket:recv(...,nowait) is already armed
maybe_arm_recv(#state{sock = Socket} = State) ->
  case socket:recv(Socket, 0, nowait) of
    {ok, Data} ->
      pump_decode(State#state{buf = <<(State#state.buf)/binary, Data/binary>>});
    {select, {select_info, recv, Ref}} ->
      State#state{select_ref = Ref};
    {error, _} = Error ->
      fail_all_pending(Error, State)
  end.

decode_one(Buf, #state{protocol = Proto, fifo = true, fifo_q = Q}) ->
  case queue:peek(Q) of
    {value, ReqID} ->
      case Proto:decode_reply(ReqID, Buf) of
        {ok, Result, Rest} -> {ok, ReqID, Result, Rest};
        {more, Rest}       -> {more, Rest};
        {error, Reason}    -> {error, ReqID, Reason, Buf}
      end;
    empty ->
      {more, Buf}
  end;
decode_one(Buf, #state{protocol = Proto, fifo = false, pending = Map}) ->
  case maps:keys(Map) of
    [ReqID | _] ->
      case Proto:decode_reply(ReqID, Buf) of
        {ok, Result, Rest} -> {ok, ReqID, Result, Rest};
        {more, Rest}       -> {more, Rest};
        {error, Reason}    -> {error, ReqID, Reason, Buf}
      end;
    [] ->
      {more, Buf}
  end.

reply_one(ReqID, Result, State) ->
  case take_pending(ReqID, State) of
    {undefined, State1} -> State1;
    {#pending{caller = CallerPid, ref = Ref}, State1} ->
      CallerPid ! {Ref, Result},
      State1
  end.

fail_all_pending(Reason, #state{pending = Map} = State) ->
  maps:foreach(fun(_ReqID, #pending{caller = CallerPid, ref = Ref, timer_ref = TimerRef}) ->
    cancel_timer(TimerRef),
    CallerPid ! {Ref, {error, Reason}}
  end, Map),
  State#state{pending = #{}, fifo_q = queue:new()}.
