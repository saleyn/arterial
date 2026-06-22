-module(arterial_connection).

-moduledoc """
Per-connection `gen_server` worker: owns one pool connection slot's
socket lifecycle (connect, reconnect-with-backoff, disconnect) and drives
the paired `arterial_client` callback module's `init/1`/`setup/2`/
`terminate/2` callbacks around it.

Started once per connection slot by `arterial_pool`'s supervisor,
immediately after the matching `arterial_conn_owner` (see
`arterial_pool:init/1`). Once connected, this worker hands the live
socket to that owner via `arterial_conn_owner:set_socket/4`, then marks
the connection available for checkout via `arterial_nif:make_available/2`
-- it is not itself involved in individual requests, only connection
lifecycle (connect/reconnect/backoff/disconnect).

This process monitors its owner sibling (see `init/1`): `arterial_pool`'s
supervisor is `one_for_one`, so an owner crash/restart doesn't restart
this process too -- on the resulting `'DOWN'`, if a live socket is still
held, it's republished directly to the freshly restarted (socket-less)
owner, without going through a full reconnect.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-compile(inline).
-compile({inline_size, 128}).

-export([start_link/4]).
-export([bounce/2]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2,
          terminate/2]).

-ifdef(TEST).
%% Exposed only so arterial_connection_tests can exercise the
%% reconnect-backoff interval sequence directly and quickly, without
%% needing a real dead socket and wall-clock timing assertions.
-export([recon_state/1, backoff_timeout/1]).
-endif.

-define(BOUNCE_DRAIN_POLL_INTERVAL_MS, 100).
-define(DISCONNECT_DRAIN_MAX_WAIT_MS, 5000).
-define(AWAIT_OWNER_RETRIES, 20).
-define(AWAIT_OWNER_POLL_INTERVAL_MS, 10).

-record(recon_state, {
  cur    :: undefined | arterial:time(),
  policy :: reconnect_time()
}).

-record(srv_state, {
  pfx          :: binary(),
  client       :: arterial:client(),
  id           :: id(),
  pool         :: arterial_pool:name(),
  conn_id      :: non_neg_integer(),
  init_opts    :: init_options(),
  addresses    :: [address_entry(), ...], % non-empty
  port         :: arterial:inet_port(),
  proto        :: tcp | udp | ssl, % wire-level transport, NOT arterial:protocol() (the codec module)
  recon_state  :: undefined | reconnect_state(),
  conn_timeout :: non_neg_integer(),
  sock_opts    :: arterial:socket_options(),
  tls_opts     :: arterial:tls_options()
}).

-type impl_state()      :: any().

-record(bounce_state, {
  from        :: gen_server:from(),
  deadline    :: integer() % erlang:monotonic_time(millisecond)
}).

-record(state, {
  ss                    :: #srv_state{},
  is                    :: impl_state(),
  sock                  :: undefined | arterial:socket(),
  timer_ref             :: undefined | reference(),
  bounce                :: undefined | #bounce_state{},
  owner_mon             :: undefined | reference()
}).

-type state()           :: #state{}.

-doc "Opaque value passed through to the paired `c:arterial_client:init/1` callback.".
-type init_options()    :: term().

-doc "The `{Pool, ConnID}` pair identifying a connection slot within a pool.".
-type id()              :: {arterial_pool:name(), non_neg_integer()}.

-type opts()            :: arterial_client:options().

-doc """
One entry of the `addresses` option: either a plain address (using the
connection's shared `port`/`sock_opts`/`tls_options`), or a map
overriding any of those for that entry alone -- e.g. several independent
server instances on localhost, each on its own port.

## Examples

```
1> Entry1 = "db1.internal".
"db1.internal"
2> Entry2 = #{address => "127.0.0.1", port => 9001}.
#{address => "127.0.0.1",port => 9001}
3> Entry3 = #{address => "127.0.0.1", port => 9002,
4>            sock_opts => [{{socket, reuseaddr}, true}]}.
#{address => "127.0.0.1",port => 9002,
  sock_opts => [{{socket,reuseaddr},true}]}
5> Entry4 = #{address => "ssl.internal", port => 9443,
6>            tls_opts => [{verify, verify_peer}, {cacertfile, "/etc/ssl/ca.pem"}]}.
#{address => "ssl.internal",port => 9443,
  tls_opts => [{verify,verify_peer},{cacertfile,"/etc/ssl/ca.pem"}]}
```
""".
-type address_entry()  :: arterial:inet_address() | #{
  address   := arterial:inet_address(),
  port      => arterial:inet_port(),
  sock_opts => arterial:socket_options(),
  tls_opts  => arterial:tls_options()
}.

-doc """
The `reconnect_time` option: how long to wait, after the whole
`addresses` list has been tried and failed, before retrying from the
first address again.

* `Ms :: non_neg_integer()` - a fixed interval, e.g. `1000`.
* `{backoff, Min, Max}` - exponential backoff: the first retry waits
  `Min`, each subsequent one roughly doubles (jittered) up to `Max`,
  where it stays; reset back to `Min` after a successful reconnect (see
  `reset_backoff/1`). `Max` may be `infinity` to grow unbounded.

## Examples

```
1> Time1 = 1000.
1000
2> Time2 = {backoff, 500, 30000}.
{backoff,500,30000}
3> Time3 = {backoff, 500, infinity}.
{backoff,500,infinity}
```
""".
-type reconnect_time() ::
  non_neg_integer()
  | {backoff, Min::non_neg_integer(), Max::non_neg_integer() | infinity}.

-doc "Internal reconnect-backoff bookkeeping; opaque to callers.".
-type reconnect_state() :: #recon_state{}.

-export_type([
  id/0,
  init_options/0,
  address_entry/0,
  reconnect_time/0,
  reconnect_state/0
]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a connection worker for slot `ConnID` (0-based, matching the pool's
NIF-level connection index) of `Pool`. The worker connects (and
reconnects, with backoff) on its own; this call returns as soon as the
`gen_server` process itself has started, not once a socket is up.

`CliOpts` may give either a single `address` (or `ip`) or a list of
`addresses` (see `t:address_entry/0`); with a list, every reconnect
tries each entry in order (no delay between attempts) until one
connects, only backing off (see `reconnect_time_min`/`reconnect_time_max`)
once the whole list has been tried and failed -- useful for a fixed set
of known-good backup addresses to fail over across. Plain entries share
the connection's `port`/`sock_opts`; a map entry can override either (or
both) for that one address alone, e.g. to reach several independent
server instances on localhost, each on its own port.

## Examples

```
1> arterial_connection:start_link(my_pool, 0, my_client,
2>                                 #{address => "localhost", port => 9000}).
{ok,<0.142.0>}
3> arterial_connection:start_link(my_pool, 1, my_client,
4>                                 #{addresses => ["db1.internal", "db2.internal"],
5>                                   port => 9000}).
{ok,<0.143.0>}
6> arterial_connection:start_link(my_pool, 2, my_client,
7>                                 #{addresses => [
8>                                     #{address => "127.0.0.1", port => 9001},
9>                                     #{address => "127.0.0.1", port => 9002}
10>                                   ]}).
{ok,<0.144.0>}
```
""".
-spec start_link(arterial_pool:name(), non_neg_integer(), arterial:client(), opts()) ->
  {ok, pid()}.
start_link(Pool, ConnID, Client, CliOpts)
    when is_atom(Pool), is_integer(ConnID), ConnID >= 0, is_map(CliOpts) ->
  gen_server:start_link(?MODULE, [Pool, ConnID, Client, CliOpts], []).

-doc """
Bounce this connection: mark it unavailable for new checkouts, wait (up
to `DrainTimeoutMs`) for its backlog to drain of in-flight requests, then
disconnect and attempt one immediate reconnect, replying only once that
full cycle finishes (successfully or not).

Used by `arterial_bouncer` to recycle pool connections periodically (see
`arterial_pool`'s `bounce_interval_ms` option); not meant to be called
directly by callers of the library.

Returns `ok` once the connection is available again (reconnected) or has
entered the normal backoff/retry path after a failed reconnect attempt;
`{error, timeout}` if the backlog never drained within `DrainTimeoutMs`
(the connection is left marked unavailable and disconnected regardless,
since forcibly bouncing it is the whole point -- a stuck request
shouldn't be able to block recycling forever).

## Examples

```
1> arterial_connection:bounce(ConnPid, 30000).
ok
```
""".
-spec bounce(pid(), pos_integer()) -> ok | {error, timeout}.
bounce(Pid, DrainTimeoutMs) when is_integer(DrainTimeoutMs), DrainTimeoutMs > 0 ->
  gen_server:call(Pid, {bounce, DrainTimeoutMs}, DrainTimeoutMs + 5000).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
-spec init(list()) -> {ok, state(), {continue, reconnect}}.
init([Pool, ConnID, Client, CliOpts]) ->
  #{
    init_options   := InitOptions,
    protocol       := Protocol,
    socket_options := SockOpts,
    tls_options    := TlsOpts,
    conn_timeout   := ConnTimeout
  } = maps:merge(#{
    init_options   => #{},
    protocol       => tcp,
    socket_options => [],
    tls_options    => [],
    conn_timeout   => 15000
  }, CliOpts),
  Addresses  = addresses(CliOpts),
  Port       = maps:get(port, CliOpts, undefined),
  ReconState = recon_state(CliOpts),
  Pfx        = list_to_binary(io_lib:format("~w:~w:~w: ", [Pool, ConnID, Protocol])),

  %% `port` is only mandatory for entries that don't supply their own --
  %% a connection configured entirely with #{address := _, port := _}
  %% map entries never needs the top-level option at all.
  Port == undefined andalso lists:any(fun needs_default_port/1, Addresses)
    andalso error({missing_port_option, Client}),

  %% Belt-and-suspenders: arterial_pool_guard is the real fix for a
  %% crashed (not gracefully disconnected) previous instance of this
  %% connection slot leaving the NIF believing it's still available with
  %% a dead socket -- it reacts to that death directly, closing the
  %% window before this replacement process even starts. This call is
  %% just a cheap, idempotent backstop in case the guard hasn't caught up
  %% yet for some reason; a no-op on a normal first start.
  true = arterial_nif:make_unavailable(Pool, ConnID),

  %% arterial_pool's supervisor starts each slot's arterial_conn_owner
  %% before its matching arterial_connection (see arterial_pool:init/1),
  %% so the owner is always already registered by the time this runs.
  %% Monitored (rather than linked) so a supervisor-driven owner restart
  %% (one_for_one -- this process is NOT restarted alongside it) is
  %% observed here as a plain message, not a crash: see
  %% handle_info({'DOWN', OwnerMon, ...}, State) and publish_to_owner/2,
  %% which re-publish the socket this process already holds (if any) to
  %% the freshly restarted (socket-less) owner without going through a
  %% full reconnect.
  OwnerMon = erlang:monitor(process, arterial_conn_owner:reg_name(Pool, ConnID)),

  {ok, #state{
    owner_mon = OwnerMon,
    ss = #srv_state{
      pfx          = Pfx,
      client       = Client,
      pool         = Pool,
      id           = {Pool, ConnID},
      conn_id      = ConnID,
      init_opts    = InitOptions,
      addresses    = Addresses,
      port         = Port,
      proto        = Protocol,
      recon_state  = ReconState,
      sock_opts    = SockOpts,
      tls_opts     = TlsOpts,
      conn_timeout = ConnTimeout
  }}, {continue, reconnect}}.

-doc false.
handle_continue(reconnect, State) -> handle_info(reconnect, State).

-doc false.
handle_call({bounce, _}, _From, #state{bounce = #bounce_state{}} = State) ->
  % Already bouncing (e.g. a stale/duplicate request from the bouncer) --
  % don't start a second drain-poll cycle on top of the first.
  {reply, {error, already_bouncing}, State};

handle_call({bounce, DrainTimeoutMs}, From, #state{
  ss = #srv_state{pool = Pool, conn_id = ConnID}
} = State) ->
  arterial_nif:make_unavailable(Pool, ConnID),
  Deadline = erlang:monotonic_time(millisecond) + DrainTimeoutMs,
  self() ! bounce_check,
  {noreply, State#state{bounce = #bounce_state{from = From, deadline = Deadline}}};

handle_call(Msg, _From, #state{ss = #srv_state{pfx = Pfx}} = State) ->
  ?LOG_WARNING("~s got unexpected call: ~p", [Pfx, Msg]),
  {reply, {error, unexpected_call}, State}.

-doc false.
handle_cast(Msg, #state{ss = #srv_state{pfx = Pfx}} = State) ->
  ?LOG_WARNING("~s got unexpected cast: ~p", [Pfx, Msg]),
  {noreply, State}.

-doc false.
handle_info(reconnect, State) ->
  reconnect(State);

handle_info(bounce_check, #state{bounce = #bounce_state{}} = State) ->
  bounce_check(State);

%% This connection's owner sibling restarted (arterial_pool's supervisor
%% is one_for_one, so this process survives untouched) -- the freshly
%% started owner has no socket. arterial_pool's supervisor restarts it on
%% its own as soon as this 'DOWN' fires, but that restart isn't
%% synchronous with this message, so the registered name can briefly
%% resolve to nothing yet -- publish_to_owner/1 (a self-message retry
%% loop, never a blocking sleep inside this callback) bounds how long
%% this process waits for that restart to land before giving up on
%% republishing this cycle. Whatever State#state.sock currently holds
%% (undefined if not connected yet, the live socket otherwise) is exactly
%% what should be (re)published once the owner reappears -- no separate
%% "what were we trying to do" bookkeeping needed.
handle_info({'DOWN', OwnerMon, process, _Pid, _Reason}, #state{owner_mon = OwnerMon} = State) ->
  publish_to_owner(?AWAIT_OWNER_RETRIES, State#state{owner_mon = undefined});

handle_info({publish_to_owner, Retries}, State) ->
  publish_to_owner(Retries, State);

handle_info(Msg, #state{ss = #srv_state{pfx = Pfx}} = State) ->
  ?LOG_WARNING("~s got unexpected msg: ~p", [Pfx, Msg]),
  {noreply, State}.

-doc false.
terminate(Reason, State) ->
  disconnect(Reason, State),
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

%% Try every configured address in order, with no delay between attempts
%% (a single dead/unreachable address shouldn't cost a full backoff
%% interval) -- only once the whole list has been tried and failed does
%% the normal reconnect backoff (recon_timer/1) apply, after which the
%% next attempt restarts from the first address again.
reconnect(#state{ss = #srv_state{addresses = Addresses}} = State) ->
  try_addresses(Addresses, State).

try_addresses([], State) ->
  {noreply, recon_timer(State)};
try_addresses([Entry | Rest], #state{
  ss = #srv_state{pfx=Pfx, pool=Pool, conn_id=ConnID, port=DefaultPort, proto=Proto,
                  sock_opts=DefaultOpts, tls_opts=DefaultTlsOpts, conn_timeout=Timeout}
} = State) ->
  {Address, Port, Opts, TlsOpts} = resolve_entry(Entry, DefaultPort, DefaultOpts, DefaultTlsOpts),
  case inet:getaddrs(Address, inet) of
    {ok, IPs} ->
      IP = arterial_util:random_element(IPs),
      StartMeta = #{pool => Pool, conn_id => ConnID, address => IP, port => Port},
      case arterial_observe:span([connect], StartMeta, fun() ->
        Result = arterial_socket:connect(Proto, IP, Port, Opts, Timeout, TlsOpts),
        Outcome = case Result of {ok, _} -> ok; {error, R} -> R end,
        {Result, StartMeta#{result => Outcome}}
      end) of
        {ok, Sock} ->
          client_init(State#state{sock = Sock});
        {error, Reason} ->
          ?LOG_WARNING("~s cannot connect to ~s:~p: ~p", [Pfx, inet:ntoa(IP), Port, Reason]),
          try_addresses(Rest, State)
      end;
    {error, Reason} ->
      ?LOG_WARNING("~s failed to resolve host ~p: ~p", [Pfx, Address, Reason]),
      try_addresses(Rest, State)
  end.

%% A list entry is either a plain address (string/IP), using the
%% connection's shared `port`/`sock_opts`/`tls_opts`, or a map overriding
%% any of those per-entry -- e.g. multiple independent server instances
%% on localhost, each on its own port.
resolve_entry(#{address := Address} = Entry, DefaultPort, DefaultOpts, DefaultTlsOpts) ->
  {Address,
   maps:get(port, Entry, DefaultPort),
   maps:get(sock_opts, Entry, DefaultOpts),
   maps:get(tls_opts, Entry, DefaultTlsOpts)};
resolve_entry(Address, DefaultPort, DefaultOpts, DefaultTlsOpts) ->
  {Address, DefaultPort, DefaultOpts, DefaultTlsOpts}.

needs_default_port(#{port := _}) -> false;
needs_default_port(_)            -> true.

client_init(#state{
  sock = Sock,
  ss   = #srv_state{pfx=Pfx, client=Cli, init_opts=InitOpts}
} = State) ->
  try Cli:init(InitOpts) of
    {ok, CState0} ->
      case Cli:setup(Sock, CState0) of
        {ok, CState} ->
          %% Owner registration is guaranteed once arterial_pool's
          %% supervisor finishes its initial startup (it starts each
          %% slot's owner before its connection, see arterial_pool:init/1)
          %% -- publish_to_owner/2's retry loop only matters for the rare
          %% case where the owner independently crashed and is mid-restart
          %% at the exact moment this reconnect lands. Not awaited
          %% synchronously here: see publish_to_owner/2 for why.
          publish_to_owner(?AWAIT_OWNER_RETRIES,
            State#state{is = CState, ss = reset_backoff(State#state.ss)});
        {error, Reason, CState} ->
          ?LOG_WARNING("~s ~w:setup/2 error: ~p", [Pfx, Cli, Reason]),
          disconnect(Reason, State#state{is = CState})
      end;
    {error, Reason} ->
      ?LOG_WARNING("~s ~w:init/1 error: ~p", [Pfx, Cli, Reason]),
      disconnect(Reason, State)
  catch E:R:ST ->
    ?LOG_WARNING("~s ~w:init/1 crashed: ~p:~p\n  ~p", [Pfx, Cli, E, R, ST]),
    disconnect(R, State)
  end.

%% Poll for `State`'s connection (the one currently being bounced; see
%% handle_call({bounce, _}, ...)) to drain of in-flight requests. Once
%% drained (or the bounce's deadline passes), disconnect and make exactly
%% one immediate reconnect attempt -- bypassing the normal backoff timer,
%% since this is a deliberate recycle, not a failure -- then reply to the
%% bouncer and clear `State#state.bounce` regardless of whether that
%% attempt succeeded (a failed attempt falls back to the normal
%% reconnect/backoff loop on its own, same as any other disconnect).
bounce_check(#state{
  bounce = #bounce_state{from = From, deadline = Deadline},
  ss     = #srv_state{pool = Pool, conn_id = ConnID, pfx = Pfx}
} = State) ->
  Drained  = arterial_nif:connection_drained(Pool, ConnID),
  TimedOut = erlang:monotonic_time(millisecond) >= Deadline,
  case Drained orelse TimedOut of
    true ->
      (TimedOut andalso not Drained) andalso
        ?LOG_NOTICE("~s bounce: backlog did not drain before deadline, forcing it anyway", [Pfx]),
      %% MaxDrainWaitMs = 0: this loop already waited up to the caller's
      %% own DrainTimeoutMs above and deliberately decided to proceed
      %% regardless -- disconnect/3 must not silently re-impose its own,
      %% longer ?DISCONNECT_DRAIN_MAX_WAIT_MS wait on top of that.
      {noreply, State1} = disconnect(bounce, 0, State#state{bounce = undefined}),
      % disconnect/3 always schedules a backoff-delayed reconnect via
      % recon_timer/1 -- cancel it before making our own immediate
      % attempt below, otherwise a second, redundant reconnect would fire
      % later (harmless if it finds itself already connected, but wasteful,
      % and racy if it fires mid-handshake of this immediate attempt).
      cancel_timer(State1#state.timer_ref),
      {noreply, State2} = reconnect(State1#state{timer_ref = undefined}),
      Reply = case TimedOut andalso not Drained of
        true  -> {error, timeout};
        false -> ok
      end,
      gen_server:reply(From, Reply),
      {noreply, State2};
    false ->
      erlang:send_after(?BOUNCE_DRAIN_POLL_INTERVAL_MS, self(), bounce_check),
      {noreply, State}
  end.

%% Equivalent to disconnect/3 with MaxDrainWaitMs = ?DISCONNECT_DRAIN_MAX_WAIT_MS --
%% the right default for every caller except bounce_check/1 (see disconnect/3).
disconnect(Reason, State) ->
  disconnect(Reason, ?DISCONNECT_DRAIN_MAX_WAIT_MS, State).

%% `MaxDrainWaitMs` is a parameter (not always ?DISCONNECT_DRAIN_MAX_WAIT_MS)
%% specifically for bounce_check/1: it already ran its own, caller-supplied-
%% duration drain-poll loop (DrainTimeoutMs) before ever calling disconnect/2,
%% and deliberately proceeds anyway once that deadline passes without having
%% drained -- passing 0 here means this disconnect doesn't silently re-impose
%% a second, longer wait (?DISCONNECT_DRAIN_MAX_WAIT_MS) on top of a decision
%% the caller already made. Every other caller (a real connection error) has
%% done no such wait yet, so it gets the full default budget.
disconnect(Reason, MaxDrainWaitMs, #state{ss = #srv_state{pfx=Pfx, pool=Pool, conn_id=ConnID,
                                           client=Cli, proto=Proto}, is=ImplState, sock=Sock} = State) ->
  case Sock of
    undefined -> ok;
    _         ->
      arterial_nif:make_unavailable(Pool, ConnID),
      %% Wait for the connection to actually drain before telling the
      %% owner to clear its socket -- make_unavailable/2 above only stops
      %% NEW checkouts from selecting this connection; without waiting
      %% for in-flight reservations to release, the owner could reset its
      %% counter to 0 while a checkout still in flight from just before
      %% make_unavailable/2 took effect later tries to use it. Reuses the
      %% same drain-poll bounce_check/1 already does, not a new mechanism.
      drain(Pool, ConnID, MaxDrainWaitMs),
      %% Fails every still-pending caller on this connection with
      %% {error, disconnected} (covering sync and async callers alike,
      %% unlike the old NIF-side connection_down/2 which only covered
      %% requests explicitly registered via track_inflight/5) before the
      %% socket closes and ConnID becomes eligible for reconnect/reuse.
      ok = arterial_conn_owner:clear_socket(Pool, ConnID),
      arterial_observe:event([disconnect], #{pool => Pool, conn_id => ConnID, reason => Reason}),
      arterial_socket:close(Proto, Sock)
  end,
  case ImplState of
    undefined -> ok;
    _ ->
      try Cli:terminate(Reason, ImplState)
      catch E:R:ST ->
        ?LOG_WARNING("~s ~w:terminate/2 crashed: ~p:~p\n  ~p", [Pfx, Cli, E, R, ST])
      end
  end,
  {noreply, recon_timer(State#state{sock = undefined, is = undefined})}.

cancel_timer(undefined) -> ok;
cancel_timer(TimerRef)  -> erlang:cancel_timer(TimerRef), ok.

%% Block (this gen_server has nothing else to do mid-disconnect/terminate
%% anyway) until ConnID has zero in-flight requests, polling the same way
%% bounce_check/1 does asynchronously -- this caller can't defer the way
%% bounce_check/1 does (disconnect/3 must finish synchronously, including
%% when called from terminate/2), so it's a plain bounded sleep loop
%% instead. `RemainingMs` is the caller's own drain budget (see
%% disconnect/3 -- 0 for bounce_check/1, which already ran its own,
%% longer drain-poll loop and is calling this only to (not) wait any
%% further; ?DISCONNECT_DRAIN_MAX_WAIT_MS for every other caller). A
%% single in-flight request that never completes (e.g. a peer that
%% stopped responding entirely) must not hang a disconnect/shutdown
%% forever -- once this budget runs out, the owner's clear_socket/2
%% forces every still-pending caller to fail with {error, disconnected}
%% regardless.
drain(_Pool, _ConnID, RemainingMs) when RemainingMs =< 0 ->
  ok;
drain(Pool, ConnID, RemainingMs) ->
  case arterial_nif:connection_drained(Pool, ConnID) of
    true  -> ok;
    false ->
      timer:sleep(?BOUNCE_DRAIN_POLL_INTERVAL_MS),
      drain(Pool, ConnID, RemainingMs - ?BOUNCE_DRAIN_POLL_INTERVAL_MS)
  end.

%% Publish State#state.sock (or do nothing if undefined -- nothing to
%% publish yet) to Pool's connection ConnID's owner, then (re-)monitor
%% it. Used both right after a fresh connect (client_init/1) and after a
%% 'DOWN' for the previous owner instance (handle_info/2 above) -- in
%% both cases arterial_pool's supervisor either already has, or is about
%% to, start the owner this process needs to talk to, but that isn't
%% synchronous with the message that got us here. Rather than block this
%% gen_server in a sleep loop waiting for it, retry via a self-message
%% (handle_info({publish_to_owner, Retries}, State) above) so the mailbox
%% stays responsive (bounce requests, other monitors, ...) while waiting.
%% Gives up silently after ?AWAIT_OWNER_RETRIES polls (owner_mon stays
%% undefined, so this process simply isn't watching anyone -- the next
%% successful reconnect, or a future owner crash this process happens to
%% still be monitoring some other way, isn't possible once owner_mon is
%% undefined, so in practice this only matters if the owner is stuck down
%% far longer than ?AWAIT_OWNER_RETRIES * ?AWAIT_OWNER_POLL_INTERVAL_MS,
%% at which point arterial_pool's own restart-intensity limit is the
%% bigger problem).
publish_to_owner(0, State) ->
  {noreply, State};
publish_to_owner(Retries, #state{
  sock = Sock,
  ss = #srv_state{pool = Pool, conn_id = ConnID, proto = Proto}
} = State) ->
  case whereis(arterial_conn_owner:reg_name(Pool, ConnID)) of
    undefined ->
      erlang:send_after(?AWAIT_OWNER_POLL_INTERVAL_MS, self(), {publish_to_owner, Retries - 1}),
      {noreply, State};
    _Pid ->
      %% Only arm a fresh monitor if this process isn't already watching
      %% a (still valid) owner -- init/1's own monitor, or one armed by
      %% an earlier publish_to_owner/2 call this connection's lifetime,
      %% covers the common case where the owner never actually died;
      %% re-monitoring it here on every reconnect cycle would otherwise
      %% leak one extra monitor per cycle.
      State1 = case State#state.owner_mon of
        undefined ->
          State#state{owner_mon = erlang:monitor(process, arterial_conn_owner:reg_name(Pool, ConnID))};
        _ ->
          State
      end,
      case Sock of
        undefined ->
          ok;
        _ ->
          ok = arterial_conn_owner:set_socket(Pool, ConnID, Sock, Proto),
          true = arterial_nif:make_available(Pool, ConnID)
      end,
      {noreply, State1}
  end.

%% `addresses` (a list, tried in order on every reconnect) takes priority
%% if given; otherwise fall back to the single-address `address`/`ip`
%% options (wrapped into a one-element list) for backwards compatibility.
addresses(Opts) ->
  case maps:get(addresses, Opts, undefined) of
    undefined ->
      [case maps:get(address, Opts, undefined) of
        undefined -> maps:get(ip, Opts, "127.0.0.1");
        Value     -> Value
      end];
    [_ | _] = List ->
      List
  end.

recon_state(Options) ->
  case maps:get(reconnect, Options, true) of
    true  -> #recon_state{policy = reconnect_time(Options)};
    false -> undefined
  end.

%% `reconnect_time` takes priority if given; otherwise fall back to the
%% legacy `reconnect_time_min`/`reconnect_time_max` pair (folded into the
%% equivalent {backoff, Min, Max}) for backwards compatibility.
reconnect_time(Options) ->
  case maps:get(reconnect_time, Options, undefined) of
    undefined ->
      {backoff,
        maps:get(reconnect_time_min, Options, 500),
        maps:get(reconnect_time_max, Options, timer:minutes(5))};
    Policy ->
      Policy
  end.

reset_backoff(#srv_state{recon_state = undefined} = SS) -> SS;
reset_backoff(#srv_state{recon_state = RS} = SS) ->
  SS#srv_state{recon_state = RS#recon_state{cur = undefined}}.

recon_timer(#state{ss = #srv_state{recon_state = undefined}} = S) ->
  S#state{sock = undefined};
recon_timer(#state{ss = #srv_state{recon_state = RS} = SS} = S) ->
  {Interval, RS1} = backoff_timeout(RS),
  TimerRef = erlang:send_after(Interval, self(), reconnect),
  S#state{
    ss        = SS#srv_state{recon_state = RS1},
    sock      = undefined,
    timer_ref = TimerRef
  }.

%% Fixed interval: same wait every time, no growth.
backoff_timeout(#recon_state{policy = Ms} = S) when is_integer(Ms) ->
  {Ms, S};

%% Exponential backoff: first retry waits Min; each subsequent retry
%% roughly doubles the previous interval (+/-25% jitter, to avoid a
%% thundering herd of connections all retrying in lockstep), clamped to
%% Max once it gets there. cur=undefined (no retry attempted yet since
%% the last successful connect, see reset_backoff/1) restarts at Min.
backoff_timeout(#recon_state{cur = undefined, policy = {backoff, Min, _Max}} = S) ->
  {Min, S#recon_state{cur = Min}};
backoff_timeout(#recon_state{cur = I, policy = {backoff, _Min, Max}} = S) when Max /= infinity, I >= Max ->
  {Max, S#recon_state{cur = Max}};
backoff_timeout(#recon_state{cur = I, policy = {backoff, _Min, Max}} = S) ->
  Doubled = I * 2,
  JitterSpan = max(1, Doubled div 2), % +/-25% of Doubled
  Jittered = Doubled + (rand:uniform(JitterSpan + 1) - 1) - (JitterSpan div 2),
  Next = clamp(Jittered, 0, Max),
  {Next, S#recon_state{cur = Next}}.

clamp(V, Lo, infinity)         -> erlang:max(V, Lo);
clamp(V, Lo, Hi)               -> erlang:max(Lo, erlang:min(V, Hi)).
