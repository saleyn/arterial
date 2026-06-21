-module(arterial_connection).

-moduledoc """
Per-connection `gen_server` worker: owns one pool connection slot's
socket lifecycle (connect, reconnect-with-backoff, disconnect) and drives
the paired `arterial_client` callback module's `init/1`/`setup/2`/
`terminate/2` callbacks around it.

Started once per connection slot by `arterial_pool`'s supervisor. Once
connected, the worker publishes the socket into the pool's NIF resource
via `arterial_nif:set_socket/3` + `arterial_nif:make_available/2` so that
`arterial_client:call/3` and the asynchronous checkout path
(`arterial_nif:checkout_async/3`) can use it; it is not itself involved
in individual requests.
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
  proto        :: arterial:protocol(),
  recon_state  :: undefined | reconnect_state(),
  conn_timeout :: non_neg_integer(),
  sock_opts    :: arterial:socket_options()
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
  bounce                :: undefined | #bounce_state{}
}).

-type state()           :: #state{}.

-doc "Opaque value passed through to the paired `c:arterial_client:init/1` callback.".
-type init_options()    :: term().

-doc "The `{Pool, ConnID}` pair identifying a connection slot within a pool.".
-type id()              :: {arterial_pool:name(), non_neg_integer()}.

-type opts()            :: arterial_client:options().

-doc """
One entry of the `addresses` option: either a plain address (using the
connection's shared `port`/`sock_opts`), or a map overriding either (or
both) for that entry alone -- e.g. several independent server instances
on localhost, each on its own port.

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
```
""".
-type address_entry()  :: arterial:inet_address() | #{
  address   := arterial:inet_address(),
  port      => arterial:inet_port(),
  sock_opts => arterial:socket_options()
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
    init_options := InitOptions,
    protocol     := Protocol,
    sockopts     := SockOpts,
    conn_timeout := ConnTimeout
  } = maps:merge(#{
    init_options => #{},
    protocol     => tcp,
    sockopts     => [],
    conn_timeout => 15000
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

  {ok, #state{
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
  ss = #srv_state{pfx=Pfx, port=DefaultPort, proto=Proto, sock_opts=DefaultOpts, conn_timeout=Timeout}
} = State) ->
  {Address, Port, Opts} = resolve_entry(Entry, DefaultPort, DefaultOpts),
  case inet:getaddrs(Address, inet) of
    {ok, IPs} ->
      IP = arterial_util:random_element(IPs),
      case arterial_socket:connect(Proto, IP, Port, Opts, Timeout) of
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
%% connection's shared `port`/`sock_opts`, or a map overriding either
%% (or both) per-entry -- e.g. multiple independent server instances on
%% localhost, each on its own port.
resolve_entry(#{address := Address} = Entry, DefaultPort, DefaultOpts) ->
  {Address, maps:get(port, Entry, DefaultPort), maps:get(sock_opts, Entry, DefaultOpts)};
resolve_entry(Address, DefaultPort, DefaultOpts) ->
  {Address, DefaultPort, DefaultOpts}.

needs_default_port(#{port := _}) -> false;
needs_default_port(_)            -> true.

client_init(#state{
  sock = Sock,
  ss   = #srv_state{pfx=Pfx, pool=Pool, conn_id=ConnID, client=Cli, init_opts=InitOpts}
} = State) ->
  try Cli:init(InitOpts) of
    {ok, CState0} ->
      case Cli:setup(Sock, CState0) of
        {ok, CState} ->
          true = arterial_nif:set_socket(Pool, ConnID, Sock),
          true = arterial_nif:make_available(Pool, ConnID),
          {noreply, State#state{is = CState, ss = reset_backoff(State#state.ss)}};
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
      {noreply, State1} = disconnect(bounce, State#state{bounce = undefined}),
      % disconnect/2 always schedules a backoff-delayed reconnect via
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

disconnect(Reason, #state{ss = #srv_state{pfx=Pfx, pool=Pool, conn_id=ConnID,
                                           client=Cli}, is=ImplState, sock=Sock} = State) ->
  case Sock of
    undefined -> ok;
    _         ->
      arterial_nif:make_unavailable(Pool, ConnID),
      arterial_socket:close(Sock)
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
