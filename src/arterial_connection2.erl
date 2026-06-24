-module(arterial_connection2).

-moduledoc """
Per-connection `gen_server` worker for `arterial_pool2`: owns connect/
reconnect-with-backoff via `arterial_nif_pool:connect/7` (which opens and
connects the socket *inside* the NIF -- see below), and -- unlike
`arterial_connection` (the original backend, where the *caller* does the
actual socket I/O) -- this worker is also the long-lived "owner" process
that every read/write-readiness notification for its slot targets: it
decodes newly-read bytes via the pool's `c:arterial_codec2` module and
dispatches each reply directly to its waiting caller's mailbox via the
pool's public correlation-id ETS table (see `arterial_pool2:corr_table/1`).
`arterial_client2:call/3`/`cast/2` write requests directly via
`arterial_nif_pool:send_and_release/3` in their *own* process, never
through this worker -- it is only ever in the read path.

## Only `tcp`

`arterial_nif_pool:connect/7` opens a plain IPv4 `SOCK_STREAM` socket
itself; there's no portable way to make a NIF do the same for `ssl` (no
single plain kernel fd to own) or `udp` (connectionless, doesn't fit this
backend's one-fd-per-connection model), so only `tcp` is supported here.

Earlier iterations of this backend connected via Erlang's `socket`
module and handed the resulting fd to the NIF after the fact
(`socket:getopt(Sock, otp, fd)` + `arterial_nif_pool:register_socket/4`,
still available for callers that need it). That fd then had two
resources believing they owned it -- the NIF and the `socket()` term's
own `esock` resource -- which erts flags at runtime ("stealing control of
fd=N") and which could, in the worst case, double-close a since-reused
fd on disconnect. `connect/7` has no such competing owner: the fd is
born inside the NIF and never touches Erlang's `socket` module at all.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/3]).
-export([bounce/2]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2,
          terminate/2]).

-define(BOUNCE_DRAIN_POLL_INTERVAL_MS, 100).

-record(recon_state, {
  cur    :: undefined | arterial:time(),
  policy :: arterial_connection:reconnect_time()
}).

-record(bounce_state, {
  from     :: gen_server:from(),
  deadline :: integer()
}).

-record(state, {
  pfx          :: binary(),
  pool         :: arterial_pool2:name(),
  conn_id      :: non_neg_integer(),
  codec        :: module(),
  addresses    :: [address_entry(), ...],
  port         :: arterial:inet_port() | undefined,
  nodelay      :: boolean(),
  conn_timeout :: non_neg_integer(),
  recon_state  :: undefined | #recon_state{},
  connected    :: boolean(),
  buffer       :: binary(),
  timer_ref    :: undefined | reference(),
  bounce       :: undefined | #bounce_state{}
}).

-doc """
One entry of the `addresses` option: either a plain address (using the
connection's shared `port`), or a map overriding `port` for that entry
alone.
""".
-type address_entry() :: arterial:inet_address() | #{
  address := arterial:inet_address(),
  port    => arterial:inet_port()
}.

-export_type([address_entry/0]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a connection worker for slot `ConnID` (0-based) of `Pool`. Connects
(and reconnects, with backoff) on its own; returns as soon as the
`gen_server` itself has started, not once a socket is up.

`Opts` accepts `address`/`addresses`/`ip`/`port`/`nodelay`/`reconnect`/
`reconnect_time` (and the legacy `reconnect_time_min`/`reconnect_time_max`
pair) -- see the moduledoc for why there's no `protocol`/`socket_options`/
`tls_options` here, unlike `t:arterial_client:options/0`.
""".
-spec start_link(arterial_pool2:name(), non_neg_integer(), map()) -> {ok, pid()}.
start_link(Pool, ConnID, Opts)
    when is_atom(Pool), is_integer(ConnID), ConnID >= 0, is_map(Opts) ->
  gen_server:start_link(?MODULE, [Pool, ConnID, Opts], []).

-doc """
Bounce this connection: mark it unavailable for new sends, wait (up to
`DrainTimeoutMs`) for its in-flight correlation entries to drain, then
disconnect and attempt one immediate reconnect. Used by
`arterial_bouncer2`; see `arterial_connection:bounce/2`'s moduledoc for
the rationale (identical to this backend).
""".
-spec bounce(pid(), pos_integer()) -> ok | {error, timeout}.
bounce(Pid, DrainTimeoutMs) when is_integer(DrainTimeoutMs), DrainTimeoutMs > 0 ->
  gen_server:call(Pid, {bounce, DrainTimeoutMs}, DrainTimeoutMs + 5000).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Pool, ConnID, Opts]) ->
  #{
    nodelay      := Nodelay,
    conn_timeout := ConnTimeout
  } = maps:merge(#{nodelay => true, conn_timeout => 15000}, Opts),
  Codec      = arterial_pool2:codec(Pool),
  Addresses  = addresses(Opts),
  Port       = maps:get(port, Opts, undefined),
  ReconState = recon_state(Opts),
  Pfx        = list_to_binary(io_lib:format("~w:~w: ", [Pool, ConnID])),

  Port == undefined andalso lists:any(fun needs_default_port/1, Addresses)
    andalso error({missing_port_option, Pool}),

  {ok, #state{
    pfx          = Pfx,
    pool         = Pool,
    conn_id      = ConnID,
    codec        = Codec,
    addresses    = Addresses,
    port         = Port,
    nodelay      = Nodelay,
    conn_timeout = ConnTimeout,
    recon_state  = ReconState,
    connected    = false,
    buffer       = <<>>
  }, {continue, reconnect}}.

-doc false.
handle_continue(reconnect, State) -> handle_info(reconnect, State).

-doc false.
handle_call({bounce, _}, _From, #state{bounce = #bounce_state{}} = State) ->
  {reply, {error, already_bouncing}, State};

handle_call({bounce, DrainTimeoutMs}, From, #state{pool = Pool, conn_id = ConnID} = State) ->
  arterial_pool2:set_unavailable(Pool, ConnID),
  Deadline = erlang:monotonic_time(millisecond) + DrainTimeoutMs,
  self() ! bounce_check,
  {noreply, State#state{bounce = #bounce_state{from = From, deadline = Deadline}}};

handle_call(Msg, _From, #state{pfx = Pfx} = State) ->
  ?LOG_WARNING("~s got unexpected call: ~p", [Pfx, Msg]),
  {reply, {error, unexpected_call}, State}.

-doc false.
handle_cast(Msg, #state{pfx = Pfx} = State) ->
  ?LOG_WARNING("~s got unexpected cast: ~p", [Pfx, Msg]),
  {noreply, State}.

-doc false.
handle_info(reconnect, State) ->
  reconnect(State);

handle_info(bounce_check, #state{bounce = #bounce_state{}} = State) ->
  bounce_check(State);

handle_info({arterial_pool_event, ConnID, 0, read}, #state{conn_id = ConnID} = State) ->
  handle_read_event(State);

handle_info({arterial_pool_event, ConnID, 0, write}, #state{conn_id = ConnID, pool = Pool} = State) ->
  _ = arterial_nif_pool:handle_writable(arterial_pool2:pool_ref(Pool), ConnID, 0),
  {noreply, State};

handle_info({arterial_pool_event, ConnID, 0, closed}, #state{conn_id = ConnID} = State) ->
  disconnect(closed, State#state{buffer = <<>>});

handle_info(Msg, #state{pfx = Pfx} = State) ->
  ?LOG_WARNING("~s got unexpected msg: ~p", [Pfx, Msg]),
  {noreply, State}.

-doc false.
terminate(Reason, State) ->
  disconnect(Reason, State),
  ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions: connect/reconnect
%%%-----------------------------------------------------------------------------

reconnect(#state{addresses = Addresses} = State) ->
  try_addresses(Addresses, State).

try_addresses([], State) ->
  {noreply, recon_timer(State)};
try_addresses([Entry | Rest], #state{
  pfx = Pfx, pool = Pool, conn_id = ConnID, port = DefaultPort,
  nodelay = Nodelay, conn_timeout = Timeout
} = State) ->
  {Address, Port} = resolve_entry(Entry, DefaultPort),
  case inet:getaddrs(Address, inet) of
    {ok, IPs} ->
      IP = arterial_util:random_element(IPs),
      PoolRef = arterial_pool2:pool_ref(Pool),
      case arterial_observe:span([connect], #{pool => Pool, conn_id => ConnID, address => IP, port => Port}, fun() ->
        Result = arterial_nif_pool:connect(PoolRef, ConnID, IP, Port, Timeout, Nodelay, self()),
        Outcome = case Result of {ok, _} -> ok; {error, R} -> R end,
        {Result, #{pool => Pool, conn_id => ConnID, address => IP, port => Port, result => Outcome}}
      end) of
        {ok, _SlotId} ->
          arterial_pool2:set_available(Pool, ConnID),
          {noreply, reset_backoff(State#state{connected = true})};
        {error, Reason} ->
          ?LOG_WARNING("~s cannot connect to ~s:~p: ~p", [Pfx, inet:ntoa(IP), Port, Reason]),
          try_addresses(Rest, State)
      end;
    {error, Reason} ->
      ?LOG_WARNING("~s failed to resolve host ~p: ~p", [Pfx, Address, Reason]),
      try_addresses(Rest, State)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions: read path (decode + dispatch)
%%%-----------------------------------------------------------------------------

handle_read_event(#state{pool = Pool, conn_id = ConnID} = State) ->
  PoolRef = arterial_pool2:pool_ref(Pool),
  case arterial_nif_pool:handle_readable(PoolRef, ConnID, 0) of
    {ok, Bin} ->
      append_and_decode(Bin, State);
    closed ->
      disconnect(closed, State#state{buffer = <<>>})
  end.

append_and_decode(Bin, #state{pfx = Pfx, pool = Pool, codec = Codec, buffer = Buffer} = State) ->
  NewBuffer = <<Buffer/binary, Bin/binary>>,
  CorrTable = arterial_pool2:corr_table(Pool),
  try decode_loop(Codec, NewBuffer, CorrTable) of
    Rest -> {noreply, State#state{buffer = Rest}}
  catch error:{codec_decode_error, Reason} ->
    ?LOG_WARNING("~s codec decode error, dropping connection: ~p", [Pfx, Reason]),
    disconnect({codec_error, Reason}, State#state{buffer = <<>>})
  end.

decode_loop(Codec, Buffer, CorrTable) ->
  case Codec:decode(Buffer) of
    {ok, CorrId, Reply, Rest} ->
      dispatch_reply(CorrTable, CorrId, Reply),
      decode_loop(Codec, Rest, CorrTable);
    more ->
      Buffer;
    {error, Reason} ->
      error({codec_decode_error, Reason})
  end.

dispatch_reply(CorrTable, CorrId, Reply) ->
  case ets:take(CorrTable, CorrId) of
    [{CorrId, Pid, _ConnID, _Deadline}] ->
      Pid ! {arterial_reply, CorrId, Reply},
      ok;
    [] ->
      %% Already timed out (arterial_sweeper2) or a stray/duplicate
      %% reply -- nothing to deliver it to.
      ok
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions: bounce
%%%-----------------------------------------------------------------------------

bounce_check(#state{
  bounce = #bounce_state{from = From, deadline = Deadline},
  pool   = Pool, conn_id = ConnID, pfx = Pfx
} = State) ->
  Drained  = connection_drained(Pool, ConnID),
  TimedOut = erlang:monotonic_time(millisecond) >= Deadline,
  case Drained orelse TimedOut of
    true ->
      (TimedOut andalso not Drained) andalso
        ?LOG_NOTICE("~s bounce: in-flight requests did not drain before deadline, forcing it anyway", [Pfx]),
      {noreply, State1} = disconnect(bounce, State#state{bounce = undefined, buffer = <<>>}),
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

connection_drained(Pool, ConnID) ->
  ets:match(arterial_pool2:corr_table(Pool), {'_', '_', ConnID, '_'}, 1) =:= '$end_of_table'.

%%%-----------------------------------------------------------------------------
%%% Internal functions: disconnect + notification
%%%-----------------------------------------------------------------------------

disconnect(Reason, #state{pool = Pool, conn_id = ConnID, connected = Connected} = State) ->
  Connected andalso begin
    arterial_pool2:set_unavailable(Pool, ConnID),
    notify_inflight_disconnected(Pool, ConnID),
    _ = arterial_nif_pool:close_slot(arterial_pool2:pool_ref(Pool), ConnID, 0),
    arterial_observe:event([disconnect], #{pool => Pool, conn_id => ConnID, reason => Reason})
  end,
  {noreply, recon_timer(State#state{connected = false})}.

%% Every correlation entry still tied to this ConnID belongs to a caller
%% that will otherwise wait out its full timeout for nothing -- tell it
%% now instead, same contract as arterial_nif:connection_down/2 in the
%% original backend.
notify_inflight_disconnected(Pool, ConnID) ->
  CorrTable = arterial_pool2:corr_table(Pool),
  Matches = ets:match_object(CorrTable, {'_', '_', ConnID, '_'}),
  lists:foreach(fun({CorrId, Pid, _ConnID, _Deadline}) ->
    ets:delete(CorrTable, CorrId),
    Pid ! {arterial_disconnected, Pool, CorrId}
  end, Matches).

cancel_timer(undefined) -> ok;
cancel_timer(TimerRef)  -> erlang:cancel_timer(TimerRef), ok.

%%%-----------------------------------------------------------------------------
%%% Internal functions: options/backoff (mirrors arterial_connection's
%%% logic; kept separate since the two backends' connection lifecycle
%%% differs enough -- e.g. no protocol/client behaviour layering here --
%%% that sharing it would mean threading both through a shape neither
%%% really needs).
%%%-----------------------------------------------------------------------------

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

resolve_entry(#{address := Address} = Entry, DefaultPort) ->
  {Address, maps:get(port, Entry, DefaultPort)};
resolve_entry(Address, DefaultPort) ->
  {Address, DefaultPort}.

needs_default_port(#{port := _}) -> false;
needs_default_port(_)            -> true.

recon_state(Options) ->
  case maps:get(reconnect, Options, true) of
    true  -> #recon_state{policy = reconnect_time(Options)};
    false -> undefined
  end.

reconnect_time(Options) ->
  case maps:get(reconnect_time, Options, undefined) of
    undefined ->
      {backoff,
        maps:get(reconnect_time_min, Options, 500),
        maps:get(reconnect_time_max, Options, timer:minutes(5))};
    Policy ->
      Policy
  end.

reset_backoff(#state{recon_state = undefined} = State) -> State;
reset_backoff(#state{recon_state = RS} = State) ->
  State#state{recon_state = RS#recon_state{cur = undefined}}.

recon_timer(#state{recon_state = undefined} = S) ->
  S;
recon_timer(#state{recon_state = RS} = S) ->
  {Interval, RS1} = backoff_timeout(RS),
  TimerRef = erlang:send_after(Interval, self(), reconnect),
  S#state{recon_state = RS1, timer_ref = TimerRef}.

backoff_timeout(#recon_state{policy = Ms} = S) when is_integer(Ms) ->
  {Ms, S};
backoff_timeout(#recon_state{cur = undefined, policy = {backoff, Min, _Max}} = S) ->
  {Min, S#recon_state{cur = Min}};
backoff_timeout(#recon_state{cur = I, policy = {backoff, _Min, Max}} = S) when Max /= infinity, I >= Max ->
  {Max, S#recon_state{cur = Max}};
backoff_timeout(#recon_state{cur = I, policy = {backoff, _Min, Max}} = S) ->
  Doubled = I * 2,
  JitterSpan = max(1, Doubled div 2),
  Jittered = Doubled + (rand:uniform(JitterSpan + 1) - 1) - (JitterSpan div 2),
  Next = clamp(Jittered, 0, Max),
  {Next, S#recon_state{cur = Next}}.

clamp(V, Lo, infinity) -> erlang:max(V, Lo);
clamp(V, Lo, Hi)       -> erlang:max(Lo, erlang:min(V, Hi)).
