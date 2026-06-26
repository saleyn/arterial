-module(arterial_connection).

-moduledoc """
Per-connection `gen_server` worker for `arterial_pool`: owns connect/
reconnect-with-backoff via `arterial_nif:connect/7` (which opens and
connects the socket *inside* the NIF -- see below), and -- unlike
`arterial_connection` (the original backend, where the *caller* does the
actual socket I/O) -- this worker is also the long-lived "owner" process
that every read/write-readiness notification for its slot targets: it
decodes newly-read bytes via the pool's `c:arterial_codec` module and
dispatches each reply directly to its waiting caller's mailbox via the
pool's public correlation-id ETS table (see `arterial_pool:corr_table/1`).
`arterial_client:call/3`/`cast/2` write requests directly via
`arterial_nif:send_and_release/3` in their *own* process, never
through this worker -- it is only ever in the read path.

## Multi-Protocol Support

`arterial_nif` supports multiple protocols:
- `tcp`: IPv4 TCP sockets (default)
- `udp`: IPv4 UDP sockets with connection-oriented semantics
- `ssl`: SSL/TLS over TCP (requires OpenSSL at compile time)

Protocol is selected via the `protocol` option in `arterial_pool` configuration.

## DNS Resolution

Enhanced DNS resolution with the following features:

- **Hostname to IP resolution**: Automatically resolves hostnames to IPv4 addresses
- **IP address passthrough**: IPv4 addresses in string or tuple format are passed through unchanged
- **DNS caching**: Results are cached for 5 minutes to improve performance on repeated connections
- **Error handling**: Comprehensive error reporting for resolution failures
- **IPv6 awareness**: Detects IPv6-only hostnames and provides appropriate error messages

### DNS Resolution Examples

```erlang
% Using hostnames in address configuration
arterial_pool:start_link(my_pool, #{
  address => "example.com",           % Hostname will be resolved to IPv4
  port => 80,
  protocol => tcp
}).

% Multiple addresses with hostnames
arterial_pool:start_link(my_pool, #{
  addresses => ["primary.example.com", "backup.example.com", "127.0.0.1"],
  port => 80,
  protocol => tcp
}).

% Per-entry addresses with hostnames
arterial_pool:start_link(my_pool, #{
  addresses => [
    #{address => "primary.example.com", port => 80},
    #{address => "backup.example.com", port => 8080},
    #{address => "127.0.0.1", port => 9090}
  ],
  protocol => tcp
}).
```

### DNS Caching

DNS resolution results are automatically cached for 5 minutes to improve performance:
- Successful resolutions are cached and reused
- Cache is per-process (stored in process dictionary)
- Failed resolutions are not cached
- Cache automatically expires after 5 minutes

### IPv6 Limitations

Currently, the arterial NIF only supports IPv4 addresses. Hostnames that resolve only to IPv6 addresses will result in appropriate error messages indicating IPv6 is not yet supported.

Earlier iterations of this backend connected via Erlang's `socket`
module and handed the resulting fd to the NIF after the fact
(`socket:getopt(Sock, otp, fd)` + `arterial_nif:register_socket/4`,
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
-export([resolve_address/1, resolve_k8s_service/1]). % Export for testing and external use

%% Export Kubernetes detection functions for testing
-ifdef(TEST).
-export([
  is_k8s_service_name/1,
  is_fully_qualified_k8s_name/1,
  is_external_hostname/1,
  detect_k8s_environment/0,
  get_current_namespace/1,
  get_cluster_domain/0,
  build_k8s_search_domains/2,
  is_k8s_environment/0
]).
-endif.
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2,
          terminate/2]).

-define(BOUNCE_DRAIN_POLL_INTERVAL_MS, 100).

-record(recon_state, {
  cur          :: undefined | arterial:time(),
  policy       :: reconnect_time()
}).

-record(bounce_state, {
  from         :: gen_server:from(),
  deadline     :: integer()
}).

-record(state, {
  pfx          :: binary(),
  pool         :: arterial_pool:name(),
  conn_id      :: non_neg_integer(),
  codec        :: module(),
  addresses    :: [address_entry(), ...],
  port         :: arterial:inet_port() | undefined,
  protocol     :: tcp | udp | ssl | module(),
  nodelay      :: boolean(),
  socket_opts  :: [gen_tcp:option() | gen_udp:option()],
  conn_timeout :: non_neg_integer(),
  recon_state  :: undefined | #recon_state{},
  connected    :: boolean(),
  buffer       :: binary(),
  timer_ref    :: undefined | reference(),
  bounce       :: undefined | #bounce_state{}
}).

-doc """
One entry of the `addresses` option: either a plain address (using the
connection's shared `port`), or a map overriding `port`, `sock_opts`,
or `tls_options` for that entry alone. Socket and TLS options use
arterial's custom types `t:arterial_pool:sockopt/0` and `t:arterial_pool:tlsopt/0`.
""".
-type address_entry() :: arterial:inet_address() | #{
  address := arterial:inet_address(),
  port    => arterial:inet_port(),
  sock_opts => [arterial_pool:sockopt()],
  tls_options => [arterial_pool:tlsopt()]
}.

-type reconnect_time() :: {backoff, pos_integer(), pos_integer()} | any().

-export_type([address_entry/0, reconnect_time/0]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a connection worker for slot `ConnID` (0-based) of `Pool`. Connects
(and reconnects, with backoff) on its own; returns as soon as the
`gen_server` itself has started, not once a socket is up.

`Opts` accepts `address`/`addresses`/`ip`/`port`/`nodelay`/`reconnect`/
`reconnect_time` (and the legacy `reconnect_time_min`/`reconnect_time_max`
pair) -- see the moduledoc for why there's no `protocol`/`sock_opts`/
`tls_options` here, unlike `t:arterial_client:options/0`.
""".
-spec start_link(arterial_pool:name(), non_neg_integer(), map()) -> {ok, pid()}.
start_link(Pool, ConnID, Opts)
    when is_atom(Pool), is_integer(ConnID), ConnID >= 0, is_map(Opts) ->
  gen_server:start_link(?MODULE, [Pool, ConnID, Opts], []).

-doc """
Bounce this connection: mark it unavailable for new sends, wait (up to
`DrainTimeoutMs`) for its in-flight correlation entries to drain, then
disconnect and attempt one immediate reconnect. Used by
`arterial_bouncer`; see `arterial_connection:bounce/2`'s moduledoc for
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
  Codec      = arterial_pool:codec(Pool),
  Addresses   = addresses(Opts),
  Port        = maps:get(port, Opts, undefined),
  Protocol    = maps:get(protocol, Opts, tcp),
  SocketOpts  = maps:get(sock_opts, Opts, []),
  ReconState  = recon_state(Opts),
  Pfx         = list_to_binary(io_lib:format("~w:~w: ", [Pool, ConnID])),

  Port == undefined andalso lists:any(fun needs_default_port/1, Addresses)
    andalso error({missing_port_option, Pool}),

  {ok, #state{
    pfx          = Pfx,
    pool         = Pool,
    conn_id      = ConnID,
    codec        = Codec,
    addresses    = Addresses,
    port         = Port,
    protocol     = Protocol,
    nodelay      = Nodelay,
    socket_opts  = SocketOpts,
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
  arterial_pool:set_unavailable(Pool, ConnID),
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

handle_info({arterial_event, ConnID, 0, read}, #state{conn_id = ConnID} = State) ->
  handle_read_event(State);

handle_info({arterial_event, ConnID, 0, write}, #state{conn_id = ConnID, pool = Pool} = State) ->
  case arterial_observe:enabled() of
    false ->
      _ = arterial_nif:handle_writable(arterial_pool:pool_ref(Pool), ConnID, 0);
    true ->
      arterial_observe:span([nif, write], #{pool => Pool, conn_id => ConnID}, fun() ->
        case arterial_nif:handle_writable(arterial_pool:pool_ref(Pool), ConnID, 0) of
          ok -> {ok, #{result => ok}};
          closed -> {closed, #{result => closed}}
        end
      end)
  end,
  {noreply, State};

handle_info({arterial_event, ConnID, 0, closed}, #state{conn_id = ConnID} = State) ->
  disconnect(closed, State#state{buffer = <<>>});

handle_info({arterial_event, ConnID, 0, connect_result, Result}, #state{conn_id = ConnID} = State) ->
  handle_connect_result(Result, State);

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
  protocol = Protocol, nodelay = Nodelay, socket_opts = SocketOpts,
  conn_timeout = _Timeout
} = State) ->
  {Address, Port, EntrySocketOpts} = resolve_entry(Entry, DefaultPort),
  % Use per-entry socket options if specified, otherwise use connection-level options
  ActualSocketOpts = case EntrySocketOpts of
    undefined -> SocketOpts;
    _ -> EntrySocketOpts
  end,
  case resolve_address(Address) of
    {ok, IPs} when length(IPs) > 0 ->
      IP = arterial_util:random_element(IPs),
      PoolRef = arterial_pool:pool_ref(Pool),
      case arterial_observe:span([connect], #{pool => Pool, conn_id => ConnID, address => IP, port => Port, protocol => Protocol}, fun() ->
        % Determine if protocol is built-in (NIF) or custom (callback)
        Result = case Protocol of
          P when P == tcp; P == udp; P == ssl ->
            % Built-in NIF protocols
            case ActualSocketOpts of
              [] ->
                arterial_nif:connect_async_proto(PoolRef, ConnID, IP, Port, Protocol, Nodelay, self());
              _ ->
                % Use the new socket options aware function
                arterial_nif:connect_proto_with_opts(PoolRef, ConnID, IP, Port,
                    State#state.conn_timeout, Protocol, Nodelay, self(), ActualSocketOpts)
            end;
          ProtocolModule when is_atom(ProtocolModule) ->
            % Custom protocol module - use arterial_protocol callbacks
            connect_with_protocol_module(ProtocolModule, PoolRef, ConnID, IP, Port, ActualSocketOpts, Nodelay, self())
        end,
        Outcome = case Result of
          {ok, _} -> ok;
          {ok, connecting, _} -> connecting;
          {error, R} -> R
        end,
        {Result, #{pool => Pool, conn_id => ConnID, address => IP, port => Port, result => Outcome}}
      end) of
        {ok, _SlotId} ->
          % Connection completed immediately
          arterial_pool:set_available(Pool, ConnID),
          {noreply, reset_backoff(State#state{connected = true})};
        {ok, connecting, _SlotId} ->
          % Connection in progress, wait for completion message
          ?LOG_DEBUG("~s connecting asynchronously to ~s:~p", [Pfx, inet:ntoa(IP), Port]),
          {noreply, State};
        {error, Reason} ->
          ?LOG_WARNING("~s cannot connect to ~s:~p: ~p", [Pfx, inet:ntoa(IP), Port, Reason]),
          try_addresses(Rest, State)
      end;
    {error, Reason} ->
      ?LOG_WARNING("~s failed to resolve host ~p: ~p", [Pfx, Address, Reason]),
      try_addresses(Rest, State)
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions: connection lifecycle
%%%-----------------------------------------------------------------------------

handle_connect_result(ok, #state{pfx = Pfx, pool = Pool, conn_id = ConnID} = State) ->
  ?LOG_INFO("~s async connect completed successfully", [Pfx]),
  arterial_pool:set_available(Pool, ConnID),
  {noreply, reset_backoff(State#state{connected = true})};

handle_connect_result(Error, #state{pfx = Pfx} = State) ->
  ?LOG_WARNING("~s async connect failed: ~p", [Pfx, Error]),
  disconnect({connect_failed, Error}, State).

%%%-----------------------------------------------------------------------------
%%% Internal functions: read path (decode + dispatch)
%%%-----------------------------------------------------------------------------

handle_read_event(#state{pool = Pool, conn_id = ConnID} = State) ->
  PoolRef = arterial_pool:pool_ref(Pool),
  case arterial_observe:enabled() of
    false ->
      case arterial_nif:handle_readable(PoolRef, ConnID, 0) of
        {ok, Bin} -> append_and_decode(Bin, State);
        closed -> disconnect(closed, State#state{buffer = <<>>})
      end;
    true ->
      case arterial_observe:span([nif, read], #{pool => Pool, conn_id => ConnID}, fun() ->
        case arterial_nif:handle_readable(PoolRef, ConnID, 0) of
          {ok, Bin} ->
            {{ok, Bin}, #{result => ok, bytes => byte_size(Bin)}};
          closed ->
            {closed, #{result => closed}}
        end
      end) of
        {ok, Bin} -> append_and_decode(Bin, State);
        closed -> disconnect(closed, State#state{buffer = <<>>})
      end
  end.

append_and_decode(Bin, #state{pfx = Pfx, pool = Pool, codec = Codec, buffer = Buffer} = State) ->
  NewBuffer = <<Buffer/binary, Bin/binary>>,
  CorrTable = arterial_pool:corr_table(Pool),
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
      %% Already timed out (arterial_sweeper) or a stray/duplicate
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
  ets:match(arterial_pool:corr_table(Pool), {'_', '_', ConnID, '_'}, 1) =:= '$end_of_table'.

%%%-----------------------------------------------------------------------------
%%% Internal functions: disconnect + notification
%%%-----------------------------------------------------------------------------

disconnect(Reason, #state{pool = Pool, conn_id = ConnID, connected = Connected} = State) ->
  PoolRef = arterial_pool:pool_ref(Pool),
  if Connected ->
    arterial_pool:set_unavailable(Pool, ConnID),
    notify_inflight_disconnected(Pool, ConnID),
    _ = arterial_nif:close_slot(PoolRef, ConnID, 0),
    arterial_observe:event([disconnect], #{pool => Pool, conn_id => ConnID, reason => Reason});
  true ->
    % Not yet connected — slot may still be leased (SLOT_CONNECTING).
    % close_slot clears the lease bit so the next reconnect can claim it.
    _ = arterial_nif:close_slot(PoolRef, ConnID, 0)
  end,
  {noreply, recon_timer(State#state{connected = false})}.

%% Every correlation entry still tied to this ConnID belongs to a caller
%% that will otherwise wait out its full timeout for nothing -- tell it
%% now instead, same contract as arterial_nif:connection_down/2 in the
%% original backend.
notify_inflight_disconnected(Pool, ConnID) ->
  CorrTable = arterial_pool:corr_table(Pool),
  Matches = ets:match_object(CorrTable, {'_', '_', ConnID, '_'}),
  lists:foreach(fun({CorrId, Pid, _ConnID, _Deadline}) ->
    ets:delete(CorrTable, CorrId),
    Pid ! {arterial_disconnected, Pool, CorrId}
  end, Matches).

cancel_timer(undefined) -> ok;
cancel_timer(TimerRef)  -> erlang:cancel_timer(TimerRef), ok.

%% Connect using custom arterial_protocol module
connect_with_protocol_module(ProtocolModule, PoolRef, ConnID, IP, Port, SocketOpts, _Nodelay, OwnerPid) ->
  try
    % Use the protocol module to establish connection
    case ProtocolModule:connect(IP, Port, SocketOpts) of
      {ok, Socket} ->
        % Extract raw file descriptor from the socket
        case socket:getopt(Socket, otp, fd) of
          {ok, RawFd} ->
            % Register the socket with the NIF pool
            case arterial_nif:register_socket(PoolRef, ConnID, RawFd, OwnerPid) of
              {ok, SlotId} ->
                % Store the socket handle for later use in send/recv
                put({arterial_socket, ConnID}, Socket),
                put({arterial_protocol_module, ConnID}, ProtocolModule),
                {ok, SlotId};
              Error ->
                % Failed to register with NIF, close the socket
                ProtocolModule:close(Socket),
                Error
            end;
          {error, _} ->
            % Can't get file descriptor, close socket
            ProtocolModule:close(Socket),
            {error, no_fd_access}
        end;
      Error ->
        Error
    end
  catch
    error:undef ->
      {error, {protocol_module_not_found, ProtocolModule}};
    Class:Reason:Stack ->
      {error, {protocol_connect_failed, Class, Reason, Stack}}
  end.

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
  Port = maps:get(port, Entry, DefaultPort),
  SockOpts = maps:get(sock_opts, Entry, undefined),
  {Address, Port, SockOpts};
resolve_entry(Address, DefaultPort) ->
  {Address, DefaultPort, undefined}.

needs_default_port(#{port := _}) -> false;
needs_default_port(_)            -> true.

%% Enhanced DNS resolution with IPv4/IPv6 support and better error handling
resolve_address(Address) when is_binary(Address) ->
  resolve_address(binary_to_list(Address));
resolve_address(Address) when is_list(Address) ->
  % First check if it's already an IP address
  case inet:parse_address(Address) of
    {ok, IP} ->
      % Already an IP address, return it directly
      {ok, [IP]};
    {error, einval} ->
      % Not an IP address, perform DNS resolution
      resolve_hostname(Address)
  end;
resolve_address({A, B, C, D} = IP) when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
  % IPv4 tuple, return directly
  {ok, [IP]};
resolve_address({A, B, C, D, E, F, G, H} = IP)
  when is_integer(A), is_integer(B), is_integer(C), is_integer(D),
       is_integer(E), is_integer(F), is_integer(G), is_integer(H) ->
  % IPv6 tuple, return directly
  {ok, [IP]};
resolve_address(Address) ->
  {error, {invalid_address_format, Address}}.

%% Perform hostname resolution with Kubernetes awareness and caching
resolve_hostname(Hostname) ->
  CacheKey = {dns_cache, Hostname},
  CurrentTime = erlang:system_time(second),

  % Determine cache TTL based on hostname type
  CacheTTL = case is_k8s_service_name(Hostname) of
    true -> 60;   % Shorter TTL for Kubernetes services (1 minute)
    false -> 300  % Standard TTL for external hostnames (5 minutes)
  end,

  % Check cache first
  case get(CacheKey) of
    {CachedAddrs, CacheTime} when CurrentTime - CacheTime < CacheTTL ->
      % Cache hit and not expired
      {ok, CachedAddrs};
    _ ->
      % Cache miss or expired, perform actual DNS resolution
      case resolve_hostname_uncached(Hostname) of
        {ok, Addrs} = Result ->
          % Cache the result
          put(CacheKey, {Addrs, CurrentTime}),
          Result;
        {error, _} = Error ->
          % Don't cache errors, return immediately
          Error
      end
  end.

%% Perform actual hostname resolution without caching
%% Note: Currently only IPv4 is supported by arterial_nif, so we filter out IPv6
resolve_hostname_uncached(Hostname) ->
  % Check if this looks like a Kubernetes service and handle accordingly
  case is_k8s_service_name(Hostname) of
    true ->
      resolve_k8s_hostname(Hostname);
    false ->
      resolve_standard_hostname(Hostname)
  end.

%% Resolve standard (non-Kubernetes) hostnames
resolve_standard_hostname(Hostname) ->
  % Try IPv4 first - this is what the NIF supports
  case inet:getaddrs(Hostname, inet) of
    {ok, IPv4Addrs} when length(IPv4Addrs) > 0 ->
      % IPv4 addresses found - return them directly
      {ok, IPv4Addrs};
    {error, nxdomain} ->
      % No IPv4 addresses - check if IPv6 is available but warn user
      case inet:getaddrs(Hostname, inet6) of
        {ok, IPv6Addrs} when length(IPv6Addrs) > 0 ->
          % TODO: Add IPv6 support to arterial_nif
          % For now, return an error indicating IPv6 is not supported
          {error, {ipv6_not_supported, IPv6Addrs}};
        {error, _} = Error ->
          Error
      end;
    {error, _} = Error ->
      % IPv4 resolution failed for other reasons
      % Check if IPv6 would work, but still return IPv4 error since that's preferred
      case inet:getaddrs(Hostname, inet6) of
        {ok, IPv6Addrs} when length(IPv6Addrs) > 0 ->
          {error, {ipv4_failed_ipv6_available, Error, IPv6Addrs}};
        {error, _} ->
          % Both failed, return original IPv4 error
          Error
      end
  end.

%% Resolve Kubernetes service names with search domain expansion
resolve_k8s_hostname(Hostname) ->
  case is_fully_qualified_k8s_name(Hostname) of
    true ->
      % Already fully qualified, resolve directly
      resolve_standard_hostname(Hostname);
    false ->
      % Short name, try expansion with Kubernetes search domains
      resolve_k8s_short_name(Hostname)
  end.

%% Try to resolve a short Kubernetes service name by expanding with search domains
resolve_k8s_short_name(ServiceName) ->
  K8sEnvironment = detect_k8s_environment(),
  SearchDomains = build_k8s_search_domains(ServiceName, K8sEnvironment),
  try_k8s_search_domains(SearchDomains, ServiceName).

%% Try each search domain until one resolves
try_k8s_search_domains([], ServiceName) ->
  % All search domains failed, try the original name as fallback
  case resolve_standard_hostname(ServiceName) of
    {ok, _} = Success -> Success;
    {error, _} -> {error, {k8s_service_not_found, ServiceName}}
  end;
try_k8s_search_domains([Domain | Rest], ServiceName) ->
  FullName = ServiceName ++ "." ++ Domain,
  case resolve_standard_hostname(FullName) of
    {ok, _} = Success -> Success;
    {error, nxdomain} -> try_k8s_search_domains(Rest, ServiceName);
    {error, _} = OtherError -> OtherError
  end.

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

%%%-----------------------------------------------------------------------------
%%% Kubernetes DNS resolution support
%%%-----------------------------------------------------------------------------

%% Detect if a hostname looks like a Kubernetes service
is_k8s_service_name(Hostname) ->
  % Check for Kubernetes patterns:
  % - Contains ".svc." (fully qualified service name)
  % - Contains ".cluster.local" (default cluster domain)
  % - Environment suggests Kubernetes context
  string:str(Hostname, ".svc.") > 0 orelse
  string:str(Hostname, ".cluster.local") > 0 orelse
  (not is_external_hostname(Hostname) andalso is_k8s_environment()).

%% Check if hostname is fully qualified Kubernetes service name
is_fully_qualified_k8s_name(Hostname) ->
  string:str(Hostname, ".svc.") > 0.

%% Check if this looks like an external hostname (has TLD)
is_external_hostname(Hostname) ->
  % First check if it's a Kubernetes service name - if so, it's not external
  case string:str(Hostname, ".svc.") of
    N when N > 0 -> false;  % Kubernetes service, not external
    0 ->
      % Simple heuristic: contains common TLDs or IP-like patterns
      string:str(Hostname, ".com") > 0 orelse
      string:str(Hostname, ".org") > 0 orelse
      string:str(Hostname, ".net") > 0 orelse
      string:str(Hostname, ".io") > 0 orelse
      string:str(Hostname, ".local") > 0 orelse
      % Check for IP address patterns
      case string:tokens(Hostname, ".") of
        [A, B, C, D] ->
          lists:all(fun(Part) ->
            try list_to_integer(Part) of
              N when N >= 0, N =< 255 -> true;
              _ -> false
            catch
              _:_ -> false
            end
          end, [A, B, C, D]);
        _ -> false
      end
  end.

%% Detect Kubernetes execution environment
detect_k8s_environment() ->
  case filelib:is_file("/var/run/secrets/kubernetes.io/serviceaccount/token") of
    true ->
      {in_cluster, read_current_namespace()};
    false ->
      case os:getenv("KUBERNETES_SERVICE_HOST") of
        false -> external;
        _ -> {external_with_k8s_env, os:getenv("KUBERNETES_NAMESPACE", "default")}
      end
  end.

%% Read the current namespace from the service account
read_current_namespace() ->
  NamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace",
  case file:read_file(NamespaceFile) of
    {ok, NamespaceBin} ->
      string:trim(binary_to_list(NamespaceBin));
    {error, _} ->
      "default"  % Fallback to default namespace
  end.

%% Build search domains for Kubernetes service resolution
build_k8s_search_domains(_ServiceName, K8sEnvironment) ->
  ClusterDomain = get_cluster_domain(),
  CurrentNamespace = get_current_namespace(K8sEnvironment),

  % Build search domains in order of preference:
  % 1. Current namespace
  % 2. Default namespace (if not current)
  % 3. kube-system namespace (for system services)
  SearchDomains = [
    CurrentNamespace ++ ".svc." ++ ClusterDomain
  ],

  % Add default namespace if different from current
  SearchDomains2 = case CurrentNamespace of
    "default" -> SearchDomains;
    _ -> SearchDomains ++ ["default.svc." ++ ClusterDomain]
  end,

  % Add kube-system for system services
  SearchDomains3 = case CurrentNamespace of
    "kube-system" -> SearchDomains2;
    _ -> SearchDomains2 ++ ["kube-system.svc." ++ ClusterDomain]
  end,

  SearchDomains3.

%% Get cluster domain (usually cluster.local)
get_cluster_domain() ->
  case os:getenv("CLUSTER_DOMAIN") of
    false -> "cluster.local";
    Domain -> Domain
  end.

%% Get current namespace based on environment
get_current_namespace({in_cluster, Namespace}) -> Namespace;
get_current_namespace({external_with_k8s_env, Namespace}) -> Namespace;
get_current_namespace(external) -> "default".

%% Check if running in Kubernetes environment
is_k8s_environment() ->
  case detect_k8s_environment() of
    external -> false;
    _ -> true
  end.

%% Explicit Kubernetes service resolution
resolve_k8s_service(ServiceSpec) when is_map(ServiceSpec) ->
  resolve_k8s_service_from_map(ServiceSpec);
resolve_k8s_service(ServiceName) when is_binary(ServiceName) ->
  resolve_k8s_service(binary_to_list(ServiceName));
resolve_k8s_service(ServiceName) when is_list(ServiceName) ->
  resolve_k8s_service_from_name(ServiceName).

%% Resolve from service specification map
resolve_k8s_service_from_map(#{name := ServiceName} = Spec) ->
  Namespace = maps:get(namespace, Spec, get_current_namespace(detect_k8s_environment())),
  ClusterDomain = maps:get(cluster_domain, Spec, get_cluster_domain()),

  FullName = ServiceName ++ "." ++ Namespace ++ ".svc." ++ ClusterDomain,
  resolve_address(FullName).

%% Resolve from service name with automatic expansion
resolve_k8s_service_from_name(ServiceName) ->
  % Use the same logic as the enhanced resolve_address
  resolve_address(ServiceName).
