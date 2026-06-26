-module(arterial_pool).

-moduledoc """
Per-pool supervisor for `arterial`'s second connection-pool backend: raw
socket I/O lives inside `arterial_nif` (the NIF), correlation-id
dispatch lives in a public ETS table this module owns, and everything
else (connect/reconnect, decode/dispatch, disconnect notification,
sweeping, bouncing) is plain Erlang on top -- see `arterial_nif`'s
moduledoc for why this backend exists and how it differs structurally
from `arterial_pool`/`arterial_nif` (the original backend).

## Why a second backend?

The original backend's per-request path is: caller checks out a
connection from the NIF-resident lock-free pool (which also tracks
per-connection backlog/throttle state), sends/receives on the socket
itself via `arterial_protocol`, then checks the connection back in. That
NIF call only ever does bookkeeping -- actual `send`/`recv` happen via
the `socket` module, in plain Erlang, on every single request.

This backend instead does the `write(2)`/`read(2)` syscalls themselves
*inside* the NIF (see `arterial_nif`), invoked directly from
whichever Erlang process needs them, and decouples "send a request" from
"receive its reply" entirely: `arterial_client:call/3` writes inline in
the caller's own process (no intermediate `arterial_connection` hop to
reach the socket), then blocks in a plain `receive` for a reply message
sent directly to its mailbox once `arterial_connection` decodes it off
the wire -- no per-request backlog/checkout accounting at all. The
tradeoff: this only supports protocol matching modes 1/2 (native or
surrogate wire-level request id; see `arterial_codec`'s moduledoc) --
there is no FIFO/no-request-id backlog mode here, and (for now) no
queue-when-busy wait-list equivalent to `arterial_nif:checkout_async/3`.

## Stripes, slots, and why this module always uses one slot per stripe

`arterial_nif`'s pool resource is organized into `Stripes` (each one
independent lock-free `uint64` lease bitmask) of up to 64 `Slots` (real
sockets) each -- multiple slots sharing one stripe let several physical
connections share one atomic's contention, at the cost of the NIF having
to pick which slot services a given write. `arterial_pool` always
creates exactly one stripe per connection, with exactly one slot in it
(`arterial_nif:init_pool(Size, 1)`): every connection already gets
its own independent atomic this way, so there is no contention to spread
across slots in the first place, and `ConnID`, `StripeId`, and the slot's
(always `0`) `SlotId` are all the same number throughout this backend.
Multiple slots per stripe remains fully supported by the NIF for
whoever wants to build a different pool layout on top of it later (e.g.
several replica sockets behind one logical connection).
""".

-behaviour(supervisor).

-export([start_link/2, stop/1]).
-export([init/1]).
-export([sup_name/1]).
-export([pool_ref/1, codec/1, size/1, default_timeout_ms/1, throttle/1, corr_table/1]).
-export([avail_ref/1, set_available/2, set_unavailable/2, is_available/2, wait_connected/2, wait_connected/3]).

-doc "The atom identifying a pool; shared with `arterial_nif:pool_ref/0`'s owner.".
-type name() :: atom().

-doc """
Socket options supported by arterial's NIF layer for TCP/UDP connections.
These are a subset of standard socket options that can be efficiently
processed by the NIF, formatted as atoms or `{atom(), term()}` tuples.

Supported options include:
* `keepalive` - Enable TCP keep-alive (equivalent to `{keepalive, true}`)
* `{keepalive, boolean()}` - Enable or disable TCP keep-alive
* `{sndbuf, pos_integer()}` - Send buffer size in bytes
* `{recvbuf, pos_integer()}` - Receive buffer size in bytes
* `{priority, 0..6}` - Socket priority level
* `{tos, non_neg_integer()}` - Type of Service bits
* `{linger, {boolean(), non_neg_integer()}}` - Linger behavior on close

These options are passed to `arterial_nif:connect_with_opts/8` and
`arterial_nif:connect_proto_with_opts/9` for socket configuration during
connection establishment.
""".
-type sockopt() ::
    keepalive |
    {keepalive, boolean()} |
    {sndbuf,    pos_integer()} |
    {recvbuf,   pos_integer()} |
    {priority,  0..6} |
    {tos,       non_neg_integer()} |
    {linger,    {boolean(), non_neg_integer()}}.

-doc """
TLS options supported by arterial's NIF layer for SSL connections.
These are a subset of standard SSL/TLS options that can be efficiently
processed by the NIF's OpenSSL integration.

Supported options include:
* `{verify, verify_none | verify_peer}` - Peer certificate verification mode
* `{cacerts, [binary()]}` - List of trusted CA certificates in DER format
* `{cert, binary()}` - Client certificate in DER format
* `{key, binary()}` - Private key in DER format
* `{versions, [atom()]}` - Supported TLS versions (e.g., `['tlsv1.3', 'tlsv1.2']`)
* `{ciphers, [binary()]}` - Allowed cipher suites
* `{honor_cipher_order, boolean()}` - Whether server's cipher preference takes precedence

These options are processed by the NIF when `protocol => ssl` is specified
and are used to configure the OpenSSL context during connection establishment.
""".
-type tlsopt() ::
    {verify,             verify_none | verify_peer} |
    {cacerts,            [binary()]} |
    {cert,               binary()}   |
    {key,                binary()}   |
    {versions,           [atom()]}   |
    {ciphers,            [binary()]} |
    {honor_cipher_order, boolean()}.

-doc """
Per-connection rate limit configuration for the NIF's time-spacing throttle.
The NIF uses a time-spacing algorithm that ensures a minimum time interval
between requests rather than a traditional token bucket.

* `rate_per_sec` - Maximum requests per second (required). The NIF enforces
  a minimum interval of `window_msec / rate_per_sec` milliseconds between
  successive requests on each connection.
* `window_msec` - Time window in milliseconds for rate calculation (default: 1000).
  This determines the precision of the time-spacing algorithm.

The throttling is applied at the NIF level in `arterial_nif:send_and_release/3`
where it checks `arterial::time_spacing_throttle::add/2` before allowing
requests to proceed. Failed throttle checks cause connection retry logic
in `arterial_client`.

## Examples

```erlang
#{rate_per_sec => 100, window_msec => 1000}   % 100 req/sec, 10ms intervals
#{rate_per_sec => 50,  window_msec => 500}    % 50 req/sec, 10ms intervals
#{rate_per_sec => 10}                         % 10 req/sec, default 1000ms window
```
""".
-type throttle_opts() :: #{
  rate_per_sec := pos_integer(),
  window_msec  => pos_integer()
}.

-doc """
Options accepted by `start_link/2`.

* `size` - number of connections in the pool (default: `1`); also the
  number of NIF stripes created (see the moduledoc's "Stripes, slots"
  section).
* `codec` - the `c:arterial_codec` callback module for this pool
  (required, no default).
* `address`/`addresses`/`ip`/`port` - connection address options (see
  `arterial_connection:address_entry/0` for advanced per-address configuration).
* `protocol` - connection protocol: `tcp` (default), `udp`, `ssl` for built-in
  NIF protocols, or a module implementing `c:arterial_protocol` for custom protocols.
* `nodelay` - sets `TCP_NODELAY` on TCP/SSL connections (default: `true`).
* `sock_opts` - list of `t:sockopt/0` options to apply to each connection
  (e.g., `[keepalive, {sndbuf, 8192}]`). These are processed by the NIF layer.
* `reconnect`/`reconnect_time` - reconnection behavior configuration.
* `default_timeout_ms` - default `call/3` timeout if the caller doesn't
  pass one (default: `5000`).
* `sweep_interval_ms` - how often `arterial_sweeper` evicts expired
  in-flight requests (default: `1000`).
* `bounce_interval_ms` - if set, starts an `arterial_bouncer` (default:
  `undefined`, disabled) -- see `arterial_bouncer`'s moduledoc.
* `bounce_drain_timeout_ms` - see `arterial_bouncer` (default: `30000`).
* `throttle` - `t:throttle_opts/0`, or `undefined` to disable per-
  connection rate limiting entirely (default: `undefined`). Uses time-spacing
  algorithm in the NIF.

## Examples

```
1> Opts = #{size => 4, codec => my_codec, address => "localhost", port => 9000}.
2> arterial_pool:start_link(my_pool, Opts).
{ok, <0.123.0>}
```
""".
-type options() :: #{
  size                    => pos_integer(),
  codec                   => module(),
  address                 => arterial:inet_address(),
  addresses               => [arterial_connection:address_entry(), ...],
  ip                      => arterial:inet_address(),
  port                    => arterial:inet_port(),
  protocol                => tcp | udp | ssl | module(),
  nodelay                 => boolean(),
  sock_opts               => [sockopt()],
  tls_options             => [tlsopt()],
  reconnect               => boolean(),
  reconnect_time          => arterial_connection:reconnect_time(),
  default_timeout_ms      => pos_integer(),
  sweep_interval_ms       => pos_integer(),
  bounce_interval_ms      => undefined | pos_integer(),
  bounce_drain_timeout_ms => pos_integer(),
  throttle                => undefined | throttle_opts()
}.

-export_type([name/0, options/0, throttle_opts/0, sockopt/0, tlsopt/0]).

-define(POOL_OPT_KEYS, [
  size, codec, default_timeout_ms, sweep_interval_ms,
  bounce_interval_ms, bounce_drain_timeout_ms, throttle
]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc """
Start the supervisor for pool `Name`: creates the pool's NIF resource
(one stripe per connection, see the moduledoc), the pool's public
correlation-id ETS table, then one `arterial_connection` worker per
connection, an `arterial_sweeper`, and (if `bounce_interval_ms` is set)
an `arterial_bouncer`.
""".
-spec start_link(name(), options()) -> {ok, pid()} | {error, term()}.
start_link(Name, Opts) when is_atom(Name), is_map(Opts) ->
  supervisor:start_link({local, sup_name(Name)}, ?MODULE, [Name, Opts]).

-doc """
Stop pool `Name`'s supervisor and release its bookkeeping (correlation
table, `persistent_term` entries). The pool's NIF resource itself is
plain-refcounted and is freed once nothing references it any more --
no separate destroy call is needed (compare `arterial_nif:destroy/1`).
""".
-spec stop(name()) -> ok.
stop(Name) ->
  case whereis(sup_name(Name)) of
    undefined -> ok;
    Pid       -> _ = supervisor:stop(Pid)
  end,
  case ets:whereis(corr_table(Name)) of
    undefined -> ok;
    _Tid      -> ets:delete(corr_table(Name))
  end,
  lists:foreach(fun(Key) -> persistent_term:erase(Key) end, [
    pool_key(Name), codec_key(Name), size_key(Name),
    timeout_key(Name), throttle_key(Name), avail_key(Name)
  ]),
  ok.

%%%-----------------------------------------------------------------------------
%%% supervisor callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Name, Opts]) ->
  #{
    size                    := Size,
    codec                   := Codec,
    default_timeout_ms      := DefaultTimeoutMs,
    sweep_interval_ms       := SweepIntervalMs,
    bounce_interval_ms      := BounceIntervalMs,
    bounce_drain_timeout_ms := BounceDrainTimeoutMs,
    throttle                := Throttle
  } = maps:merge(#{
    size                    => 1,
    default_timeout_ms      => 5000,
    sweep_interval_ms       => 1000,
    bounce_interval_ms      => undefined,
    bounce_drain_timeout_ms => 30000,
    throttle                => undefined
  }, Opts),

  {ok, PoolRef} = arterial_observe:span([nif, init_pool], #{pool => Name, size => Size}, fun() ->
    case arterial_nif:init_pool(Size, 1) of
      {ok, Ref} -> {{ok, Ref}, #{result => ok}};
      Error -> {Error, #{result => error}}
    end
  end),

  CorrTable = corr_table(Name),
  ets:new(CorrTable, [
    set, public, named_table,
    {read_concurrency, true}, {write_concurrency, true}
  ]),

  persistent_term:put(pool_key(Name),    PoolRef),
  persistent_term:put(codec_key(Name),   Codec),
  persistent_term:put(size_key(Name),    Size),
  persistent_term:put(timeout_key(Name), DefaultTimeoutMs),

  % Configure throttling in the NIF if enabled
  ThrottleState = case Throttle of
    undefined ->
      undefined;
    % New format with rate_per_sec and window_msec
    #{rate_per_sec := RPS, window_msec := WinMS}
        when is_integer(RPS), RPS > 0, is_integer(WinMS), WinMS > 0 ->
      ok   = arterial_nif:configure_throttle(PoolRef, RPS, WinMS),
      {RPS, WinMS};
    % New format with rate_per_sec only (default window)
    #{rate_per_sec := RPS} when is_integer(RPS), RPS > 0 ->
      WinMS = 1000,
      ok    = arterial_nif:configure_throttle(PoolRef, RPS, WinMS),
      {RPS, WinMS}
  end,
  persistent_term:put(throttle_key(Name), ThrottleState),
  %% All connections start unavailable (no socket yet); each
  %% arterial_connection worker flips its own bit once registered.
  persistent_term:put(avail_key(Name), atomics:new(Size, [{signed, false}])),

  ConnOpts = maps:without(?POOL_OPT_KEYS, Opts),
  ConnChildren = [
    #{
      id       => {arterial_connection, ConnID},
      start    => {arterial_connection, start_link, [Name, ConnID, ConnOpts]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [arterial_connection]
    }
    || ConnID <- lists:seq(0, Size - 1)
  ],

  SweeperChild = #{
    id       => arterial_sweeper,
    start    => {arterial_sweeper, start_link, [Name, SweepIntervalMs]},
    restart  => permanent,
    shutdown => 1000,
    type     => worker,
    modules  => [arterial_sweeper]
  },

  BouncerChildren = case BounceIntervalMs of
    undefined ->
      [];
    _ ->
      [#{
        id       => arterial_bouncer,
        start    => {arterial_bouncer, start_link, [Name, Size, BounceIntervalMs, BounceDrainTimeoutMs]},
        restart  => permanent,
        shutdown => 1000,
        type     => worker,
        modules  => [arterial_bouncer]
      }]
  end,

  {ok, {{one_for_one, 5, 10}, ConnChildren ++ [SweeperChild] ++ BouncerChildren}}.

%%%-----------------------------------------------------------------------------
%%% Accessors (used by arterial_client/arterial_connection/arterial_sweeper/
%%% arterial_bouncer)
%%%-----------------------------------------------------------------------------

-doc "The registered name of `Name`'s supervisor.".
-spec sup_name(name()) -> atom().
sup_name(Name) -> list_to_atom("arterial_pool_" ++ atom_to_list(Name)).

-doc "`Name`'s `arterial_nif` resource.".
-spec pool_ref(name()) -> arterial_nif:pool_ref().
pool_ref(Name) -> get_pt(pool_key(Name), Name).

-doc "`Name`'s configured `c:arterial_codec` callback module.".
-spec codec(name()) -> module().
codec(Name) -> get_pt(codec_key(Name), Name).

-doc "`Name`'s configured connection count (and stripe count).".
-spec size(name()) -> pos_integer().
size(Name) -> get_pt(size_key(Name), Name).

-doc "`Name`'s default `call/3` timeout.".
-spec default_timeout_ms(name()) -> pos_integer().
default_timeout_ms(Name) -> get_pt(timeout_key(Name), Name).

-doc "`Name`'s throttle state, or `undefined` if throttling is disabled. Returns `{RatePerSec, WindowMsec}`.".
-spec throttle(name()) -> undefined | {pos_integer(), pos_integer()}.
throttle(Name) -> get_pt(throttle_key(Name), Name).

-doc "The name of `Name`'s public correlation-id ETS table.".
-spec corr_table(name()) -> atom().
corr_table(Name) -> list_to_atom("arterial2_corr_" ++ atom_to_list(Name)).

-doc """
`Name`'s availability bitmap (`atomics`, one cell per `ConnID`, `1` =
available for new sends, `0` = no socket yet / disconnected / draining
for a bounce). Checked by `arterial_client` before picking a stripe.
""".
-spec avail_ref(name()) -> atomics:atomics_ref().
avail_ref(Name) -> get_pt(avail_key(Name), Name).

-doc "Mark connection `ConnID` of `Name` available for new sends.".
-spec set_available(name(), non_neg_integer()) -> ok.
set_available(Name, ConnID) ->
  atomics:put(avail_ref(Name), ConnID + 1, 1).

-doc "Mark connection `ConnID` of `Name` unavailable for new sends.".
-spec set_unavailable(name(), non_neg_integer()) -> ok.
set_unavailable(Name, ConnID) ->
  atomics:put(avail_ref(Name), ConnID + 1, 0).

-doc "Whether connection `ConnID` of `Name` is currently available for new sends.".
-spec is_available(name(), non_neg_integer()) -> boolean().
is_available(Name, ConnID) ->
  atomics:get(avail_ref(Name), ConnID + 1) =:= 1.

-doc """
Block until `all` connections or the first `N` connections in pool `Name` are available.

This function polls the pool's availability bitmap and blocks until the required
number of connections are established and ready for use. Useful for testing and
scenarios where you need to ensure connections are ready before proceeding.

The first argument can be either:
- `all` - wait for all connections in the pool to be available
- `N` (integer) - wait for the first N connections (0 through N-1) to be available

Defaults to a 5-second timeout with 20ms polling intervals.

## Examples

```erlang
%% Wait for all connections to be ready
ok = arterial_pool:wait_connected(my_pool, all).

%% Wait for the first 3 connections to be ready
ok = arterial_pool:wait_connected(my_pool, 3).

%% Wait for 2 connections with custom timeout
ok = arterial_pool:wait_connected(my_pool, 2, 10000).
```
""".
-spec wait_connected(name(), all | non_neg_integer()) -> ok | {error, timeout}.
wait_connected(Name, Connections) ->
  wait_connected(Name, Connections, 5000).

-doc "Like `wait_connected/2` but with a custom timeout in milliseconds.".
-spec wait_connected(name(), all | non_neg_integer(), timeout()) -> ok | {error, timeout}.
wait_connected(Name, all, TimeoutMs) ->
  try
    PoolSize = ?MODULE:size(Name),
    wait_connected(Name, PoolSize, TimeoutMs)
  catch
    error:{unknown_pool, _} -> {error, timeout}
  end;
wait_connected(Name, N, TimeoutMs) when is_integer(N), N >= 0 ->
  wait_connected_loop(Name, N, TimeoutMs, erlang:monotonic_time(millisecond)).

%% Internal function to implement the polling loop
wait_connected_loop(Name, N, TimeoutMs, StartTime) ->
  case check_connections_available(Name, N) of
    true ->
      ok;
    false ->
      Now = erlang:monotonic_time(millisecond),
      ElapsedMs = Now - StartTime,
      if
        ElapsedMs >= TimeoutMs ->
          {error, timeout};
        true ->
          timer:sleep(20),  % 20ms polling interval, same as existing tests
          wait_connected_loop(Name, N, TimeoutMs, StartTime)
      end
  end.

%% Check if the first N connections are available
check_connections_available(Name, N) ->
  try
    lists:all(fun(ConnID) ->
      is_available(Name, ConnID)
    end, lists:seq(0, N - 1))
  catch
    error:badarg -> false;  % Pool might not exist yet
    error:{unknown_pool, _} -> false  % Pool doesn't exist
  end.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

%% persistent_term:get/2's Default argument is evaluated eagerly (Erlang
%% has no laziness), so `persistent_term:get(Key, error(...))` would
%% always raise regardless of whether Key exists -- a sentinel value
%% (guaranteed never to be a real stored value, see throttle/1's own
%% legitimate `undefined`) plus a case is the correct lazy-default idiom.
get_pt(Key, Name) ->
  case persistent_term:get(Key, '$arterial_pool_undefined') of
    '$arterial_pool_undefined' -> error({unknown_pool, Name});
    Value -> Value
  end.

%% Throttle initialization is now handled directly in the NIF via configure_throttle/3

pool_key(Name)     -> {?MODULE, Name, pool_ref}.
codec_key(Name)    -> {?MODULE, Name, codec}.
size_key(Name)     -> {?MODULE, Name, size}.
timeout_key(Name)  -> {?MODULE, Name, default_timeout_ms}.
throttle_key(Name) -> {?MODULE, Name, throttle}.
avail_key(Name)    -> {?MODULE, Name, avail_ref}.
