-module(arterial_pool2).

-moduledoc """
Per-pool supervisor for `arterial`'s second connection-pool backend: raw
socket I/O lives inside `arterial_nif_pool` (the NIF), correlation-id
dispatch lives in a public ETS table this module owns, and everything
else (connect/reconnect, decode/dispatch, disconnect notification,
sweeping, bouncing) is plain Erlang on top -- see `arterial_nif_pool`'s
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
*inside* the NIF (see `arterial_nif_pool`), invoked directly from
whichever Erlang process needs them, and decouples "send a request" from
"receive its reply" entirely: `arterial_client2:call/3` writes inline in
the caller's own process (no intermediate `arterial_connection2` hop to
reach the socket), then blocks in a plain `receive` for a reply message
sent directly to its mailbox once `arterial_connection2` decodes it off
the wire -- no per-request backlog/checkout accounting at all. The
tradeoff: this only supports protocol matching modes 1/2 (native or
surrogate wire-level request id; see `arterial_codec2`'s moduledoc) --
there is no FIFO/no-request-id backlog mode here, and (for now) no
queue-when-busy wait-list equivalent to `arterial_nif:checkout_async/3`.

## Stripes, slots, and why this module always uses one slot per stripe

`arterial_nif_pool`'s pool resource is organized into `Stripes` (each one
independent lock-free `uint64` lease bitmask) of up to 64 `Slots` (real
sockets) each -- multiple slots sharing one stripe let several physical
connections share one atomic's contention, at the cost of the NIF having
to pick which slot services a given write. `arterial_pool2` always
creates exactly one stripe per connection, with exactly one slot in it
(`arterial_nif_pool:init_pool(Size, 1)`): every connection already gets
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
-export([avail_ref/1, set_available/2, set_unavailable/2, is_available/2]).

-doc "The atom identifying a pool; shared with `arterial_nif_pool:pool_ref/0`'s owner.".
-type name() :: atom().

-doc """
Per-connection rate limit, checked by `arterial_client2` before every
`send_and_release/3` -- see `arterial_throttle2`.
""".
-type throttle_opts() :: #{rate := pos_integer(), burst := pos_integer()}.

-doc """
Options accepted by `start_link/2`.

* `size` - number of connections in the pool (default: `1`); also the
  number of NIF stripes created (see the moduledoc's "Stripes, slots"
  section).
* `codec` - the `c:arterial_codec2` callback module for this pool
  (required, no default).
* `address`/`addresses`/`ip`/`port`/`nodelay`/`reconnect`/
  `reconnect_time` - connection options; `nodelay` sets `TCP_NODELAY`
  on every connection (default: `true`) -- there's no `socket_options`/
  `protocol => udp | ssl` here (unlike `t:arterial_client:options/0`),
  since `arterial_nif_pool:connect/7` opens a plain IPv4 TCP socket
  itself rather than accepting an arbitrary pre-configured one; see
  `arterial_connection2`'s moduledoc.
* `default_timeout_ms` - default `call/3` timeout if the caller doesn't
  pass one (default: `5000`).
* `sweep_interval_ms` - how often `arterial_sweeper2` evicts expired
  in-flight requests (default: `1000`).
* `bounce_interval_ms` - if set, starts an `arterial_bouncer2` (default:
  `undefined`, disabled) -- see `arterial_bouncer2`'s moduledoc.
* `bounce_drain_timeout_ms` - see `arterial_bouncer2` (default: `30000`).
* `throttle` - `t:throttle_opts/0`, or `undefined` to disable per-
  connection rate limiting entirely (default: `undefined`).

## Examples

```
1> Opts = #{size => 4, codec => my_codec, address => "localhost", port => 9000}.
2> arterial_pool2:start_link(my_pool, Opts).
{ok, <0.123.0>}
```
""".
-type options() :: #{
  size                    => pos_integer(),
  codec                   => module(),
  address                 => arterial:inet_address(),
  addresses               => [arterial_connection2:address_entry(), ...],
  ip                      => arterial:inet_address(),
  port                    => arterial:inet_port(),
  nodelay                 => boolean(),
  reconnect               => boolean(),
  reconnect_time          => arterial_connection:reconnect_time(),
  default_timeout_ms      => pos_integer(),
  sweep_interval_ms       => pos_integer(),
  bounce_interval_ms      => undefined | pos_integer(),
  bounce_drain_timeout_ms => pos_integer(),
  throttle                => undefined | throttle_opts()
}.

-export_type([name/0, options/0, throttle_opts/0]).

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
correlation-id ETS table, then one `arterial_connection2` worker per
connection, an `arterial_sweeper2`, and (if `bounce_interval_ms` is set)
an `arterial_bouncer2`.
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

  {ok, PoolRef} = arterial_nif_pool:init_pool(Size, 1),

  CorrTable = corr_table(Name),
  ets:new(CorrTable, [
    set, public, named_table,
    {read_concurrency, true}, {write_concurrency, true}
  ]),

  persistent_term:put(pool_key(Name), PoolRef),
  persistent_term:put(codec_key(Name), Codec),
  persistent_term:put(size_key(Name), Size),
  persistent_term:put(timeout_key(Name), DefaultTimeoutMs),
  persistent_term:put(throttle_key(Name), init_throttle(Size, Throttle)),
  %% All connections start unavailable (no socket yet); each
  %% arterial_connection2 worker flips its own bit once registered.
  persistent_term:put(avail_key(Name), atomics:new(Size, [{signed, false}])),

  ConnOpts = maps:without(?POOL_OPT_KEYS, Opts),
  ConnChildren = [
    #{
      id       => {arterial_connection2, ConnID},
      start    => {arterial_connection2, start_link, [Name, ConnID, ConnOpts]},
      restart  => permanent,
      shutdown => 5000,
      type     => worker,
      modules  => [arterial_connection2]
    }
    || ConnID <- lists:seq(0, Size - 1)
  ],

  SweeperChild = #{
    id       => arterial_sweeper2,
    start    => {arterial_sweeper2, start_link, [Name, SweepIntervalMs]},
    restart  => permanent,
    shutdown => 1000,
    type     => worker,
    modules  => [arterial_sweeper2]
  },

  BouncerChildren = case BounceIntervalMs of
    undefined ->
      [];
    _ ->
      [#{
        id       => arterial_bouncer2,
        start    => {arterial_bouncer2, start_link, [Name, Size, BounceIntervalMs, BounceDrainTimeoutMs]},
        restart  => permanent,
        shutdown => 1000,
        type     => worker,
        modules  => [arterial_bouncer2]
      }]
  end,

  {ok, {{one_for_one, 5, 10}, ConnChildren ++ [SweeperChild] ++ BouncerChildren}}.

%%%-----------------------------------------------------------------------------
%%% Accessors (used by arterial_client2/arterial_connection2/arterial_sweeper2/
%%% arterial_bouncer2)
%%%-----------------------------------------------------------------------------

-doc "The registered name of `Name`'s supervisor.".
-spec sup_name(name()) -> atom().
sup_name(Name) -> list_to_atom("arterial_pool2_" ++ atom_to_list(Name)).

-doc "`Name`'s `arterial_nif_pool` resource.".
-spec pool_ref(name()) -> arterial_nif_pool:pool_ref().
pool_ref(Name) -> get_pt(pool_key(Name), Name).

-doc "`Name`'s configured `c:arterial_codec2` callback module.".
-spec codec(name()) -> module().
codec(Name) -> get_pt(codec_key(Name), Name).

-doc "`Name`'s configured connection count (and stripe count).".
-spec size(name()) -> pos_integer().
size(Name) -> get_pt(size_key(Name), Name).

-doc "`Name`'s default `call/3` timeout.".
-spec default_timeout_ms(name()) -> pos_integer().
default_timeout_ms(Name) -> get_pt(timeout_key(Name), Name).

-doc "`Name`'s throttle state, or `undefined` if throttling is disabled.".
-spec throttle(name()) -> undefined | {atomics:atomics_ref(), pos_integer(), pos_integer()}.
throttle(Name) -> get_pt(throttle_key(Name), Name).

-doc "The name of `Name`'s public correlation-id ETS table.".
-spec corr_table(name()) -> atom().
corr_table(Name) -> list_to_atom("arterial2_corr_" ++ atom_to_list(Name)).

-doc """
`Name`'s availability bitmap (`atomics`, one cell per `ConnID`, `1` =
available for new sends, `0` = no socket yet / disconnected / draining
for a bounce). Checked by `arterial_client2` before picking a stripe.
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

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

%% persistent_term:get/2's Default argument is evaluated eagerly (Erlang
%% has no laziness), so `persistent_term:get(Key, error(...))` would
%% always raise regardless of whether Key exists -- a sentinel value
%% (guaranteed never to be a real stored value, see throttle/1's own
%% legitimate `undefined`) plus a case is the correct lazy-default idiom.
get_pt(Key, Name) ->
  case persistent_term:get(Key, '$arterial_pool2_undefined') of
    '$arterial_pool2_undefined' -> error({unknown_pool, Name});
    Value -> Value
  end.

init_throttle(_Size, undefined) ->
  undefined;
init_throttle(Size, #{rate := Rate, burst := Burst})
    when is_integer(Rate), Rate > 0, is_integer(Burst), Burst > 0 ->
  {arterial_throttle2:new(Size), Rate, Burst}.

pool_key(Name)     -> {?MODULE, Name, pool_ref}.
codec_key(Name)    -> {?MODULE, Name, codec}.
size_key(Name)     -> {?MODULE, Name, size}.
timeout_key(Name)  -> {?MODULE, Name, default_timeout_ms}.
throttle_key(Name) -> {?MODULE, Name, throttle}.
avail_key(Name)    -> {?MODULE, Name, avail_ref}.
