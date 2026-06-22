-module(arterial_pool).

-moduledoc """
Per-pool supervisor: creates the pool's NIF resource and supervises an
`arterial_pool_guard` (reacts to a connection worker crashing by forcing
it unavailable in the NIF immediately, see that module's moduledoc),
then, per connection slot, an `arterial_conn_owner` (the only process
that ever touches that connection's raw socket) followed by its matching
`arterial_connection` worker (connect/reconnect/backoff lifecycle), plus
(only if `bounce_interval_ms` is configured) an `arterial_bouncer`.

Typically started as a child of the `arterial_app` top-level supervisor
(via `arterial_app:start/0` + a `simple_one_for_one` child spec), but can
also be started standalone with `start_link/2` for tests or applications
that manage their own supervision tree.
""".

-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).
-export([sup_name/1]).

-doc "The atom identifying a pool; shared with `t:arterial_nif:pool/0`.".
-type name() :: atom().

-doc """
Options accepted by `start_link/2`. All keys are optional and fall back to
the defaults shown in the example below.

* `size` - number of connections in the pool (default: `1`).
* `backlog` - max in-flight requests per connection (default: `1`).
* `fifo` - reply demux mode used by each connection's `arterial_conn_owner`;
  `true` assumes replies arrive in the same order requests were sent (no
  wire-level request id needed), `false` matches replies to requests by
  id instead (default: `true`).
* `ttl_shards` - shard count for the queue-when-busy waiter registry; see
  `arterial_nif:create/5` (default: `nproc`, value can be an integer,
  a tuple `{nproc, Times::integer()}`, or `nproc`, where `nproc` is the
  number of cores).
* `bounce_interval_ms` - if set, starts an `arterial_bouncer` that
  recycles one connection (disconnect + reconnect) every this many
  milliseconds, round-robin across the pool; `undefined` disables
  periodic bouncing entirely (default: `undefined`). Useful behind a
  Kubernetes `Service`/load balancer that performs DNS- or
  connection-level balancing across a `Deployment`'s pods: without
  this, a long-lived pool's connections are resolved/balanced once at
  connect time and then never re-balanced, so they go stale -- pods
  added by autoscaling never receive traffic, and pods removed during
  scale-down leave connections pinned to an endpoint that's gone.
  Periodic bouncing forces each connection to re-resolve/re-balance on
  its next reconnect, keeping load spread evenly across whatever set of
  backend pods currently exists.
* `bounce_drain_timeout_ms` - how long a single connection's bounce waits
  for its backlog to drain of in-flight requests before forcing the
  disconnect anyway; only meaningful if `bounce_interval_ms` is set
  (default: `30000`).
* `protocol` - the `c:arterial_protocol` callback module for this pool.
* `client` - the `c:arterial_client` callback module for this pool.
* `client_opts` - opaque options passed to `client`'s `setup/2` (default:
  `#{}`).

## Examples

```
1> Opts = #{size => 4, backlog => 16, fifo => true,
2>          protocol => my_protocol,
3>          client => my_client, client_opts => #{address => "localhost", port => 9000}}.
#{size => 4,backlog => 16,fifo => true,
  protocol => my_protocol,client => my_client,
  client_opts => #{address => "localhost",port => 9000}}
```
""".
-type options() :: #{
  size                   => pos_integer(),
  backlog                => pos_integer(),
  fifo                   => boolean(),
  ttl_shards             => non_neg_integer() | nproc | {nproc, pos_integer()},
  bounce_interval_ms     => undefined | pos_integer(),
  bounce_drain_timeout_ms => pos_integer(),
  protocol               => arterial:protocol(),
  client                 => arterial:client(),
  client_opts            => arterial_client:options()
}.

-export_type([name/0, options/0]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc """
Start the supervisor for pool `Name`: creates the pool's NIF resource,
then starts, per connection slot, an `arterial_conn_owner` (the only
process that ever touches that connection's raw socket -- see its
moduledoc) followed by its matching `arterial_connection` worker
(connect/reconnect/backoff lifecycle), and, if `bounce_interval_ms` is
set, an `arterial_bouncer` that periodically recycles connections one at
a time (see `arterial_bouncer`).

## Examples

```
1> arterial_pool:start_link(my_pool,
2>   #{size => 4, protocol => my_protocol,
3>     client => my_client,
4>     client_opts => #{address => "localhost", port => 9000}}).
{ok,<0.120.0>}
```
""".
-spec start_link(name(), options()) -> {ok, pid()} | {error, term()}.
start_link(Name, Opts) when is_atom(Name), is_map(Opts) ->
  supervisor:start_link({local, sup_name(Name)}, ?MODULE, [Name, Opts]).

%%%-----------------------------------------------------------------------------
%%% supervisor callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Name, Opts]) ->
  #{
    size                    := Size,
    backlog                 := Backlog,
    fifo                    := Fifo,
    ttl_shards              := TtlShards,
    bounce_interval_ms      := BounceIntervalMs,
    bounce_drain_timeout_ms := BounceDrainTimeoutMs,
    protocol                := Protocol,
    client                  := Client,
    client_opts             := ClientOpts
  } = maps:merge(#{
    size                    => 1,
    backlog                 => 1,
    fifo                    => true,
    ttl_shards              => nproc,
    bounce_interval_ms      => undefined,
    bounce_drain_timeout_ms => 30000,
    client_opts             => #{}
  }, Opts),

  ok = arterial_nif:create(Name, Size, Backlog, 0, TtlShards),

  %% Started before every connection slot's children, so it's already
  %% monitoring (or, for the very first batch, about to start polling
  %% for) each one before any of them can possibly crash -- see
  %% arterial_pool_guard's moduledoc for why this needs to exist at all.
  GuardChild = #{
    id       => arterial_pool_guard,
    start    => {arterial_pool_guard, start_link, [Name, Size]},
    restart  => permanent,
    shutdown => 1000,
    type     => worker,
    modules  => [arterial_pool_guard]
  },

  %% Each connection slot's owner must already be registered before its
  %% arterial_connection worker starts (the worker publishes its socket
  %% to the owner as soon as it connects, see arterial_connection:
  %% client_init/1) -- interleave owner/connection pairs rather than
  %% starting every owner first, so a supervisor restart of one
  %% connection's pair never depends on every other slot's owner already
  %% being up.
  ConnChildren = lists:flatmap(fun(ConnID) ->
    [
      #{
        id       => {arterial_conn_owner, ConnID},
        start    => {arterial_conn_owner, start_link, [Name, ConnID, Protocol, Fifo]},
        restart  => permanent,
        shutdown => 1000,
        type     => worker,
        modules  => [arterial_conn_owner]
      },
      #{
        id       => {arterial_connection, ConnID},
        start    => {arterial_connection, start_link, [Name, ConnID, Client, ClientOpts]},
        restart  => permanent,
        shutdown => 5000,
        type     => worker,
        modules  => [arterial_connection]
      }
    ]
  end, lists:seq(0, Size - 1)),

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

  {ok, {{one_for_one, 5, 10}, [GuardChild] ++ ConnChildren ++ BouncerChildren}}.

-doc """
The registered name of `Name`'s supervisor, as started by `start_link/2`.
Used by `arterial_bouncer` to look up a pool's `arterial_connection`
worker pids via `supervisor:which_children/1`.
""".
-spec sup_name(name()) -> atom().
sup_name(Name) -> list_to_atom("arterial_pool_" ++ atom_to_list(Name)).
