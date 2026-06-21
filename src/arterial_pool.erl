-module(arterial_pool).

-moduledoc """
Per-pool supervisor: creates the pool's NIF resource and supervises one
`arterial_connection` worker per connection slot, an `arterial_sweeper`,
and (only if `bounce_interval_ms` is configured) an `arterial_bouncer`.

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
* `fifo` - backlog/reply ordering per connection; `true` assumes replies
  arrive in the same order requests were sent, `false` allows out-of-order
  replies matched by request id (default: `true`).
* `fixed_timeout_us` - shared TTL (microseconds) for every in-flight
  request; `0` means each request uses its own TTL instead (default: `0`).
* `sweep_interval_ms` - how often `arterial_sweeper` evicts expired
  in-flight requests (default: `1000`).
* `ttl_shards` - shard count for the in-flight/waiter registries; see
  `arterial_nif:create/8` (default: `nproc`, value can be an integer,
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
1> Opts = #{size => 4, backlog => 16, fifo => true, fixed_timeout_us => 0,
2>          sweep_interval_ms => 1000, protocol => my_protocol,
3>          client => my_client, client_opts => #{address => "localhost", port => 9000}}.
#{size => 4,backlog => 16,fifo => true,fixed_timeout_us => 0,
  sweep_interval_ms => 1000,protocol => my_protocol,client => my_client,
  client_opts => #{address => "localhost",port => 9000}}
```
""".
-type options() :: #{
  size                   => pos_integer(),
  backlog                => pos_integer(),
  fifo                   => boolean(),
  fixed_timeout_us       => non_neg_integer(),
  sweep_interval_ms      => pos_integer(),
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
then starts one `arterial_connection` worker per connection slot plus an
`arterial_sweeper` that periodically evicts timed-out in-flight async
requests (see `arterial_nif:track_inflight/5`), and, if
`bounce_interval_ms` is set, an `arterial_bouncer` that periodically
recycles connections one at a time (see `arterial_bouncer`).

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
    fixed_timeout_us        := FixedTimeoutUs,
    sweep_interval_ms       := SweepIntervalMs,
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
    fixed_timeout_us        => 0,
    sweep_interval_ms       => 1000,
    ttl_shards              => nproc,
    bounce_interval_ms      => undefined,
    bounce_drain_timeout_ms => 30000,
    client_opts             => #{}
  }, Opts),

  ok = arterial_nif:create(Name, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, 0, TtlShards),

  ConnChildren = [
    #{
      id       => {arterial_connection, ConnID},
      start    => {arterial_connection, start_link, [Name, ConnID, Client, ClientOpts]},
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

-doc """
The registered name of `Name`'s supervisor, as started by `start_link/2`.
Used by `arterial_bouncer` to look up a pool's `arterial_connection`
worker pids via `supervisor:which_children/1`.
""".
-spec sup_name(name()) -> atom().
sup_name(Name) -> list_to_atom("arterial_pool_" ++ atom_to_list(Name)).
