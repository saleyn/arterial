-module(arterial_pool).

-moduledoc """
Per-pool supervisor: creates the pool's NIF resource and supervises one
`arterial_connection` worker per connection slot plus an `arterial_sweeper`.

Typically started as a child of the `arterial_app` top-level supervisor
(via `arterial_app:start/0` + a `simple_one_for_one` child spec), but can
also be started standalone with `start_link/2` for tests or applications
that manage their own supervision tree.
""".

-behaviour(supervisor).

-export([start_link/2]).
-export([init/1]).

-doc "The atom identifying a pool; shared with `t:arterial_nif:pool/0`.".
-type name() :: atom().

-doc """
Options accepted by `start_link/2`. All keys are optional and fall back to
the defaults shown in the example below.

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
  size              => pos_integer(),
  backlog           => pos_integer(),
  fifo              => boolean(),
  fixed_timeout_us  => non_neg_integer(),
  sweep_interval_ms => pos_integer(),
  ttl_shards        => non_neg_integer() | nproc | {nproc, pos_integer()},
  protocol          => arterial:protocol(),
  client            => arterial:client(),
  client_opts       => arterial_client:options()
}.

-export_type([name/0, options/0]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc """
Start the supervisor for pool `Name`: creates the pool's NIF resource,
then starts one `arterial_connection` worker per connection slot plus an
`arterial_sweeper` that periodically evicts timed-out in-flight async
requests (see `arterial_nif:track_inflight/5`).

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
    size              := Size,
    backlog           := Backlog,
    fifo              := Fifo,
    fixed_timeout_us  := FixedTimeoutUs,
    sweep_interval_ms := SweepIntervalMs,
    ttl_shards        := TtlShards,
    protocol          := Protocol,
    client            := Client,
    client_opts       := ClientOpts
  } = maps:merge(#{
    size              => 1,
    backlog           => 1,
    fifo              => true,
    fixed_timeout_us  => 0,
    sweep_interval_ms => 1000,
    ttl_shards        => nproc,
    client_opts       => #{}
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

  {ok, {{one_for_one, 5, 10}, ConnChildren ++ [SweeperChild]}}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

sup_name(Name) -> list_to_atom("arterial_pool_" ++ atom_to_list(Name)).
