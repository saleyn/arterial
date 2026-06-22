-module(arterial_nif).

-moduledoc """
NIF bindings to the C++ connection-pool engine: pool lifecycle,
checkout/checkin, and asynchronous queue-when-busy.

This module is the low-level engine used internally by `arterial_pool`,
`arterial_connection`, and `arterial_client`. It only ever tracks pool-wide
connection *capacity* -- which connections are enabled, and whether each
has room for one more in-flight request -- as a single atomic counter per
connection. It has no visibility into individual requests, their wire-level
ids, demuxing, or timeouts: that's entirely the job of the connection's
`arterial_conn_owner` process, one per connection, which is the only thing
that ever does socket I/O.

Every connection reservation made via `checkout_connection/2,3` or
`checkout_async/2,3` monitors the calling process for as long as the
reservation is outstanding. If that process dies before calling
`checkin_connection/2,3`, the C++ pool's resource `down` callback releases
the reserved slot(s) and makes the connection available again --
equivalent to that process having called `checkin_connection/2` itself --
so a crashing caller can never permanently strand a connection. This is the
same semaphore-with-process-monitoring pattern used by
[erlsem](https://github.com/x0id/erlsem)'s `sema_nif.cpp` (atomic count +
monitor-on-first-acquire + release-on-process-death), scoped per
connection instead of one global counter.
""".

-export([create/3, create/4, create/5]).
-export([destroy/1]).
-export([checkin_connection/2, checkin_connection/3]).
-export([checkout_connection/2, checkout_connection/3]).
-export([checkout_async/2, checkout_async/3]).
-export([make_available/2, make_unavailable/2, connection_drained/2]).
-export([start_link/0]).

-on_load(init/0).

-define(LIBNAME, arterial).
-define(NOT_LOADED_ERROR,
  erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})).

-doc "The atom identifying a pool, as passed to `arterial_pool:start_link/2`.".
-type pool() :: atom().

-doc """
How the caller intends to use a checked-out connection. Purely
informational (see `checkout_connection/2`); does not change pool
behavior.
""".
-type mode() :: sync | async.

-export_type([pool/0, mode/0]).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

-doc "Equivalent to `create/4` with `MaxWaiters = 0` (queue-when-busy disabled).".
-spec create(pool(), pos_integer(), pos_integer()) -> ok.
create(Pool, Size, Backlog) ->
  create(Pool, Size, Backlog, 0).

-doc "Equivalent to `create/5` with `TtlShards = nproc`.".
-spec create(pool(), pos_integer(), pos_integer(), non_neg_integer()) -> ok.
create(Pool, Size, Backlog, MaxWaiters) ->
  create(Pool, Size, Backlog, MaxWaiters, nproc).

-doc """
Create a pool of `Size` connections, each with a backlog capacity of
`Backlog` in-flight requests. Connections have no socket and are
unavailable for checkout until `make_available/2` is called on each one
(see `arterial_connection`'s reconnect logic and
`arterial_conn_owner:set_socket/3`).

`MaxWaiters` bounds the queue-when-busy feature used by `checkout_async/2,3`:
`0` disables it (a checkout attempt with no connection available fails
immediately, like `checkout_connection/2` does); a positive value allows
up to that many callers to be queued while every connection is busy, each
serviced (or timed out) later.

`TtlShards` controls the shard count of the pool's internal queued-waiter
registry (see `c_src/sharded_ttl_map.hpp`):

* `nproc` (the default) picks a shard count scaled to
  `std::thread::hardware_concurrency()` on the C++ side.
* `{nproc, N}` multiplies that same core count by `N` instead.
* A positive integer overrides this directly with an exact shard count.

## Examples

```
1> arterial_nif:create(my_pool, 4, 16, 100, nproc).
ok
```
""".
-spec create(pool(), pos_integer(), pos_integer(), non_neg_integer(),
              non_neg_integer() | nproc | {nproc, pos_integer()}) -> ok.
create(Pool, Size, Backlog, MaxWaiters, TtlShards)
    when is_atom(Pool), is_integer(Size), Size > 0,
         is_integer(Backlog), Backlog > 0,
         is_integer(MaxWaiters), MaxWaiters >= 0 ->
  TtlShards1 = ttl_shards(TtlShards),
  {ok, Resource} = create_pool(Size, Backlog, MaxWaiters, TtlShards1),
  persistent_term:put(pool_key(Pool), Resource),
  ok.

%% `nproc` -> 0, which create_pool/3 (the create_nif NIF) reads as "pick
%% ShardedTTLMap::DefaultShardCount() based on
%% std::thread::hardware_concurrency()". Resolving plain `nproc` to an
%% actual number here instead would just duplicate that core-count logic
%% on the Erlang side with a different (and possibly inconsistent)
%% source of "how many cores" -- simplest to let the C++ side own that
%% one default. `{nproc, N}` can't defer to C++ the same way (it needs
%% its own core count, not DefaultShardCount()'s fixed multiplier), so it
%% resolves the core count on the Erlang side via
%% erlang:system_info/1 instead.
ttl_shards(nproc) ->
  0;
ttl_shards({nproc, N}) when is_integer(N), N > 0 ->
  Cores = case erlang:system_info(logical_processors_available) of
    unknown -> erlang:system_info(logical_processors);
    Known   -> Known
  end,
  N * max(1, Cores);
ttl_shards(N) when is_integer(N), N >= 0 ->
  N.

-doc """
Destroy `Pool`'s NIF resource. The pool's supervisor and connection workers
must already be stopped; this does not stop any processes itself.

## Examples

```
1> arterial_nif:destroy(my_pool).
ok
```
""".
-spec destroy(pool()) -> ok.
destroy(Pool) when is_atom(Pool) ->
  Resource = resource(Pool),
  persistent_term:erase(pool_key(Pool)),
  destroy_pool(Resource).

-doc """
Mark connection `ConnID` of `Pool` as available for checkout. Called by the
connection worker once the matching `arterial_conn_owner` process has the
live socket (see `arterial_connection`'s reconnect logic).

## Examples

```
1> arterial_nif:make_available(my_pool, 0).
true
```
""".
-spec make_available(pool(), non_neg_integer()) -> boolean().
make_available(Pool, ConnID) ->
  make_available_nif(resource(Pool), ConnID).

-doc """
Mark connection `ConnID` of `Pool` as unavailable for checkout (e.g. the
connection just dropped). Called by the connection worker before
disconnecting.

## Examples

```
1> arterial_nif:make_unavailable(my_pool, 0).
true
```
""".
-spec make_unavailable(pool(), non_neg_integer()) -> boolean().
make_unavailable(Pool, ConnID) ->
  make_unavailable_nif(resource(Pool), ConnID).

-doc """
Returns `true` if connection `ConnID` of `Pool` currently has zero
in-flight requests -- i.e. it's safe to disconnect without abandoning a
pending request. Used to implement a disconnect/"bounce" cycle: mark the
connection unavailable (see `make_unavailable/2`) so no new request
selects it, poll this until it drains, then disconnect and reconnect.

## Examples

```
1> arterial_nif:connection_drained(my_pool, 0).
true
```
""".
-spec connection_drained(pool(), non_neg_integer()) -> boolean().
connection_drained(Pool, ConnID) ->
  connection_drained_nif(resource(Pool), ConnID).

-doc "Equivalent to `checkout_connection/3` with `Samples = 1`.".
-spec checkout_connection(pool(), mode()) ->
  {ok, non_neg_integer()} | {error, no_connection}.
checkout_connection(Pool, Mode) when Mode =:= sync; Mode =:= async ->
  checkout_connection(Pool, Mode, 1).

-doc """
Check out a connection able to accept `Samples` new requests in one shot
-- i.e. reserve `Samples` backlog slots on a single connection, so the
caller can multiplex that many concurrently outstanding requests on it via
the connection's `arterial_conn_owner` process. This only succeeds if one
connection currently has `Samples` free slots AND is itself available; it
never spreads `Samples` across multiple connections. `Mode` does not
change the underlying reservation; it only selects how the caller intends
to use the connection (kept for callers that want to log/instrument sync
vs async use).

The calling process is monitored for as long as it holds the returned
reservation: if it dies before calling `checkin_connection/2,3`, all
`Samples` reserved slots are released and the connection is made available
again automatically.

## Examples

```
1> arterial_nif:checkout_connection(my_pool, sync, 3).
{ok, 0}
```
""".
-spec checkout_connection(pool(), mode(), pos_integer()) ->
  {ok, non_neg_integer()} | {error, no_connection}.
checkout_connection(Pool, Mode, Samples)
    when (Mode =:= sync orelse Mode =:= async), is_integer(Samples), Samples > 0 ->
  checkout_nif(resource(Pool), self(), Samples).

-doc "Equivalent to `checkin_connection/3` with `Samples = 1`.".
-spec checkin_connection(pool(), non_neg_integer()) -> ok.
checkin_connection(Pool, ConnID) ->
  checkin_connection(Pool, ConnID, 1).

-doc """
Release `Samples` previously reserved slots on connection `ConnID` of
`Pool` back to the pool, then try to service the head of `Pool`'s
queue-when-busy wait-list (see `checkout_async/2,3`) now that capacity may
have freed up.

Must be called by the same process that checked the connection out (see
`checkout_connection/2,3`, `checkout_async/2,3`), since that's the process
this call's matching `checkin_nif` call releases bookkeeping for.

## Examples

```
1> arterial_nif:checkin_connection(my_pool, 0, 3).
ok
```
""".
-spec checkin_connection(pool(), non_neg_integer(), pos_integer()) -> ok.
checkin_connection(Pool, ConnID, Samples)
    when is_integer(Samples), Samples > 0 ->
  checkin_nif(resource(Pool), ConnID, Samples, self(), Pool).

-doc "Equivalent to `checkout_async/3` with `Samples = 1`.".
-spec checkout_async(pool(), pid()) ->
  {ok, non_neg_integer()} | {queued, non_neg_integer()} | {error, no_connection}.
checkout_async(Pool, Pid) ->
  checkout_async(Pool, Pid, 1).

-doc """
Check out a connection for asynchronous use with `Samples` backlog slots
reserved on it in one shot (see `checkout_connection/3` for what
multiplexing `Samples > 1` slots on a single connection means), queuing
the request (up to `Pool`'s `MaxWaiters`, see `create/3,4`) if no
connection currently has `Samples` free slots instead of failing
immediately.

Three outcomes:

- `{ok, ConnID}` -- a connection was available immediately, exactly like
  `checkout_connection/3`.
- `{queued, WaiterID}` -- no connection currently had `Samples` free
  slots, so the request was queued; the caller's process will later
  receive either `{arterial_ready, Pool, ConnID}` or `{arterial_timeout,
  Pool, WaiterID}` if it times out first while still queued. `WaiterID`
  (an opaque internal id) is only ever used to match that eventual
  message back to this call.
- `{error, no_connection}` -- no connection qualified AND the wait-list
  was disabled (`MaxWaiters = 0`) or already full.

## Examples

```
1> arterial_nif:checkout_async(my_pool, self(), 1).
{ok, 0}
2> arterial_nif:checkout_async(my_pool, self(), 1). % all connections busy
{queued, 7}
```
""".
-spec checkout_async(pool(), pid(), pos_integer()) ->
  {ok, non_neg_integer()} | {queued, non_neg_integer()} | {error, no_connection}.
checkout_async(Pool, Pid, Samples)
    when is_pid(Pid), is_integer(Samples), Samples > 0 ->
  checkout_async_nif(resource(Pool), Pid, Samples, Pool).

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

pool_key(Pool) -> {?MODULE, Pool, resource}.

resource(Pool) ->
  case persistent_term:get(pool_key(Pool), undefined) of
    undefined -> error({unknown_pool, Pool});
    Resource  -> Resource
  end.

-doc """
No-op kept only for supervision-tree compatibility: earlier versions of
this module owned a singleton process here (a per-pool ETS buffer table
owner). Connection-local buffering now lives entirely inside each
connection's `arterial_conn_owner` process, so there is nothing left for
this process to own.
""".
-spec start_link() -> {ok, pid()}.
start_link() ->
  {ok, spawn_link(fun park/0)}.

-spec park() -> no_return().
park() ->
  receive after infinity -> ok end.

%%%-----------------------------------------------------------------------------
%%% NIF functions
%%%-----------------------------------------------------------------------------

create_pool(_Size, _Backlog, _MaxWaiters, _TtlShards) -> ?NOT_LOADED_ERROR.
destroy_pool(_Rsrc)                                -> ?NOT_LOADED_ERROR.
make_available_nif(_Rsrc, _ConnID)                 -> ?NOT_LOADED_ERROR.
make_unavailable_nif(_Rsrc, _ConnID)                -> ?NOT_LOADED_ERROR.
connection_drained_nif(_Rsrc, _ConnID)               -> ?NOT_LOADED_ERROR.
checkout_nif(_Rsrc, _Pid, _Samples)                 -> ?NOT_LOADED_ERROR.
checkin_nif(_Rsrc, _ConnID, _Samples, _Pid, _Pool)  -> ?NOT_LOADED_ERROR.
checkout_async_nif(_Rsrc, _Pid, _Samples, _Pool)    -> ?NOT_LOADED_ERROR.

init() ->
  SoName  =
    case code:priv_dir(?LIBNAME) of
      {error, bad_name} ->
        case code:which(?MODULE) of
          Filename when is_list(Filename) ->
            Dir = filename:dirname(filename:dirname(Filename)),
            filename:join([Dir, "priv", "arterial"]);
          _ ->
            filename:join("../priv", "arterial")
        end;
      Dir ->
        filename:join(Dir, "arterial")
  end,
  erlang:load_nif(SoName, 0).
