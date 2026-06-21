-module(arterial_nif).

-moduledoc """
NIF bindings to the C++ connection-pool engine: pool lifecycle, checkout/
checkin, asynchronous queue-when-busy, and in-flight request timeout
tracking/sweeping.

This module is the low-level engine used internally by `arterial_pool`,
`arterial_connection`, and `arterial_client`; most users of the library
only need `arterial_client:call/3` (synchronous) or this module's
`checkout_async/3` + `track_inflight/5` (asynchronous) directly when
implementing their own asynchronous request path.

Every connection reservation made via `checkout_connection/2` or
`checkout_async/3` monitors the calling process for as long as the
reservation is outstanding. If that process dies before calling
`checkin_connection/2,4`, the C++ pool's resource `down` callback releases
the reserved backlog slot(s) and makes the connection available again --
equivalent to that process having called `checkin_connection/2` itself --
so a crashing caller can never permanently strand a connection. This is
independent of (and in addition to) the TTL-based timeout registry (see
`track_inflight/5`), which only covers requests explicitly registered as
in-flight.
""".

-export([create/5, create/6, create/7, create/8, destroy/1]).
-export([checkout_connection/2, checkin_connection/2, checkin_connection/4]).
-export([checkout_async/3]).
-export([set_socket/3, make_available/2, make_unavailable/2, connection_drained/2]).
-export([track_inflight/5, sweep_timeouts/1]).
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

-doc """
Equivalent to `create/6` with `FixedTimeoutUs = 0` (per-request timeouts,
set individually via `track_inflight/5`).

## Examples

```
1> arterial_nif:create(my_pool, 4, 16, true, my_protocol).
ok
```
""".
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module()) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol) ->
  create(Pool, Size, Backlog, Fifo, Protocol, 0).

-doc """
Equivalent to `create/7` with `MaxWaiters = 0` (queue-when-busy disabled;
`checkout_async/3` behaves like `checkout_connection/2`).

## Examples

```
1> arterial_nif:create(my_pool, 4, 16, true, my_protocol, 5000000).
ok
```
""".
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module(),
              non_neg_integer()) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs) ->
  create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, 0).

-doc """
Create a pool of `Size` connections with a per-connection backlog of
`Backlog` in-flight requests (FIFO order if `Fifo` is true, otherwise
random-access by wire-level request ID), using `Protocol` to encode/decode
wire-level messages on every connection in the pool. Connections have no
socket and are unavailable for checkout until `set_socket/3` +
`make_available/2` are called on each one (see `arterial_connection`'s
reconnect logic).

`FixedTimeoutUs` selects how asynchronous in-flight requests tracked via
`track_inflight/5` expire: `0` means each request uses its own timeout
(the `TtlUs` passed to `track_inflight/5`); a positive value makes every
in-flight request in this pool expire after that same fixed duration (the
`TtlUs` argument to `track_inflight/5` is then ignored).

`MaxWaiters` bounds the queue-when-busy feature used by `checkout_async/3`:
`0` disables it (a checkout attempt with no connection available fails
immediately, like `checkout_connection/2` does); a positive value allows
up to that many callers to be queued while every connection is busy, each
serviced (or timed out) later.

## Examples

```
1> arterial_nif:create(my_pool, 4, 16, true, my_protocol, 5000000, 100).
ok
```
""".
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module(),
              non_neg_integer(), non_neg_integer()) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, MaxWaiters) ->
  create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, MaxWaiters, nproc).

-doc """
Like `create/7`, but additionally controls the shard count of the pool's
internal in-flight-request/queued-waiter registries (see
`c_src/sharded_ttl_map.hpp`) via `TtlShards`:

* `nproc` (the default via `create/5`, `create/6`, `create/7`) picks a
  shard count scaled to `std::thread::hardware_concurrency()` on the C++
  side (see `ShardedTTLMap::DefaultShardCount()`) -- a good default for a
  pool that doesn't share its scheduler budget with many sibling pools.
* `{nproc, N}` multiplies that same core count by `N` instead of the
  fixed factor `DefaultShardCount()` uses internally -- e.g. `{nproc, 4}`
  for a pool expected to see unusually high concurrent contention, or
  `{nproc, 1}` to shard more conservatively than the default on a pool
  that mostly serves one caller at a time.
* A positive integer overrides this directly with an exact shard count,
  e.g. if many pools run on one machine and each should claim a smaller,
  fixed share rather than each independently sizing itself off the whole
  machine's core count.

Contention on a single shard's lock only happens when multiple concurrent
calls hash to the *same* shard at the *same* instant -- `nproc`/`{nproc,
N}` bound how many calls can truly run concurrently (BEAM schedulers), but
hashing isn't perfectly uniform, so a hot pool under heavy load may still
see occasional collisions even with core-scaled sharding; pass a larger
fixed value if profiling shows this registry's lock as a bottleneck for a
specific pool.

## Examples

```
1> arterial_nif:create(my_pool, 4, 16, true, my_protocol, 5000000, 100, nproc).
ok
2> arterial_nif:create(my_pool2, 4, 16, true, my_protocol, 5000000, 100, {nproc, 4}).
ok
3> arterial_nif:create(my_pool3, 4, 16, true, my_protocol, 5000000, 100, 8).
ok
```
""".
-spec create(pool(), pos_integer(), pos_integer(), boolean(), module(),
              non_neg_integer(), non_neg_integer(),
              non_neg_integer() | nproc | {nproc, pos_integer()}) -> ok.
create(Pool, Size, Backlog, Fifo, Protocol, FixedTimeoutUs, MaxWaiters, TtlShards)
    when is_atom(Pool), is_integer(Size), Size > 0,
         is_integer(Backlog), Backlog > 0, is_boolean(Fifo), is_atom(Protocol),
         is_integer(FixedTimeoutUs), FixedTimeoutUs >= 0,
         is_integer(MaxWaiters), MaxWaiters >= 0 ->
  TtlShards1 = ttl_shards(TtlShards),
  {ok, Resource} = create_pool(Size, Backlog, Fifo, FixedTimeoutUs, MaxWaiters, TtlShards1),
  persistent_term:put(pool_key(Pool), Resource),
  persistent_term:put(proto_key(Pool), Protocol),
  Owner = ensure_table_owner(),
  Owner ! {new_table, self(), buf_table(Pool)},
  receive {Owner, table_ready} -> ok end,
  ok.

%% `nproc` -> 0, which create_pool/6 (the create_nif NIF) reads as "pick
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
Destroy `Pool`'s NIF resource and release its associated bookkeeping
(per-connection buffer table, persistent_term entries). The pool's
supervisor and connection workers must already be stopped; this does not
stop any processes itself.

Safe to call even if `Pool`'s buffer table's owner process (see
`create/7`) is gone for some other reason -- the table is simply assumed
already collected in that case.

## Examples

```
1> arterial_nif:destroy(my_pool).
ok
```
""".
-spec destroy(pool()) -> ok.
destroy(Pool) when is_atom(Pool) ->
  Resource = resource(Pool),
  case ets:whereis(buf_table(Pool)) of
    undefined -> ok;
    _Tid      -> ets:delete(buf_table(Pool))
  end,
  persistent_term:erase(pool_key(Pool)),
  persistent_term:erase(proto_key(Pool)),
  destroy_pool(Resource).

-doc """
Set the socket currently used by connection `ConnID` of `Pool`. Called by
the connection worker right after it (re)connects, before
`make_available/2`.

## Examples

```
1> arterial_nif:set_socket(my_pool, 0, Socket).
true
```
""".
-spec set_socket(pool(), non_neg_integer(), arterial:socket()) -> boolean().
set_socket(Pool, ConnID, Socket) ->
  ets:insert_new(buf_table(Pool), {ConnID, <<>>}),
  set_socket_nif(resource(Pool), ConnID, Socket).

-doc """
Mark connection `ConnID` of `Pool` as available for checkout. Called by
the connection worker after `set_socket/3` succeeds.

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
in-flight requests checked out of its backlog -- i.e. it's safe to
disconnect without abandoning a pending request. Used to implement a
"bounce" cycle: mark the connection unavailable (see `make_unavailable/2`)
so no new request selects it, poll this until it drains, then disconnect
and reconnect.

## Examples

```
1> arterial_nif:connection_drained(my_pool, 0).
true
```
""".
-spec connection_drained(pool(), non_neg_integer()) -> boolean().
connection_drained(Pool, ConnID) ->
  connection_drained_nif(resource(Pool), ConnID).

-doc """
Check out a connection able to accept one new request. `Mode` does not
change the underlying reservation (always 1 backlog slot); it only
selects how the caller intends to use the connection (kept for callers
that want to log/instrument sync vs async use).

The calling process is monitored for as long as it holds the returned
reservation: if it dies before calling `checkin_connection/2,4`, the
reserved backlog slot(s) are released and the connection is made
available again automatically (see `c:arterial_protocol`/death handling
in the moduledoc).

## Examples

```
1> arterial_nif:checkout_connection(my_pool, sync).
{ok,#{conn_ref => #Ref<0.123.456.789>,conn_id => 0,
      protocol => my_protocol,socket => Socket,buffer => <<>>,
      req_ids => [42]}}
```
""".
-spec checkout_connection(pool(), mode()) ->
  {ok, #{
    conn_ref => reference(),
    conn_id  => non_neg_integer(),
    protocol => module(),
    socket   => arterial:socket(),
    buffer   => binary(),
    req_ids  => [non_neg_integer()]
  }} | {error, no_connection}.
checkout_connection(Pool, Mode) when Mode =:= sync; Mode =:= async ->
  case checkout_nif(resource(Pool), self(), 1) of
    {ok, {ConnID, Socket, ReqIDs}} ->
      Buffer = case ets:lookup(buf_table(Pool), ConnID) of
        [{_, Buf}] -> Buf;
        []         -> <<>>
      end,
      {ok, #{
        conn_ref => make_ref(),
        conn_id  => ConnID,
        protocol => protocol(Pool),
        socket   => Socket,
        buffer   => Buffer,
        req_ids  => ReqIDs
      }};
    {error, no_connection} ->
      {error, no_connection}
  end.

-doc """
Release a connection back to the pool without completing any of its
reserved requests (e.g. the connection died before a reply was received).
The reserved backlog slots are intentionally not released here: the
corresponding requests may still arrive on the wire once the connection
is reused, and FIFO backlogs in particular require slots to be released
in checkout order.

Also tries to service the head of `Pool`'s queue-when-busy wait-list (see
`checkout_async/3`) now that this connection is available again.

Must be called by the same process that checked the connection out (see
`checkout_connection/2`, `checkout_async/3`), since that's the process
this call's matching `checkin_nif` call releases bookkeeping for.

## Examples

```
1> arterial_nif:checkin_connection(my_pool, 0).
ok
```
""".
-spec checkin_connection(pool(), non_neg_integer()) -> ok.
checkin_connection(Pool, ConnID) ->
  checkin_nif(resource(Pool), ConnID, [], Pool, self()).

-doc """
Release a connection back to the pool after successfully completing
requests `ReqIDs` (freeing their backlog slots), storing `Buffer` as the
connection's leftover (undecoded) bytes for the next checkout.

Also tries to service the head of `Pool`'s queue-when-busy wait-list (see
`checkout_async/3`) now that this connection is available again.

Must be called by the same process that checked the connection out (see
`checkout_connection/2`, `checkout_async/3`), since that's the process
this call's matching `checkin_nif` call releases bookkeeping for.

## Examples

```
1> arterial_nif:checkin_connection(my_pool, 0, [42], <<>>).
ok
```
""".
-spec checkin_connection(pool(), non_neg_integer(), [non_neg_integer()], binary()) -> ok.
checkin_connection(Pool, ConnID, ReqIDs, Buffer)
    when is_list(ReqIDs), is_binary(Buffer) ->
  ets:insert(buf_table(Pool), {ConnID, Buffer}),
  checkin_nif(resource(Pool), ConnID, ReqIDs, Pool, self()).

-doc """
Check out a connection for asynchronous use, queuing the request (up to
`Pool`'s `MaxWaiters`, see `create/7`) if every connection is currently
busy instead of failing immediately. `TtlUs` bounds the total time from
this call until a reply is checked in (covering both time spent queued
and time spent in-flight once a connection is assigned); it's ignored if
`Pool` was created with a fixed timeout (see `create/7`).

Three outcomes:

- `{ok, Map}` -- a connection was available immediately, exactly like
  `checkout_connection/2`. No message will follow; `req_ids` in the
  returned map is the correlation id to use with `checkin_connection/4`.
- `{queued, WaiterID}` -- every connection was busy but the request was
  queued; the caller's process will later receive either
  `{arterial_ready, Pool, ReqID, ConnID, Socket, ReqIDs}` (call
  `checkin_connection/4` when done, using the real wire-level
  `ReqID`/`ReqIDs` from that message, exactly as with a synchronous
  checkout) or `{arterial_timeout, Pool, WaiterID}` if `TtlUs`
  microseconds pass first while still queued. `WaiterID` (an opaque
  internal id, NOT a wire-level request id) is only ever used to match
  that eventual message back to this call.
- `{error, no_connection}` -- every connection was busy AND the wait-list
  was disabled (`MaxWaiters = 0`) or already full.

## Examples

```
1> arterial_nif:checkout_async(my_pool, self(), 5000000).
{ok,#{conn_id => 0,protocol => my_protocol,socket => Socket,
      buffer => <<>>,req_ids => [42]}}
2> arterial_nif:checkout_async(my_pool, self(), 5000000). % all connections busy
{queued,7}
```
""".
-spec checkout_async(pool(), pid(), non_neg_integer()) ->
  {ok, #{
    conn_id  => non_neg_integer(),
    protocol => module(),
    socket   => arterial:socket(),
    buffer   => binary(),
    req_ids  => [non_neg_integer()]
  }} | {queued, non_neg_integer()} | {error, no_connection}.
checkout_async(Pool, Pid, TtlUs)
    when is_pid(Pid), is_integer(TtlUs), TtlUs >= 0 ->
  case checkout_async_nif(resource(Pool), Pid, TtlUs) of
    {ok, {ConnID, Socket, ReqIDs}} ->
      Buffer = case ets:lookup(buf_table(Pool), ConnID) of
        [{_, Buf}] -> Buf;
        []         -> <<>>
      end,
      {ok, #{
        conn_id  => ConnID,
        protocol => protocol(Pool),
        socket   => Socket,
        buffer   => Buffer,
        req_ids  => ReqIDs
      }};
    {queued, WaiterID} ->
      {queued, WaiterID};
    {error, no_connection} ->
      {error, no_connection}
  end.

-doc """
Register `ReqID` (reserved on connection `ConnID`, as returned by
`checkout_connection/2`) as an in-flight asynchronous request owned by
`Pid`, so that if no matching `checkin_connection/2,4` call (which
untracks it) happens within `TtlUs` microseconds, `Pid` receives
`{arterial_timeout, Pool, ReqID}` the next time `sweep_timeouts/1` runs,
and `ReqID`'s backlog slot on `ConnID` is released automatically.

`Pid` must be the same process that performed the checkout
(`checkout_connection/2` or `checkout_async/3`) -- on timeout, the
reservation is released under that same pid's bookkeeping, so passing a
different pid here is undefined behavior (and may eventually exhaust the
pool's owner-tracking capacity instead of releasing anything).

Only meant for the asynchronous request path: synchronous callers block
on their own socket-level timeout and never need this. `TtlUs` is ignored
if `Pool` was created with a fixed timeout (see `create/6`).

## Examples

```
1> arterial_nif:track_inflight(my_pool, 0, 42, self(), 5000000).
ok
```
""".
-spec track_inflight(pool(), non_neg_integer(), non_neg_integer(), pid(),
                      non_neg_integer()) -> ok.
track_inflight(Pool, ConnID, ReqID, Pid, TtlUs)
    when is_integer(ConnID), is_integer(ReqID), is_pid(Pid),
         is_integer(TtlUs), TtlUs >= 0 ->
  track_inflight_nif(resource(Pool), ConnID, ReqID, Pid, TtlUs).

-doc """
Evict every in-flight request of `Pool` that has expired, sending each
owning process `{arterial_timeout, Pool, ReqID}`. Meant to be called
periodically (e.g. via `erlang:send_after/3`) by a supervised process;
see `arterial_sweeper`.

## Examples

```
1> arterial_nif:sweep_timeouts(my_pool).
{ok,2}
```
""".
-spec sweep_timeouts(pool()) -> {ok, non_neg_integer()}.
sweep_timeouts(Pool) ->
  sweep_timeouts_nif(resource(Pool), Pool).

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

pool_key(Pool)  -> {?MODULE, Pool, resource}.
proto_key(Pool) -> {?MODULE, Pool, protocol}.
buf_table(Pool) -> list_to_atom("arterial_buf_" ++ atom_to_list(Pool)).

resource(Pool) ->
  case persistent_term:get(pool_key(Pool), undefined) of
    undefined -> error({unknown_pool, Pool});
    Resource  -> Resource
  end.

protocol(Pool) ->
  case persistent_term:get(proto_key(Pool), undefined) of
    undefined -> error({unknown_pool, Pool});
    Protocol  -> Protocol
  end.

%% ETS tables die with the process that created them. create/7 may run
%% inside a caller-supplied process (e.g. an arterial_pool supervisor's
%% init/1), which can legitimately stop independently of destroy/1 being
%% called (e.g. a supervisor:stop/1 during teardown, or a crash/restart).
%% A buffer table created there would vanish under the connections still
%% using it. Routing every create/7 through one long-lived owner process
%% decouples each pool's buffer table lifetime from whichever process
%% happened to call create/7.
%%
%% Deliberately a bare spawn/receive loop instead of a gen_server: it has
%% exactly one message and no state besides "am I alive" -- start_link/
%% init/handle_call scaffolding would add ceremony without adding safety
%% here. start_link/0 below lets a supervisor own its lifecycle like any
%% other OTP worker even though its loop isn't gen_server-shaped.
-define(TABLE_OWNER, arterial_nif_table_owner).

-doc """
Start (and register) the singleton process that owns every pool's
per-connection buffer ETS table, linked to the caller.

Started as a permanent child of `arterial_sup` when the `arterial`
application is running (see `arterial_app`), so that stopping the
application takes this process -- and every buffer table it owns -- down
with it, rather than leaking them across application restarts.

Pools created without the `arterial` application running at all (e.g. in
tests that call `arterial_pool:start_link/2` directly) never call this;
`create/7` falls back to lazily spawning its own unsupervised, unlinked
owner the first time it's needed in that case (see `ensure_table_owner/0`).
""".
-spec start_link() -> {ok, pid()}.
start_link() ->
  Pid = spawn_link(fun table_owner_loop/0),
  true = register(?TABLE_OWNER, Pid),
  {ok, Pid}.

ensure_table_owner() ->
  case whereis(?TABLE_OWNER) of
    undefined ->
      Pid = spawn(fun table_owner_loop/0),
      %% Lost the race against a concurrent create/7: use whichever
      %% registration won instead of leaking our own spawn.
      try register(?TABLE_OWNER, Pid) of
        true -> Pid
      catch
        error:badarg -> exit(Pid, kill), whereis(?TABLE_OWNER)
      end;
    Pid ->
      Pid
  end.

table_owner_loop() ->
  receive
    {new_table, From, Table} ->
      ets:new(Table, [set, public, named_table]),
      From ! {self(), table_ready},
      table_owner_loop()
  end.

%%%-----------------------------------------------------------------------------
%%% NIF functions
%%%-----------------------------------------------------------------------------

create_pool(_Size, _Backlog, _Fifo, _FixedTtlUs, _MaxWaiters, _TtlShards) -> ?NOT_LOADED_ERROR.
destroy_pool(_Rsrc)                                      -> ?NOT_LOADED_ERROR.
set_socket_nif(_Rsrc, _ConnID, _Socket)                  -> ?NOT_LOADED_ERROR.
make_available_nif(_Rsrc, _ConnID)                       -> ?NOT_LOADED_ERROR.
make_unavailable_nif(_Rsrc, _ConnID)                     -> ?NOT_LOADED_ERROR.
connection_drained_nif(_Rsrc, _ConnID)                    -> ?NOT_LOADED_ERROR.
checkout_nif(_Rsrc, _Pid, _Samples)                      -> ?NOT_LOADED_ERROR.
checkin_nif(_Rsrc, _ConnID, _ReqIDs, _PoolName, _Pid)    -> ?NOT_LOADED_ERROR.
checkout_async_nif(_Rsrc, _Pid, _TtlUs)                  -> ?NOT_LOADED_ERROR.
track_inflight_nif(_Rsrc, _ConnID, _ReqID, _Pid, _TtlUs) -> ?NOT_LOADED_ERROR.
sweep_timeouts_nif(_Rsrc, _PoolName)                     -> ?NOT_LOADED_ERROR.

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
