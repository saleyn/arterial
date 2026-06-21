-module(arterial_bouncer).

-moduledoc """
Periodic `gen_server` that recycles a pool's connections one at a time:
on each `bounce_interval_ms` tick, it bounces the next connection (in
round-robin order) via `arterial_connection:bounce/2` -- marking it
unavailable, waiting for its backlog to drain, then disconnecting and
reconnecting it -- before going idle until the next tick. A full pool
rotation therefore takes `Size * bounce_interval_ms`.

Started once per pool by `arterial_pool`'s supervisor only if the
`bounce_interval_ms` option is set; not meant to be used directly by
callers of the library.

## Why bounce connections at all?

A connection pool's sockets are normally connected once and kept open
indefinitely -- great for latency, but it means whatever backend
endpoint a connection resolved to at connect time is the endpoint it
talks to forever, even if better/different endpoints become available
later. This is a real problem behind a Kubernetes `Service` fronting a
`Deployment` that autoscales: the Service's DNS name or virtual IP only
balances *new* connections across the current set of pod endpoints --
existing long-lived connections are never rebalanced. So:

* When the `Deployment` scales up, pods added by the autoscaler receive
  no traffic at all, since every existing connection is already pinned
  to one of the older pods.
* When it scales down, connections pinned to a terminated pod either
  break outright or (worse, depending on the Service/proxy setup) sit
  silently broken until a request actually tries to use them.

Periodically bouncing each connection forces it to re-resolve and
reconnect through the Service on a regular cadence, so load gradually
spreads across whatever set of backend pods currently exists -- without
ever taking more than one connection out of service at a time.
""".

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/4]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
  pool             :: arterial_pool:name(),
  size             :: pos_integer(),
  interval_ms      :: pos_integer(),
  drain_timeout_ms :: pos_integer(),
  next_conn_id     :: non_neg_integer()
}).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a process that bounces one connection of `Pool` (out of `Size`
total, ids `0..Size-1`) every `IntervalMs` milliseconds, round-robin.
`DrainTimeoutMs` bounds how long a single bounce waits for its
connection's backlog to drain before forcing the disconnect anyway (see
`arterial_connection:bounce/2`).

## Examples

```
1> arterial_bouncer:start_link(my_pool, 8, 60000, 30000).
{ok,<0.151.0>}
```
""".
-spec start_link(arterial_pool:name(), pos_integer(), pos_integer(), pos_integer()) ->
  {ok, pid()}.
start_link(Pool, Size, IntervalMs, DrainTimeoutMs)
    when is_atom(Pool), is_integer(Size), Size > 0,
         is_integer(IntervalMs), IntervalMs > 0,
         is_integer(DrainTimeoutMs), DrainTimeoutMs > 0 ->
  gen_server:start_link(?MODULE, [Pool, Size, IntervalMs, DrainTimeoutMs], []).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Pool, Size, IntervalMs, DrainTimeoutMs]) ->
  State = #state{
    pool             = Pool,
    size             = Size,
    interval_ms      = IntervalMs,
    drain_timeout_ms = DrainTimeoutMs,
    next_conn_id     = 0
  },
  schedule(State),
  {ok, State}.

-doc false.
handle_call(Msg, _From, State) ->
  {reply, {error, {unexpected_call, Msg}}, State}.

-doc false.
handle_cast(_Msg, State) ->
  {noreply, State}.

-doc false.
handle_info(bounce, #state{
  pool = Pool, size = Size, next_conn_id = ConnID, drain_timeout_ms = DrainTimeoutMs
} = State) ->
  bounce(Pool, ConnID, DrainTimeoutMs),
  schedule(State),
  {noreply, State#state{next_conn_id = (ConnID + 1) rem Size}}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

schedule(#state{interval_ms = IntervalMs}) ->
  erlang:send_after(IntervalMs, self(), bounce).

bounce(Pool, ConnID, DrainTimeoutMs) ->
  case conn_pid(Pool, ConnID) of
    {ok, Pid} ->
      try arterial_connection:bounce(Pid, DrainTimeoutMs) of
        ok               -> ok;
        {error, timeout} ->
          ?LOG_WARNING("~w: bounce of connection ~p timed out waiting for drain", [Pool, ConnID])
      catch
        exit:{noproc, _} -> ok; % worker not up right now; skip this tick
        exit:{normal, _} -> ok;
        E:R              ->
          ?LOG_WARNING("~w: bounce of connection ~p failed: ~p:~p", [Pool, ConnID, E, R])
      end;
    error ->
      ?LOG_WARNING("~w: bounce skipped, connection ~p worker not found", [Pool, ConnID])
  end.

conn_pid(Pool, ConnID) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
    {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
    _                               -> error
  end.
