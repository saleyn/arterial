-module(arterial_pool_guard).

-moduledoc """
Per-pool `gen_server` that monitors every `arterial_connection` worker in
the pool and forces `arterial_nif:make_unavailable/2` the instant one
dies, for any reason other than a graceful shutdown.

Started once per pool by `arterial_pool`'s supervisor, alongside (not
instead of) the per-slot `arterial_conn_owner`/`arterial_connection`
pairs -- it never touches sockets itself, it only reacts to deaths.

## Why this exists

`arterial_nif:make_unavailable/2` is otherwise only ever called from
`arterial_connection:disconnect/2`'s own graceful path. A crash (an
unhandled exception, a `kill`) bypasses `disconnect/2` entirely --
without this guard, the NIF is left believing the connection is still
available, with a now-dead socket, for as long as it takes
`arterial_pool`'s supervisor to restart the worker *and* that
replacement's own `init/1` to run. A checkout landing in that window is
handed a connection whose `arterial_conn_owner` has no live socket to
send on, surfacing as a spurious `{error, closed}` to an otherwise
healthy-looking caller.

This guard closes that window to (effectively) zero: it monitors each
connection worker directly, so the `'DOWN'` it reacts to races nothing
except the supervisor's own restart decision -- `make_unavailable/2`
itself happens before the replacement worker's `init/1` even starts. It
intentionally does not attempt to also restart, reconnect, or otherwise
manage the worker; `arterial_pool`'s supervisor (`restart => permanent`)
already does that independently.
""".

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
  pool :: arterial_pool:name(),
  size :: pos_integer(),
  %% ConnID => monitor reference, one entry per slot whose worker is
  %% currently known and monitored. A slot temporarily missing from this
  %% map (not yet started, or just died and not yet replaced) is simply
  %% retried on the next poll tick rather than tracked as an error state.
  monitors :: #{non_neg_integer() => reference()}
}).

-define(POLL_INTERVAL_MS, 20).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a guard for `Pool`'s `Size` connection slots (ids `0..Size-1`).

## Examples

```
1> arterial_pool_guard:start_link(my_pool, 8).
{ok,<0.151.0>}
```
""".
-spec start_link(arterial_pool:name(), pos_integer()) -> {ok, pid()}.
start_link(Pool, Size) when is_atom(Pool), is_integer(Size), Size > 0 ->
  gen_server:start_link(?MODULE, [Pool, Size], []).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Pool, Size]) ->
  self() ! poll,
  {ok, #state{pool = Pool, size = Size, monitors = #{}}}.

-doc false.
handle_call(Msg, _From, State) ->
  {reply, {error, {unexpected_call, Msg}}, State}.

-doc false.
handle_cast(_Msg, State) ->
  {noreply, State}.

%% Picks up any connection slot not yet monitored -- either because its
%% worker hasn't started for the first time yet (right after
%% arterial_pool's supervisor starts this guard, before any
%% arterial_connection child exists), or because its previous monitor
%% just fired (handle_info({'DOWN', ...}) below removes that slot from
%% `monitors` before rescheduling this same message). Reschedules itself
%% only while some slot is still unmonitored -- a fully monitored pool
%% goes quiet until the next death.
handle_info(poll, #state{pool = Pool, size = Size, monitors = Monitors} = State) ->
  Monitors1 = monitor_missing(Pool, Size, Monitors),
  map_size(Monitors1) < Size andalso erlang:send_after(?POLL_INTERVAL_MS, self(), poll),
  {noreply, State#state{monitors = Monitors1}};

handle_info({'DOWN', Ref, process, _Pid, Reason}, #state{pool = Pool, monitors = Monitors} = State) ->
  case find_conn_id(Ref, Monitors) of
    {ok, ConnID} ->
      (Reason =/= normal andalso Reason =/= shutdown) andalso
        arterial_nif:make_unavailable(Pool, ConnID),
      self() ! poll,
      {noreply, State#state{monitors = maps:remove(ConnID, Monitors)}};
    error ->
      % A stale 'DOWN' for a monitor already removed -- ignore.
      {noreply, State}
  end;

handle_info(_Msg, State) ->
  {noreply, State}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

monitor_missing(Pool, Size, Monitors) ->
  Children = supervisor:which_children(arterial_pool:sup_name(Pool)),
  lists:foldl(fun(ConnID, Acc) ->
    case maps:is_key(ConnID, Acc) of
      true ->
        Acc;
      false ->
        case lists:keyfind({arterial_connection, ConnID}, 1, Children) of
          {_, Pid, _, _} when is_pid(Pid) ->
            Ref = erlang:monitor(process, Pid),
            Acc#{ConnID => Ref};
          _ ->
            Acc
        end
    end
  end, Monitors, lists:seq(0, Size - 1)).

find_conn_id(Ref, Monitors) ->
  case [ConnID || {ConnID, R} <- maps:to_list(Monitors), R =:= Ref] of
    [ConnID] -> {ok, ConnID};
    []       -> error
  end.
