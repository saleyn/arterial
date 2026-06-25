-module(arterial_sweeper).

-moduledoc """
Periodic `gen_server` that evicts expired in-flight requests from
`arterial_pool`'s correlation-id ETS table, mirroring
`arterial_sweeper`'s role in the original backend (see
`arterial_nif:sweep_timeouts/1`) -- except entirely in plain Erlang here,
since the bookkeeping it sweeps lives in an ETS table this backend owns
directly, not inside a NIF resource.

Started once per pool by `arterial_pool`'s supervisor; not meant to be
used directly by callers of the library.
""".

-behaviour(gen_server).

-export([start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
  pool        :: arterial_pool:name(),
  interval_ms :: pos_integer()
}).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a process that evicts every expired entry of `Pool`'s correlation
table every `IntervalMs` milliseconds, sending each owning process
`{arterial_timeout, Pool, CorrId}` (matching `arterial_client:call/3`'s
own `receive` clause for it, for entries whose owner is still waiting
when the sweep beats its own `after Timeout`).

## Examples

```
1> arterial_sweeper:start_link(my_pool, 1000).
{ok,<0.150.0>}
```
""".
-spec start_link(arterial_pool:name(), pos_integer()) -> {ok, pid()}.
start_link(Pool, IntervalMs) when is_atom(Pool), is_integer(IntervalMs), IntervalMs > 0 ->
  gen_server:start_link(?MODULE, [Pool, IntervalMs], []).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init([Pool, IntervalMs]) ->
  State = #state{pool = Pool, interval_ms = IntervalMs},
  schedule(State),
  {ok, State}.

-doc false.
handle_call(Msg, _From, State) ->
  {reply, {error, {unexpected_call, Msg}}, State}.

-doc false.
handle_cast(_Msg, State) ->
  {noreply, State}.

-doc false.
handle_info(sweep, #state{pool = Pool} = State) ->
  Count = sweep(Pool),
  arterial_observe:event([sweep, stop], #{expired_count => Count}, #{pool => Pool}),
  schedule(State),
  {noreply, State}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

%% Deadlines are microsecond timestamps (see arterial_util:calc_expiration/2)
%% or the atom `infinity` -- `infinity < Now` is always false under
%% Erlang's standard term order (atoms compare greater than any number),
%% so infinity-deadline entries are naturally never selected here.
sweep(Pool) ->
  Now = os:system_time(microsecond),
  Table = arterial_pool:corr_table(Pool),
  Expired = ets:select(Table, [{{'$1', '$2', '$3', '$4'}, [{'<', '$4', Now}], [{{'$1', '$2'}}]}]),
  lists:foreach(fun({CorrId, Pid}) ->
    ets:delete(Table, CorrId),
    Pid ! {arterial_timeout, Pool, CorrId}
  end, Expired),
  length(Expired).

schedule(#state{interval_ms = IntervalMs}) ->
  erlang:send_after(IntervalMs, self(), sweep).
