-module(arterial_sweeper).
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
%% @doc Start a process that calls `arterial_nif:sweep_timeouts/1' for
%% `Pool' every `IntervalMs' milliseconds, evicting any in-flight async
%% request whose TTL has expired and notifying its owning process with
%% `{arterial_timeout, Pool, ReqID}' (see arterial_nif:track_inflight/5).
-spec start_link(arterial_pool:name(), pos_integer()) -> {ok, pid()}.
start_link(Pool, IntervalMs) when is_atom(Pool), is_integer(IntervalMs), IntervalMs > 0 ->
  gen_server:start_link(?MODULE, [Pool, IntervalMs], []).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
init([Pool, IntervalMs]) ->
  State = #state{pool = Pool, interval_ms = IntervalMs},
  schedule(State),
  {ok, State}.

handle_call(Msg, _From, State) ->
  {reply, {error, {unexpected_call, Msg}}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(sweep, State) ->
  arterial_nif:sweep_timeouts(State#state.pool),
  schedule(State),
  {noreply, State}.

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

schedule(#state{interval_ms = IntervalMs}) ->
  erlang:send_after(IntervalMs, self(), sweep).
