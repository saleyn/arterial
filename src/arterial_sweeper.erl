-module(arterial_sweeper).

-moduledoc """
Periodic `gen_server` that evicts expired in-flight requests from
the per-stripe NIF corr maps, sending each waiting caller
`{arterial_timeout, CorrId}` so they don't block until their own
`after Timeout` fires.

Started once per pool by `arterial_pool`'s supervisor.
""".

-behaviour(gen_server).

-export([start_link/1, start_link/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {
  pool        :: arterial_pool:name(),
  interval_ms :: pos_integer()
}).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------
-doc """
Start a sweeper for `Pool` that fires every `IntervalMs` milliseconds.
""".
-spec start_link(arterial_pool:name(), pos_integer()) -> {ok, pid()}.
start_link(Pool, IntervalMs) when is_atom(Pool), is_integer(IntervalMs), IntervalMs > 0 ->
  start_link(#{pool => Pool, interval => IntervalMs}).

-spec start_link(#{pool => atom(), interval => pos_integer(),
                   batch_size => pos_integer()}) -> {ok, pid()}.
start_link(Opts) when is_map(Opts) ->
  gen_server:start_link(?MODULE, Opts, []).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------
-doc false.
init(#{pool := Pool, interval := IntervalMs} = _Opts) ->
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

sweep(Pool) ->
  PoolRef = arterial_pool:pool_ref(Pool),
  NowUs   = os:system_time(microsecond),
  arterial_nif:sweep_corr_map(PoolRef, NowUs).

schedule(#state{interval_ms = IntervalMs}) ->
  erlang:send_after(IntervalMs, self(), sweep).
