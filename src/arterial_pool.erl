-module(arterial_pool).
-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").

-export([start_link/2]).

-record(state, {
    name :: atom(),
    pfx  :: binary(),
    id :: non_neg_integer(),
    pool_name :: arterial_pool:name(),
    pool_pid :: pid(),
    address :: arterial:inet_address(),
    port :: arterial:inet_port(),
    protocol :: arterial:protocol(),
    client :: arterial:client(),
    init_options :: init_options(),
    reconnect_state :: undefined | reconnect_state(),
    socket    :: undefined | arterial:socket(),
    sock_opts :: arterial:socket_options(),
    timer_ref :: undefined | reference()
}).

start_link(Name, Opts) when is_atom(Name), is_map(Opts) ->
    Opts1 = maps:merge(Opts, #{
        size => application:get_env(arterial, default_pool_size, 1)
    }),
    Pids = [arterial_app:start_connections(Name, Opts1)],
    gen_server:start_link({local, Name}, ?MODULE, [Pids, Opts1]).

%%%-----------------------------------------------------------------------------
%%% gen_server callbacks
%%%-----------------------------------------------------------------------------

init([Pids, Opts]) ->
  Name = maps:get(name, Opts),
  ID   = maps:get(id,   Opts),
  Pfx = list_to_binary(io_lib:format("~p:~w: ", [Name, ID])),
  {ok, #state{pfx = Pfx}, {continue, connect}}.

handle_continue(connect, State) ->
  {ok, Socket} = connect(State),
  {noreply, State#state{socket = Socket}}.
