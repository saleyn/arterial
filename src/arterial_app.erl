-module(arterial_app).

-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

-export([init/1]).

-define(APP, arterial).

%% public
-spec start() -> {ok, [atom()]} | {error, term()}.
start() ->
  application:ensure_all_started(?APP).

-spec stop() -> ok | {error, term()}.
stop() ->
  application:stop(?APP).

%% application callbacks
-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
  supervisor:start_link({local, arterial_sup}, ?MODULE, []).

-spec stop(term()) -> ok.
stop(_State) ->
  ok.

%% supervisor callbacks
-spec init(atom()) -> {ok, {{one_for_one, 5, 10}, [supervisor:child_spec()]}}.
init([]) ->
  {ok, {{simple_one_for_one, 5, 10}, [
    #{
      id       => undefined,                               % Id       = internal id
      start    => {arterial_pool, start_link, []},         % StartFun = {M, F, A}
      restart  => permanent,                               % Restart  = permanent | transient | temporary
      shutdown => 5000,                                    % Shutdown = brutal_kill | int() >= 0 | infinity
      type     => supervisor,                              % Type     = worker | supervisor
      modules  => []                                       % Modules  = [Module] | dynamic
    }
  ]}}.