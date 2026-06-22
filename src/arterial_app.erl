-module(arterial_app).

-moduledoc """
The `arterial` OTP application and its top-level supervisor.

`arterial_sup` is a static `one_for_one` supervisor with two children:

- `arterial_nif` (a worker; see `arterial_nif:start_link/0`) -- the
  singleton process that owns every pool's per-connection buffer ETS
  table, so that those tables are torn down when the `arterial`
  application stops rather than leaking across restarts.
- `arterial_pool_sup`, a `simple_one_for_one` supervisor whose children
  are `arterial_pool` supervisors, one per pool started via
  `arterial_pool:start_link/2`.
""".

-behaviour(application).
-behaviour(supervisor).

-export([start/0, stop/0]).
-export([start/2, stop/1]).

-export([init/1]).

-define(APP, arterial).

%% public
-doc """
Start the `arterial` application (and its dependencies).

## Examples

```
1> arterial:start().
{ok,[arterial]}
```
""".
-spec start() -> {ok, [atom()]} | {error, term()}.
start() ->
  application:ensure_all_started(?APP).

-doc """
Stop the `arterial` application.

## Examples

```
1> arterial:stop().
ok
```
""".
-spec stop() -> ok | {error, term()}.
stop() ->
  application:stop(?APP).

%% application callbacks
-doc false.
-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_StartType, _StartArgs) ->
  supervisor:start_link({local, arterial_sup}, ?MODULE, []).

-doc false.
-spec stop(term()) -> ok.
stop(_State) ->
  ok.

%% supervisor callbacks
-doc false.
-spec init([] | pool_sup) ->
  {ok, {{one_for_one | simple_one_for_one, non_neg_integer(), non_neg_integer()}, [map()]}}.
init([]) ->
  %% Start observability module if configured
  {ObsImplMod, ObsOpts} =
    case application:get_env(arterial, observability, nil) of
      prometheus            -> {arterial_observe_prometheus,  #{}};
      telemetry             -> {arterial_observe_telemetry,   #{}};
      {prometheus, Opts}    -> {arterial_observe_prometheus, Opts};
      {telemetry,  Opts}    -> {arterial_observe_telemetry,  Opts};
      nil                   -> {nil, []};
      Mod when is_atom(Mod) -> {Mod, []};
      {Mod, Opts} when is_atom(Mod) -> {Mod, Opts}
    end,

  %% Top-level: the singleton arterial_nif table owner plus the
  %% simple_one_for_one sub-supervisor for pools below. A
  %% simple_one_for_one supervisor can only ever hold one (homogeneous)
  %% child spec, so arterial_nif can't be a sibling inside it directly --
  %% it needs this static one_for_one wrapper instead.
  {ok, {{one_for_one, 5, 10}, [
    #{
      id       => arterial_observe,
      start    => {arterial_observe, start_link, [ObsImplMod, ObsOpts]},
      restart  => permanent,
      shutdown => 1000,
      type     => worker,
      modules  => [arterial_nif]
    },
    #{
      id       => arterial_nif,
      start    => {arterial_nif, start_link, []},
      restart  => permanent,
      shutdown => 1000,
      type     => worker,
      modules  => [arterial_nif]
    },
    #{
      id       => arterial_pool_sup,
      start    => {supervisor, start_link, [{local, arterial_pool_sup}, ?MODULE, pool_sup]},
      restart  => permanent,
      shutdown => infinity,
      type     => supervisor,
      modules  => [?MODULE]
    }
  ]}};
init(pool_sup) ->
  {ok, {{simple_one_for_one, 5, 10}, [
    #{
      id       => undefined,                       % Id       = internal id
      start    => {arterial_pool, start_link, []}, % StartFun = {M, F, A}
      restart  => permanent,                       % Restart  = permanent | transient | temporary
      shutdown => infinity,                        % Shutdown = brutal_kill | int() >= 0 | infinity
      type     => supervisor,                      % Type     = worker | supervisor
      modules  => [arterial_pool]                  % Modules  = [Module] | dynamic
    }
  ]}}.
