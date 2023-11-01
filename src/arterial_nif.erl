-module(arterial_nif).
-export([create/2, destroy/1]).
-export([checkout_connection/1, checkin_connection/2, checkin_connection/3]).
-export([make_available/1, make_unavailable/1]).
-export([acquire_async/1, release_async/1]).

-on_load(on_load/0).

-define(LIBNAME, arterial).
-define(NOT_LOADED_ERROR,
  erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, ?LINE}]})).

%%%-----------------------------------------------------------------------------
%%% Public API
%%%-----------------------------------------------------------------------------

create(Name, Connections) when is_atom(Name), is_list(Connections) ->
  Pool = create_pool(Name, Connections),
  persistent_term:put({arterial, Name}, Pool),
  Pool.

destroy(Name) when is_atom(Name) ->
  persistent_term:erase({arterial, Name}),
  destroy_pool(Name).

%%%-----------------------------------------------------------------------------
%%% NIF functions
%%%-----------------------------------------------------------------------------

create_pool(_Name, _Connections)           -> ?NOT_LOADED_ERROR.
destroy_pool(_Name)                        -> ?NOT_LOADED_ERROR.
checkout_connection(_Name)                 -> ?NOT_LOADED_ERROR.
checkin_connection(_Name, _ConnRef)        -> ?NOT_LOADED_ERROR.
checkin_connection(_Name, _ConnRef, _Bin)  -> ?NOT_LOADED_ERROR.
make_available(ConnRef)                    -> ?NOT_LOADED_ERROR.
make_unavailable(ConnRef)                  -> ?NOT_LOADED_ERROR.
acquire_async(_Pool)                       -> ?NOT_LOADED_ERROR.
release_async(_Pool)                       -> ?NOT_LOADED_ERROR.
%set_socket(_Pool, _ConnRef)

on_load() ->
  SoName  =
    case code:priv_dir(?LIBNAME) of
      {error, bad_name} ->
        case code:which(?MODULE) of
          Filename when is_list(Filename) ->
            Dir = filename:dirname(filename:dirname(Filename)),
            filename:join([Dir, "priv", ?LIBNAME]);
          _ ->
            filename:join("../priv", ?LIBNAME)
        end;
      Dir ->
        filename:join(Dir, ?LIBNAME)
  end,
  erlang:load_nif(SoName, []).