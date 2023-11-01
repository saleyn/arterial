-module(arterial_util).
-export([random_element/1, calc_timeout/1, calc_expiration/2]).

%% @doc Return a random element from the list
random_element(L) when is_list(L) ->
  lists:nth(rand:uniform(length(L), L)).

%% @doc Calculate a timeout from now to Expiration
-spec calc_timeout(non_neg_integer() | infinity) -> infinity | non_neg_integer().
calc_timeout(infinity) ->
  infinity;
calc_timeout(Expiration) when is_integer(Expiration) ->
  case os:system_time(microsecond) of
    T when T > Expiration -> 0;
    T                     -> (Expiration - T) div 1000
  end.

-spec calc_expiration(integer(), integer() | infinity) -> infinity | integer().
calc_expiration(_TS, infinity) -> infinity;
calc_expiration( TS, Timeout)  -> TS + Timeout*1000.
