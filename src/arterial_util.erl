-module(arterial_util).

-moduledoc """
Small standalone helpers shared by `arterial_connection` (random IP
selection for reconnect) and `arterial_client` (deadline/timeout math for
the synchronous `call/3` path).
""".

-export([random_element/1, calc_timeout/1, calc_expiration/2]).

-doc """
Return a random element from non-empty list `L`. Used by
`arterial_connection` to pick one IP out of the addresses a hostname
resolves to.

## Examples

```
1> arterial_util:random_element([a, b, c]).
b
```
""".
-spec random_element(list()) -> term().
random_element(L) when is_list(L), L =/= [] ->
  lists:nth(rand:uniform(length(L)), L).

-doc """
Calculate the timeout (in milliseconds) remaining from now until the
absolute deadline `Expiration` (in microseconds, as produced by
`calc_expiration/2`), clamped to 0 if already past. `infinity` passes
through unchanged.

## Examples

```
1> Expire = arterial_util:calc_expiration(os:system_time(microsecond), 5000).
2> arterial_util:calc_timeout(Expire).
4999
3> arterial_util:calc_timeout(infinity).
infinity
```
""".
-spec calc_timeout(non_neg_integer() | infinity) -> infinity | non_neg_integer().
calc_timeout(infinity) ->
  infinity;
calc_timeout(Expiration) when is_integer(Expiration) ->
  case os:system_time(microsecond) of
    T when T > Expiration -> 0;
    T                     -> (Expiration - T) div 1000
  end.

-doc """
Calculate an absolute deadline (in microseconds) by adding `Timeout`
milliseconds to timestamp `TS` (in microseconds, e.g.
`os:system_time(microsecond)`). `infinity` passes through unchanged.

## Examples

```
1> arterial_util:calc_expiration(1000000, 5000).
6000000
2> arterial_util:calc_expiration(1000000, infinity).
infinity
```
""".
-spec calc_expiration(integer(), integer() | infinity) -> infinity | integer().
calc_expiration(_TS, infinity) -> infinity;
calc_expiration( TS, Timeout)  -> TS + Timeout*1000.
