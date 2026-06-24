-module(arterial_throttle2).

-moduledoc """
Per-connection token-bucket rate limiter for `arterial_pool2`, backed by
a single `atomics` array (two cells per connection: milli-tokens and the
microsecond timestamp they were last refilled at) -- no NIF involved,
just plain `atomics` operations, checked by `arterial_client2:call/3`/
`cast/2` before attempting `arterial_nif_pool:send_and_release/3` on a
connection's stripe.

Best-effort, not perfectly linearizable: under heavy concurrent
contention on the *same* connection (several processes racing to send
on it at once), a `compare_exchange/4` retry loop resolves token
accounting safely, but the timestamp cell is written outside that loop
and can occasionally lose a race -- harmless in practice (it only ever
makes the next refill's elapsed-time estimate very slightly off), and
unlike the original backend's NIF-resident `basic_time_spacing_throttle`
(`c_src/throttle.hpp`), this is intentionally simple Erlang-side
bookkeeping, consistent with this whole backend's design of keeping
`arterial_nif_pool` itself down to raw I/O only.

Tokens are tracked scaled by 1000 ("milli-tokens") so fractional
per-microsecond refill rates don't need floats.
""".

-export([new/1, allow/4]).

-doc """
Allocate a fresh token-bucket state for `Size` connections (ids
`0..Size-1`), each starting with a full bucket on its first `allow/4`
call (see below).

## Examples

```
1> arterial_throttle2:new(4).
#Ref<0.123.456.789>
```
""".
-spec new(pos_integer()) -> atomics:atomics_ref().
new(Size) when is_integer(Size), Size > 0 ->
  atomics:new(Size * 2, [{signed, false}]).

-doc """
Try to consume one token for connection `ConnID` against a bucket
refilling at `RatePerSec` tokens/second, capped at `Burst` tokens.
Returns `true` (and consumes the token) if one was available, `false`
otherwise -- callers should treat `false` like a busy connection (e.g.
try a different stripe; see `arterial_client2`).

A connection's bucket starts implicitly full (every cell in the
`atomics` array `new/1` allocates starts at `0`, which this function
reads as "infinite elapsed time since last refill", clamping the first
refill to a full `Burst`) -- no separate initialization call needed.

## Examples

```
1> Ref = arterial_throttle2:new(4).
2> arterial_throttle2:allow(Ref, 0, 100, 10).
true
```
""".
-spec allow(atomics:atomics_ref(), non_neg_integer(), pos_integer(), pos_integer()) ->
  boolean().
allow(Ref, ConnID, RatePerSec, Burst) ->
  TokIdx = ConnID * 2 + 1,
  TsIdx  = ConnID * 2 + 2,
  Now    = erlang:monotonic_time(microsecond),
  try_consume(Ref, TokIdx, TsIdx, Now, RatePerSec, Burst).

%%%-----------------------------------------------------------------------------
%%% Internal functions
%%%-----------------------------------------------------------------------------

try_consume(Ref, TokIdx, TsIdx, Now, RatePerSec, Burst) ->
  LastTs       = atomics:get(Ref, TsIdx),
  Tokens       = atomics:get(Ref, TokIdx),
  ElapsedUs    = max(0, Now - LastTs),
  BurstMilli   = Burst * 1000,
  RefillMilli  = (ElapsedUs * RatePerSec) div 1000,
  NewTokens0   = min(BurstMilli, Tokens + RefillMilli),
  case NewTokens0 >= 1000 of
    true ->
      NewTokens = NewTokens0 - 1000,
      case atomics:compare_exchange(Ref, TokIdx, Tokens, NewTokens) of
        ok ->
          atomics:put(Ref, TsIdx, Now),
          true;
        _Actual ->
          try_consume(Ref, TokIdx, TsIdx, Now, RatePerSec, Burst)
      end;
    false ->
      false
  end.
