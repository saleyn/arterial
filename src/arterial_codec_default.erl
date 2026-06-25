-module(arterial_codec_default).

-moduledoc """
Default `arterial_codec` implementation: frames each message as
`<<CorrId:32, Len:32, Payload:Len/binary>>` with `Payload =
term_to_binary(Term)` -- the same convention `test_echo_protocol` uses
for the original backend, just with the request id repurposed as the
pool-wide dispatch correlation id (see `arterial_codec`'s moduledoc).

Good enough for tests and for any protocol willing to adopt this exact
framing; real-world wire protocols with their own request-id field and
framing should implement `arterial_codec` directly instead.
""".

-behaviour(arterial_codec).

-export([encode_request/2, decode/1]).
-export([frame/2, unframe/1]).

-doc "Frame `Payload` (owned by `CorrId`) as `<<CorrId:32, Len:32, Payload/binary>>`.".
-spec frame(non_neg_integer(), binary()) -> binary().
frame(CorrId, Payload) when is_binary(Payload) ->
  <<CorrId:32, (byte_size(Payload)):32, Payload/binary>>.

-doc "Try to split one complete frame off the front of `Buffer`.".
-spec unframe(binary()) ->
  {ok, CorrId::non_neg_integer(), Payload::binary(), Rest::binary()} | more.
unframe(<<CorrId:32, Len:32, Rest/binary>>) when byte_size(Rest) >= Len ->
  <<Payload:Len/binary, Tail/binary>> = Rest,
  {ok, CorrId, Payload, Tail};
unframe(_Buffer) ->
  more.

-doc false.
encode_request(CorrId, Term) ->
  frame(CorrId, term_to_binary(Term)).

-doc false.
decode(Buffer) ->
  case unframe(Buffer) of
    {ok, CorrId, Payload, Rest} ->
      try binary_to_term(Payload) of
        Term -> {ok, CorrId, Term, Rest}
      catch _:_ ->
        {error, {bad_payload, Payload}}
      end;
    more ->
      more
  end.
