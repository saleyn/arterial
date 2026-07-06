-module(bench_echo_codec).

-moduledoc """
Simple echo codec for benchmarking purposes.

Provides basic encode/decode functionality for benchmark requests and replies.
Uses term_to_binary/binary_to_term for simplicity and consistency across
all benchmark implementations.
""".

-behavior(arterial_codec).

-export([encode_request/2, decode/1]).

%%%-----------------------------------------------------------------------------
%%% Codec Implementation
%%%-----------------------------------------------------------------------------

-doc """
Encode a request with correlation ID to binary data for transmission.
""".
-spec encode_request(non_neg_integer(), term()) -> iodata().
encode_request(CorrId, Request) ->
  term_to_binary({CorrId, Request}).

-doc """
Decode reply binary data back to correlation ID, term, and remaining buffer.
""".
-spec decode(binary()) ->
  {ok, non_neg_integer(), term(), binary()} | more | {error, term()}.
decode(Buffer) ->
  try
    {CorrId, Reply} = binary_to_term(Buffer),
    {ok, CorrId, Reply, <<>>}
  catch
    error:badarg ->
      more;  % Not enough data for a complete frame
    Class:Reason ->
      {error, {Class, Reason}}
  end.