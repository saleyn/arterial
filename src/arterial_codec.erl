-module(arterial_codec).

-moduledoc """
Behaviour implemented by wire-codec modules for `arterial_pool`'s
backend (NIF-resident raw socket I/O, see `arterial_nif`'s
moduledoc).

Unlike `arterial_protocol` (the original backend's codec behaviour),
there is no `connect/3`/`send/2`/`recv/2` here: the transport itself is
owned entirely by `arterial_connection` + `arterial_nif`, and every
request is dispatched purely by a wire-level correlation id (`CorrId`)
looked up in a pool-wide public ETS table -- this backend only supports
arterial's protocol matching modes 1/2 (native or surrogate request id,
out-of-order replies; see the top-level README's "Protocol" section),
never FIFO/no-request-id framing.

`CorrId` must fit whatever width your wire format reserves for it (e.g.
32 bits for the default `arterial_codec_default` framing below); pick a
width wide enough that `erlang:unique_integer/1`-derived ids (see
`arterial_client:new_corr_id/0`) don't collide after truncation for as
long as a request can plausibly still be in flight.
""".

-doc """
Encode `Request` (assigned `CorrId` by `arterial_client:call/3` or
`cast/2`) into the bytes to write on the wire.
""".
-callback encode_request(CorrId :: non_neg_integer(), Request :: term()) -> iodata().

-doc """
Try to decode exactly one complete frame off the front of `Buffer` (the
unconsumed bytes accumulated so far for one connection, across one or
more reads).

Returns `{ok, CorrId, Reply, Rest}` for a complete frame (`Rest` is
fed right back into `decode/1` in case `Buffer` held more than one
frame), `more` if `Buffer` doesn't yet hold a complete frame (wait for
the next read), or `{error, Reason}` for unrecoverable, framing-breaking
corruption -- on `{error, _}`, `arterial_connection` logs and drops the
connection (forcing a reconnect), since there's no way to know where the
next valid frame would even start.
""".
-callback decode(Buffer :: binary()) ->
  {ok, CorrId :: non_neg_integer(), Reply :: term(), Rest :: binary()} |
  more |
  {error, term()}.
