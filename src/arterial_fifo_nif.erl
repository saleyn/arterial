-module(arterial_fifo_nif).

-moduledoc """
NIF module for FIFO request/reply matching modes.

This module provides the NIF interface for FIFO modes (3, 4, 6-bulk)
with complete separation from the existing `arterial_nif` module to
ensure ZERO performance impact on random-access modes.

All functions in this module are implemented in C++ and loaded from
the same shared library as arterial_nif, but with separate function
entry points to maintain isolation.

## Safety and Error Handling

All NIF functions include comprehensive error checking and will return
appropriate error tuples rather than crashing. Invalid arguments result
in `badarg` errors as per Erlang NIF conventions.

## Memory Management

FIFO extensions are allocated on-demand and automatically cleaned up
when connections are released. No manual memory management is required
from the Erlang side.
""".

-export([
  % Mode 3: FIFO Single Request
  reserve_fifo_connection/3,
  send_fifo_request/6,
  release_fifo_connection/4,
  fifo_connection_status/3,
  handle_fifo_reply/4,
  % Combined operations for performance (#3)
  reserve_send_fifo_request/5
]).

-on_load(init/0).

%%%-----------------------------------------------------------------------------
%%% NIF Loading
%%%-----------------------------------------------------------------------------

%% Use arterial_nif functions directly (FIFO functions are integrated)
init() ->
  ok.

%%%-----------------------------------------------------------------------------
%%% FIFO Mode 3: Single Request (Synchronous) NIFs
%%%-----------------------------------------------------------------------------

-doc """
Reserve a connection for FIFO mode operation.

## Arguments

- `PoolRef`: Pool context reference from arterial_pool
- `StripeId`: Which stripe to reserve from (0-based)
- `TimeoutMs`: Reservation timeout in milliseconds

## Returns

- `{ok, fifo_reserved, StripeId, SlotId, ReservationId}`: Success
- `{error, no_connections_available}`: All connections busy
- `{error, fifo_init_failed}`: Failed to initialize FIFO extension
""".
-spec reserve_fifo_connection(reference(), non_neg_integer(), non_neg_integer()) ->
  {ok, fifo_reserved, non_neg_integer(), non_neg_integer(), non_neg_integer()} |
  {error, atom()}.
reserve_fifo_connection(PoolRef, StripeId, TimeoutMs) ->
  arterial_nif:reserve_fifo_connection(PoolRef, StripeId, TimeoutMs).

-doc """
Send request on reserved FIFO connection.

## Arguments

- `PoolRef`: Pool context reference
- `StripeId`: Stripe ID from reservation
- `SlotId`: Slot ID from reservation
- `ReservationId`: Reservation ID for validation
- `RequestData`: List of binaries containing request data
- `TimeoutMs`: Request timeout in milliseconds

## Returns

- `{ok, fifo_request_sent}`: Request successfully sent
- `{error, invalid_reservation}`: Reservation invalid/expired
- `{error, write_failed}`: Failed to write to socket
""".
-spec send_fifo_request(reference(), non_neg_integer(), non_neg_integer(),
                       non_neg_integer(), [binary()], non_neg_integer()) ->
  {ok, fifo_request_sent} | {error, atom()}.
send_fifo_request(PoolRef, StripeId, SlotId, ReservationId, RequestData, TimeoutMs) ->
  arterial_nif:send_fifo_request(PoolRef, StripeId, SlotId, ReservationId, RequestData, TimeoutMs).

-doc """
Release FIFO connection reservation.

## Arguments

- `PoolRef`: Pool context reference
- `StripeId`: Stripe ID from reservation
- `SlotId`: Slot ID from reservation
- `ReservationId`: Reservation ID for validation

## Returns

- `ok`: Successfully released
- `{error, invalid_reservation}`: Reservation was invalid
""".
-spec release_fifo_connection(reference(), non_neg_integer(), non_neg_integer(), non_neg_integer()) ->
  ok | {error, atom()}.
release_fifo_connection(PoolRef, StripeId, SlotId, ReservationId) ->
  arterial_nif:release_fifo_connection(PoolRef, StripeId, SlotId, ReservationId).

-doc """
Get FIFO connection status and statistics.

## Arguments

- `PoolRef`: Pool context reference
- `StripeId`: Stripe ID to check
- `SlotId`: Slot ID to check

## Returns

- `{ok, Status, TotalRequests, TotalTimeouts}`: Current status and stats
- `{error, not_fifo}`: Slot is not in FIFO mode
""".
-spec fifo_connection_status(reference(), non_neg_integer(), non_neg_integer()) ->
  {ok, atom(), non_neg_integer(), non_neg_integer()} | {error, atom()}.
fifo_connection_status(PoolRef, StripeId, SlotId) ->
  arterial_nif:fifo_connection_status(PoolRef, StripeId, SlotId).

-doc """
Handle FIFO reply data (called by arterial_connection).

This function is called internally by arterial_connection when reply
data arrives for a FIFO connection. It routes the reply to the
appropriate waiting process.

## Arguments

- `PoolRef`: Pool context reference
- `StripeId`: Stripe ID where reply arrived
- `SlotId`: Slot ID where reply arrived
- `ReplyData`: The reply data received

## Returns

- `ok`: Reply successfully routed
- `{error, fifo_not_enabled}`: Slot not in FIFO mode
""".
-spec handle_fifo_reply(reference(), non_neg_integer(), non_neg_integer(), binary()) ->
  ok | {error, atom()}.
handle_fifo_reply(PoolRef, StripeId, SlotId, ReplyData) ->
  arterial_nif:handle_fifo_reply(PoolRef, StripeId, SlotId, ReplyData).

-doc """
Combined reserve and send FIFO request operation (NIF call optimization).

This is a performance optimization that combines connection reservation and
request sending into a single NIF call, reducing the overhead from the
Reserve -> Send pattern. It includes intelligent connection queuing/waiting.

## Arguments

- `PoolRef`: Pool context reference
- `StripeId`: Which stripe to reserve from (0-based)
- `RequestData`: List of binaries containing request data to send
- `ReservationTimeoutMs`: Timeout for connection reservation (with queuing)
- `RequestTimeoutMs`: Timeout for sending the request

## Returns

- `{ok, fifo_request_sent, StripeId, SlotId, ReservationId}`: Success
- `{error, no_connections_available}`: All connections busy after timeout
- `{error, write_failed}`: Failed to write to socket
- `{error, timeout}`: Timeout during reservation or send
""".
-spec reserve_send_fifo_request(reference(), non_neg_integer(), [binary()],
                               non_neg_integer(), non_neg_integer()) ->
  {ok, fifo_request_sent, non_neg_integer(), non_neg_integer(), non_neg_integer()} |
  {error, atom()}.
reserve_send_fifo_request(PoolRef, StripeId, RequestData, ReservationTimeoutMs, RequestTimeoutMs) ->
  arterial_nif:reserve_send_fifo_request(PoolRef, StripeId, RequestData, ReservationTimeoutMs, RequestTimeoutMs).