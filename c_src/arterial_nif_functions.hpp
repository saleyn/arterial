#pragma once

//=============================================================================
// NIF Function Declarations
//=============================================================================
// This header contains all NIF function declarations for arterial_nif.cpp

#include "arterial_core.hpp"

//=============================================================================
// Core Pool Management NIFs
//=============================================================================

static ERL_NIF_TERM init_pool_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM configure_throttle_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// Connection Management NIFs
//=============================================================================

static ERL_NIF_TERM register_socket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_async_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_async_proto_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM connect_proto_with_opts_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// I/O Operation NIFs
//=============================================================================

static ERL_NIF_TERM send_and_release_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM send_on_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_readable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_writable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// Slot Management NIFs
//=============================================================================

static ERL_NIF_TERM close_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_connection_timeout_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// Reactor server NIFs — pure-NIF listen/accept, no OTP socket involvement.
//=============================================================================
// reactor_listen(PoolRef, Port::integer()) → {ok, ListenFd::integer()} | {error, Reason}
//   Creates a non-blocking TCP listen socket bound to 127.0.0.1:Port (or any
//   interface if Port=0 for ephemeral).  Returns the raw fd for use with
//   reactor_accept.
static ERL_NIF_TERM reactor_listen_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// reactor_accept(PoolRef, ListenFd::integer(), OwnerPid::pid()) → ok | {error, Reason}
//   Registers ListenFd with the pool's Reactor.  Whenever a client connects,
//   the reactor calls accept4() and sends:
//     {arterial_accept, ListenFd, ClientFd::integer(), {IP4, Port}}
//   to OwnerPid.  The server process should then register ClientFd with the
//   reactor for I/O and re-arm accept if needed (it is persistent/multishot).
static ERL_NIF_TERM reactor_accept_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// reactor_close_fd(PoolRef, Fd::integer()) → ok
//   Removes Fd from the reactor and closes it (remove_fd).
static ERL_NIF_TERM reactor_close_fd_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

// reactor_register_client(PoolRef, StripeId, ClientFd, OwnerPid) → {ok, SlotId} | {error, Reason}
//   Registers a ClientFd (from reactor_accept) with the NIF pool for read/write
//   events.  OwnerPid receives {arterial_event, StripeId, SlotId, read, Bin}.
static ERL_NIF_TERM reactor_register_client_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM is_slot_available_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM set_slot_available_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM set_slot_unavailable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// FIFO Mode 3 NIFs
//=============================================================================

static ERL_NIF_TERM reserve_fifo_connection_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM send_fifo_request_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM release_fifo_connection_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM fifo_connection_status_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_fifo_reply_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM reserve_send_fifo_request_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// Corr-map NIFs (in-NIF correlation-id → caller-pid mapping)
//=============================================================================

static ERL_NIF_TERM register_corr_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM register_and_send_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM unregister_corr_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM lookup_and_remove_corr_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM corr_count_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM drain_corr_map_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM sweep_corr_map_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// Misc NIFs
//=============================================================================

static ERL_NIF_TERM info_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// NIF Module Lifecycle
//=============================================================================

extern "C" {

static int  load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info);
static void unload(ErlNifEnv* env, void* priv_data);

} // extern "C"

//=============================================================================
// NIF Function Table
//=============================================================================

static ErlNifFunc nif_funcs[] = {
  // Core pool management
  {"init_pool",                   2, init_pool_nif,                 0},
  {"init_pool",                   3, init_pool_nif,                 0},
  {"configure_throttle",          3, configure_throttle_nif,        0},

  // Connection management
  {"register_socket",             4, register_socket_nif,           0},
  {"connect",                     7, connect_nif,                   0},
  {"connect_async",               6, connect_async_nif,             0},
  {"connect_proto",               8, connect_proto_nif,             0},
  {"connect_async_proto",         7, connect_async_proto_nif,       0},
  {"connect_with_opts",           8, connect_with_opts_nif,         0},
  {"connect_proto_with_opts",     9, connect_proto_with_opts_nif,   0},

  // I/O operations
  {"send_and_release",            3, send_and_release_nif,          0},
  {"send_on_slot",                4, send_on_slot_nif,              0},
  {"handle_readable",             3, handle_readable_nif,           0},
  {"handle_writable",             3, handle_writable_nif,           0},

  // Reactor server (pure-NIF listen/accept, no OTP socket)
  {"reactor_listen",              2, reactor_listen_nif,            0},
  {"reactor_accept",              3, reactor_accept_nif,            0},
  {"reactor_close_fd",            2, reactor_close_fd_nif,          0},
  {"reactor_register_client",     4, reactor_register_client_nif,   0},

  // Slot management
  {"close_slot",                  3, close_slot_nif,                0},
  {"handle_connection_timeout",   3, handle_connection_timeout_nif, 0},
  {"is_slot_available",           3, is_slot_available_nif,         0},
  {"set_slot_available",          3, set_slot_available_nif,        0},
  {"set_slot_unavailable",        3, set_slot_unavailable_nif,      0},

  // FIFO Mode 3 functions
  {"reserve_fifo_connection",     3, reserve_fifo_connection_nif,   0},
  {"send_fifo_request",           6, send_fifo_request_nif,         0},
  {"release_fifo_connection",     4, release_fifo_connection_nif,   0},
  {"fifo_connection_status",      3, fifo_connection_status_nif,    0},
  {"handle_fifo_reply",           4, handle_fifo_reply_nif,         0},
  {"reserve_send_fifo_request",   5, reserve_send_fifo_request_nif, 0},

  // Corr-map NIFs
  {"register_corr",               6, register_corr_nif,             0},
  {"register_and_send",           6, register_and_send_nif,         0},
  {"unregister_corr",             3, unregister_corr_nif,           0},
  {"lookup_and_remove_corr",      3, lookup_and_remove_corr_nif,    0},
  {"corr_count",                  2, corr_count_nif,                0},
  {"drain_corr_map",              3, drain_corr_map_nif,            0},
  {"sweep_corr_map",              2, sweep_corr_map_nif,            0},

  {"info",                        0, info_nif,                      0},
};