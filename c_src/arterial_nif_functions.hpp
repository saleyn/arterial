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
static ERL_NIF_TERM handle_readable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_writable_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//=============================================================================
// Slot Management NIFs
//=============================================================================

static ERL_NIF_TERM close_slot_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM handle_connection_timeout_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
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
  {"handle_readable",             3, handle_readable_nif,           0},
  {"handle_writable",             3, handle_writable_nif,           0},

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
  {"reserve_send_fifo_request",   5, reserve_send_fifo_request_nif, 0}
};