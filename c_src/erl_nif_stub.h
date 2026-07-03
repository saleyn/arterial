// Minimal stub of erl_nif.h for building reactor_test without Erlang ERTS.
// Used only when REACTOR_TEST_STUB_NIF is defined.
#pragma once
#include <stdint.h>
#include <stddef.h>

typedef struct enif_environment_t ErlNifEnv;
typedef unsigned long ERL_NIF_TERM;

typedef struct {
  // Must be large enough for the real struct (64 bytes is safe).
  char opaque[64];
} ErlNifPid;

#ifdef __cplusplus
extern "C" {
#endif
ErlNifEnv*   enif_alloc_env(void);
void         enif_free_env(ErlNifEnv*);
int          enif_send(ErlNifEnv*, const ErlNifPid*, ErlNifEnv*, ERL_NIF_TERM);
ERL_NIF_TERM enif_make_atom(ErlNifEnv*, const char*);
ERL_NIF_TERM enif_make_uint(ErlNifEnv*, unsigned int);
ERL_NIF_TERM enif_make_tuple3(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM);
ERL_NIF_TERM enif_make_tuple4(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM);
ERL_NIF_TERM enif_make_tuple5(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM,
                               ERL_NIF_TERM, ERL_NIF_TERM);
ERL_NIF_TERM enif_make_int    (ErlNifEnv*, int);
ERL_NIF_TERM enif_make_string (ErlNifEnv*, const char*, unsigned int);
#ifdef __cplusplus
}
#endif
