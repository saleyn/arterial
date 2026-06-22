#include "arterial.hpp"
#include "enif.hpp"
#include <cassert>
#include <iostream>

using namespace nifpp;
using namespace arterial;

namespace {

// Resource `down` event: fires when a process holding a monitored
// checkout (see ConnectionPool::MonitorOwner()) dies, so its reservations
// don't leak forever. See ConnectionPool::OnProcessDown().
void on_pool_down(ConnectionPool* pool, ErlNifEnv*, ErlNifPid* pid, ErlNifMonitor*)
{
  pool->OnProcessDown(*pid);
}

ERL_NIF_TERM create_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 4);

  unsigned int size;
  unsigned int backlog;
  unsigned int max_waiters;
  unsigned int ttl_shards;

  if (!get(env, argv[0], size) || size == 0 ||
      !get(env, argv[1], backlog) || backlog == 0 ||
      !get(env, argv[2], max_waiters) ||
      !get(env, argv[3], ttl_shards)) [[unlikely]]
    return enif_make_badarg(env);

  auto pool = construct_resource_with_events<ConnectionPool>(
    resource_events<ConnectionPool>(on_pool_down),
    size, backlog, size_t(max_waiters), size_t(ttl_shards));

  return make(env, std::make_tuple(am_ok, pool));
}

ERL_NIF_TERM destroy_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 1);

  resource_ptr<ConnectionPool> ptr;
  if (!get(env, argv[0], ptr)) [[unlikely]]
    return enif_make_badarg(env);

  return make(env, am_ok);
}

ERL_NIF_TERM make_available_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 2);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 id;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], id)) [[unlikely]]
    return enif_make_badarg(env);

  return make(env, ptr->MakeAvailable(id));
}

ERL_NIF_TERM make_unavailable_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 2);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 id;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], id)) [[unlikely]]
    return enif_make_badarg(env);

  return make(env, ptr->MakeUnavailable(id));
}

ERL_NIF_TERM connection_drained_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 2);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 id;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], id)) [[unlikely]]
    return enif_make_badarg(env);

  return make(env, ptr->IsDrained(id));
}

ERL_NIF_TERM checkout_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 3);

  resource_ptr<ConnectionPool> ptr;
  ErlNifPid                    pid;
  unsigned int                 samples;

  if (!get(env, argv[0], ptr) ||
      !enif_get_local_pid(env, argv[1], &pid) ||
      !get(env, argv[2], samples) || samples == 0) [[unlikely]]
    return enif_make_badarg(env);

  auto* conn = ptr->CheckOut(samples);

  if (!conn)
    return make(env, std::make_tuple(am_error, atom(env, "no_connection")));

  // The calling process now owns this connection's reserved backlog
  // slot(s); monitor it so they're released automatically if it dies
  // before checking them back in (see ConnectionPool::OnProcessDown()).
  if (!ptr->MonitorOwner(env, pid, conn->ID(), samples)) [[unlikely]] {
    // pid is already gone (a race with the caller's own death) -- undo
    // the reservation instead of leaking it forever.
    conn->CheckIn(samples);
    return make(env, std::make_tuple(am_error, atom(env, "no_connection")));
  }

  return make(env, std::make_tuple(am_ok, conn->ID()));
}

ERL_NIF_TERM checkin_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 5);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 id;
  unsigned int                 samples;
  ErlNifPid                    pid;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], id) ||
      !get(env, argv[2], samples) ||
      !enif_get_local_pid(env, argv[3], &pid)) [[unlikely]]
    return enif_make_badarg(env);

  if (!ptr->Get(id)) [[unlikely]]
    return enif_make_badarg(env);

  ptr->ReleaseOwnerReservation(env, pid, id, samples);
  ptr->CheckIn(id, samples, env, argv[4]);

  return make(env, am_ok);
}

ERL_NIF_TERM checkout_async_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 4);

  resource_ptr<ConnectionPool> ptr;
  ErlNifPid                    pid;
  unsigned int                 samples;

  if (!get(env, argv[0], ptr) ||
      !enif_get_local_pid(env, argv[1], &pid) ||
      !get(env, argv[2], samples) || samples == 0) [[unlikely]]
    return enif_make_badarg(env);

  auto result = ptr->CheckOutAsync(env, argv[3], pid, samples);

  switch (result.status) {
    case AsyncCheckoutStatus::Ok:
      return make(env, std::make_tuple(am_ok, result.conn->ID()));
    case AsyncCheckoutStatus::Queued:
      return make(env, std::make_tuple(atom(env, "queued"),
        (ErlNifUInt64)result.waiter_id));
    case AsyncCheckoutStatus::Rejected:
    default:
      return make(env, std::make_tuple(am_error, atom(env, "no_connection")));
  }
}

int load(ErlNifEnv* env, void**, ERL_NIF_TERM)
{
  nifpp::initialize_known_atoms(env);

  if (!register_resource<ConnectionPool>(env, "arterial_pool")) {
    std::cerr << "Cannot register resource arterial_pool ["
              << __FILE__ << ":" << __LINE__ << "]\n";
    return 1;
  }

  return 0;
}

int upgrade(ErlNifEnv*, void**, void** old_priv_data, ERL_NIF_TERM)
{
  if (old_priv_data && *old_priv_data)
    enif_release_resource(*old_priv_data);
  return 0;
}

ErlNifFunc nif_funcs[] = {
  {"create_pool",          4, create_nif,           0},
  {"destroy_pool",         1, destroy_nif,          0},
  {"make_available_nif",   2, make_available_nif,   0},
  {"make_unavailable_nif", 2, make_unavailable_nif, 0},
  {"connection_drained_nif", 2, connection_drained_nif, 0},
  {"checkout_nif",         3, checkout_nif,         0},
  {"checkin_nif",          5, checkin_nif,          0},
  {"checkout_async_nif",   4, checkout_async_nif,   0},
};

} // namespace

ERL_NIF_INIT(arterial_nif, nif_funcs, load, nullptr, upgrade, nullptr)
