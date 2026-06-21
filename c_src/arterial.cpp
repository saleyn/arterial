#include "arterial.hpp"
#include "nifpp.h"
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
  assert(argc == 6);

  unsigned int size;
  unsigned int backlog;
  bool         fifo;
  ErlNifUInt64 fixed_ttl_us;
  unsigned int max_waiters;
  unsigned int ttl_shards;

  if (!get(env, argv[0], size) || size == 0 ||
      !get(env, argv[1], backlog) || backlog == 0 ||
      backlog > std::numeric_limits<BaseReqID>::max() ||
      !get(env, argv[2], fifo) ||
      !enif_get_uint64(env, argv[3], &fixed_ttl_us) ||
      !get(env, argv[4], max_waiters) ||
      !get(env, argv[5], ttl_shards)) [[unlikely]]
    return enif_make_badarg(env);

  auto pool = construct_resource_with_events<ConnectionPool>(
    resource_events<ConnectionPool>(on_pool_down),
    size, BaseReqID(backlog), fifo, uint64_t(fixed_ttl_us), size_t(max_waiters),
    size_t(ttl_shards));

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

ERL_NIF_TERM set_socket_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 3);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 id;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], id)) [[unlikely]]
    return enif_make_badarg(env);

  return make(env, ptr->SetSocket(id, argv[2]));
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

  auto [conn, ids] = ptr->CheckOut(samples);

  if (!conn)
    return make(env, std::make_tuple(am_error, atom(env, "no_connection")));

  // The calling process now owns this connection's reserved backlog
  // slot(s); monitor it so they're released automatically if it dies
  // before checking them back in (see ConnectionPool::OnProcessDown()).
  if (!ptr->MonitorOwner(env, pid, conn->ID(), ids)) [[unlikely]] {
    // pid is already gone (a race with the caller's own death) -- undo
    // the reservation instead of leaking it forever.
    for (auto id : ids)
      conn->Requests().CheckIn(id);
    ptr->CheckIn(conn->ID());
    return make(env, std::make_tuple(am_error, atom(env, "no_connection")));
  }

  std::vector<unsigned int> req_ids(ids.begin(), ids.end());

  return make(env, std::make_tuple(am_ok,
    std::make_tuple(conn->ID(), TERM(conn->Socket(env)), req_ids)));
}

ERL_NIF_TERM checkin_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 5);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 id;
  std::vector<unsigned int>    req_ids;
  ErlNifPid                    pid;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], id) ||
      !get(env, argv[2], req_ids) ||
      !enif_get_local_pid(env, argv[4], &pid)) [[unlikely]]
    return enif_make_badarg(env);

  auto* conn = ptr->Get(id);
  if (!conn) [[unlikely]]
    return enif_make_badarg(env);

  std::vector<ReqID> ids(req_ids.begin(), req_ids.end());

  for (auto rid : ids) {
    conn->Requests().CheckIn(rid);
    ptr->UntrackInflight(rid);
  }

  ptr->ReleaseOwnerReservation(env, pid, id, ids);
  ptr->CheckIn(id, env, argv[3]);

  return make(env, am_ok);
}

ERL_NIF_TERM checkout_async_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 3);

  resource_ptr<ConnectionPool> ptr;
  ErlNifPid                    pid;
  ErlNifUInt64                 ttl_us;

  if (!get(env, argv[0], ptr) ||
      !enif_get_local_pid(env, argv[1], &pid) ||
      !enif_get_uint64(env, argv[2], &ttl_us)) [[unlikely]]
    return enif_make_badarg(env);

  auto result = ptr->CheckOutAsync(pid, uint64_t(ttl_us), 1);

  switch (result.status) {
    case AsyncCheckoutStatus::Ok: {
      // Mirrors checkout_nif(): the caller now owns this connection's
      // reserved backlog slot(s), so monitor it the same way.
      if (!ptr->MonitorOwner(env, pid, result.conn->ID(), result.ids)) [[unlikely]] {
        for (auto id : result.ids) {
          result.conn->Requests().CheckIn(id);
          ptr->UntrackInflight(id);
        }
        ptr->CheckIn(result.conn->ID());
        return make(env, std::make_tuple(am_error, atom(env, "no_connection")));
      }
      std::vector<unsigned int> req_ids(result.ids.begin(), result.ids.end());
      return make(env, std::make_tuple(am_ok,
        std::make_tuple(result.conn->ID(), TERM(result.conn->Socket(env)), req_ids)));
    }
    case AsyncCheckoutStatus::Queued:
      return make(env, std::make_tuple(atom(env, "queued"),
        (ErlNifUInt64)result.waiter_id));
    case AsyncCheckoutStatus::Rejected:
    default:
      return make(env, std::make_tuple(am_error, atom(env, "no_connection")));
  }
}

ERL_NIF_TERM track_inflight_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 5);

  resource_ptr<ConnectionPool> ptr;
  unsigned int                 conn_id;
  unsigned int                 req_id;
  ErlNifPid                    pid;
  ErlNifUInt64                 ttl_us;

  if (!get(env, argv[0], ptr) || !get(env, argv[1], conn_id) ||
      !get(env, argv[2], req_id) ||
      !enif_get_local_pid(env, argv[3], &pid) ||
      !enif_get_uint64(env, argv[4], &ttl_us)) [[unlikely]]
    return enif_make_badarg(env);

  auto* conn = ptr->Get(conn_id);
  if (!conn) [[unlikely]]
    return enif_make_badarg(env);

  ptr->TrackInflight(ReqID(req_id), pid, conn, uint64_t(ttl_us));

  return make(env, am_ok);
}

ERL_NIF_TERM sweep_timeouts_nif(ErlNifEnv* env, [[maybe_unused]] int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 2);

  resource_ptr<ConnectionPool> ptr;

  if (!get(env, argv[0], ptr)) [[unlikely]]
    return enif_make_badarg(env);

  auto n = ptr->SweepTimeouts(env, argv[1]);

  return make(env, std::make_tuple(am_ok, (unsigned long)n));
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
  {"create_pool",          6, create_nif,           0},
  {"destroy_pool",         1, destroy_nif,          0},
  {"set_socket_nif",       3, set_socket_nif,       0},
  {"make_available_nif",   2, make_available_nif,   0},
  {"make_unavailable_nif", 2, make_unavailable_nif, 0},
  {"checkout_nif",         3, checkout_nif,         0},
  {"checkin_nif",          5, checkin_nif,          0},
  {"checkout_async_nif",   3, checkout_async_nif,   0},
  {"track_inflight_nif",   5, track_inflight_nif,   0},
  {"sweep_timeouts_nif",   2, sweep_timeouts_nif,   0},
};

} // namespace

ERL_NIF_INIT(arterial_nif, nif_funcs, load, nullptr, upgrade, nullptr)
