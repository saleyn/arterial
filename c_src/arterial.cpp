#include "arterial.hpp"
#include <cassert>

use namespace nifpp;

static ERL_NIF_TERM create_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 2);

  std::string       name;
  std::vector<TERM> sockets;

  if (!get(env, argv[0], name) || !get(env, argv[1], sockets) || sockets.size() == 0) [[unlikely]]
    return enif_make_badarg(env);

  std::vector<std::unique_ptr<Connection>> connections;
  connections.reserve(sockets.size());

  for (auto s : sockets)
    connections.emplace_back(new Connection(s));

  resource_events<ConnectionPool> events(
    [](Connection* conn, ErlNifEnv* env, ErlNifPid* pid, ErlNifMonitor* mon) {
      conn->OnPidDown(env, pid, mon);
    }
  );
  auto pool = construct_resource_with_events<ConnectionPool>(events, connections);

  // The call above transfers the ownership of objects to the pool.
  for (auto& c : connections) {
    assert(!c);
  }

  return make(env, make_tuple(am_ok, pool));
}

static ERL_NIF_TERM destroy_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
  assert(argc == 1);

  resource_ptr<ConnectionPool> ptr;
  if (!get(env, argv[0], ptr)) [[unlikely]]
    return enif_make_badarg(env);

  
}

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
  nifpp::initialize_known_atoms(env);

  if (!register_resource<ConnectionPool>(env, "Elixir.Arterial.Pool")) {
    std::cerr << "Cannot register resource Arterial.Pool ["
              << __FILE__ << ":" << __LINE__ << "]\n";
    return 1;
  }

  return 0;
}

static int upgrade(ErlNifEnv* env, void** priv_data, void** old_priv_data, ERL_NIF_TERM load_info) {
  if (old_priv_data)
    enif_release_resource(old_priv_data);
}

static ErlNifFunc nif_funcs[] = {
  {"create_nif",   2, create_nif},
  {"destroy_nif",  1, destroy_nif},
  {"checkout_nif", 1, checkout_nif},
  {"checkin_nif",  2, checkin_nif},
};

ERL_NIF_INIT(Elixir.Arterial.Pool, nif_funcs, load, nullptr, upgrade, nullptr);

