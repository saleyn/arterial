///
/// A connection has N requests of type RequestInfo tracked in a backlog,
/// and a vector of rate throttles that must all pass before the connection
/// can be used to send new requests.
///
#pragma once

#include "throttle.hpp"
#include "backlog.hpp"
#include <erl_nif.h>
#include <memory>
#include <vector>

namespace arterial {

struct ThrottleInit {
  uint32_t rate;
  uint32_t window_msec;
};

//-----------------------------------------------------------------------------
/// @brief Connections live across many NIF calls, each with its own
/// short-lived ErlNifEnv. An ERL_NIF_TERM captured in one call's env is not
/// valid in another, so the connection's socket term is kept in a private,
/// long-lived env owned by the connection, and copied in/out of the
/// caller's env on every access via enif_make_copy().
//-----------------------------------------------------------------------------
struct Connection {
  using Throttle    = basic_time_spacing_throttle<uint32_t>;
  using ThrottleVec = std::vector<Throttle>;

  /// @param a_id        unique connection id (index in the owning pool)
  /// @param a_backlog   max number of in-flight requests on this connection
  /// @param a_fifo      true: FIFO backlog (no wire-level request IDs);
  ///                    false: random-access backlog (wire-level request IDs)
  /// @param a_throttles rate throttles that must all pass before checkout
  Connection(uint32_t a_id, BaseReqID a_backlog, bool a_fifo,
             std::vector<ThrottleInit> const& a_throttles = {})
  : m_id(a_id)
  , m_requests(AbstractBackLog::Create(a_backlog, a_fifo))
  , m_env(enif_alloc_env())
  {
    m_throttles.reserve(a_throttles.size());
    for (auto& t : a_throttles)
      m_throttles.emplace_back(t.rate, t.window_msec);
  }

  ~Connection() { enif_free_env(m_env); }

  Connection(Connection const&)            = delete;
  Connection& operator=(Connection const&) = delete;

  uint32_t           ID()                  const { return m_id;        }
  ThrottleVec&       Throttles()                 { return m_throttles; }
  size_t             ThrottlesCount()      const { return m_throttles.size(); }
  AbstractBackLog&   Requests()                  { return *m_requests; }

  /// @brief Copy the connection's socket term into the caller's env.
  ERL_NIF_TERM Socket(ErlNifEnv* call_env) const
  {
    return m_has_socket ? enif_make_copy(call_env, m_socket) : 0;
  }

  /// @brief Copy `socket` (from the caller's env) into this connection's
  /// own long-lived env, replacing any previously stored socket term.
  void Socket(ERL_NIF_TERM socket)
  {
    enif_clear_env(m_env);
    m_socket     = enif_make_copy(m_env, socket);
    m_has_socket = true;
  }

private:
  uint32_t                          m_id;
  std::unique_ptr<AbstractBackLog>  m_requests;
  ThrottleVec                       m_throttles;
  ErlNifEnv*                        m_env;
  ERL_NIF_TERM                      m_socket     = 0;
  bool                              m_has_socket = false;
};

} // namespace arterial
