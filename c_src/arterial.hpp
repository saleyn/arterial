#pragma once

#include "nifpp.h"
#include "arterial.hpp"

using nifpp;

struct Connection
{
  explicit Connection(TERM socket) : m_socket(socket) {}

  TERM Socket() const { return m_socket; }

  void Socket(TERM socket) { m_socket = socket; }

private:
  TERM m_socket;
};

struct ConnectionPool : arterial::ObjectPool<Connection>
{
  using BaseT = arterial::ObjectPool<Connection>;

  ConnectionPool(std::string const& name, size_t size)
    : BaseT(size)
    , m_name(name)
  {}

  ~ConnectionPool() {
    std::cerr << "Releasing connection pool '" << m_name << "'\r\n";
  }

private:
  std::string m_name;
};
