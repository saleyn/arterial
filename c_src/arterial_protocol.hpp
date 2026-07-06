#pragma once

#include "arterial_core.hpp"
#include "arterial_ssl.hpp"

namespace arterial {

//=============================================================================
// Protocol Abstraction Interface
//=============================================================================

// Zero-cost protocol abstraction using compile-time dispatch
// This provides a clean interface without virtual function overhead

template<ProtocolType Proto>
struct ProtocolHandler {
  static int     create_socket();
  static bool    configure_socket(int fd, bool nodelay);
  static int     connect_socket(int fd, const struct sockaddr_in& addr);
  static bool    setup_connection(Connection& slot, int fd);
  static int     handle_connect_result(Connection& slot, ErlNifEnv* env);
  static ssize_t read_data(Connection& slot, char* buffer, size_t buffer_size);
  static ssize_t write_data(Connection& slot, const struct iovec* iov, int iov_count);
  static void    cleanup_connection(Connection& slot);
};

//=============================================================================
// TCP Protocol Handler
//=============================================================================

template<>
struct ProtocolHandler<PROTO_TCP> {
  static int create_socket() {
    return socket(AF_INET, SOCK_STREAM, 0);
  }

  static bool configure_socket(int fd, bool nodelay) {
    // Set non-blocking
    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
      return false;
    }

    // Set TCP_NODELAY if requested
    if (nodelay) {
      int flag = 1;
      if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag)) == -1) {
        return false;
      }
    }

    return true;
  }

  static int connect_socket(int fd, const struct sockaddr_in& addr) {
    return connect(fd, (struct sockaddr*)&addr, sizeof(addr));
  }

  static bool setup_connection(Connection& slot, int fd) {
    slot.fd = fd;
#ifdef HAVE_OPENSSL
    slot.protocol = PROTO_TCP;
    slot.ssl = nullptr;
#endif
    return true;
  }

  static int handle_connect_result(Connection& slot, [[maybe_unused]] ErlNifEnv* env) {
    // Check if connection completed successfully
    int so_err = 0;
    socklen_t len = sizeof(so_err);

    if (getsockopt(slot.fd, SOL_SOCKET, SO_ERROR, &so_err, &len) == -1 || so_err != 0)
      return -1; // Connection failed

    slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
    return 1; // Success
  }

  static ssize_t read_data(Connection& slot, char* buffer, size_t buffer_size) {
    return read(slot.fd, buffer, buffer_size);
  }

  static ssize_t write_data(Connection& slot, const struct iovec* iov, int iov_count) {
    return writev(slot.fd, iov, iov_count);
  }

  static void cleanup_connection(Connection& slot) {
    // TCP has no special cleanup beyond closing the fd
    if (slot.fd != -1) {
      close(slot.fd);
      slot.fd = -1;
    }
  }
};

//=============================================================================
// UDP Protocol Handler
//=============================================================================

template<>
struct ProtocolHandler<PROTO_UDP> {
  static int create_socket() {
    return socket(AF_INET, SOCK_DGRAM, 0);
  }

  static bool configure_socket(int fd, bool /* nodelay - ignored for UDP */) {
    // Set non-blocking
    if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) {
      return false;
    }
    return true;
  }

  static int connect_socket(int fd, const struct sockaddr_in& addr) {
    // For UDP, we can optionally "connect" to set default destination
    return connect(fd, (struct sockaddr*)&addr, sizeof(addr));
  }

  static bool setup_connection(Connection& slot, int fd) {
    slot.fd = fd;
#ifdef HAVE_OPENSSL
    slot.protocol = PROTO_UDP;
    slot.ssl = nullptr;
#endif
    return true;
  }

  static int handle_connect_result(Connection& slot, ErlNifEnv* /* env */) {
    // UDP connections are generally immediate
    slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
    return 1; // Success
  }

  static ssize_t read_data(Connection& slot, char* buffer, size_t buffer_size) {
    return read(slot.fd, buffer, buffer_size);
  }

  static ssize_t write_data(Connection& slot, const struct iovec* iov, int iov_count) {
    return writev(slot.fd, iov, iov_count);
  }

  static void cleanup_connection(Connection& slot) {
    // UDP has no special cleanup beyond closing the fd
    if (slot.fd != -1) {
      close(slot.fd);
      slot.fd = -1;
    }
  }
};

#ifdef HAVE_OPENSSL
//=============================================================================
// SSL Protocol Handler
//=============================================================================

template<>
struct ProtocolHandler<PROTO_SSL> {
  static int create_socket() {
    // SSL uses TCP socket underneath
    return socket(AF_INET, SOCK_STREAM, 0);
  }

  static bool configure_socket(int fd, bool nodelay) {
    // Same as TCP configuration
    return ProtocolHandler<PROTO_TCP>::configure_socket(fd, nodelay);
  }

  static int connect_socket(int fd, const struct sockaddr_in& addr) {
    // Same as TCP connection
    return connect(fd, (struct sockaddr*)&addr, sizeof(addr));
  }

  static bool setup_connection(Connection& slot, int fd) {
    slot.fd = fd;
    slot.protocol = PROTO_SSL;
    return setup_ssl_on_socket(slot, fd);
  }

  static int handle_connect_result(Connection& slot, [[maybe_unused]] ErlNifEnv* env) {
    // First check if TCP connection is established
    int so_err = 0;
    socklen_t len = sizeof(so_err);

    if (getsockopt(slot.fd, SOL_SOCKET, SO_ERROR, &so_err, &len) == -1 || so_err != 0) {
      return -1; // TCP connection failed
    }

    // TCP is connected, now perform SSL handshake
    int handshake_result = ssl_handshake_nonblocking(slot, 5000);
    if (handshake_result == 1) {
      slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
      return 1; // SSL handshake completed
    } else if (handshake_result == 0 || handshake_result == -2) {
      slot.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
      return 0; // Handshake in progress
    } else {
      return -1; // Handshake failed
    }
  }

  static ssize_t read_data(Connection& slot, char* buffer, size_t buffer_size) {
    if (slot.ssl) {
      return SSL_read(slot.ssl, buffer, buffer_size);
    }
    return -1; // SSL not initialized
  }

  static ssize_t write_data(Connection& slot, const struct iovec* iov, int iov_count) {
    if (!slot.ssl) return -1;

    // SSL_write doesn't support iovec, so we need to flatten
    if (iov_count == 1) {
      return SSL_write(slot.ssl, iov[0].iov_base, iov[0].iov_len);
    }

    // For multiple iovec entries, we need to write them separately
    ssize_t total_written = 0;
    for (int i = 0; i < iov_count; ++i) {
      ssize_t written = SSL_write(slot.ssl, iov[i].iov_base, iov[i].iov_len);
      if (written <= 0) {
        return total_written > 0 ? total_written : written;
      }
      total_written += written;
    }
    return total_written;
  }

  static void cleanup_connection(Connection& slot) {
    cleanup_slot_ssl(slot);
  }
};
#endif

//=============================================================================
// Protocol Dispatch Functions
//=============================================================================

// Helper function to create socket for a specific protocol
static int create_socket_for_protocol(ProtocolType protocol) {
  switch (protocol) {
    case PROTO_TCP:
    case PROTO_SSL: // SSL uses TCP socket - SSL handshake happens after connection
      return socket(AF_INET, SOCK_STREAM, 0);
    case PROTO_UDP:
      return socket(AF_INET, SOCK_DGRAM, 0);
    default:
      return -1;
  }
}

// Helper function to set socket options based on protocol
static bool configure_socket_for_protocol(int fd, ProtocolType protocol, bool nodelay) {
  if (fcntl(fd, F_SETFL, O_NONBLOCK) == -1) return false;

  switch (protocol) {
    case PROTO_TCP:
    case PROTO_SSL:  // SSL uses TCP socket underneath
      if (nodelay) {
        static constexpr int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      }
      break;
    case PROTO_UDP:
      // UDP doesn't need nodelay, already connectionless
      break;
    default:
      return false;
  }
  return true;
}

// Helper to parse protocol atom
static ProtocolType parse_protocol(ErlNifEnv* env, ERL_NIF_TERM term) {
  atom proto_atom;
  if (!get(env, term, proto_atom))
    return static_cast<ProtocolType>(-1);

  ProtocolType result;
  if (proto_atom == am_tcp) result = PROTO_TCP;
  else if (proto_atom == am_udp) result = PROTO_UDP;
  else if (proto_atom == am_ssl) result = PROTO_SSL;
  else result = static_cast<ProtocolType>(-1);


  return result;
}

static bool parse_protocol(ErlNifEnv* env, ERL_NIF_TERM term, ProtocolType& proto) {
  proto = parse_protocol(env, term);
  return proto != static_cast<ProtocolType>(-1);
}

} // namespace arterial