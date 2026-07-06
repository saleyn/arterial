#pragma once

#include "arterial_connection.hpp"
#include "arterial_ssl.hpp"
#include "arterial_core.hpp"
#include "arterial_connection_timer.hpp"

namespace arterial {

// RAII timeout management - no forward declarations needed

//=============================================================================
// Connection Event Handler Implementations
//=============================================================================

inline Connection::ReadResultData
Connection::handle_readable(ErlNifEnv* env, PoolContext* ctx)
{
  if (fd == -1) [[unlikely]]
    return ReadResultData(ReadResult::CLOSED);

  // Stale read event from a previous connection on this slot: ignore.
  // This can happen when the slot is reused before pool_resource_stop
  // deregisters the old fd's enif_select.
  uint32_t current_status = status.load(std::memory_order_acquire);
  if (current_status == SLOT_CONNECTING || current_status == SLOT_EMPTY)
    return ReadResultData(ReadResult::DATA, nifpp::binary{0});

#ifdef HAVE_OPENSSL
  // Handle ongoing SSL handshake
  if (current_status == SLOT_SSL_HANDSHAKE) {
    if (protocol == PROTO_SSL && ssl) {
      int handshake_result = ssl_handshake_nonblocking(*this, 5000);

      if (handshake_result == 1) {
        // Handshake completed successfully - cancel any active timeout
        cancel_connection_timeout(*this);
        status.store(SLOT_AVAILABLE, std::memory_order_release);
        auto& stripe = *ctx->stripes[stripe_id];
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        arm_read(env, ctx); // TODO: error handling?

        ReadResultData result(ReadResult::CONNECT_OK);
        result.send_connect_msg = true;
        result.connect_result = am_ok;
        return result;
      } else if (handshake_result == 0) {
        // Still needs READ - arm read event and return
        arm_read(env, ctx); // TODO: error handling?
        return ReadResultData(ReadResult::HANDSHAKE_READ, nifpp::binary{0});
      } else if (handshake_result == -2) {
        // Still needs WRITE - arm write event
        arm_write(env, ctx); // TODO: handle errors
        return ReadResultData(ReadResult::HANDSHAKE_WRITE, nifpp::binary{0});
      } else {
        // Handshake failed
        cleanup_slot_ssl(*this);
        ReadResultData result(ReadResult::CONNECT_FAILED);
        result.send_connect_msg = true;
        result.connect_result = am_connect_failed;
        return result;
      }
    }
  }
#endif

  // FIONREAD is only a sizing hint, never proof of anything: it can
  // legitimately report 0 on a perfectly healthy connection (e.g. under
  // heavy concurrent load) without that meaning EOF -- only read(2)'s
  // own return value (0 = EOF, -1/EAGAIN = nothing available right now,
  // not closed) is authoritative.
  int bytes_available = 0;
  ioctl(fd, FIONREAD, &bytes_available);
  size_t read_size = bytes_available > 0 ? static_cast<size_t>(bytes_available) : 8192;

  nifpp::binary bin(read_size);
  if (!bin) return ReadResultData(ReadResult::ERROR);

  ssize_t n;

#ifdef HAVE_OPENSSL
  if (ssl) {
  RETRY0:
    n = SSL_read(ssl, bin.data, static_cast<int>(read_size));
    if (n <= 0) {
      int ssl_error = SSL_get_error(ssl, static_cast<int>(n));

      if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
        arm_read(env, ctx); // TODO: error handling?
        return ReadResultData(ReadResult::DATA, nifpp::binary{0});
      }

      // Handle retryable SSL errors more gracefully
      if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
        arm_read(env, ctx);
        return ReadResultData(ReadResult::DATA, nifpp::binary{0});
      }

      if (ssl_error == SSL_ERROR_SYSCALL) {
        if (errno == EINTR)
          goto RETRY0;
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
          cleanup_slot_ssl(*this);
          return ReadResultData(ReadResult::CLOSED);
        }
        arm_read(env, ctx);
        return ReadResultData(ReadResult::DATA, nifpp::binary{0});
      }

      if (ssl_error == SSL_ERROR_ZERO_RETURN) {
        return ReadResultData(ReadResult::DATA, nifpp::binary{0});
      }

      cleanup_slot_ssl(*this);
      return ReadResultData(ReadResult::CLOSED);
    }
  }
  else
#endif
  {
  RETRY1:
    n = read(fd, bin.data, read_size);

    if (n <= 0) {
      if (errno == EINTR)
        goto RETRY1;
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        arm_read(env, ctx); // TODO: error handling?
        return ReadResultData(ReadResult::DATA, nifpp::binary{0});
      }
      return ReadResultData(ReadResult::CLOSED);
    }
  }

  if (static_cast<size_t>(n) < bin.size && !bin.realloc(n)) {
    #ifdef HAVE_OPENSSL
    cleanup_slot_ssl(*this);
    #endif
    return ReadResultData(ReadResult::CLOSED);
  }

  // No arm_read: the reactor's persistent multishot EPOLLIN poll registered
  // at connect time continuously delivers read events without re-registration.

  // FIFO Mode 3: if this slot is reserved, deliver reply directly
  // to the waiting caller instead of returning bytes for codec decoding.
  if (fifo_request_active.load(std::memory_order_acquire)) {
    if (is_fifo_enabled()) {
      nifpp::msg_env msg_env;
      auto reply_msg = make_tuple(msg_env,
        am_arterial_fifo_reply,
        stripe_id,
        slot_id,
        std::move(bin)
      );
      enif_send(env, &fifo_requester_pid, msg_env, reply_msg);
      clear_fifo_request();
      // Return empty binary so arterial_connection has nothing to decode.
      return ReadResultData(ReadResult::DATA, nifpp::binary{0});
    } else {
      clear_fifo_request();
    }
  }

  return ReadResultData(ReadResult::DATA, std::move(bin));
}

inline Connection::WriteResultData Connection::handle_writable(ErlNifEnv* env, PoolContext* ctx) {
  if (fd == -1) [[unlikely]]
    return WriteResultData(WriteResult::CLOSED);

  uint32_t current_status = status.load(std::memory_order_acquire);

  // Handle connection completion for async connect
  if (current_status == SLOT_CONNECTING) {
    int so_err = 0;
    socklen_t len = sizeof(so_err);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &len) == -1 || so_err != 0) {
      // Connection failed - cancel timeout using RAII cleanup
      cancel_connection_timeout(*this);
      return WriteResultData(WriteResult::CONNECT_FAILED, true, am_connect_failed);
    } else {
      // Connection succeeded - cancel timeout using RAII cleanup
      cancel_connection_timeout(*this);
      // Check if we need SSL handshake
#ifdef HAVE_OPENSSL
      if (protocol == PROTO_SSL) {
        if (!setup_ssl_on_socket(*this, fd)) {
          return WriteResultData(WriteResult::CONNECT_FAILED, true, am_connect_failed);
        }

        // Start non-blocking SSL handshake
        int handshake_result = ssl_handshake_nonblocking(*this, 5000);

        if (handshake_result == 1) {
          // Handshake completed successfully - cancel any active timeout
          cancel_connection_timeout(*this);
          status.store(SLOT_AVAILABLE, std::memory_order_release);
          auto& stripe = *ctx->stripes[stripe_id];
          stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
          arm_read(env, ctx); // TODO: error handling?

          return WriteResultData(WriteResult::CONNECT_OK, true, am_ok);
        } else if (handshake_result == 0) {
          // Handshake needs READ - set status and arm read event
          status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          arm_read(env, ctx); // TODO: error handling?
          return WriteResultData(WriteResult::HANDSHAKE_READ);
        } else if (handshake_result == -2) {
          // Handshake needs WRITE - set status and arm write event
          status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          arm_write(env, ctx); // TODO: error handling?
          return WriteResultData(WriteResult::HANDSHAKE_WRITE);
        } else {
          // Handshake failed
          cleanup_slot_ssl(*this);
          return WriteResultData(WriteResult::CONNECT_FAILED, true, am_connect_failed);
        }
      } else
#endif
      {
        // Plain TCP connection succeeded
        status.store(SLOT_AVAILABLE, std::memory_order_release);
        auto& stripe = *ctx->stripes[stripe_id];
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        arm_read(env, ctx); // TODO: error handling?

        return WriteResultData(WriteResult::CONNECT_OK, true, am_ok);
      }
    }
  }

#ifdef HAVE_OPENSSL
  // Handle ongoing SSL handshake
  if (current_status == SLOT_SSL_HANDSHAKE && protocol == PROTO_SSL && ssl) {
    switch (ssl_handshake_nonblocking(*this, 5000)) {
      case 1:
      {
        // Handshake completed successfully - cancel any active timeout
        cancel_connection_timeout(*this);
        status.store(SLOT_AVAILABLE, std::memory_order_release);
        auto& stripe = *ctx->stripes[stripe_id];
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        arm_read(env, ctx); // TODO: error handling?

        return WriteResultData(WriteResult::CONNECT_OK, true, am_ok);
      }
      case 0:
        // Still needs READ - arm read event
        arm_read(env, ctx); // TODO: error handling?
        return WriteResultData(WriteResult::HANDSHAKE_READ);
      case -2:
        // Still needs WRITE - arm write event
        arm_write(env, ctx); // TODO: error handling?
        return WriteResultData(WriteResult::HANDSHAKE_WRITE);
      default:
      {
        // Handshake failed
        cleanup_slot_ssl(*this);
        return WriteResultData(WriteResult::CONNECT_FAILED, true, am_connect_failed);
      }
    }
  }
#endif

  // Handle pending write operations
  size_t remaining = pending_buffer.size() - bytes_written;
  while (remaining > 0) {
    ssize_t n;

#ifdef HAVE_OPENSSL
    if (ssl) {
      n = SSL_write(ssl, pending_buffer.data() + bytes_written, static_cast<int>(remaining));
      if (n <= 0) {
        int ssl_error = SSL_get_error(ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
          arm_write(env, ctx); // TODO: handle errors
          return WriteResultData(WriteResult::OK);
        }

        if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
          arm_write(env, ctx);
          return WriteResultData(WriteResult::OK);
        }

        if (ssl_error == SSL_ERROR_SYSCALL) {
          if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            cleanup_slot_ssl(*this);
            return WriteResultData(WriteResult::CLOSED);
          }
          arm_write(env, ctx);
          return WriteResultData(WriteResult::OK);
        }

        if (ssl_error == SSL_ERROR_ZERO_RETURN) {
          arm_write(env, ctx);
          return WriteResultData(WriteResult::OK);
        }

        cleanup_slot_ssl(*this);
        return WriteResultData(WriteResult::CLOSED);
      }
    } else
#endif
    {
    RETRYW:
      n = write(fd, pending_buffer.data() + bytes_written, remaining);
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          arm_write(env, ctx); // TODO: handle errors
          return WriteResultData(WriteResult::OK);
        }
        if (errno == EINTR) [[unlikely]]
          goto RETRYW;
        return WriteResultData(WriteResult::CLOSED);
      }
    }

    bytes_written += static_cast<size_t>(n);
    remaining     -= static_cast<size_t>(n);
  }

  pending_buffer.clear();
  bytes_written = 0;
  status.store(SLOT_AVAILABLE, std::memory_order_release);

  auto& stripe = *ctx->stripes[stripe_id];
  stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
  return WriteResultData(WriteResult::OK);
}

//=============================================================================
// Connection Establishment Method Implementations
//=============================================================================

inline Connection::ConnectResultData
Connection::connect_proto(ErlNifEnv* env, PoolContext* ctx,
                         unsigned int stripe_id,
                         const IP4Tuple& octets,
                         int port, unsigned int timeout_ms,
                         ProtocolType protocol, bool nodelay,
                         const ErlNifPid& owner_pid)
{
#ifndef HAVE_OPENSSL
  // SSL requires OpenSSL at compile time
  if (protocol == PROTO_SSL)
    return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_unsupported_protocol);
#endif

  // RAII socket creation - automatic cleanup on all exit paths
  auto socket_fd = FileDescriptor::create([&]() {
    return create_socket_for_protocol(protocol);
  });

  if (!socket_fd)
    return ConnectResultData(ConnectResult::SOCKET_FAILED, -1, am_socket_failed);

  if (!configure_socket_for_protocol(socket_fd.get(), protocol, nodelay)) {
    // No manual close() needed - RAII handles cleanup automatically
    return ConnectResultData(ConnectResult::CONFIG_FAILED, -1, am_failed_to_set_nonblocking);
  }

  auto [o0, o1, o2, o3] = octets;
  uint32_t ip_host = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  struct sockaddr_in server_addr{};
  server_addr.sin_family      = AF_INET;
  server_addr.sin_port        = htons(static_cast<uint16_t>(port));
  server_addr.sin_addr.s_addr = htonl(ip_host);

  // For UDP, "connecting" just sets the default destination
  // For TCP/SSL, this is a real connection
  int rc = connect(socket_fd.get(), (struct sockaddr*)&server_addr, sizeof(server_addr));

  if (protocol == PROTO_UDP)
    // UDP connect() just sets default peer, always succeeds immediately
    rc = 0;
  else if (rc != 0 && errno != EINPROGRESS)
    return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);

  // Handle non-blocking connect
  if (rc == 0) {
    // Connection completed immediately
#ifdef HAVE_OPENSSL
    if (protocol == PROTO_SSL) {
      auto& stripe = *ctx->stripes[stripe_id];
      int  slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);

      if (slot_id < 0)
        return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);

      auto& conn = stripe.slots[slot_id];
      conn.protocol = protocol;

      if (!setup_ssl_on_socket(conn, socket_fd.get())) {
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_connect_failed);
      }

      // Transfer socket ownership to connection after successful setup
      conn.fd = socket_fd.release();

      // Perform non-blocking SSL handshake
      int handshake_result = ssl_handshake_nonblocking(conn, timeout_ms);
      if (handshake_result == 1) {
        // Handshake completed successfully - cancel any active timeout
        cancel_connection_timeout(conn);
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        conn.arm_read(env, ctx);
        return ConnectResultData(ConnectResult::OK, slot_id);
      } else if (handshake_result == 0) {
        // Handshake needs READ - set status and arm read event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_read(env, ctx);
        return ConnectResultData(ConnectResult::CONNECTING, slot_id);
      } else if (handshake_result == -2) {
        // Handshake needs WRITE - set status and arm write event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_write(env, ctx);
        return ConnectResultData(ConnectResult::CONNECTING, slot_id);
      } else {
        // Handshake failed - clean up the claimed slot
        cleanup_slot_ssl(conn);
        // No manual close() needed - socket_fd destructor handles it
        conn.status.store(SLOT_EMPTY, std::memory_order_release);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_connect_failed);
      }
    } else
#endif
    {
      // For TCP/UDP, proceed with immediate slot claiming
      auto& stripe = *ctx->stripes[stripe_id];
      int slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);
      if (slot_id < 0)
        return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);

      // Transfer socket ownership to connection after successful claiming
      stripe.slots[slot_id].fd = socket_fd.release();
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return ConnectResultData(ConnectResult::OK, slot_id);
    }
  } else if (errno == EINPROGRESS) {
    // Connection in progress, register and set up for async notification
    auto& stripe = *ctx->stripes[stripe_id];
    int slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);

    if (slot_id < 0)
      return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);

    auto& conn = stripe.slots[slot_id];
#ifdef HAVE_OPENSSL
    conn.protocol = protocol;
#endif
    conn.status.store(SLOT_CONNECTING, std::memory_order_release);

    // Transfer socket ownership to connection before ARM operations
    conn.fd = socket_fd.release();

    // Check if select registration succeeds
    if (conn.arm_connect(env, ctx) < 0) {
      // Revert status and lease bit, then return error
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      close(conn.fd); // Manual close needed since ownership was transferred
      conn.fd = -1;
      return ConnectResultData(ConnectResult::SELECT_FAILED, -1, am_select_failed);
    }

    return ConnectResultData(ConnectResult::CONNECTING, slot_id);
  }

  // Connection failed immediately - RAII handles cleanup automatically
  return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);
}

inline Connection::ConnectResultData
Connection::connect_async_proto(ErlNifEnv* env, PoolContext* ctx,
                               unsigned int stripe_id,
                               const IP4Tuple& octets,
                               int port, ProtocolType protocol, bool nodelay,
                               const ErlNifPid& owner_pid)
{
#ifndef HAVE_OPENSSL
  // SSL requires OpenSSL at compile time
  if (protocol == PROTO_SSL)
    return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_unsupported_protocol);
#endif

  // RAII socket creation - automatic cleanup on all exit paths
  auto socket_fd = FileDescriptor::create([&]() {
    return create_socket_for_protocol(protocol);
  });

  if (!socket_fd)
    return ConnectResultData(ConnectResult::SOCKET_FAILED, -1, am_socket_failed);

  if (!configure_socket_for_protocol(socket_fd.get(), protocol, nodelay)) {
    // No manual close() needed - RAII handles cleanup automatically
    return ConnectResultData(ConnectResult::CONFIG_FAILED, -1, am_failed_to_set_nonblocking);
  }

  auto [o0, o1, o2, o3] = octets;
  uint32_t ip_host = (o0 << 24) | (o1 << 16) | (o2 << 8) | o3;
  struct sockaddr_in server_addr{};
  server_addr.sin_family      = AF_INET;
  server_addr.sin_port        = htons(static_cast<uint16_t>(port));
  server_addr.sin_addr.s_addr = htonl(ip_host);

  int rc = connect(socket_fd.get(), (struct sockaddr*)&server_addr, sizeof(server_addr));

  if (protocol == PROTO_UDP) {
    // UDP "connect" sets default destination, but can still fail
    if (rc != 0)
      // No manual close() needed - RAII handles cleanup automatically
      return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);

    // UDP connect succeeded, set up the connection immediately
    auto& stripe = *ctx->stripes[stripe_id];
    auto slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);

    // CRITICAL FIX: For UDP, we need to make the conn available for send_and_release
    // immediately after claiming it, since UDP has no connection handshake phase.
    if (slot_id >= 0 && slot_id < int(stripe.capacity)) {
      // Transfer socket ownership to connection after successful claiming
      stripe.slots[slot_id].fd = socket_fd.release();
      // Clear the lease mask bit to make the conn available for send_and_release
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
    }

    return slot_id < 0
         ? ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full)
         : ConnectResultData(ConnectResult::OK, slot_id);
  }

  // For TCP/SSL, handle async connection
  if (rc == 0) {
    // Connection completed immediately
#ifdef HAVE_OPENSSL
    if (protocol == PROTO_SSL) {
      // Use the centralized claim_slot function
      auto& stripe = *ctx->stripes[stripe_id];
      auto slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);

      // Check if slot claiming failed
      if (slot_id < 0) {
        // No manual close() needed - RAII handles cleanup automatically
        return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_connect_failed);
      }

      auto& conn = stripe.slots[slot_id];
      conn.protocol = protocol;

      if (!setup_ssl_on_socket(conn, socket_fd.get())) {
        // No manual close() needed - RAII handles cleanup automatically
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_connect_failed);
      }

      // Transfer socket ownership to connection after successful SSL setup
      conn.fd = socket_fd.release();

      // Perform SSL handshake
      int handshake_result = ssl_handshake_nonblocking(conn, 5000);
      if (handshake_result == 1) {
        // Handshake completed successfully - cancel any active timeout
        cancel_connection_timeout(conn);
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        conn.arm_read(env, ctx);
        return ConnectResultData(ConnectResult::OK, slot_id);
      } else if (handshake_result == 0) {
        // Handshake needs READ - set status and arm read event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_read(env, ctx);
        return ConnectResultData(ConnectResult::CONNECTING, slot_id);
      } else if (handshake_result == -2) {
        // Handshake needs WRITE - set status and arm write event
        conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
        conn.arm_write(env, ctx);
        return ConnectResultData(ConnectResult::CONNECTING, slot_id);
      } else {
        // Handshake failed - clean up the claimed slot
        cleanup_slot_ssl(conn);
        // No manual close() needed - socket_fd destructor handles it
        conn.status.store(SLOT_EMPTY, std::memory_order_release);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_connect_failed);
      }
    } else
#endif
    {
      auto& stripe = *ctx->stripes[stripe_id];
      int slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);
      if (slot_id < 0) {
        // No manual close() needed - RAII handles cleanup automatically
        return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);
      }
      // Transfer socket ownership to connection after successful claiming
      stripe.slots[slot_id].fd = socket_fd.release();
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return ConnectResultData(ConnectResult::OK, slot_id);
    }
  } else if (errno == EINPROGRESS) {
    // Connection in progress, register and set up for async notification
    auto& stripe = *ctx->stripes[stripe_id];
    auto slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);

    // Check if slot claiming failed
    if (slot_id < 0) // No manual close() needed - RAII handles cleanup automatically
      return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);

    auto& conn = stripe.slots[slot_id];
#ifdef HAVE_OPENSSL
    conn.protocol = protocol;
#endif
    conn.status.store(SLOT_CONNECTING, std::memory_order_relaxed);

    // Transfer socket ownership to connection before ARM operations
    conn.fd = socket_fd.release();

    // Check if select registration succeeds
    if (conn.arm_connect(env, ctx) < 0) {
      // Revert status and lease bit, then return error
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      close(conn.fd); // Manual close needed since ownership was transferred
      conn.fd = -1;
      return ConnectResultData(ConnectResult::SELECT_FAILED, -1, am_select_failed);
    }

    return ConnectResultData(ConnectResult::CONNECTING, slot_id);
  }

  // Connection failed immediately - RAII handles cleanup automatically
  return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);
}

//=============================================================================
// Send and Release Method Implementation
//=============================================================================

inline Connection::SendResultData
Connection::send_and_release(ErlNifEnv* env, PoolContext* ctx,
                            unsigned int stripe_id,
                            ERL_NIF_TERM data_list)
{
  auto& stripe = *ctx->stripes[stripe_id];
  auto current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  auto slot_id = -1;

  // Loop with retry limit to prevent infinite loops
  int retry_count = 0;
  const int max_retries = stripe.capacity * 2; // Allow reasonable number of retries

  do {
    slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return SendResultData(SendResult::POOL_BUSY, -1, am_pool_busy);

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask = current_mask | target_bit;

    if (!stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) [[unlikely]] {
      retry_count++;
      if (retry_count >= max_retries) [[unlikely]]
        return SendResultData(SendResult::POOL_BUSY, -1, am_pool_busy);
      continue;
    }

    // CAS succeeded - now check if slot is available and passes throttling
    auto& candidate_slot = stripe.slots[slot_id];
    uint32_t slot_status = candidate_slot.status.load(std::memory_order_acquire);

    if (slot_status == SLOT_AVAILABLE && candidate_slot.fd >= 0 &&
        throttle_allow(ctx, candidate_slot))
      break; // Success - slot is leased and passes throttling

    // Slot doesn't pass throttling or isn't available - release it and try next.
    // Mark the bit in our local view so countr_zero advances past this slot;
    // without this, reloading the mask (which now has the bit cleared) would
    // cause the next iteration to pick the same slot again.
    stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
    current_mask = stripe.lease_mask.load(std::memory_order_relaxed);

    retry_count++;
    if (retry_count >= max_retries) [[unlikely]]
      return SendResultData(SendResult::POOL_BUSY, -1, am_pool_busy);

    current_mask |= target_bit;  // skip this slot on the next scan
  } while (true);

  auto& conn = stripe.slots[slot_id];
  conn.status.store(SLOT_LEASED, std::memory_order_relaxed);

  unsigned int list_len = 0;
  enif_get_list_length(env, data_list, &list_len);

  // Inline storage for the common case (arterial_client always calls
  // this with a single-element list) -- avoids a heap allocation on
  // every write; only lists longer than this fall back to the heap.
  constexpr size_t s_inline_iov_size = 16;
  std::array<struct iovec, s_inline_iov_size> inline_iov;
  std::vector<struct iovec> heap_iov;
  struct iovec* iov;
  if (list_len <= s_inline_iov_size)
    iov = inline_iov.data();
  else {
    heap_iov.resize(list_len);
    iov = heap_iov.data();
  }

  unsigned int i = 0;
  size_t total_bytes = 0;

  list_for_each(env, data_list, [&](ERL_NIF_TERM item) {
    ErlNifBinary bin;
    if (enif_inspect_binary(env, item, &bin)) {
      iov[i].iov_base = bin.data;
      iov[i].iov_len = bin.size;
      total_bytes += bin.size;
      i++;
    }
  }); 

  ssize_t  written = 0;
  uint64_t target_bit = (1ULL << slot_id);

  // Handle pending buffer data - combine with new data if necessary
  size_t pending_bytes = conn.pending_buffer.size() - conn.bytes_written;

  // If there's pending data, we need to handle it specially
  if (pending_bytes > 0) {
    // Extend the pending buffer with new data
    size_t old_size = conn.pending_buffer.size();
    conn.pending_buffer.resize(old_size + total_bytes);

    // Copy new data to the end of pending buffer
    size_t offset = old_size;
    for (unsigned int j = 0; j < i; ++j) {
      std::memcpy(conn.pending_buffer.data() + offset, iov[j].iov_base, iov[j].iov_len);
      offset += iov[j].iov_len;
    }

    // Try to write from where we left off
    size_t remaining = conn.pending_buffer.size() - conn.bytes_written;

#ifdef HAVE_OPENSSL
    if (conn.ssl) {
    RETRY_PENDING:
      ssize_t n = SSL_write(conn.ssl,
                           conn.pending_buffer.data() + conn.bytes_written,
                           static_cast<int>(remaining));
      if (n > 0) {
        conn.bytes_written += static_cast<size_t>(n);
        written = static_cast<ssize_t>(conn.bytes_written);
      } else {
        int ssl_error = SSL_get_error(conn.ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE) {
          // Would block, handle as partial write below
          written = static_cast<ssize_t>(conn.bytes_written);
        } else {
          // Handle other SSL errors like in the original code
          if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
            written = static_cast<ssize_t>(conn.bytes_written);
          } else if (ssl_error == SSL_ERROR_SYSCALL) {
            if (errno == EINTR) goto RETRY_PENDING;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
              written = static_cast<ssize_t>(conn.bytes_written);
            } else {
              // Unrecoverable SSL error
              cleanup_slot_ssl(conn);
              ctx->notify_and_close(env, conn);
              return SendResultData(SendResult::WRITE_FAILED, slot_id, am_write_failed);
            }
          } else if (ssl_error == SSL_ERROR_ZERO_RETURN) {
            written = static_cast<ssize_t>(conn.bytes_written);
          } else {
            // Unrecoverable SSL error
            cleanup_slot_ssl(conn);
            ctx->notify_and_close(env, conn);
            return SendResultData(SendResult::WRITE_FAILED, slot_id, am_write_failed);
          }
        }
      }
    }
    else
#endif
    {
    RETRY_PENDING2:
      ssize_t n = write(conn.fd,
                       conn.pending_buffer.data() + conn.bytes_written,
                       remaining);
      if (n > 0) {
        conn.bytes_written += static_cast<size_t>(n);
        written = static_cast<ssize_t>(conn.bytes_written);
      } else if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          written = static_cast<ssize_t>(conn.bytes_written);
        } else if (errno == EINTR) [[unlikely]] {
          goto RETRY_PENDING2;
        } else {
          ctx->notify_and_close(env, conn);
          return SendResultData(SendResult::WRITE_FAILED, slot_id, am_write_failed);
        }
      }
    }

    // Update total_bytes to reflect the combined buffer size
    total_bytes = conn.pending_buffer.size();
  } else {
    // No pending data - proceed with normal write

#ifdef HAVE_OPENSSL
  if (conn.ssl) {
    // SSL doesn't support writev, so we need to write sequentially
    for (unsigned int j = 0; j < i && written >= 0; ++j) {
    RETRY1:
      ssize_t n = SSL_write(conn.ssl, iov[j].iov_base, static_cast<int>(iov[j].iov_len));
      if (n > 0)
        written += n;
      else {
        int ssl_error = SSL_get_error(conn.ssl, static_cast<int>(n));
        if (ssl_error == SSL_ERROR_WANT_READ || ssl_error == SSL_ERROR_WANT_WRITE)
          // Would block, we'll handle partial write below
          break;

        // Handle retryable SSL errors
        if (ssl_error == SSL_ERROR_WANT_X509_LOOKUP) {
          // Temporary X.509 error - will retry
          break;
        }

        if (ssl_error == SSL_ERROR_SYSCALL) {
          // Check if it's a temporary system error
          if (errno == EINTR) [[unlikely]]
            goto RETRY1;
          if (errno == EAGAIN || errno == EWOULDBLOCK)
            // Temporary system error - retry
            break;
        }

        if (ssl_error == SSL_ERROR_ZERO_RETURN)
          // Clean SSL shutdown from peer - treat as partial write completion
          break;

        // Unrecoverable SSL error
        cleanup_slot_ssl(conn);
        ctx->notify_and_close(env, conn);
        return SendResultData(SendResult::WRITE_FAILED, slot_id, am_write_failed);
      }

      // Check if we wrote the complete iovec entry
      if (n < static_cast<ssize_t>(iov[j].iov_len))
        // Partial write, we need to handle this in the buffer logic below
        break;
    }
  }
  else
#endif
  {
  RETRY2:
    written = (i > 0) ? writev(conn.fd, iov, i) : 0;

    if (written < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        written = 0;
      else if (errno == EINTR) [[unlikely]]
        goto RETRY2;
      else {
        ctx->notify_and_close(env, conn);
        return SendResultData(SendResult::WRITE_FAILED, slot_id, am_write_failed);
      }
    }
  }
  } // End of else block for no pending data

  if (static_cast<size_t>(written) < total_bytes) {
    conn.pending_buffer.resize(total_bytes);
    size_t offset = 0;
    for (unsigned int j = 0; j < i; ++j) {
      std::memcpy(conn.pending_buffer.data() + offset, iov[j].iov_base, iov[j].iov_len);
      offset += iov[j].iov_len;
    }
    conn.bytes_written = static_cast<size_t>(written);
    conn.status.store(SLOT_WRITE_POLLING, std::memory_order_release);

    conn.arm_write(env, ctx); // TODO: handle errors
    // No arm_read: reactor's persistent multishot EPOLLIN handles read events.
    return SendResultData(SendResult::PARTIAL, slot_id);
  }

  conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
  stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);

  // No arm_read needed: the reactor's persistent multishot EPOLLIN poll
  // (registered once at connect time) continuously delivers read events.
  // Calling arm_read on every send would cancel+re-add the poll SQE 10k
  // times per connection, creating unnecessary overhead.

  return SendResultData(SendResult::OK, slot_id);
}

//=============================================================================
// Socket Options Connection Method Implementations
//=============================================================================

inline Connection::ConnectResultData
Connection::connect_with_opts(ErlNifEnv* env, PoolContext* ctx,
                             unsigned int stripe_id,
                             const IP4Tuple& octets,
                             int port, unsigned int timeout_ms,
                             bool nodelay, const ErlNifPid& owner_pid,
                             ERL_NIF_TERM socket_opts)
{
  // RAII socket creation - automatic cleanup on all exit paths
  auto socket_fd = FileDescriptor::create([]() {
    return socket(AF_INET, SOCK_STREAM, 0);
  });

  if (!socket_fd)
    return ConnectResultData(ConnectResult::SOCKET_FAILED, -1, am_socket_failed);

  // Set non-blocking before applying custom options
  if (fcntl(socket_fd.get(), F_SETFL, O_NONBLOCK) == -1) {
    // No manual close() needed - RAII handles cleanup automatically
    return ConnectResultData(ConnectResult::CONFIG_FAILED, -1, am_failed_to_set_nonblocking);
  }

  ERL_NIF_TERM err;

  // Apply custom socket options
  if (!arterial::apply_sock_opts(socket_fd.get(), env, socket_opts, err)) {
    TERM error_reason = (err == 0) ? am_socket_option_failed : TERM(err);
    return ConnectResultData(ConnectResult::CONFIG_FAILED, -1, error_reason);
  }

  // Apply nodelay if requested
  if (nodelay) {
    static constexpr int one = 1;
    setsockopt(socket_fd.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(static_cast<uint16_t>(port));
  addr.sin_addr.s_addr = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);

  int result = connect(socket_fd.get(), (struct sockaddr*)&addr, sizeof(addr));
  if (result == -1 && errno != EINPROGRESS)
    return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);

  // For EINPROGRESS, connection is in progress - proceed with slot claiming
  // The slot will be marked as SLOT_CONNECTING and completion will be
  // handled via enif_select write-ready notifications

  if (stripe_id >= ctx->stripe_count)
    return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);

  PoolStripe& stripe = *ctx->stripes[stripe_id];
  int slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);
  if (slot_id < 0)
    return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);

  auto& conn = stripe.slots[slot_id];

  // Transfer socket ownership first so conn.fd is live before timerfd_create.
  // This ensures timerfd_create cannot reuse the socket's fd number.
  conn.fd = socket_fd.release();

  if (result == 0) {
    // Connection succeeded immediately
    conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
    conn.arm_read(env, ctx);
  } else {
    // Connection in progress: arm socket write-ready first, then create timer.
    // Socket fd is already held open, so timerfd_create cannot reuse its number.
    conn.status.store(SLOT_CONNECTING, std::memory_order_release);
    conn.arm_connect(env, ctx);

    if (timeout_ms > 0)
      conn.set_connect_timeout(ctx, timeout_ms);
  }

  stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
  return ConnectResultData(result == 0 ? ConnectResult::OK : ConnectResult::CONNECTING, slot_id);
}

inline Connection::ConnectResultData
Connection::connect_proto_with_opts(ErlNifEnv* env, PoolContext* ctx,
                                   unsigned int stripe_id,
                                   const IP4Tuple& octets,
                                   int port, unsigned int timeout_ms,
                                   ProtocolType protocol, bool nodelay,
                                   const ErlNifPid& owner_pid,
                                   ERL_NIF_TERM socket_opts)
{
#ifndef HAVE_OPENSSL
  if (protocol == PROTO_SSL)
    return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_ssl_not_supported);
#endif

  // RAII socket creation - automatic cleanup on all exit paths
  auto socket_fd = FileDescriptor::create([&]() {
    return create_socket_for_protocol(protocol);
  });

  if (!socket_fd)
    return ConnectResultData(ConnectResult::SOCKET_FAILED, -1, am_socket_failed);

  // Configure socket for protocol (sets non-blocking)
  if (!configure_socket_for_protocol(socket_fd.get(), protocol, false))
    // No manual close() needed - RAII handles cleanup automatically
    return ConnectResultData(ConnectResult::CONFIG_FAILED, -1, am_failed_to_set_nonblocking);

  ERL_NIF_TERM err;

  // Apply custom socket options
  if (!arterial::apply_sock_opts(socket_fd.get(), env, socket_opts, err)) {
    TERM error_reason = (err == 0) ? am_socket_option_failed : TERM(err);
    return ConnectResultData(ConnectResult::CONFIG_FAILED, -1, error_reason);
  }

  // Apply nodelay if requested
  if (nodelay && (protocol == PROTO_TCP || protocol == PROTO_SSL)) {
    static constexpr int one = 1;
    setsockopt(socket_fd.get(), IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  }

  auto [o0, o1, o2, o3] = octets;
  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_port        = htons(static_cast<uint16_t>(port));
  addr.sin_addr.s_addr = htonl((o0 << 24) | (o1 << 16) | (o2 << 8) | o3);

  int result        = -1;
  int connect_errno = 0;  // Save errno from connect() call

  // For UDP, we "connect" to set default destination (client mode)
  // This allows send/recv to work with the default peer. For UDP the connect
  // call shouldn't fail.  For TCP/SSL, it should be set to EINPROGRESS.
  result = connect(socket_fd.get(), (struct sockaddr*)&addr, sizeof(addr));
  connect_errno = errno;  // Save errno immediately
  if (result < 0 && (protocol == PROTO_UDP || connect_errno != EINPROGRESS))
    return ConnectResultData(ConnectResult::FAILED, -1, am_connect_failed);

  // For EINPROGRESS, connection is in progress - proceed with slot claiming

  // Get the stripe for claiming
  PoolStripe& stripe = *ctx->stripes[stripe_id];

  auto slot_id = ctx->claim_slot(env, stripe, socket_fd.get(), owner_pid);
  if (slot_id < 0)
    return ConnectResultData(ConnectResult::STRIPE_FULL, -1, am_stripe_full);

  auto& conn = stripe.slots[slot_id];
  #ifdef HAVE_OPENSSL
  conn.protocol = protocol;
  #endif

  // Transfer socket ownership first so conn.fd is live before timerfd_create.
  // This ensures timerfd_create cannot reuse the socket's fd number.
  conn.fd = socket_fd.release();

  switch (protocol) {
  #ifdef HAVE_OPENSSL
    case PROTO_SSL: {
      if (!setup_ssl_on_socket(conn, conn.fd)) {
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_connect_failed);
      }

      // Perform SSL handshake
      switch (ssl_handshake_nonblocking(conn, timeout_ms)) {
        case 1: // Handshake completed successfully - cancel any active timeout
          cancel_connection_timeout(conn);
          conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
          conn.arm_read(env, ctx);
          return ConnectResultData(ConnectResult::OK, slot_id);

        case 0: // Handshake needs READ - set status and arm read event
          conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          conn.arm_read(env, ctx);
          return ConnectResultData(ConnectResult::CONNECTING, slot_id);

        case -2: // Handshake needs WRITE - set status and arm write event
          conn.status.store(SLOT_SSL_HANDSHAKE, std::memory_order_release);
          conn.arm_write(env, ctx);
          return ConnectResultData(ConnectResult::CONNECTING, slot_id);

        default: // Handshake failed - clean up the claimed slot
          cleanup_slot_ssl(conn);
          conn.status.store(SLOT_EMPTY, std::memory_order_release);
          stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
          return ConnectResultData(ConnectResult::SSL_FAILED, -1, am_connect_failed);
      }
    }
  #endif

    case PROTO_TCP:
      if (result == 0) {
        cancel_connection_timeout(conn);
        conn.status.store(SLOT_AVAILABLE, std::memory_order_release);
        conn.arm_read(env, ctx);
        stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
        return ConnectResultData(ConnectResult::OK, slot_id);
      }
      // Connection in progress: arm the socket write-ready first, then set up
      // the timeout timer. Socket fd is already open so timerfd_create cannot
      // reuse its number.
      conn.status.store(SLOT_CONNECTING, std::memory_order_release);
      conn.arm_connect(env, ctx);

      if (timeout_ms > 0)
        conn.set_connect_timeout(ctx, timeout_ms);

      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return ConnectResultData(ConnectResult::CONNECTING, slot_id);

    case PROTO_UDP:
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return ConnectResultData(ConnectResult::OK, slot_id);

    default:
      conn.status.store(SLOT_EMPTY, std::memory_order_release);
      stripe.lease_mask.fetch_and(~(1ULL << slot_id), std::memory_order_release);
      return ConnectResultData(ConnectResult::SOCKET_FAILED, -1, am_bad_protocol);
  }
}

//=============================================================================
// FIFO Operations Method Implementations
//=============================================================================

inline Connection::FifoResultData
Connection::reserve_send_fifo_request(ErlNifEnv* env, PoolContext* ctx,
                                     unsigned int stripe_id,
                                     ERL_NIF_TERM data_list,
                                     unsigned int reserv_timeout,
                                     unsigned int req_timeout)
{
  // TODO: Implement timeout support for FIFO reservations and requests
  (void)reserv_timeout;  // Reserved for future implementation
  (void)req_timeout;     // Reserved for future implementation

  auto& stripe = *ctx->stripes[stripe_id];

  // Get caller PID for queuing if needed
  ErlNifPid caller_pid;
  enif_self(env, &caller_pid);

  // Try immediate reservation first (fast path)
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);

  // Immediate reservation attempt (limited retries for performance)
  int retry_count = 0;
  const int max_retries = 3;  // Reduce retries for better performance

  while (retry_count < max_retries) {
    int slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) // No immediate slots - try queuing
      break;

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_relaxed, std::memory_order_relaxed)) {

      auto& conn = stripe.slots[slot_id];
      // Use relaxed ordering for performance - status check still provides safety
      if (conn.status.load(std::memory_order_relaxed) != SLOT_AVAILABLE) {
        // Slot not ready - release and try next
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_relaxed);
        current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
        retry_count++;
        continue;
      }

      // Success - we have a slot, now send the request immediately
      conn.enable_fifo_mode();

      // Generate reservation ID
      static std::atomic<uint64_t> reservation_counter{1000000};
      uint64_t id = reservation_counter.fetch_add(1, std::memory_order_relaxed);

      if (!conn.set_fifo_request(caller_pid, id)) {
        // Failed to set request - release conn
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        return FifoResultData(FifoResult::SLOT_BUSY, -1, 0, am_fifo_slot_busy);
      }

      conn.status.store(SLOT_FIFO_RESERVED, std::memory_order_release);

      // Send the request data immediately (combined operation)
      // Use stack-allocated array for better performance (most requests have few segments)
      static constexpr size_t MAX_IOVECS = 32;
      iovec iovecs[MAX_IOVECS];
      size_t iovec_count = 0;
      size_t total_bytes = 0;

      // Process the request data and send it
      ERL_NIF_TERM head, tail = data_list;
      while (enif_get_list_cell(env, tail, &head, &tail) && iovec_count < MAX_IOVECS) {
        ErlNifBinary bin;
        if (!enif_inspect_binary(env, head, &bin)) {
          // Cleanup on error
          stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
          return FifoResultData(FifoResult::PARTIAL, -1, 0, am_partial);
        }
        iovecs[iovec_count].iov_base = const_cast<void*>(reinterpret_cast<const void*>(bin.data));
        iovecs[iovec_count].iov_len = bin.size;
        total_bytes += bin.size;
        iovec_count++;
      }

      if (iovec_count == 0) {
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        return FifoResultData(FifoResult::PARTIAL, -1, 0, am_partial);
      }

      // Check if there's already data in the pending buffer that needs to be sent first
      if (!conn.pending_buffer.empty()) {
        // Connection has pending data - can't perform atomic operation
        conn.clear_fifo_request();
        stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
        return FifoResultData(FifoResult::PARTIAL, -1, 0, am_partial);
      }

    REPEAT_WRITE:
      // Perform the write
      ssize_t bytes_written = writev(conn.fd, iovecs, static_cast<int>(iovec_count));

      if (bytes_written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // Socket would block - atomic operation can't complete, caller should retry
          conn.clear_fifo_request();
          stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
          return FifoResultData(FifoResult::PARTIAL, -1, 0, am_partial);
        }
        else if (errno == EINTR) [[unlikely]]
          goto REPEAT_WRITE;
        else {
          conn.clear_fifo_request();
          stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
          return FifoResultData(FifoResult::WRITE_FAILED, -1, 0, am_write_failed);
        }
      }

      // Handle partial writes properly using the connection's pending buffer
      if (static_cast<size_t>(bytes_written) < total_bytes) {
        // Copy all data to pending buffer for later completion
        conn.pending_buffer.resize(total_bytes);
        size_t offset = 0;
        for (size_t j = 0; j < iovec_count; ++j) {
          std::memcpy(conn.pending_buffer.data() + offset, iovecs[j].iov_base, iovecs[j].iov_len);
          offset += iovecs[j].iov_len;
        }
        conn.bytes_written = static_cast<size_t>(bytes_written);
        conn.status.store(SLOT_WRITE_POLLING, std::memory_order_release);

        // Set up write polling to complete the send later
        conn.arm_write(env, ctx);

        // Also arm read for the eventual response
        conn.arm_read(env, ctx);

        // Return success - the write will complete asynchronously
        return FifoResultData(FifoResult::REQUEST_SENT, slot_id, id);
      }

      conn.status.store(SLOT_FIFO_REQUEST_SENT, std::memory_order_release);

      // Return success with reservation info for later release
      return FifoResultData(FifoResult::REQUEST_SENT, slot_id, id);
    }
    retry_count++;
  }

  // Fast path failed - return error immediately
  // TODO: Implement proper queuing and async notification
  return FifoResultData(FifoResult::POOL_BUSY, -1, 0, am_pool_busy);
}

// RAII integration complete - demonstration function removed

} // namespace arterial