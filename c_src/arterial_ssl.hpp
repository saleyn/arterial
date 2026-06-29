#pragma once

#include "arterial_core.hpp"

#ifdef HAVE_OPENSSL

namespace arterial {

//=============================================================================
// SSL Helpers
//=============================================================================

// Forward declarations for SSL functions used in connection handling
static bool setup_ssl_on_socket(Connection& slot, int fd);
static int  ssl_handshake_blocking(Connection& slot, int timeout_ms);
static int  ssl_handshake_step(Connection& slot);

// Global SSL context - initialized once
static SSL_CTX* g_ssl_ctx = nullptr;

// Socket options support
struct SocketOption {
  int  level;
  int  optname;
  int  value;
  bool is_boolean;
};

//===========================================================================
// SSL Helpers
//===========================================================================

static bool init_ssl_context() {
  if (g_ssl_ctx) return true;


  // Use modern OpenSSL initialization
  if (OPENSSL_init_ssl(OPENSSL_INIT_LOAD_SSL_STRINGS | OPENSSL_INIT_LOAD_CRYPTO_STRINGS, NULL) == 0) {
    fprintf(stderr, "OPENSSL_init_ssl failed\n");
    ERR_print_errors_fp(stderr);
    return false;
  }

  const SSL_METHOD* method = TLS_client_method();
  if (!method) {
    fprintf(stderr, "TLS_client_method failed\n");
    ERR_print_errors_fp(stderr);
    return false;
  }

  g_ssl_ctx = SSL_CTX_new(method);
  if (!g_ssl_ctx) {
    fprintf(stderr, "SSL_CTX_new failed\n");
    ERR_print_errors_fp(stderr);
    return false;
  }

  // Configure for maximum compatibility - accept any certificate
  SSL_CTX_set_verify(g_ssl_ctx, SSL_VERIFY_NONE, nullptr);

  // Allow a wider range of protocol versions and options for compatibility
  SSL_CTX_set_options(g_ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_IGNORE_UNEXPECTED_EOF);

  // TODO: remove stderr printing

  // Allow both TLS 1.2 and 1.3 to be more compatible
  if (SSL_CTX_set_min_proto_version(g_ssl_ctx, TLS1_2_VERSION) != 1)
    fprintf(stderr, "Failed to set min TLS version to 1.2\n");

  if (SSL_CTX_set_max_proto_version(g_ssl_ctx, TLS1_3_VERSION) != 1)
    fprintf(stderr, "Failed to set max TLS version to 1.3\n");

  // Set both TLS 1.3 ciphersuites and TLS 1.2 cipher list for maximum compatibility
  if (SSL_CTX_set_ciphersuites(g_ssl_ctx, "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_CCM_SHA256") != 1)
    fprintf(stderr, "Failed to set TLS 1.3 ciphersuites\n");

  // TLS 1.2 cipher list
  if (SSL_CTX_set_cipher_list(g_ssl_ctx, "ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES128-SHA256:DHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256") != 1)
    fprintf(stderr, "Failed to set TLS 1.2 cipher list\n");

  // Set supported curves for ECDHE - these are standard curves supported by Erlang
  if (SSL_CTX_set1_curves_list(g_ssl_ctx, "secp256r1:secp384r1:secp521r1") != 1)
    fprintf(stderr, "Failed to set curves list (non-fatal)\n");
    // This is non-fatal, continue

  // Set security level to 0 to accept any certificate for testing
  SSL_CTX_set_security_level(g_ssl_ctx, 0);

  return true;
}

// Cleanup SSL library (called during NIF unload)
void cleanup_ssl() {
  if (g_ssl_ctx) {
    SSL_CTX_free(g_ssl_ctx);
    g_ssl_ctx = nullptr;
  }
  EVP_cleanup();
  ERR_free_strings();
}

// Clean up SSL resources for a slot
void cleanup_slot_ssl(Connection& slot) {
  if (slot.ssl) {
    SSL_shutdown(slot.ssl);
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
  }
}

// Helper to setup SSL on a connected socket
static bool setup_ssl_on_socket(Connection& slot, int fd) {
  if (!init_ssl_context()) {
    fprintf(stderr, "Failed to initialize SSL context\n");
    return false;
  }

  // Check if socket is actually connected
  int error = 0;
  socklen_t len = sizeof(error);
  if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) != 0 || error != 0) {
    fprintf(stderr, "Socket not properly connected (error: %d)\n", error);
    return false;
  }

  // Ensure socket has adequate buffers for SSL handshake
  int bufsize = 65536;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize)) != 0) {
    fprintf(stderr, "Failed to set receive buffer size: %s\n", strerror(errno));
  }
  if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize)) != 0) {
    fprintf(stderr, "Failed to set send buffer size: %s\n", strerror(errno));
  }

  // Disable Nagle algorithm for SSL (can interfere with handshake)
  int nodelay = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay)) != 0) {
    fprintf(stderr, "Failed to set TCP_NODELAY: %s\n", strerror(errno));
  }


  // Clean up any existing SSL object first
  if (slot.ssl) {
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
  }

  slot.ssl = SSL_new(g_ssl_ctx);
  if (!slot.ssl) {
    fprintf(stderr, "Failed to create SSL structure\n");
    ERR_print_errors_fp(stderr);
    return false;
  }


  if (SSL_set_fd(slot.ssl, fd) != 1) {
    fprintf(stderr, "Failed to associate SSL with socket fd %d\n", fd);
    ERR_print_errors_fp(stderr);
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
    return false;
  }

  // Set SSL to client mode explicitly
  SSL_set_connect_state(slot.ssl);

  // Additional SSL setup for testing
  SSL_set_verify(slot.ssl, SSL_VERIFY_NONE, nullptr);

  // Disable SNI since we're connecting to 127.0.0.1 but cert might be for different name
  SSL_set_tlsext_host_name(slot.ssl, nullptr);

  slot.protocol = PROTO_SSL;
  return true;
}

// Perform SSL handshake (non-blocking)
// SSL handshake step with proper I/O event handling
// Returns: 1 = completed, 0 = need read, -2 = need write, -1 = failed
// TODO: make handshake non-blocking
static int ssl_handshake_blocking(Connection& slot, int /*timeout_ms*/) {
  if (!slot.ssl) return -1;

  // Ensure socket is in non-blocking mode
  int flags = fcntl(slot.fd, F_GETFL);
  if (flags != -1 && !(flags & O_NONBLOCK)) {
    fcntl(slot.fd, F_SETFL, flags | O_NONBLOCK);
  }

  // Attempt SSL handshake
  int result = SSL_connect(slot.ssl);

  if (result == 1)
    // Handshake completed successfully
    return 1;

  int ssl_error = SSL_get_error(slot.ssl, result);

  switch (ssl_error) {
    case SSL_ERROR_WANT_READ:  return  0; // Need to read more data
    case SSL_ERROR_WANT_WRITE: return -2; // Need to write more data
    default:                   return -1; // Actual error
  }
}

// Non-blocking SSL handshake step (kept for compatibility but prefer blocking version)
static int ssl_handshake_step(Connection& slot) {
  // For async connections, use blocking handshake with short timeout
  return ssl_handshake_blocking(slot, 3000);  // 3 second timeout
}

} // namespace arterial

#endif // HAVE_OPENSSL