#pragma once

#include "arterial_core.hpp"

#ifdef HAVE_OPENSSL

namespace arterial {

//=============================================================================
// SSL Helpers
//=============================================================================

// Global SSL context - initialized once
static SSL_CTX* g_ssl_ctx = nullptr;

// Initialize SSL library (called once during NIF load)
void init_ssl() {
  if (g_ssl_ctx) return; // Already initialized

  SSL_library_init();
  SSL_load_error_strings();
  OpenSSL_add_all_algorithms();

  g_ssl_ctx = SSL_CTX_new(TLS_client_method());
  if (!g_ssl_ctx) {
    // Handle error - but continue as SSL is optional
    return;
  }

  // Set reasonable default options
  SSL_CTX_set_options(g_ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);
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

// Set up SSL on a socket
bool setup_ssl_on_socket(ConnSlot& slot, int fd) {
  if (!g_ssl_ctx) {
    init_ssl(); // Try to initialize if not done
    if (!g_ssl_ctx) return false;
  }

  slot.ssl = SSL_new(g_ssl_ctx);
  if (!slot.ssl) return false;

  if (SSL_set_fd(slot.ssl, fd) != 1) {
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
    return false;
  }

  // Set SSL to client mode
  SSL_set_connect_state(slot.ssl);
  return true;
}

// Perform SSL handshake with blocking behavior (but with timeout)
int ssl_handshake_blocking(ConnSlot& slot, int timeout_ms) {
  if (!slot.ssl) return -1;

  int result = SSL_connect(slot.ssl);
  if (result == 1) {
    // Handshake completed successfully
    return 1;
  }

  int ssl_error = SSL_get_error(slot.ssl, result);
  switch (ssl_error) {
    case SSL_ERROR_WANT_READ:
      return 0; // Need to wait for readable
    case SSL_ERROR_WANT_WRITE:
      return -2; // Need to wait for writable
    default:
      // Handshake failed
      return -1;
  }
}

// Clean up SSL resources for a slot
void cleanup_slot_ssl(ConnSlot& slot) {
  if (slot.ssl) {
    SSL_shutdown(slot.ssl);
    SSL_free(slot.ssl);
    slot.ssl = nullptr;
  }
}

} // namespace arterial

#endif // HAVE_OPENSSL