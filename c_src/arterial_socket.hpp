#pragma once

#include "arterial_core.hpp"
#include "arterial_protocol.hpp"

namespace arterial {

//=============================================================================
// Socket Options Management
//=============================================================================

// Apply socket options to a file descriptor
bool apply_sock_opts(int fd, ErlNifEnv* env, ERL_NIF_TERM sock_opts_list) {
  ERL_NIF_TERM head, tail = sock_opts_list;

  while (enif_get_list_cell(env, tail, &head, &tail)) {
    const ERL_NIF_TERM* tuple_elements;
    int tuple_arity;

    if (enif_is_atom(env, head)) {
      // Handle atom-only options like 'keepalive', 'nodelay', 'reuseaddr'
      char atom_name[64];
      if (!enif_get_atom(env, head, atom_name, sizeof(atom_name), ERL_NIF_LATIN1)) {
        continue; // Skip invalid atoms
      }

      if (strcmp(atom_name, "keepalive") == 0) {
        int val = 1;
        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val));
      } else if (strcmp(atom_name, "nodelay") == 0) {
        int val = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
      } else if (strcmp(atom_name, "reuseaddr") == 0) {
        int val = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
      }
      // Add other atom options as needed

    } else if (enif_get_tuple(env, head, &tuple_arity, &tuple_elements) && tuple_arity == 2) {
      // Handle {Option, Value} tuples
      char option_name[64];
      if (!enif_get_atom(env, tuple_elements[0], option_name, sizeof(option_name), ERL_NIF_LATIN1)) {
        continue; // Skip invalid option names
      }

      if (strcmp(option_name, "keepalive") == 0) {
        char bool_val[8];
        int val = 0;
        if (enif_get_atom(env, tuple_elements[1], bool_val, sizeof(bool_val), ERL_NIF_LATIN1) &&
            strcmp(bool_val, "true") == 0) {
          val = 1;
        }
        setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val));

      } else if (strcmp(option_name, "sndbuf") == 0) {
        int val;
        if (enif_get_int(env, tuple_elements[1], &val) && val > 0) {
          setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &val, sizeof(val));
        }

      } else if (strcmp(option_name, "recvbuf") == 0 || strcmp(option_name, "rcvbuf") == 0) {
        int val;
        if (enif_get_int(env, tuple_elements[1], &val) && val > 0) {
          setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &val, sizeof(val));
        }

      } else if (strcmp(option_name, "priority") == 0) {
        int val;
        if (enif_get_int(env, tuple_elements[1], &val) && val >= 0 && val <= 6) {
          setsockopt(fd, SOL_SOCKET, SO_PRIORITY, &val, sizeof(val));
        }

      } else if (strcmp(option_name, "tos") == 0) {
        int val;
        if (enif_get_int(env, tuple_elements[1], &val)) {
          setsockopt(fd, IPPROTO_IP, IP_TOS, &val, sizeof(val));
        }

      } else if (strcmp(option_name, "linger") == 0) {
        const ERL_NIF_TERM* linger_tuple;
        int linger_arity;
        if (enif_get_tuple(env, tuple_elements[1], &linger_arity, &linger_tuple) && linger_arity == 2) {
          struct linger ling = {0};
          char bool_val[8];
          if (enif_get_atom(env, linger_tuple[0], bool_val, sizeof(bool_val), ERL_NIF_LATIN1) &&
              strcmp(bool_val, "true") == 0) {
            ling.l_onoff = 1;
          }
          enif_get_int(env, linger_tuple[1], &ling.l_linger);
          setsockopt(fd, SOL_SOCKET, SO_LINGER, &ling, sizeof(ling));
        }

      } else if (strcmp(option_name, "multicast_ttl") == 0) {
        int ttl;
        if (enif_get_int(env, tuple_elements[1], &ttl) && ttl >= 0 && ttl <= 255) {
          setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
        }

      } else if (strcmp(option_name, "multicast_loop") == 0) {
        char bool_val[8];
        unsigned char loop = 0;
        if (enif_get_atom(env, tuple_elements[1], bool_val, sizeof(bool_val), ERL_NIF_LATIN1) &&
            strcmp(bool_val, "true") == 0) {
          loop = 1;
        }
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));

      } else if (strcmp(option_name, "multicast_if") == 0) {
        const ERL_NIF_TERM* addr_tuple;
        int addr_arity;
        if (enif_get_tuple(env, tuple_elements[1], &addr_arity, &addr_tuple) && addr_arity == 4) {
          struct in_addr addr;
          int a, b, c, d;
          if (enif_get_int(env, addr_tuple[0], &a) && enif_get_int(env, addr_tuple[1], &b) &&
              enif_get_int(env, addr_tuple[2], &c) && enif_get_int(env, addr_tuple[3], &d)) {
            addr.s_addr = htonl((a << 24) | (b << 16) | (c << 8) | d);
            setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &addr, sizeof(addr));
          }
        }

      } else if (strcmp(option_name, "add_membership") == 0 || strcmp(option_name, "drop_membership") == 0) {
        const ERL_NIF_TERM* membership_tuple;
        int membership_arity;
        if (enif_get_tuple(env, tuple_elements[1], &membership_arity, &membership_tuple) && membership_arity == 2) {
          const ERL_NIF_TERM* mcast_addr_tuple;
          const ERL_NIF_TERM* iface_addr_tuple;
          int mcast_arity, iface_arity;

          if (enif_get_tuple(env, membership_tuple[0], &mcast_arity, &mcast_addr_tuple) && mcast_arity == 4 &&
              enif_get_tuple(env, membership_tuple[1], &iface_arity, &iface_addr_tuple) && iface_arity == 4) {

            struct ip_mreq mreq;
            int ma, mb, mc, md, ia, ib, ic, id;

            if (enif_get_int(env, mcast_addr_tuple[0], &ma) && enif_get_int(env, mcast_addr_tuple[1], &mb) &&
                enif_get_int(env, mcast_addr_tuple[2], &mc) && enif_get_int(env, mcast_addr_tuple[3], &md) &&
                enif_get_int(env, iface_addr_tuple[0], &ia) && enif_get_int(env, iface_addr_tuple[1], &ib) &&
                enif_get_int(env, iface_addr_tuple[2], &ic) && enif_get_int(env, iface_addr_tuple[3], &id)) {

              mreq.imr_multiaddr.s_addr = htonl((ma << 24) | (mb << 16) | (mc << 8) | md);
              mreq.imr_interface.s_addr = htonl((ia << 24) | (ib << 16) | (ic << 8) | id);

              int opt = (strcmp(option_name, "add_membership") == 0) ? IP_ADD_MEMBERSHIP : IP_DROP_MEMBERSHIP;
              setsockopt(fd, IPPROTO_IP, opt, &mreq, sizeof(mreq));
            }
          }
        }
      }
      // Add other {Option, Value} options as needed
    }
  }

  return true; // Continue gracefully even if some options fail
}

//=============================================================================
// Socket Operations
//=============================================================================

// Set up the event notification system for a slot
inline int arm_read(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  return enif_select(env, slot.fd, ERL_NIF_SELECT_READ | ERL_NIF_SELECT_CUSTOM_MSG,
                     ctx, &slot.owner_pid, am_arterial_event);
}

inline int arm_write(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  return enif_select(env, slot.fd, ERL_NIF_SELECT_WRITE | ERL_NIF_SELECT_CUSTOM_MSG,
                     ctx, &slot.owner_pid, am_arterial_event);
}

inline int arm_connect(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  return enif_select(env, slot.fd, ERL_NIF_SELECT_WRITE | ERL_NIF_SELECT_CUSTOM_MSG,
              ctx, &slot.owner_pid, am_arterial_event);
}

// Message generation for connection results
inline TERM make_connect_result_msg(
  nifpp::msg_env& msg_env, unsigned int stripe_id, unsigned int slot_id, TERM result)
{
  return make(msg_env, std::make_tuple(am_arterial_event, stripe_id, slot_id,
                                       am_connect_result, result));
}

// Cleanup a slot and notify of closure
void notify_and_close(ErlNifEnv* env, PoolContext* ctx, ConnSlot& slot) {
  nifpp::msg_env msg_env;
  auto msg = make(msg_env, std::make_tuple(am_arterial_event,
                                          slot.stripe_id, slot.slot_id, am_closed));
  enif_send(env, &slot.owner_pid, msg_env, msg);  // TODO: error checking?

  if (slot.fd != -1) {
    close(slot.fd);
    slot.fd = -1;
  }

  slot.pending_buffer.clear();
  slot.bytes_written = 0;
  slot.status.store(SLOT_EMPTY, std::memory_order_release);

  auto& stripe = *ctx->stripes[slot.stripe_id];
  stripe.lease_mask.fetch_or(1ULL << slot.slot_id, std::memory_order_release);
}

// Claim the first unregistered slot in `stripe` for `fd`/`owner_pid` via
// CAS on its lease mask, arm its first read-readiness notification, and
// return the claimed slot id
ERL_NIF_TERM claim_slot(ErlNifEnv* env, PoolContext* ctx,
                       PoolStripe& stripe, int fd,
                       ErlNifPid owner_pid) {
  uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
  while (true) {
    int slot_id = std::countr_zero(~current_mask);
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]]
      return make(env, std::make_tuple(am_error, am_stripe_full));

    auto& slot = stripe.slots[slot_id];
    if (slot.fd > -1) {
      current_mask |= (1ULL << slot_id);
      continue;
    }

    slot.fd = fd;
    slot.owner_pid = owner_pid;
    slot.status.store(SLOT_AVAILABLE, std::memory_order_relaxed);

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask   = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_release,
          std::memory_order_relaxed)) {
      arm_read(env, ctx, slot);  // TODO: error checking?
      return make(env, std::make_tuple(am_ok,
                                      static_cast<unsigned int>(slot_id)));
    }

    slot.fd = -1;
    slot.status.store(SLOT_EMPTY, std::memory_order_relaxed);
  }
}

} // namespace arterial