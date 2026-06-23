#include <erl_nif.h>
#include <atomic>
#include <vector>
#include <unistd.h>
#include <sys/uio.h>
#include <sys/ioctl.h>
#include <bit>
#include <errno.h>
#include <fcntl.h>
#include <cstring>
#include <mutex>

enum SlotStatus : uint32_t {
    SLOT_EMPTY = 0,
    SLOT_AVAILABLE = 1,
    SLOT_LEASED = 2,
    SLOT_WRITE_POLLING = 3
};

struct alignas(64) ConnSlot {
    std::atomic<uint32_t> status{SLOT_EMPTY};
    int fd{-1};
    unsigned int stripe_id{0};
    unsigned int slot_id{0};
    
    // Static owner process responsible for routing inbound packets
    ErlNifPid owner_pid;

    std::vector<char> pending_buffer;
    size_t bytes_written{0};
};

struct PoolStripe {
    std::atomic<uint64_t> lease_mask{~0ULL}; 
    std::vector<ConnSlot> slots;
    size_t capacity{0};
};

struct PoolContext {
    std::vector<PoolStripe> stripes;
    size_t stripe_count{0};
    
    std::vector<ConnSlot*> fd_map;
    std::mutex fd_map_mutex; 
};

ErlNifResourceType* POOL_RES_TYPE = nullptr;

static void flush_pending_bytes(ConnSlot& slot, PoolStripe& stripe) {
    size_t remaining = slot.pending_buffer.size() - slot.bytes_written;
    
    while (remaining > 0) {
        ssize_t n = write(slot.fd, slot.pending_buffer.data() + slot.bytes_written, remaining);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return; 
            
            close(slot.fd);
            slot.fd = -1;
            slot.pending_buffer.clear();
            slot.bytes_written = 0;
            slot.status.store(SLOT_EMPTY, std::memory_order_release);
            return;
        }
        slot.bytes_written += n;
        remaining -= n;
    }

    slot.pending_buffer.clear();
    slot.bytes_written = 0;
    slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
    
    uint64_t target_bit = (1ULL << slot.slot_id);
    stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
}

void write_ready_callback(ErlNifEnv* env, void* obj, int fd, int is_ready) {
    PoolContext* ctx = static_cast<PoolContext*>(obj);
    if (fd < 0 || static_cast<size_t>(fd) >= ctx->fd_map.size()) [[unlikely]] return;
    
    ConnSlot* slot_ptr = ctx->fd_map[fd];
    if (slot_ptr && slot_ptr->status.load(std::memory_order_relaxed) == SLOT_WRITE_POLLING) [[likely]] {
        auto& stripe = ctx->stripes[slot_ptr->stripe_id];
        flush_pending_bytes(*slot_ptr, stripe);
    }
}

void read_ready_callback(ErlNifEnv* env, void* obj, int fd, int is_ready) {
    PoolContext* ctx = static_cast<PoolContext*>(obj);
    if (fd < 0 || static_cast<size_t>(fd) >= ctx->fd_map.size()) [[unlikely]] return;

    ConnSlot* slot = ctx->fd_map[fd];
    if (!slot || slot->status.load(std::memory_order_relaxed) == SLOT_EMPTY) [[unlikely]] return;

    int bytes_available = 0;
    if (ioctl(fd, FIONREAD, &bytes_available) == -1 || bytes_available <= 0) {
        goto handle_disconnect;
    }

    ErlNifBinary bin;
    if (!enif_alloc_binary(static_cast<size_t>(bytes_available), &bin)) [[unlikely]] return;

    ssize_t n = read(fd, bin.data, bytes_available);

    if (n <= 0) {
        enif_release_binary(&bin);
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        goto handle_disconnect;
    }

    // Always deliver straight to the static socket owner process
    ErlNifEnv* msg_env = enif_alloc_env();
    ERL_NIF_TERM bin_term = enif_make_binary(msg_env, &bin);
    ERL_NIF_TERM msg = enif_make_tuple3(msg_env, 
        enif_make_atom(msg_env, "tcp_data"), 
        enif_make_uint(msg_env, slot->slot_id), 
        bin_term
    );

    enif_send(nullptr, &(slot->owner_pid), msg_env, msg);
    enif_free_env(msg_env);
    return;

handle_disconnect:
    ErlNifEnv* msg_env = enif_alloc_env();
    ERL_NIF_TERM closed_msg = enif_make_tuple2(msg_env, 
        enif_make_atom(msg_env, "tcp_closed"), 
        enif_make_uint(msg_env, slot->slot_id)
    );
    enif_send(nullptr, &(slot->owner_pid), msg_env, closed_msg);
    enif_free_env(msg_env);

    {
        std::lock_guard<std::mutex> lock(ctx->fd_map_mutex);
        ctx->fd_map[fd] = nullptr;
    }
    close(fd);
    slot->fd = -1;
    slot->status.store(SLOT_EMPTY, std::memory_order_release);
}

void pool_context_destroy(ErlNifEnv* env, void* obj) {
    PoolContext* ctx = static_cast<PoolContext*>(obj);
    for (auto& stripe : ctx->stripes) {
        for (size_t i = 0; i < stripe.capacity; ++i) {
            if (stripe.slots[i].fd != -1) {
                close(stripe.slots[i].fd);
            }
        }
    }
    ctx->~PoolContext();
}

static ERL_NIF_TERM init_pool_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    unsigned int num_stripes;
    unsigned int slots_per_stripe;

    if (!enif_get_uint(env, argv[0], &num_stripes) || !enif_get_uint(env, argv[1], &slots_per_stripe)) {
        return enif_make_badarg(env);
    }

    if (slots_per_stripe > 64) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "max_slots_exceeded_64"));
    }

    void* res_ptr = enif_alloc_resource(POOL_RES_TYPE, sizeof(PoolContext));
    PoolContext* ctx = new (res_ptr) PoolContext();
    
    ctx->stripe_count = num_stripes;
    ctx->stripes.resize(num_stripes);
    ctx->fd_map.resize(1024, nullptr); 

    for (unsigned int i = 0; i < num_stripes; ++i) {
        ctx->stripes[i].capacity = slots_per_stripe;
        ctx->stripes[i].lease_mask.store(~0ULL, std::memory_order_relaxed);
        ctx->stripes[i].slots.resize(64);
        for (int j = 0; j < 64; ++j) {
            ctx->stripes[i].slots[j].fd = -1;
            ctx->stripes[i].slots[j].stripe_id = i;
            ctx->stripes[i].slots[j].slot_id = j;
        }
    }

    ERL_NIF_TERM term = enif_make_resource(env, res_ptr);
    enif_release_resource(res_ptr);
    return enif_make_tuple2(env, enif_make_atom(env, "ok"), term);
}

static ERL_NIF_TERM register_socket_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    PoolContext* ctx;
    unsigned int stripe_id;
    int raw_fd;
    ErlNifPid owner_pid;

    if (!enif_get_resource(env, argv[0], POOL_RES_TYPE, (void**)&ctx) ||
        !enif_get_uint(env, argv[1], &stripe_id) ||
        !enif_get_int(env, argv[2], &raw_fd) || raw_fd < 0 ||
        !enif_get_local_pid(env, argv[3], &owner_pid)) {
        return enif_make_badarg(env);
    }

    if (stripe_id >= ctx->stripe_count) return enif_make_badarg(env);
    auto& stripe = ctx->stripes[stripe_id];
    
    int flags = fcntl(raw_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(raw_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "failed_to_set_nonblocking"));
    }

    uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
    while (true) {
        int slot_id = std::countr_zero(current_mask);
        if (slot_id >= stripe.capacity) [[unlikely]] {
            return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "stripe_full"));
        }

        auto& slot = stripe.slots[slot_id];
        if (slot.fd == -1) {
            slot.fd = raw_fd;
            slot.owner_pid = owner_pid;
            slot.status.store(SLOT_AVAILABLE, std::memory_order_relaxed);
            
            {
                std::lock_guard<std::mutex> lock(ctx->fd_map_mutex);
                if (static_cast<size_t>(raw_fd) >= ctx->fd_map.size()) {
                    ctx->fd_map.resize(raw_fd + 256, nullptr);
                }
                ctx->fd_map[raw_fd] = &slot;
            }

            uint64_t target_bit = (1ULL << slot_id);
            uint64_t new_mask = current_mask & ~target_bit;

            if (stripe.lease_mask.compare_exchange_weak(
                    current_mask, new_mask, 
                    std::memory_order_release, 
                    std::memory_order_relaxed)) {
                
                enif_select(env, raw_fd, ERL_NIF_SELECT_READ, argv[0], nullptr, enif_make_badarg(env));
                return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_uint(env, slot_id));
            }
            
            {
                std::lock_guard<std::mutex> lock(ctx->fd_map_mutex);
                ctx->fd_map[raw_fd] = nullptr;
            }
            slot.fd = -1; 
            slot.status.store(SLOT_EMPTY, std::memory_order_relaxed);
        } else {
            current_mask &= ~(1ULL << slot_id);
        }
    }
}

static ERL_NIF_TERM send_and_release_nif(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    PoolContext* ctx;
    unsigned int stripe_id;
    ERL_NIF_TERM list = argv[2];
    
    if (!enif_get_resource(env, argv[0], POOL_RES_TYPE, (void**)&ctx) ||
        !enif_get_uint(env, argv[1], &stripe_id) ||
        !enif_is_list(env, list)) {
        return enif_make_badarg(env);
    }

    auto& stripe = ctx->stripes[stripe_id];
    uint64_t current_mask = stripe.lease_mask.load(std::memory_order_relaxed);
    int slot_id = -1;

    while (true) {
        slot_id = std::countr_zero(~current_mask);
        if (slot_id >= stripe.capacity) [[unlikely]] {
            return enif_make_tuple2(env, enif_make_atom(env, "error"), enif_make_atom(env, "no_connections_available"));
        }

        uint64_t target_bit = (1ULL << slot_id);
        uint64_t new_mask = current_mask | target_bit;

        if (stripe.lease_mask.compare_exchange_weak(
                current_mask, new_mask, 
                std::memory_order_acquire, 
                std::memory_order_relaxed)) [[likely]] {
            break;
        }
    }

    auto& slot = stripe.slots[slot_id];
    slot.status.store(SLOT_LEASED, std::memory_order_relaxed);

    unsigned int list_len = 0;
    enif_get_list_length(env, list, &list_len);
    
    std::vector<struct iovec> iov(list_len);
    ERL_NIF_TERM head, tail = list;
    unsigned int i = 0;
    size_t total_bytes = 0;
    
    while (enif_get_list_cell(env, tail, &head, &tail)) {
        ErlNifBinary bin;
        if (enif_inspect_binary(env, head, &bin)) {
            iov[i].iov_base = bin.data;
            iov[i].iov_len = bin.size;
            total_bytes += bin.size;
            i++;
        }
    }

    ssize_t written = writev(slot.fd, iov.data(), i);
    uint64_t target_bit = (1ULL << slot_id);

    if (written < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            written = 0;
        } else {
            {
                std::lock_guard<std::mutex> lock(ctx->fd_map_mutex);
                if (static_cast<size_t>(slot.fd) < ctx->fd_map.size()) ctx->fd_map[slot.fd] = nullptr;
            }
            close(slot.fd);
            slot.fd = -1;
            slot.status.store(SLOT_EMPTY, std::memory_order_release);
            return enif_make_tuple3(env, enif_make_atom(env, "error"), enif_make_atom(env, "write_failed"), enif_make_uint(env, slot_id));
        }
    }

    if (static_cast<size_t>(written) < total_bytes) {
        slot.pending_buffer.resize(total_bytes);
        size_t offset = 0;
        for (unsigned int j = 0; j < i; ++j) {
            std::memcpy(slot.pending_buffer.data() + offset, iov[j].iov_base, iov[j].iov_len);
            offset += iov[j].iov_len;
        }
        slot.bytes_written = static_cast<size_t>(written);
        slot.status.store(SLOT_WRITE_POLLING, std::memory_order_release);

        enif_select(env, slot.fd, ERL_NIF_SELECT_WRITE, argv[0], nullptr, enif_make_badarg(env));
        return enif_make_atom(env, "ok"); 
    }

    slot.status.store(SLOT_AVAILABLE, std::memory_order_release);
    stripe.lease_mask.fetch_and(~target_bit, std::memory_order_release);
    return enif_make_atom(env, "ok");
}

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    ErlNifCustomResourceCallbacks cb;
    std::memset(&cb, 0, sizeof(cb));
    cb.write_ready = write_ready_callback;
    cb.read_ready = read_ready_callback; 

    POOL_RES_TYPE = enif_open_resource_type_x(
        env, "nif_pool_context", &cb, ERL_NIF_RT_CREATE, nullptr
    );
    return (POOL_RES_TYPE == nullptr) ? -1 : 0;
}

static ErlNifFunc nif_funcs[] = {
    {"init_pool", 2, init_pool_nif},
    {"register_socket", 4, register_socket_nif}, // Arity 4
    {"send_and_release", 3, send_and_release_nif}
};

ERL_NIF_INIT(my_nif_pool, nif_funcs, load, nullptr, nullptr, nullptr)
