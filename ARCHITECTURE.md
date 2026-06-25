# Arterial Architecture

## NIF Pool Architecture

### Stripe Selection and Scheduler Affinity

The `arterial_nif_pool` uses a **stripe-based** architecture to minimize atomic operation contention across multiple Erlang schedulers.

#### How Stripe Selection Works

**Stripe ID determination happens on the Erlang side** before calling into the NIF:

```erlang
% In arterial_client2.erl
next_candidate(Pool, Size, Tried) ->
  Start = erlang:system_info(scheduler_id) rem Size,
  find_candidate(Pool, Size, Tried, Start, 0).
```

The stripe selection is based on `erlang:system_info(scheduler_id) rem Size`, spreading load across stripes to reduce atomic CAS contention.

#### Context Switch Considerations

**Context switches between stripe calculation and NIF execution are possible but irrelevant** because:

1. **The stripe calculation happens immediately before the NIF call** - minimal window for context switches
2. **The stripe ID is already calculated and passed as a parameter** - it won't change even if a context switch occurs
3. **The purpose is load distribution, not strict scheduler affinity** - the design aims to "spread contention" rather than guarantee the same scheduler processes the request

#### Architecture Rationale

This design achieves optimal performance by:

- **Erlang side**: Performs cheap calculation (`scheduler_id rem stripe_count`) for load distribution
- **C++ side**: Receives the pre-calculated stripe ID and performs expensive atomic operations on that specific stripe's `lease_mask`
- **Contention reduction**: Each stripe has its own atomic `uint64_t lease_mask`, so different schedulers typically operate on different atomic variables

### Lock-Free Slot Management

Each stripe contains up to 64 slots (limited by the 64-bit lease mask), where:

- **Bit = 1**: Slot is unregistered or currently leased/busy
- **Bit = 0**: Slot is registered and idle (available for leasing)

#### Slot Claiming Logic

Both `connect_async_nif` and `send_and_release_nif` use consistent slot claiming:

```cpp
do {
    slot_id = std::countr_zero(~current_mask);  // Find first available slot
    if (static_cast<size_t>(slot_id) >= stripe.capacity) [[unlikely]] {
        return make(env, std::make_tuple(am_error, am_stripe_full));
    }

    uint64_t target_bit = (1ULL << slot_id);
    uint64_t new_mask = current_mask | target_bit;

    if (stripe.lease_mask.compare_exchange_weak(
          current_mask, new_mask,
          std::memory_order_acquire,
          std::memory_order_relaxed)) [[likely]]
        break;
} while (true);
```

- **Infinite retry loop**: Keeps trying until successful (CAS failures are usually brief)
- **Capacity check**: Prevents infinite loops when stripe is actually full
- **Memory ordering**: `acquire` on success, `relaxed` on failure for optimal performance

### I/O Model

#### Message-Driven I/O

The NIF doesn't use callbacks for I/O readiness. Instead:

1. **`enif_select` sends messages** to the slot's owner process
2. **Message format**: `{arterial_event, StripeId, SlotId, read | write | closed}`
3. **Owner process calls back** into NIF (`handle_readable/3`, `handle_writable/3`) to perform actual I/O

#### Write Path

1. **Caller process** invokes `send_and_release/3` directly
2. **Atomic slot selection** with integrated availability + throttling check
3. **Synchronous `writev(2)`** inside the calling process
4. **Partial writes** get buffered and completed via `handle_writable/3` by the slot owner
5. **Lock-free slot selection** via CAS on the stripe's lease mask

**Throttling Integration**: The connection selection loop now atomically checks both slot availability and throttling limits:
```cpp
if (candidate_slot.status == SLOT_AVAILABLE && throttle_allow(ctx, candidate_slot)) {
    // Proceed with CAS slot leasing
}
```

#### Read Path

1. **Owner process** receives `{arterial_event, StripeId, SlotId, read}`
2. **Calls `handle_readable/3`** which performs synchronous `read(2)`
3. **Decoded data dispatched** directly to waiting callers via ETS correlation table

### Connection Management

#### Protocol Support

- **TCP**: Full support via `connect/7` and `connect_async/6`
- **SSL**: Supported via `connect_proto/8` with OpenSSL integration
- **UDP**: Supported for connectionless operations

#### Connection Lifecycle

1. **Connection establishment**: NIF opens socket internally (no competing owners)
2. **Slot registration**: CAS-based claim of available slot
3. **Read notifications**: Armed once, re-armed after each read
4. **Connection cleanup**: `ERL_NIF_SELECT_STOP` followed by async `pool_resource_stop`

### Throttling Architecture

#### C++ NIF-Based Throttling

The throttling system has been **moved from Erlang to C++** for better performance and atomic integration with connection selection:

- **Location**: Integrated directly in `send_and_release_nif` connection selection loop
- **Algorithm**: Uses `arterial::time_spacing_throttle` from `throttle.hpp`
- **Per-slot throttling**: Each `ConnSlot` has its own independent throttle state

#### Time Spacing Throttle Implementation

**Algorithm**: Time spacing reservation algorithm (not token bucket):
```cpp
struct ConnSlot {
    arterial::time_spacing_throttle throttle{0, 1000};  // Per-slot throttle
    // ... other fields
};

static bool throttle_allow(PoolContext* ctx, ConnSlot& slot) {
    if (ctx->throttle_rate_per_sec == 0) {
        return true;  // No throttling configured
    }
    auto allowed = slot.throttle.add(1, arterial::now_utc());
    return allowed > 0;
}
```

#### Throttle Configuration

**Configuration**: Via `configure_throttle_nif` which initializes all slot throttles:
```cpp
// Initialize throttle for each slot when configured
auto now = arterial::now_utc();
for (auto& stripe_ptr : ctx->stripes) {
    for (auto& slot : stripe_ptr->slots) {
        slot.throttle.init(rate_per_sec, 1000, now);  // 1 second window
    }
}
```

#### Integration with Connection Selection

**Atomic check**: Throttling is checked atomically during slot selection:
```cpp
// In send_and_release_nif connection selection loop
if (candidate_slot.status.load(std::memory_order_relaxed) == SLOT_AVAILABLE &&
    throttle_allow(ctx, candidate_slot)) {
    // Proceed with slot leasing
}
```

#### Architecture Benefits vs Previous Erlang Implementation

**Before (Erlang `arterial_throttle`)**:
- Separate `arterial_throttle:allow/4` calls in Erlang
- Token bucket algorithm with `atomics` arrays
- Two-stage process: availability check + throttling check
- Race conditions possible between availability and throttling checks

**After (C++ NIF throttling)**:
- Integrated in `send_and_release_nif` connection selection
- Time spacing reservation algorithm from `throttle.hpp`
- Atomic single-stage process: combined availability + throttling check
- No race conditions, better performance, more accurate timing

#### Performance Characteristics

- **Reduced syscalls**: No separate Erlang function calls for throttling
- **Better cache locality**: Throttle state co-located with `ConnSlot` data
- **Atomic operations**: Combined check eliminates race windows
- **High-precision timing**: Uses `arterial::now_utc()` instead of Erlang timing

### Resource Management

#### File Descriptor Ownership

- **NIF-owned fds**: Created via `connect/7` - no competing resources
- **External fds**: Via `register_socket/4` - may conflict with `socket()` terms
- **Cleanup**: Only via `pool_resource_stop` callback (never direct `close()`)

#### Memory Management

- **PoolContext**: RAII wrapper with explicit destructor for fd cleanup
- **ConnSlot**: Cache-line aligned (`alignas(64)`) for performance
- **SSL resources**: Proper OpenSSL cleanup when `HAVE_OPENSSL` is defined

### Performance Characteristics

#### Concurrency

- **Lock-free slot claiming**: CAS-based with per-stripe atomic masks
- **Scheduler affinity**: Load distribution based on `scheduler_id`
- **Minimal contention**: Separate atomic variables per stripe

#### Memory Layout

- **Cache-line alignment**: `ConnSlot` structs aligned to 64-byte boundaries
- **Fixed-size arrays**: Prevents vector reallocation of non-movable atomics
- **Inline buffers**: Small I/O vectors avoid heap allocation

#### I/O Efficiency

- **Direct syscalls**: `read(2)`, `write(2)`, `writev(2)` in calling process
- **Non-blocking**: All sockets set to `O_NONBLOCK`
- **Vectored I/O**: `writev(2)` for efficient multi-buffer writes