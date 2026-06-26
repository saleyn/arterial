---
name: project-fifo-benchmark
description: "Active work fixing arterial FIFO Mode 3 benchmark — stripe_full NIF bug, reply routing, stripe selection"
metadata:
  node_type: memory
  type: project
  originSessionId: 59da41a2-2728-46f0-a949-6899aabb2c9a
---

## Goal
`make bench-fifo BENCH_OPTS='#{pool_size => 16, workers => 16}'` must produce real nonzero request counts.

## What's Fixed ✅
1. **NIF loading** — FIFO functions exported from `src/arterial_nif.erl`, stubs present
2. **Module references** — `arterial_client_fifo` calls `arterial_nif:` (not `arterial_fifo_nif:`)
3. **Function name** — `arterial_pool:pool_ref/1` (not `get_pool_ref/1`)
4. **FIFOSlotExtension eliminated** — FIFO fields integrated directly into `ConnSlot` struct; no dynamic allocation
5. **`pool_resource_stop` lease bit** — was `fetch_or` (bug: set bit on close), fixed to `fetch_and` (clear bit)
6. **`badarith` in benchmark** — `requests_per_sec` division now guards `Duration > 0`
7. **Stripe selection** — `select_stripe_id` was hardcoded `rem 8`; fixed to `rem pool_size` using `Opts` map
8. **FIFO reply routing** — `handle_readable_nif` now detects `fifo_request_active` and sends `{arterial_fifo_reply, StripeId, SlotId, Data}` directly to `fifo_requester_pid` instead of returning bytes for codec decoding
9. **Caller pid capture** — `send_fifo_request_nif` captures `enif_self` and stores in `slot.fifo_requester_pid` + sets `fifo_request_active = true` before returning `{ok, fifo_request_sent}`
10. **`disconnect` for non-connected slots** — `arterial_connection.erl` `disconnect/2` now always calls `close_slot` even when `connected=false`, so `SLOT_CONNECTING` lease bits get cleared on failed initial connects
11. **`close_slot_nif`** — now immediately clears lease bit and sets `SLOT_EMPTY` (before async `pool_resource_stop` fires)
12. **`notify_and_close`** — same: clears lease bit and sets `SLOT_EMPTY` immediately
13. **`claim_slot` and async-connect loops** — changed `slot.fd > -1` checks to `slot.status != SLOT_EMPTY` so reconnect can claim a slot that has `fd` still set but `status = SLOT_EMPTY`

## Current Bug 🔴 — `stripe_full` on first connect
**Symptom**: Pool connections fail immediately with `stripe_full` on the very first `connect_async_proto` call, even with a fresh pool where lease mask should have bit 0 clear.

**Evidence**:
- Direct NIF test: `init_pool(1,1)` → `connect_async_proto` → works
- Direct NIF cycle: connect → handle_writable → close_slot → connect → works
- Full pool test: `arterial_pool:start_link(...)` → BOTH conn 0 and conn 1 fail immediately

**Hypothesis**: Something in `arterial_pool:init` or `arterial_connection:init` is setting the lease bit before the first connect attempt. Look at:
- `arterial_pool.erl` init sequence — does anything touch the NIF pool ref before connections start?
- `arterial_connection.erl` — does it call `set_slot_unavailable` anywhere at startup?
- The `arterial_observe:span` wrapper around `init_pool` — does it do anything odd?

**Key files to check**:
- `c_src/arterial_nif.cpp`: `connect_async_proto_nif` EINPROGRESS path (around line 1851-1898)
- `src/arterial_pool.erl`: `init/1` function
- `src/arterial_connection.erl`: `init/1` and first `reconnect` call

## Key Design: FIFO Reply Flow
```
arterial_client_fifo:call/4
  → arterial_nif:send_fifo_request/6  (stores caller pid in slot.fifo_requester_pid, fifo_request_active=true)
  → writev() to socket fd
  → receive {arterial_fifo_reply, StripeId, SlotId, ReplyData}

When data arrives on the socket:
arterial_connection receives {arterial_event, ConnID, 0, read}
  → arterial_nif:handle_readable/3
  → handle_readable_nif: if fifo_request_active → enif_send to fifo_requester_pid
  → returns {ok, <<>>} so arterial_connection's codec sees empty binary (→ `more`, no-op)
```

## Files Modified (branch: pool-nif)
- `c_src/arterial_nif.cpp` — major changes throughout
- `src/arterial_nif.erl` — added FIFO exports and stubs
- `src/arterial_client_fifo.erl` — fixed module/function references
- `src/arterial_connection.erl` — disconnect fix
- `test/bench_arterial_fifo.erl` — badarith fix, stripe selection fix, diagnostic logging

## shackle Benchmark Bug (separate issue)
`make bench-shackle` crashes with `{badarg, size/1}` at `shackle_server:handle_msg_data/4:342`.
Root cause: shackle receives list data instead of binary from the socket.
The config `{sock_opts, [{mode, binary}, ...]}` is in `bench_shackle.erl` but isn't working.
This was "already fixed before" per user — need to find what the original fix was.

**Why:** `pool_resource_stop` was changed from `fetch_or` (bug) to `fetch_and` but that isn't the shackle issue.
