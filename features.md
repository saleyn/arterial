# Arterial Features Analysis

This document analyzes the features available in the original arterial backend and their status in arterial2, to guide porting efforts.

## Feature Analysis Summary

Based on analysis of the arterial codebase, here are the key features and questions about porting to arterial2:

### **1. FIFO/Backlog Mode Support**
The original `arterial_pool` supports:
- `backlog` option (max in-flight requests per connection)
- `fifo` option (FIFO vs correlation-id based reply matching)
- `fixed_timeout_us` (shared timeout for all requests)

**arterial2 status**: The moduledoc explicitly states "there is no FIFO/no-request-id backlog mode here" - it only supports correlation-id based matching.

**Question**: Do you want to add FIFO/backlog support to arterial2, or is the correlation-id-only approach intentional for the NIF-based backend?

### **2. TTL Shards Configuration**
The original `arterial_pool` has `ttl_shards` option for configuring the shard count of in-flight/waiter registries.

**arterial2 status**: Not present - the NIF pool uses a different architecture.

**Question**: Do you want to add configurable sharding to arterial2, or is this not applicable due to the different NIF architecture?

### **4. Enhanced Observability Events**
The original backend emits these events that I should check if arterial2 supports:
- `[arterial, checkout, start|stop]` events
- `[arterial, connect, start|stop]` events
- `[arterial, disconnect]` events
- `[arterial, sweep, stop]` events

**Question**: Should I audit and ensure arterial2 emits all the same observability events as the original backend for consistency?

## Additional Features Found

### **7. Request Sweeping**
Both backends support:
- `sweep_interval_ms` - periodic cleanup of timed-out requests

**Status**: ✅ Already implemented in arterial2 via `arterial_sweeper2`

### **8. Rate Limiting**
**arterial2 status**: Has `throttle` option with `arterial_throttle2` support.
**arterial1 status**: No built-in rate limiting.

**Note**: This is a feature arterial2 has that arterial1 lacks.

### **9. Observability Integration**
Both backends integrate with `arterial_observe` for telemetry and prometheus metrics.

**Status**: ✅ Partially implemented - needs audit for completeness

## Architecture Differences

### **NIF-based I/O**
- **arterial1**: Socket I/O in Erlang via `socket` module
- **arterial2**: Socket I/O in C++ NIF via `arterial_nif_pool`

### **Connection Management**
- **arterial1**: Per-request checkout/checkin with backlog tracking
- **arterial2**: Direct write + correlation-id based reply dispatch

### **Protocol Support**
- **arterial1**: Pluggable protocols via `arterial_protocol` callbacks
- **arterial2**: Fixed protocol via `arterial_codec2` callbacks

## Recommendations

1. **Focus on observability parity** - Ensure arterial2 emits all the same events
2. **Multi-address failover audit** - Verify completeness vs arterial1
3. **Consider protocol extensions** - Evaluate SSL/UDP support needs
4. **Document architecture tradeoffs** - FIFO vs correlation-id performance implications