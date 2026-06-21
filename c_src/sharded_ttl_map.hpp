//-----------------------------------------------------------------------------
/// \file   sharded_ttl_map.hpp
/// \author Serge Aleynikov
//-----------------------------------------------------------------------------
/// \brief A concurrency-safe wrapper around hashmap_with_ttl.hpp,
/// partitioning keys across N independently-locked shards so that callers
/// touching different keys never contend with each other.
///
/// UnorderedTTLMap itself has no synchronization (it's a plain
/// std::unordered_map + std::list); ConnectionPool's m_inflight/m_waiting
/// instances are reachable concurrently from multiple NIF call sites
/// (checkin_nif, checkout_async_nif, track_inflight_nif, sweep_timeouts_nif
/// all run with no dirty-scheduler serialization), so they need *some*
/// synchronization to avoid racing on the underlying map/list. Unlike
/// owner_table.hpp's fixed-capacity pid table, these maps grow with
/// in-flight traffic rather than being bounded by pool_size*backlog, which
/// rules out a similar fixed-slot CAS scheme -- so this reaches for the
/// next-cheapest option: shard by hash(key) into a fixed number of
/// independent mutex-guarded sub-maps, the same divide-and-conquer
/// principle OwnerTable uses (per-pid slots there, per-shard maps here),
/// just coarser-grained since a TTL-ordered refresh() needs a real lock
/// rather than a single CAS.
///
/// ShardedTTLMap<K,V> is deliberately a thin, swappable wrapper: every
/// method takes a single key (or sweeps all shards via for_each_shard()),
/// so the *strategy* used to make a shard safe -- currently a per-shard
/// std::mutex -- can be swapped for a lock-free per-shard structure later
/// without changing ConnectionPool's call sites.
///
/// Shard count is a runtime constructor argument, not a compile-time
/// constant: the right number of shards to avoid contention scales with
/// how many threads/schedulers can actually call in concurrently, which
/// depends on the machine arterial runs on, not on the type -- so it
/// defaults to a function of std::thread::hardware_concurrency() (see
/// DefaultShardCount()) rather than a fixed guess baked into the type.
//-----------------------------------------------------------------------------
#pragma once

#include "arterial_util.hpp"
#include "hashmap_with_ttl.hpp"
#include <algorithm>
#include <cstddef>
#include <functional>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

namespace arterial {

//-----------------------------------------------------------------------------
/// @brief Sharded, concurrency-safe wrapper around UnorderedTTLMap<K,V>.
//-----------------------------------------------------------------------------
template <typename K, typename V>
struct ShardedTTLMap {
  using Map = UnorderedTTLMap<K, V>;

  /// @brief @return a shard count scaled to the machine's parallelism:
  /// the next power of 2 >= 2x the number of hardware threads, clamped to
  /// [4, 256] (a single-core box still gets some sharding headroom; a
  /// very large box doesn't allocate hundreds of mostly-empty TTL maps).
  static size_t DefaultShardCount()
  {
    size_t cores = std::thread::hardware_concurrency();
    if (cores == 0) [[unlikely]]
      cores = 4; // hardware_concurrency() is allowed to return 0 if undetectable
    size_t n = 1;
    while (n < 2 * cores) n <<= 1;
    return std::clamp(n, size_t(4), size_t(256));
  }

  /// @param ttl per-shard fixed TTL (see UnorderedTTLMap).
  /// @param shards shard count; rounded up to a power of 2. `0` (the
  /// default) picks DefaultShardCount().
  explicit ShardedTTLMap(uint64_t ttl, size_t shards = 0)
  : m_mask(RoundUpPow2(shards ? shards : DefaultShardCount()) - 1)
  {
    m_shards.reserve(m_mask + 1);
    for (size_t i = 0; i <= m_mask; ++i)
      m_shards.push_back(std::make_unique<Shard>(ttl));
  }

  /// @brief Try to add `key`/`value`, expiring at the map's fixed ttl from
  /// `now`. @return true if added (false if `key` already present).
  bool try_add(K const& key, V&& value, uint64_t now)
  {
    auto& shard = ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mtx);
    return shard.map.try_add(key, std::move(value), now);
  }

  /// @brief Like try_add(), but with an explicit absolute expiration time.
  bool try_add_with_ttl(K const& key, V&& value, uint64_t expire_at)
  {
    auto& shard = ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mtx);
    return shard.map.try_add_with_ttl(key, std::move(value), expire_at);
  }

  /// @brief Erase `key`. @return true if `key` was present.
  bool erase(K const& key)
  {
    auto& shard = ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mtx);
    return shard.map.erase(key);
  }

  /// @brief @return true if `key` is currently present.
  bool contains(K const& key)
  {
    auto& shard = ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mtx);
    return shard.map.find(key) != shard.map.end();
  }

  /// @brief If `key` is present, call `fn(value)` then erase it -- a
  /// combined find+erase under the same shard lock, so the caller can
  /// observe the value exactly once (e.g. to notify the owning pid before
  /// dropping the in-flight entry). `fn` runs while the shard's mutex is
  /// held; keep it short (no callbacks back into this same map for the
  /// same shard).
  /// @return true if `key` was present (and `fn` was called).
  template <typename Fn>
  bool take(K const& key, Fn&& fn)
  {
    auto& shard = ShardFor(key);
    const std::lock_guard<std::mutex> lock(shard.mtx);
    auto it = shard.map.find(key);
    if (it == shard.map.end())
      return false;
    fn(it->second.value);
    shard.map.erase(key);
    return true;
  }

  /// @brief Evict every expired entry across every shard, invoking
  /// `on_erase(key, value, expire_at, now)` for each one. Unlike
  /// UnorderedTTLMap::refresh(), `on_erase` always runs *after* the
  /// owning shard's lock has been released (eviction itself is
  /// unconditional, decided purely by TTL) -- so, unlike that lower-level
  /// method, `on_erase` here MAY safely call back into this same
  /// ShardedTTLMap (e.g. to re-add a fresh entry), which matters because
  /// ConnectionPool's sweep callbacks do exactly that (a timed-out
  /// request's eviction can trigger DrainWaitList(), which tracks a new
  /// in-flight request on the same map). Shards are still visited and
  /// locked one at a time, so this can't deadlock either way.
  /// @return the total number of evicted entries across all shards.
  template <typename OnErase>
  size_t refresh(uint64_t now, OnErase const& on_erase)
  {
    std::vector<std::tuple<K, V, uint64_t>> expired;

    for (auto& pshard : m_shards) {
      auto& shard = *pshard;
      const std::lock_guard<std::mutex> lock(shard.mtx);
      shard.map.refresh(now, [&](K const& key, V& value, uint64_t expire_at, uint64_t) {
        expired.emplace_back(key, std::move(value), expire_at);
        return true; // unconditional: TTL alone decides eviction
      });
    }

    for (auto& [key, value, expire_at] : expired)
      on_erase(key, value, expire_at, now);

    return expired.size();
  }

private:
  struct Shard {
    explicit Shard(uint64_t ttl) : map(ttl) {}
    std::mutex mtx;
    Map        map;
  };

  size_t                              m_mask;
  std::vector<std::unique_ptr<Shard>> m_shards;

  size_t Index(K const& key) const { return std::hash<K>{}(key) & m_mask; }

  Shard& ShardFor(K const& key) { return *m_shards[Index(key)]; }
};

} // namespace arterial
