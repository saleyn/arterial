#include <iostream>
#include <filesystem>
#include <atomic>
#include <thread>
#include <vector>

#include "pool_fifo.hpp"

using namespace arterial;

#define ASSERT_EQUAL(a, b) \
  if ((a)!=(b)) { \
    std::cerr << #a  << " != " << #b  << " ||| " \
              << (a) << " != " << (b) << " ["    \
              << std::filesystem::path(__FILE__).stem().string().c_str() \
              << ": " << __LINE__ << "]\n"; \
    exit(1); \
  } \
  else { \
    (void)0; \
  }

struct ObjT {
    int value;
};

#define OUTPUT(Exec, Str) \
  std::cout << "==> " << std::filesystem::path(Exec).stem().string().c_str() \
                      << ":" << __LINE__ << ": " << (Str) << "\n"

constexpr const size_t SIZE = 5;

bool debug = false;

void test_fifo_pool(const char*);
void test_fifo_pool_concurrent(const char*);

int main(int argc, char* argv[]) {

  debug = argc > 1 && std::string(argv[1]) == "-d";

  test_fifo_pool(argv[0]);
  test_fifo_pool_concurrent(argv[0]);
}

//-----------------------------------------------------------------------------
// Test FIFO
//-----------------------------------------------------------------------------
void test_fifo_pool(const char* exec) {
  ObjectPoolFIFO<ObjT> pool(SIZE);

  ASSERT_EQUAL(0, pool.UnsafeSize());

  std::vector<ObjT*> objects;
  size_t count = 0;

    for (size_t i=0; i < SIZE; ++i) {
    auto* node = pool.Get(i);
    objects.push_back(node->Value());
    objects.back()->value = ++count;
  }

  for (auto p : objects)
    pool.MakeAvailable(p);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  auto print = [&](auto& node) {
    if (debug)
      std::cout << "[" << node.Index() << "] -> " << static_cast<BasePoolNode<ObjT>&>(node).Next().Index() << " " << (node.Valid() ? "valid" : "invalid") << "\n";
  };
  pool.ForEach(print);

  {
    auto obj = pool.CheckOut();

    ASSERT_EQUAL(SIZE-1, pool.UnsafeSize());

    pool.CheckIn(*obj);
  }

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  std::vector<PooledObject<ObjT>*> holder; // Temporarily holds checked out resources
  holder.reserve(SIZE);

    for (size_t i=0; i < SIZE; ++i) {
    auto obj = pool.CheckOut();
    holder.push_back(obj);
    if (debug)
      std::cout << "Object[" << i << "] value="
                << (obj->Valid() ? std::to_string(obj->Value()->value) : " <invalid>")
                << "\n";
    ASSERT_EQUAL(true, holder.back()->Valid());
  }

  ASSERT_EQUAL(nullptr, pool.CheckOut());

  for (auto nd : holder)
    pool.CheckIn(*nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  // Check resolution of a pool node by an object pointer
  count = 1;
  for (auto nd : holder) {
    auto p = pool.Find(nd->Value());
    ASSERT_EQUAL(true, p != nullptr);
    ASSERT_EQUAL(int(count++ % SIZE)+1, p->Value()->value);
  }

  std::vector<PooledObject<ObjT>*> same_indexes;
  same_indexes.reserve(SIZE);

  count = 1;
    for (size_t i = 0; i < SIZE; ++i) {
    same_indexes.push_back(pool.CheckOut());
    if (debug)
      std::cout << "Object[" << pool.Get(*same_indexes.back())->value << "] (count=" << count << ")\n";
    ASSERT_EQUAL(int(count++ % SIZE)+1, pool.Get(*same_indexes.back())->value);
  }

  ASSERT_EQUAL(0, pool.UnsafeSize());

  for (auto nd : same_indexes)
    pool.CheckIn(*nd);

  ASSERT_EQUAL(SIZE, pool.UnsafeSize());

  OUTPUT(exec, "success");
}

//-----------------------------------------------------------------------------
// Test FIFO under concurrent CheckOut()/CheckIn() load.
//
// Regression test for a real bug found via arterial_bench at pool_size=16
// (== 2x nproc on the box where it was first reproduced): the pool's
// CheckOut()/CheckIn() pair is on the hot path for every checkout/checkin
// NIF call, called concurrently from many Erlang schedulers with no
// dirty-scheduler serialization. The original hand-rolled Michael & Scott
// style lock-free FIFO had multiple distinct concurrency bugs that only
// manifested under sustained concurrent load at this scale -- none of
// which the single-threaded test above (or the rest of the eunit suite,
// whose pools are all size<=2) ever exercised. Each thread repeatedly
// checks a node out and back in; correctness here means every thread
// finishes all its iterations (no livelock/hang) and no two threads ever
// observe the same node checked out at once (which would otherwise
// silently corrupt per-connection state with zero internal synchronization
// of its own, since it relies entirely on the pool for mutual exclusion).
//-----------------------------------------------------------------------------
void test_fifo_pool_concurrent(const char* exec) {
  constexpr int kPoolSize   = 16;
  constexpr int kThreads    = 16;
  constexpr int kIterations = 200000;

  struct Slot { std::atomic<int> owner{-1}; };

  ObjectPoolFIFO<Slot> pool(kPoolSize);
  for (uint32_t i = 0; i < kPoolSize; ++i)
    pool.MakeAvailable(*pool.Get(i));

  std::atomic<int>  violations{0};
  std::atomic<int>  finished{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kIterations; ++i) {
        auto* node = pool.CheckOut();
        if (!node) continue; // pool momentarily empty; legitimate, retry

        auto* slot = node->Value();
        int expected = -1;
        if (!slot->owner.compare_exchange_strong(expected, t))
          violations.fetch_add(1, std::memory_order_relaxed);
        else
          slot->owner.store(-1, std::memory_order_relaxed);

        pool.CheckIn(*node);
      }
      finished.fetch_add(1, std::memory_order_relaxed);
    });
  }

  for (auto& th : threads)
    th.join();

  ASSERT_EQUAL(kThreads, finished.load());
  ASSERT_EQUAL(0, violations.load());

  OUTPUT(exec, "success");
}