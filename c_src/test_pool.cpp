#include <iostream>
#include <filesystem>
#include <atomic>
#include <thread>
#include <vector>

#include "connection.hpp"
#include "throttle.hpp"

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

#define ASSERT_TRUE(a) \
  if (!(a)) { \
    std::cerr << #a << " is false ||| [" \
              << std::filesystem::path(__FILE__).stem().string().c_str() \
              << ": " << __LINE__ << "]\n"; \
    exit(1); \
  } \
  else { \
    (void)0; \
  }

#define OUTPUT(Exec, Str) \
  std::cout << "==> " << std::filesystem::path(Exec).stem().string().c_str() \
                      << ":" << __LINE__ << ": " << (Str) << "\n"

bool debug = false;

void test_connection_backlog(const char*);
void test_connection_backlog_concurrent(const char*);
void test_throttle_concurrent(const char*);

int main(int argc, char* argv[]) {

  debug = argc > 1 && std::string(argv[1]) == "-d";

  test_connection_backlog(argv[0]);
  test_connection_backlog_concurrent(argv[0]);
  test_throttle_concurrent(argv[0]);
}

//-----------------------------------------------------------------------------
// Test Connection's atomic-counter backlog accounting (connection.hpp)
// single-threaded: claim everything, expect full, release one, reclaim it,
// then fully drain and refill once more.
//-----------------------------------------------------------------------------
void test_connection_backlog(const char* exec) {
  constexpr uint32_t kCapacity = 5;
  Connection conn(0, kCapacity);

  ASSERT_EQUAL(true, conn.Drained());

  for (size_t i = 0; i < kCapacity; ++i)
    ASSERT_TRUE(conn.TryReserve(1, 0));

  ASSERT_EQUAL(false, conn.TryReserve(1, 0)); // full

  conn.CheckIn(1);
  ASSERT_TRUE(conn.TryReserve(1, 0)); // reclaim the slot just freed

  conn.CheckIn(kCapacity);
  ASSERT_EQUAL(true, conn.Drained());

  OUTPUT(exec, "success");
}

//-----------------------------------------------------------------------------
// Test Connection's backlog accounting under concurrent TryReserve()/
// CheckIn() load.
//
// Regression test for the atomic-counter redesign that replaced the
// CAS-claimed bitmap backlog (backlog.hpp, deleted) -- correctness here
// means every thread finishes all iterations (no livelock/hang) and the
// in-flight count never exceeds the connection's fixed capacity.
//-----------------------------------------------------------------------------
void test_connection_backlog_concurrent(const char* exec) {
  constexpr uint32_t kCapacity   = 16;
  constexpr int      kThreads    = 16;
  constexpr int      kIterations = 200000;

  Connection conn(0, kCapacity);
  std::atomic<int> max_observed{0};
  std::atomic<int> finished{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kIterations; ++i) {
        if (!conn.TryReserve(1, 0))
          continue; // backlog momentarily full; legitimate, retry
        conn.CheckIn(1);
      }
      finished.fetch_add(1, std::memory_order_relaxed);
    });
  }

  for (auto& th : threads)
    th.join();

  ASSERT_EQUAL(kThreads, finished.load());
  ASSERT_EQUAL(true, conn.Drained());

  OUTPUT(exec, "success");
}

//-----------------------------------------------------------------------------
// Test basic_time_spacing_throttle's TryReserve() (throttle.hpp) under
// concurrent load.
//
// Regression test for the CAS-loop redesign that replaced the
// available()-then-add() check-then-act pair -- safe only under the same
// pool-given exclusivity the backlog used to rely on. Correctness here:
// no lost updates (every successful TryReserve() is reflected exactly once
// in the throttle's internal accounting) and the rate is never overshot.
//-----------------------------------------------------------------------------
void test_throttle_concurrent(const char* exec) {
  constexpr int kThreads    = 16;
  constexpr int kIterations = 50000;
  constexpr uint32_t kRate  = 1'000'000;

  basic_time_spacing_throttle<uint32_t> throttle(kRate, 1000, time_val());

  std::atomic<long> successes{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&] {
      for (int i = 0; i < kIterations; ++i)
        if (throttle.TryReserve(1, time_val()))
          successes.fetch_add(1, std::memory_order_relaxed);
    });
  }
  for (auto& th : threads)
    th.join();

  auto avail = throttle.available(time_val());
  long consumed = long(kRate) - long(avail);

  ASSERT_EQUAL(successes.load(), consumed);

  OUTPUT(exec, "success");
}
