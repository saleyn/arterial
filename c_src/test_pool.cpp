#include <iostream>
#include <filesystem>
#include <atomic>
#include <thread>
#include <vector>

#include "backlog.hpp"
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

void test_random_access_backlog(const char*);
void test_random_access_backlog_concurrent(const char*);
void test_throttle_concurrent(const char*);

int main(int argc, char* argv[]) {

  debug = argc > 1 && std::string(argv[1]) == "-d";

  test_random_access_backlog(argv[0]);
  test_random_access_backlog_concurrent(argv[0]);
  test_throttle_concurrent(argv[0]);
}

//-----------------------------------------------------------------------------
// Test RandomAccessBackLog's bitmap-based CheckOut()/CheckIn() (backlog.hpp)
// single-threaded: claim everything, expect full, release one, reclaim it
// specifically, then fully drain and refill once more.
//-----------------------------------------------------------------------------
void test_random_access_backlog(const char* exec) {
  constexpr size_t kCapacity = 5;
  RandomAccessBackLog bl(kCapacity);
  std::atomic<uint32_t> vsn{0};

  ASSERT_EQUAL(true, bl.Empty());
  ASSERT_EQUAL(false, bl.Full());

  std::vector<ReqID> ext_ids;
  for (size_t i = 0; i < kCapacity; ++i) {
    auto* req = bl.CheckOut(0, 0, vsn);
    ASSERT_TRUE(req != nullptr);
    ext_ids.push_back(req->ext_req_id);
  }

  ASSERT_EQUAL(true, bl.Full());
  ASSERT_EQUAL(nullptr, bl.CheckOut(0, 0, vsn));

  // Double check-in is rejected.
  ASSERT_TRUE(bl.CheckIn(ext_ids[0]) != nullptr);
  ASSERT_EQUAL(nullptr, bl.CheckIn(ext_ids[0]));

  // Reclaim the slot just freed.
  auto* req = bl.CheckOut(0, 0, vsn);
  ASSERT_TRUE(req != nullptr);

  for (size_t i = 1; i < ext_ids.size(); ++i)
    ASSERT_TRUE(bl.CheckIn(ext_ids[i]) != nullptr);
  ASSERT_TRUE(bl.CheckIn(req->ext_req_id) != nullptr);

  ASSERT_EQUAL(true, bl.Empty());

  OUTPUT(exec, "success");
}

//-----------------------------------------------------------------------------
// Test RandomAccessBackLog under concurrent CheckOut()/CheckIn() load.
//
// Regression test for the bitmap-based redesign (see backlog.hpp's doc
// comment) that replaced std::set<BaseReqID> -- the set version was only
// ever safe because arterial's pool gave one caller exclusive access to a
// connection (and thus its backlog) at a time; the new shackle-style
// round-robin selection has no such exclusivity, so the backlog itself
// must be safe under concurrent multi-caller access. Correctness here
// means every thread finishes all iterations (no livelock/hang) and no two
// threads ever observe the same slot checked out at once.
//-----------------------------------------------------------------------------
void test_random_access_backlog_concurrent(const char* exec) {
  constexpr size_t kCapacity  = 16;
  constexpr int    kThreads   = 16;
  constexpr int    kIterations = 200000;

  RandomAccessBackLog bl(kCapacity);
  std::atomic<uint32_t> vsn{0};
  std::vector<std::atomic<int>> owner(kCapacity);
  for (auto& o : owner) o.store(-1);

  std::atomic<int> violations{0};
  std::atomic<int> finished{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < kIterations; ++i) {
        auto* req = bl.CheckOut(0, 0, vsn);
        if (!req) continue; // backlog momentarily full; legitimate, retry

        auto idx = RequestInfo::DecodeReqID(req->ext_req_id);
        int expected = -1;
        if (!owner[idx].compare_exchange_strong(expected, t))
          violations.fetch_add(1, std::memory_order_relaxed);
        else
          owner[idx].store(-1, std::memory_order_relaxed);

        bl.CheckIn(req->ext_req_id);
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
