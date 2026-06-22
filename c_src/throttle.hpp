//-----------------------------------------------------------------------------
/// \file   rate_throttler.hpp
//-----------------------------------------------------------------------------
/// \brief Efficiently calculates the throttling rate over a number of seconds.
///
/// The algorithm implements a variation of token bucket algorithm that
/// doesn't require to add tokens to the bucket on a timer but rather it
/// maintains a circular buffer of tokens with resolution of 1/BucketsPerSec.
/// The basic_rate_throttler::add() function is used to adds items to a bucket
/// associated with the timestamp passed as the first argument to the
/// function.  The throttler::running_sum() returns the total number of
/// items over the given interval of seconds.
/// \note See also this article on a throttling algorithm
/// http://www.devquotes.com/2010/11/24/an-efficient-network-throttling-algorithm.
/// \note Another article on rate limiting
/// http://www.pennedobjects.com/2010/10/better-rate-limiting-with-dot-net.
//-----------------------------------------------------------------------------
// Created: 2011-01-20
// Copyright (C) 2011 Serge Aleynikov <saleyn@gmail.com>
//-----------------------------------------------------------------------------
// SOURCE:
// https://github.com/saleyn/utxx/blob/master/include/utxx/rate_throttler.hpp
//-----------------------------------------------------------------------------
#pragma once

#include <atomic>
#include <cstdint>
#include <cassert>
#include <algorithm>
#include <limits>
#include <time.h>

#ifndef LIKELY
#define LIKELY(Cond)   __builtin_expect(!!(Cond), 1)
#endif
#ifndef UNLIKELY
#define UNLIKELY(Cond) __builtin_expect(!!(Cond), 0)
#endif

namespace arterial {

// Representation of nanoseconds.
// SOURCE:
// https://github.com/saleyn/utxx/blob/master/include/utxx/time_val.hpp
struct nsecs {
  constexpr explicit nsecs(long   ns) : m_nsec(int64_t(ns))    {}
  constexpr explicit nsecs(long long ns) : m_nsec(int64_t(ns)) {}
  constexpr explicit nsecs(size_t ns) : m_nsec(int64_t(ns))    {}
  constexpr nsecs(long s,  long   ns) : m_nsec(int64_t(s)*1000000000LL+int64_t(ns)) {}
  constexpr int64_t   value()           const { return m_nsec;  }
  constexpr int64_t   nsec()            const { return m_nsec;  }
  constexpr int64_t   nanoseconds()     const { return m_nsec;  }
private:
  int64_t m_nsec;
};

// Representation of time.
// SOURCE:
// https://github.com/saleyn/utxx/blob/master/include/utxx/time_val.hpp
struct time_val {
  static const long   N10e6 = 1000000;
  static const size_t N10e9 = 1000000000u;

  constexpr
  time_val() noexcept         : m_tv(0)                  {}
  constexpr
  time_val(nsecs ns)          : m_tv(ns.nsec())          {}
  time_val(long s, long us)   : m_tv(s*N10e9 + us*1000)  {}
  time_val(time_val tv, long s)          : m_tv(tv.m_tv + s*N10e9)           {}
  time_val(time_val tv, long s, long us) : m_tv(tv.m_tv + s*N10e9 + us*1000) {}

  explicit time_val(const struct timeval&  a) : m_tv(long(a.tv_sec)*N10e9 + long(a.tv_usec)*1000){}
  explicit time_val(const struct timespec& a) : m_tv(long(a.tv_sec)*N10e9 + a.tv_nsec){}
  explicit time_val(struct tm& a_tm)          : m_tv(long(mktime(&a_tm))*N10e9)       {}

  long     microseconds()            const { return m_tv/1000; }
  double   seconds()                 const { return double(m_tv) / N10e9; }
  long     milliseconds()            const { return m_tv / N10e6; }
  long     nanoseconds()             const { return m_tv; }

  time_val& add_nsec(long ns)         { m_tv += ns;        return *this; }
  time_val  add_nsec(long ns)   const { return time_val(nsecs(static_cast<long>(m_tv + ns))); }

  time_val  operator+(nsecs ns)     const { return time_val(nsecs(m_tv + ns.nsec())); }
  nsecs     operator-(time_val rhs) const { return nsecs(m_tv - rhs.m_tv); }

  void now() { m_tv = universal_time().m_tv; }

  static time_val universal_time() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return time_val(ts);
  }

private:
  int64_t m_tv;
};

inline time_val now_utc() { return time_val::universal_time(); }

/// @brief Throttle given rate over a number of seconds.
/// Implementation uses time spacing reservation algorithm where each
/// allocation of samples reserves a fraction of space in the throttling
/// window. The reservation gets freed as the time goes by.  No more than
/// the "rate()" number of samples are allowed to fit in the "window_msec()"
/// window.
///
/// m_next_time is stored as a std::atomic<int64_t> (nanoseconds) rather
/// than a plain time_val so that TryReserve() can collapse the
/// check-then-act sequence (available() followed by add()) into one
/// CAS-loop: two concurrent callers checking available() before either
/// commits via add() would otherwise be a classic check-then-act race,
/// possible once a connection (and its throttles) can be used by more than
/// one caller at a time. add()/available()/reset() are kept for
/// non-concurrent callers and tests; production code reserving against a
/// shared connection should use TryReserve(). Validated standalone
/// (no lost updates under 16-thread CAS contention, no overshoot,
/// ThreadSanitizer-clean) before landing here -- see /tmp/backlogbench/
/// (atomic_throttle.hpp, test_throttle.cpp) for the prototype this was
/// ported from.
template <typename T = uint32_t>
class basic_time_spacing_throttle {
public:
  basic_time_spacing_throttle(T a_rate, uint32_t a_window_msec = 1000,
                              time_val a_now = now_utc())
    : m_rate        (a_rate)
    , m_window_ns   (long(a_window_msec) * 1000000)
    , m_step_ns     (a_rate == 0 ? 0 : m_window_ns / m_rate)
    , m_next_time_ns(a_now.nanoseconds())
  {
    assert(a_rate >= 0);
  }

  void init(T a_rate, uint32_t a_window_msec = 1000, time_val a_now = now_utc()) {
    new (this) basic_time_spacing_throttle(a_rate, a_window_msec, a_now);
  }

  /// Reset the throttle request counter
  void reset(time_val a_now = now_utc()) {
    m_next_time_ns.store(a_now.nanoseconds(), std::memory_order_relaxed);
  }

  /// Add \a a_samples to the throtlle's counter. NOT safe to call
  /// concurrently with itself or available() against the same throttle --
  /// see TryReserve() for the atomic equivalent.
  /// @return number of samples that fit in the throttling window. 0 means
  /// that the throttler is fully congested, and more time needs to elapse
  /// before the throttles gets reset to accept more samples.
  T add(T a_samples = 1, time_val a_now = now_utc()) {
    if (m_rate == 0) return a_samples;
    auto next_time_ns = m_next_time_ns.load(std::memory_order_relaxed);
    auto candidate_ns = next_time_ns + int64_t(a_samples) * m_step_ns;
    auto now_next_ns  = a_now.nanoseconds() + m_window_ns;
    auto diff         = candidate_ns - now_next_ns;
    if  (diff < -m_window_ns) {
      m_next_time_ns.store(a_now.nanoseconds() + m_step_ns, std::memory_order_relaxed);
      return a_samples;
    } else if (diff < 0)  {
      // All samples fit the throttling threshold
      m_next_time_ns.store(candidate_ns, std::memory_order_relaxed);
      return a_samples;
    }

    // Figure out how many samples fit the throttling threshold
    auto n = std::max<T>(0, a_samples - (T(diff) / m_step_ns));
    m_next_time_ns.fetch_add(int64_t(n) * m_step_ns, std::memory_order_relaxed);
    return n;
  }

  /// @brief Atomically check whether \a a_samples fit right now and, if
  /// so, reserve them, in one step -- the concurrency-safe alternative to
  /// calling available() then add() separately. All-or-nothing: either all
  /// of \a a_samples are reserved, or none are (no partial reservation),
  /// matching how ConnectionPool::CheckOut() already uses add() (its
  /// return value is currently unused there).
  /// @return true if reserved; false (no mutation at all) if a_samples
  /// doesn't fully fit right now.
  bool TryReserve(T a_samples, time_val a_now = now_utc()) {
    if (m_rate == 0) return true;

    auto now_ns = a_now.nanoseconds();
    auto next_time_ns = m_next_time_ns.load(std::memory_order_relaxed);

    for (;;) {
      auto candidate_ns = next_time_ns + int64_t(a_samples) * m_step_ns;
      auto now_next_ns  = now_ns + m_window_ns;
      auto diff         = candidate_ns - now_next_ns;

      int64_t new_next_time_ns;
      if (diff < -m_window_ns)
        new_next_time_ns = now_ns + m_step_ns;     // fully idle: snap forward
      else if (diff < 0)
        new_next_time_ns = candidate_ns;            // fits as-is
      else
        return false;                                // doesn't fully fit: no-op

      if (m_next_time_ns.compare_exchange_weak(
            next_time_ns, new_next_time_ns,
            std::memory_order_relaxed, std::memory_order_relaxed))
        return true;
      // CAS failed: next_time_ns was refreshed to the current value; retry.
    }
  }

  T        rate()        const { return m_rate;                }
  long     step_msec()   const { return m_step_ns   / 1000000; }
  long     step_usec()   const { return m_step_ns   / 1000;    }
  long     window_msec() const { return m_window_ns / 1000000; }
  long     window_usec() const { return m_window_ns / 1000;    }
  time_val next_time()   const { return time_val(nsecs(m_next_time_ns.load(std::memory_order_relaxed))); }

  /// Return the number of available samples given \a a_now current time.
  /// A rate of 0 means "unthrottled", so the max value of T is returned.
  T        available(time_val a_now = now_utc()) const {
    return UNLIKELY(m_rate == 0) ? std::numeric_limits<T>::max() : calc_available(a_now);
  }

  /// Return the number of used samples given \a a_now current time.
  T        used(time_val a_now=now_utc()) const {
    return UNLIKELY(m_rate==0) ? 0 : m_rate-calc_available(a_now);
  }

  /// Return currently used rate per second.
  double   curr_rate_per_second(time_val a_now=now_utc()) const {
    return UNLIKELY(m_rate==0)
         ? 0 : double((m_rate-calc_available(a_now))*1'000'000'000/m_window_ns);
  }

private:
  T                     m_rate;
  long                  m_window_ns;
  long                  m_step_ns;
  std::atomic<int64_t>  m_next_time_ns;

  /// Return the number of available samples given \a a_now current time.
  T        calc_available(time_val a_now = now_utc()) const {
    assert(m_rate != 0);
    auto diff = a_now.nanoseconds() - m_next_time_ns.load(std::memory_order_relaxed);
    auto res  = diff >= 0
              ? m_rate : T(std::min<T>(m_rate, std::max<T>(0, (m_window_ns+diff) / m_step_ns)));
    assert(res >= 0 && res <= m_rate);
    return res;
  }
};

using time_spacing_throttle = basic_time_spacing_throttle<>;


} // namespace arterial