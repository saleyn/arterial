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

#include <cstdint>
#include <time.h>

namespace arterial {

// Representation of time.
// SOURCE:
// https://github.com/saleyn/utxx/blob/master/include/utxx/time_val.hpp
struct time_val {
  static const long   N10e6 = 1000000;
  static const size_t N10e9 = 1000000000u;

  long     microseconds()            const { return m_tv/1000; }
  double   seconds()                 const { return double(m_tv) / N10e9; }
  long     milliseconds()            const { return m_tv / N10e6; }
  long     nanoseconds()             const { return m_tv; }


  time_val& add_nsec(long ns)         { m_tv += ns;        return *this; }
  time_val  add_nsec(long ns)   const { return time_val(m_tv + ns);      }


  void now() { m_tv = universal_time().m_tv; }

  time_val now(long add_s) const {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += add_s;
    return time_val(ts);
  }

  time_val now(long add_s, long add_us) const {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec  += add_s;
    ts.tv_nsec += add_us*1000;
    return time_val(ts);
  }

  static time_val universal_time() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return time_val(ts);
  }

private:
  int64_t m_tv;
};

inline time_val now_utc() { return time_val::now(); }

/// @brief Throttle given rate over a number of seconds.
/// Implementation uses time spacing reservation algorithm where each
/// allocation of samples reserves a fraction of space in the throttling
/// window. The reservation gets freed as the time goes by.  No more than
/// the "rate()" number of samples are allowed to fit in the "window_msec()"
/// window.
template <typename T = uint32_t>
class basic_time_spacing_throttle {
public:
    basic_time_spacing_throttle(T a_rate, uint32_t a_window_msec = 1000,
                                time_val a_now = now_utc())
        : m_rate        (a_rate)
        , m_window_ns   (long(a_window_msec) * 1000000)
        , m_step_ns     (a_rate == 0 ? 0 : m_window_ns / m_rate)
        , m_next_time   (a_now)
    {
        assert(a_rate >= 0);
    }

    void init(T a_rate, uint32_t a_window_msec = 1000, time_val a_now = now_utc()) {
        new (this) basic_time_spacing_throttle(a_rate, a_window_msec, a_now);
    }

    /// Reset the throttle request counter
    void reset(time_val a_now = now_utc()) {
        m_next_time = a_now;
    }

    /// Add \a a_samples to the throtlle's counter.
    /// @return number of samples that fit in the throttling window. 0 means
    /// that the throttler is fully congested, and more time needs to elapse
    /// before the throttles gets reset to accept more samples.
    T add(T a_samples  = 1, time_val a_now = now_utc()) {
        if (m_rate == 0) return a_samples;
        auto next_time = m_next_time;
        next_time.add_nsec(a_samples * m_step_ns);
        auto now_next  = a_now + nsecs(m_window_ns);
        auto diff      = next_time.nanoseconds() - now_next.nanoseconds();
        if  (diff < -m_window_ns) {
            m_next_time = a_now + nsecs(m_step_ns);
            return a_samples;
        } else if (diff < 0)  {
            // All samples fit the throttling threshold
            m_next_time = next_time;
            return a_samples;
        }

        // Figure out how many samples fit the throttling threshold
        auto n = std::max<T>(0, a_samples - (T(diff) / m_step_ns));
        m_next_time.add_nsec(n * m_step_ns);
        return n;
    }

    T        rate()        const { return m_rate;                }
    long     step_msec()   const { return m_step_ns   / 1000000; }
    long     step_usec()   const { return m_step_ns   / 1000;    }
    long     window_msec() const { return m_window_ns / 1000000; }
    long     window_usec() const { return m_window_ns / 1000;    }
    time_val next_time()   const { return m_next_time;           }

    /// Return the number of available samples given \a a_now current time.
    T        available(time_val a_now = now_utc()) const {
        return UNLIKELY(m_rate == 0) ? m_window_ns : calc_available(a_now);
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
    T        m_rate;
    long     m_window_ns;
    long     m_step_ns;
    time_val m_next_time;

    /// Return the number of available samples given \a a_now current time.
    T        calc_available(time_val a_now = now_utc()) const {
        assert(m_rate != 0);
        auto diff = (a_now - m_next_time).nanoseconds();
        auto res  = diff >= 0
                  ? m_rate : T(std::min<T>(m_rate, std::max<T>(0, (m_window_ns+diff) / m_step_ns)));
        assert(res >= 0 && res <= m_rate);
        return res;
    }
};

using time_spacing_throttle = basic_time_spacing_throttle<>;


} // namespace arterial