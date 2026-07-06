// vim:ts=2:sw=2:et
//-----------------------------------------------------------------------------
// reactor_bench.cpp
//
// Modes (argv[1]):
//   server                           — echo server, prints "READY <port>"
//   client <port> <conns> <reqs>     — client run, prints one result line
//   (none)                           — Scenario 1 standalone
//
// Result line (for Erlang):
//   rps=NNN lat_mean=NNN lat_p50=NNN lat_p99=NNN lat_p999=NNN
//   throughput_mbs=NNN errors=NNN error_rate=NNN
//-----------------------------------------------------------------------------

#define REACTOR_TEST_STUB_NIF 1
#include "reactor.hpp"

extern "C" {
  ErlNifEnv*   enif_alloc_env()  { return nullptr; }
  void         enif_free_env(ErlNifEnv*) {}
  int          enif_send(ErlNifEnv*, const ErlNifPid*, ErlNifEnv*, ERL_NIF_TERM) { return 1; }
  ERL_NIF_TERM enif_make_atom(ErlNifEnv*, const char*)                    { return 0; }
  ERL_NIF_TERM enif_make_uint(ErlNifEnv*, unsigned)                       { return 0; }
  ERL_NIF_TERM enif_make_tuple4(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM,
                                ERL_NIF_TERM, ERL_NIF_TERM)               { return 0; }
  ERL_NIF_TERM enif_make_tuple5(ErlNifEnv*, ERL_NIF_TERM, ERL_NIF_TERM,
                                ERL_NIF_TERM, ERL_NIF_TERM, ERL_NIF_TERM) { return 0; }
}

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <numeric>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace arterial;
using namespace std::chrono_literals;
using clk = std::chrono::steady_clock;

//-----------------------------------------------------------------------------
static void set_nonblocking(int fd)
{ fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK); }

static void set_nodelay(int fd)
{ int one=1; setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)); }

static int make_listen(int& port)
{
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  struct sockaddr_in a{}; a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  bind(fd, (sockaddr*)&a, sizeof(a));
  listen(fd, 256);
  socklen_t l = sizeof(a);
  getsockname(fd, (sockaddr*)&a, &l);
  port = ntohs(a.sin_port);
  set_nonblocking(fd);
  return fd;
}

// g_msg_size is now runtime-configurable via g_msg_size.
static int g_msg_size = 8;

static double pct(std::vector<double>& s, double p)
{
  if (s.empty()) return 0.0;
  return s[std::max<size_t>(0,
    std::min<size_t>(s.size()-1, (size_t)(p*s.size())))];
}

//=============================================================================
// EchoServer
//=============================================================================
class EchoServer {
public:
  EchoServer()
  {
    m_lfd = make_listen(m_port);
    m_r.start();
    m_r.add_fd(m_lfd,
      [this](int lfd, void*) -> int {
        for (;;) {
          int cfd = ::accept4(lfd, nullptr, nullptr, SOCK_NONBLOCK);
          if (cfd < 0) break;
          set_nodelay(cfd);
          m_r.add_fd(cfd,
            [this](int fd, void*) -> int {
              char buf[65536];
              for (;;) {
                ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                if (n > 0) { ::send(fd, buf, n, MSG_NOSIGNAL); }
                else if (n == 0) { return -1; }
                else { break; }
              }
              return 0;
            },
            [](int,void*){});
        }
        return 0;
      },
      [](int,void*){});
  }
  ~EchoServer() { m_r.stop(); ::close(m_lfd); }
  int port() const { return m_port; }

private:
  Reactor m_r{"echo_server"};
  int     m_lfd{-1};
  int     m_port{0};
};

//=============================================================================
// Benchmark client
//=============================================================================

// Promoted to file scope so send_first() can be a plain function callable
// from inside reactor callbacks without capturing a local lambda by reference.
struct BenchConn {
  int  rem, seq, roff;
  std::vector<char> sb, rb;   // sized to g_msg_size at construction
  clk::time_point ts;
  std::vector<double> lats;
  explicit BenchConn(int r)
    : rem(r), seq(0), roff(0)
    , sb(g_msg_size, 0), rb(g_msg_size, 0)
  { lats.reserve(r); }
};

// Send all bytes, blocking-style (loopback rarely needs >1 iteration).
static void send_all(int fd, const void* buf, size_t len)
{
  const char* p = static_cast<const char*>(buf);
  while (len > 0) {
    ssize_t n = ::send(fd, p, len, MSG_NOSIGNAL);
    if (n > 0) { p += n; len -= n; continue; }
    if (n < 0 && errno == EAGAIN) { std::this_thread::yield(); continue; }
    break;
  }
}

static void send_first(BenchConn* c, int fd)
{
  uint32_t s = htonl(c->seq);
  memcpy(c->sb.data(), &s, 4); memset(c->sb.data() + 4, 0xAB, g_msg_size - 4);
  c->ts = clk::now();
  send_all(fd, c->sb.data(), g_msg_size);
}

struct Result {
  long   total; double elapsed_s;
  double rps, throughput, lat_mean, lat_p50, lat_p99, lat_p999;
  long   errors; double error_rate;
};

Result bench_client(int server_port, int nconns, int nreqs)
{
  int total = nconns * nreqs;

  using C = BenchConn;
  std::vector<std::unique_ptr<C>> cs(nconns);
  for (int i = 0; i < nconns; ++i)
    cs[i] = std::make_unique<C>(nreqs);

  std::atomic<int>  done{0};
  std::atomic<long> errs{0};
  Reactor r{"bench_client"};
  r.start();

  struct sockaddr_in addr{};
  addr.sin_family      = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port        = htons(server_port);

  auto t0 = clk::now();

  for (int i = 0; i < nconns; ++i) {
    C* c = cs[i].get();
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { errs += nreqs; done += nreqs; continue; }
    set_nonblocking(fd); set_nodelay(fd);

    auto on_read = [c, &done, &errs, &r](int fd, void*) -> int {
      for (;;) {
        ssize_t n = ::recv(fd, c->rb.data() + c->roff, g_msg_size - c->roff, 0);
        if (n <= 0) {
          if (n < 0 && errno == EAGAIN) break;
          // EOF or error: count all remaining requests as errors
          errs += c->rem > 0 ? c->rem : 1;
          return -1;
        }
        c->roff += n;
        if (c->roff < g_msg_size) continue;
        c->roff = 0;
        uint32_t got; memcpy(&got, c->rb.data(), 4); got = ntohl(got);
        if (got != (uint32_t)c->seq) errs++;
        c->lats.push_back(
          std::chrono::duration<double,std::micro>(clk::now()-c->ts).count());
        ++done; --c->rem;
        if (c->rem == 0) { r.remove_fd(fd); return 0; }
        ++c->seq;
        uint32_t s = htonl(c->seq);
        memcpy(c->sb.data(), &s, 4); memset(c->sb.data()+4, 0xAB, g_msg_size-4);
        c->ts = clk::now();
        send_all(fd, c->sb.data(), g_msg_size);
      }
      return 0;
    };
    auto on_err = [c, &errs, &r](int fd, void*) {
      errs += c->rem > 0 ? c->rem : 1;
      r.remove_fd(fd);
    };

    // Use Reactor::connect so that EINPROGRESS / immediate-success /
    // connection-error are all handled correctly without a race between
    // add_fd and arm_write.  The on_writable callback fires once on connection
    // completion; on_readable handles all subsequent data.
    //
    // We pass a dummy ErlNifPid — the benchmark doesn't use Erlang messaging;
    // the connect result is observed indirectly via send_first being called.
    ErlNifPid dummy{};
    r.connect(fd, addr, 0 /*no timeout*/, dummy, 0, 0,
      on_read,
      // on_writable: connection established → send first request
      [c](int fd, void*) -> int {
        send_first(c, fd);
        return 0;
      },
      on_err,
      // on_timeout: not used (timeout=0)
      {},
      nullptr);
  }

  // Wait with a timeout to avoid hanging forever on partial failures.
  auto deadline = clk::now() + std::chrono::seconds(30);
  while (done.load() + errs.load() < total) {
    if (clk::now() > deadline) {
      fprintf(stderr, "[bench_client] TIMEOUT: done=%d errs=%ld total=%d\n",
              done.load(), errs.load(), total);
      errs.store(total - done.load());  // mark remainder as errors
      break;
    }
    std::this_thread::sleep_for(50us);
  }

  auto t1 = clk::now();
  r.stop();

  std::vector<double> all;
  all.reserve(total);
  for (auto& cu : cs)
    all.insert(all.end(), cu->lats.begin(), cu->lats.end());
  std::sort(all.begin(), all.end());

  Result res;
  res.total      = total;
  res.elapsed_s  = std::chrono::duration<double>(t1-t0).count();
  res.rps        = total / res.elapsed_s;
  res.throughput = (total * g_msg_size * 2.0) / res.elapsed_s / (1024.0*1024.0);
  res.errors     = errs.load();
  res.error_rate = 100.0 * res.errors / std::max(1, total);
  if (!all.empty()) {
    double sum   = std::accumulate(all.begin(), all.end(), 0.0);
    res.lat_mean = sum / all.size();
    res.lat_p50  = pct(all, 0.50);
    res.lat_p99  = pct(all, 0.99);
    res.lat_p999 = pct(all, 0.999);
  }
  return res;
}

//=============================================================================
// main
//=============================================================================
static void print_result(const Result& r)
{
  printf("rps=%.0f lat_mean=%.1f lat_p50=%.1f lat_p99=%.1f lat_p999=%.1f "
         "throughput_mbs=%.2f errors=%ld error_rate=%.4f\n",
         r.rps, r.lat_mean, r.lat_p50, r.lat_p99, r.lat_p999,
         r.throughput, r.errors, r.error_rate);
  fflush(stdout);
}

int main(int argc, char* argv[])
{
  if (argc >= 2 && std::string(argv[1]) == "server") {
    EchoServer srv;
    fprintf(stdout, "READY %d\n", srv.port()); fflush(stdout);
    char c;
    while (::read(STDIN_FILENO, &c, 1) > 0) {}
    return 0;
  }

  if (argc >= 5 && std::string(argv[1]) == "client") {
    if (argc >= 6) g_msg_size = atoi(argv[5]);
    Result r = bench_client(atoi(argv[2]), atoi(argv[3]), atoi(argv[4]));
    print_result(r);
    return 0;
  }

  // Standalone Scenario 1
  int nconns = (argc > 1) ? atoi(argv[1]) : 8;
  int nreqs  = (argc > 2) ? atoi(argv[2]) : 100'000;

  printf("Reactor benchmark — C++ server ↔ C++ client\n"
         "  connections=%d  reqs_per_conn=%d  msg=%d bytes\n\n",
         nconns, nreqs, g_msg_size);
  printf("  %-50s  %10s  %8s  %8s  %8s  %10s  %10s  %9s\n",
         "Scenario","req/s","MB/s","mean µs","p50 µs","p99 µs","p999 µs","err %");
  printf("  %s\n", std::string(122,'-').c_str());

  EchoServer srv;
  std::this_thread::sleep_for(5ms);
  bench_client(srv.port(), nconns, 1000);  // warmup

  Result r = bench_client(srv.port(), nconns, nreqs);
  printf("  %-50s  %10.0f  %8.2f  %8.1f  %8.1f  %10.1f  %10.1f  %9.2f\n",
         "C++ server ↔ C++ client (reactor)",
         r.rps, r.throughput, r.lat_mean, r.lat_p50, r.lat_p99, r.lat_p999, r.error_rate);
  printf("\n  Run reactor_bench.erl for all three scenarios.\n\n");
  return 0;
}
