// mini-redis, Day 1: epoll event loop + connection buffers + PING.
// Single-threaded, non-blocking, level-triggered epoll (Linux only).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <unordered_map>

static constexpr int kMaxEvents = 1024;
static constexpr size_t kReadChunk = 16 * 1024;

// ---------------------------------------------------------------- helpers

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

// ------------------------------------------------------------- connection

struct Conn {
    int fd = -1;
    std::string rbuf;       // bytes received, not yet consumed by the parser
    std::string wbuf;       // reply bytes waiting to be written
    size_t woff = 0;        // how much of wbuf is already sent
    bool want_write = false;  // is EPOLLOUT currently registered?
};

static int g_ep = -1;
static std::unordered_map<int, Conn> g_conns;

static void update_events(Conn& c) {
    // Only ask for EPOLLOUT while we have unsent data; otherwise the
    // loop would wake up constantly because sockets are almost always writable.
    bool need_write = c.woff < c.wbuf.size();
    if (need_write == c.want_write) return;
    epoll_event ev{};
    ev.events = need_write ? (EPOLLIN | EPOLLOUT) : EPOLLIN;
    ev.data.fd = c.fd;
    if (epoll_ctl(g_ep, EPOLL_CTL_MOD, c.fd, &ev) < 0) perror("epoll_ctl MOD");
    c.want_write = need_write;
}

static void close_conn(int fd) {
    epoll_ctl(g_ep, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    g_conns.erase(fd);
}

// ------------------------------------------------- command handling (stub)
// Day 2 replaces this with a real incremental RESP parser + dispatcher.
// For now we recognise exactly one command, in two forms:
//   RESP array : "*1\r\n$4\r\nPING\r\n"   (what redis-cli sends)
//   inline     : "PING\r\n"               (what `nc` / telnet sends)

static bool equals_ci(const char* a, const char* b, size_t len) {
    for (size_t i = 0; i < len; i++) {
        unsigned char ca = static_cast<unsigned char>(a[i]);
        unsigned char cb = static_cast<unsigned char>(b[i]);
        if (ca >= 'A' && ca <= 'Z') ca = static_cast<unsigned char>(ca - 'A' + 'a');
        if (cb >= 'A' && cb <= 'Z') cb = static_cast<unsigned char>(cb - 'A' + 'a');
        if (ca != cb) return false;
    }
    return true;
}

static bool is_prefix_ci(const std::string& buf, const char* full) {
    size_t n = std::strlen(full);
    return buf.size() < n && equals_ci(buf.data(), full, buf.size());
}

static void process_input(Conn& c) {
    static const char kRespPing[] = "*1\r\n$4\r\nPING\r\n";
    static const char kInlinePing[] = "PING\r\n";
    constexpr size_t kRespLen = sizeof(kRespPing) - 1;
    constexpr size_t kInlineLen = sizeof(kInlinePing) - 1;

    while (!c.rbuf.empty()) {
        if (c.rbuf.size() >= kRespLen && equals_ci(c.rbuf.data(), kRespPing, kRespLen)) {
            c.rbuf.erase(0, kRespLen);
            c.wbuf += "+PONG\r\n";
        } else if (c.rbuf.size() >= kInlineLen && equals_ci(c.rbuf.data(), kInlinePing, kInlineLen)) {
            c.rbuf.erase(0, kInlineLen);
            c.wbuf += "+PONG\r\n";
        } else if (is_prefix_ci(c.rbuf, kRespPing) ||
                   is_prefix_ci(c.rbuf, kInlinePing)) {
            return;  // partial command: wait for more bytes
        } else {
            std::fprintf(stderr, "UNMATCHED (%zu bytes): ", c.rbuf.size());
            for (unsigned char ch : c.rbuf) {
                std::fprintf(stderr, "\\x%02x", ch);
            }
            std::fprintf(stderr, "\n");
            c.wbuf += "-ERR unknown command\r\n";
            c.rbuf.clear();
            return;
        }
    }
}

// ------------------------------------------------------------ socket I/O

static void handle_accept(int listen_fd) {
    // Level-triggered would let us accept one at a time, but draining
    // is cheaper (fewer epoll_wait calls under connection bursts).
    while (true) {
        int fd = accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            perror("accept4");
            return;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(g_ep, EPOLL_CTL_ADD, fd, &ev) < 0) {
            perror("epoll_ctl ADD");
            close(fd);
            continue;
        }
        g_conns[fd].fd = fd;
    }
}

// Try to flush pending output. Returns false if the connection died.
static bool flush_write(Conn& c) {
    while (c.woff < c.wbuf.size()) {
        ssize_t n = send(c.fd, c.wbuf.data() + c.woff, c.wbuf.size() - c.woff,
                         MSG_NOSIGNAL);
        if (n > 0) {
            c.woff += static_cast<size_t>(n);
        } else if (n < 0 && errno == EINTR) {
            continue;
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;  // kernel buffer full; EPOLLOUT will tell us when to resume
        } else {
            return false;  // real error (EPIPE, ECONNRESET, ...)
        }
    }
    if (c.woff == c.wbuf.size()) {
        c.wbuf.clear();
        c.woff = 0;
    }
    update_events(c);
    return true;
}

static void handle_read(int fd) {
    auto it = g_conns.find(fd);
    if (it == g_conns.end()) return;
    Conn& c = it->second;

    char buf[kReadChunk];
    while (true) {
        ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n > 0) {
            c.rbuf.append(buf, static_cast<size_t>(n));
            if (static_cast<size_t>(n) < sizeof(buf)) break;  // likely drained
        } else if (n == 0) {
            close_conn(fd);  // peer closed
            return;
        } else if (errno == EINTR) {
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        } else {
            close_conn(fd);
            return;
        }
    }

    process_input(c);  // may execute 0, 1, or many pipelined commands
    if (!flush_write(c)) close_conn(fd);  // one send() per batch of replies
}

static void handle_write(int fd) {
    auto it = g_conns.find(fd);
    if (it == g_conns.end()) return;
    if (!flush_write(it->second)) close_conn(fd);
}

// ------------------------------------------------------------------ main

int main(int argc, char** argv) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 6380;

    signal(SIGPIPE, SIG_IGN);  // writing to a closed socket must not kill us

    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (listen_fd < 0) die("socket");

    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        die("bind");
    if (listen(listen_fd, SOMAXCONN) < 0) die("listen");

    g_ep = epoll_create1(0);
    if (g_ep < 0) die("epoll_create1");

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd;
    if (epoll_ctl(g_ep, EPOLL_CTL_ADD, listen_fd, &ev) < 0) die("epoll_ctl");

    std::printf("mini-redis listening on port %d\n", port);

    epoll_event events[kMaxEvents];
    while (true) {
        // 100 ms timeout: on Day 3 this tick drives the key-expiry sweep.
        int n = epoll_wait(g_ep, events, kMaxEvents, 100);
        if (n < 0) {
            if (errno == EINTR) continue;
            die("epoll_wait");
        }
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            uint32_t e = events[i].events;
            if (fd == listen_fd) {
                handle_accept(listen_fd);
                continue;
            }
            if (e & (EPOLLERR | EPOLLHUP)) {
                close_conn(fd);
                continue;
            }
            if (e & EPOLLIN) handle_read(fd);
            if ((e & EPOLLOUT) && g_conns.count(fd)) handle_write(fd);
        }
        // Day 3: expire_sweep();
    }
}