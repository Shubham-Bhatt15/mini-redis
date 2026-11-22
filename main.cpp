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
#include <string_view>
#include <vector>

#include "commands.hpp"
#include "resp.hpp"
#include "aof.hpp"

static constexpr int kMaxEvents = 1024;
static constexpr size_t kReadChunk = 16 * 1024;

static constexpr size_t kMaxPendingOutput = 64 * 1024 * 1024;

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
    bool close_after_write = false;  // QUIT / protocol error: hang up once flushed
    bool pending = false;  // queued in g_pending for the end-of-tick flush
};

static int g_ep = -1;
static std::unordered_map<int, Conn> g_conns;
static Store g_store;
static Aof g_aof;
static std::vector<int> g_pending;  // connections with replies to send this tick

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



// Parse every complete command in rbuf (pipelining), run it, append replies
// to wbuf. A partial trailing command stays in rbuf until more bytes arrive.
static void process_input(Conn& c) {
    static std::vector<std::string_view> args;  // reused: no allocation per command
    size_t pos = 0;

    while (pos < c.rbuf.size() && !c.close_after_write) {
        resp::ParseResult r =
            resp::parse_command(c.rbuf.data() + pos, c.rbuf.size() - pos, args);

        if (r.status == resp::Status::Incomplete) break;

        if (r.status == resp::Status::Error) {
            resp::add_error(c.wbuf, std::string("ERR Protocol error: ") + r.error);
            c.close_after_write = true;
            pos = c.rbuf.size();  // stream is garbage; discard everything
            break;
        }

        pos += r.consumed;
        if (args.empty()) continue;  // blank inline line, "*0", "*-1"
        if (cmd::execute(g_store, args, c.wbuf) == cmd::Action::Close)
            c.close_after_write = true;
    }
    // args point into rbuf, so erase only after the last command has run.
    // One erase per batch (not per command) keeps big pipelines O(n).
    c.rbuf.erase(0, pos);
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
        if (c.close_after_write) return false;  
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
    if (c.wbuf.size() - c.woff > kMaxPendingOutput) {  // client isn't reading
        close_conn(fd);
        return;
    }
    if (!c.pending) { c.pending = true; g_pending.push_back(fd); }
}

static void handle_write(int fd) {
    auto it = g_conns.find(fd);
    if (it == g_conns.end()) return;
    if (!flush_write(it->second)) close_conn(fd);
}

static void flush_pending() {
    for (int fd : g_pending) {
        auto it = g_conns.find(fd);
        if (it == g_conns.end()) continue;  // closed meanwhile
        it->second.pending = false;
        if (!flush_write(it->second)) close_conn(fd);
    }
    g_pending.clear();
}
// ------------------------------------------------------------------ main

int main(int argc, char** argv) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 6380;

    signal(SIGPIPE, SIG_IGN);  // writing to a closed socket must not kill us

        // usage: miniredis [port] [aof-file | none] [always | everysec | no]
    std::string aof_path = (argc > 2) ? argv[2] : "appendonly.aof";
    FsyncPolicy policy = FsyncPolicy::EverySec;
    if (argc > 3) {
        std::string p = argv[3];
        if (p == "always") policy = FsyncPolicy::Always;
        else if (p == "everysec") policy = FsyncPolicy::EverySec;
        else if (p == "no") policy = FsyncPolicy::No;
        else {
            std::fprintf(stderr, "usage: %s [port] [aof-file|none] [always|everysec|no]\n", argv[0]);
            return 1;
        }
    }
    if (aof_path != "none" && !g_aof.open_and_replay(aof_path, policy, g_store)) return 1;
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
                // End of tick, in this order:
            g_store.expire_sweep();                // 1. active expiry (logs DELs)
            g_aof.append(g_store.aof);             // 2. this tick's writes -> AOF buffer
            g_store.aof.clear();
            g_aof.flush();                         // 3. one write(), fsync per policy
            flush_pending();                       // 4. only now send the replies
    }
}