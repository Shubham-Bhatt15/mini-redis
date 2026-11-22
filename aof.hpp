#pragma once
// Append-only file: every write command is appended in RESP form. On startup
// the file is replayed through the normal dispatcher to rebuild memory.
#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>

#include "commands.hpp"

enum class FsyncPolicy { Always, EverySec, No };

class Aof {
public:
    bool enabled() const { return fd_ >= 0; }

    bool open_and_replay(const std::string& path, FsyncPolicy policy, Store& store) {
        policy_ = policy;
        fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) { std::perror("aof open"); return false; }
        last_sync_ = steady_ms();
        return replay(store);
    }

    // Buffer commands in memory; nothing touches the disk until flush().
    void append(const std::string& data) {
        if (enabled()) buf_ += data;
    }

    // Called once per event-loop tick: ONE write() for everything the tick
    // produced, then fsync according to the policy.
    void flush() {
        if (!enabled()) return;
        if (!buf_.empty()) {
            size_t off = 0;
            while (off < buf_.size()) {
                ssize_t n = ::write(fd_, buf_.data() + off, buf_.size() - off);
                if (n > 0) off += static_cast<size_t>(n);
                else if (n < 0 && errno == EINTR) continue;
                else fatal("AOF write failed");  // can't promise durability: stop
            }
            buf_.clear();
            dirty_ = true;
        }
        if (!dirty_) return;
        if (policy_ == FsyncPolicy::Always) sync();
        else if (policy_ == FsyncPolicy::EverySec && steady_ms() - last_sync_ >= 1000) sync();
    }

private:
    static long long steady_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
    }
    [[noreturn]] static void fatal(const char* msg) {
        std::perror(msg);
        std::exit(1);
    }
    void sync() {
        if (::fdatasync(fd_) < 0) fatal("AOF fdatasync failed");
        dirty_ = false;
        last_sync_ = steady_ms();
    }

    bool replay(Store& store) {
        std::string data;
        char buf[1 << 16];
        for (;;) {
            ssize_t n = ::read(fd_, buf, sizeof(buf));
            if (n > 0) data.append(buf, static_cast<size_t>(n));
            else if (n == 0) break;
            else if (errno == EINTR) continue;
            else { std::perror("aof read"); return false; }
        }

        store.loading = true;  // frozen time, no logging
        std::vector<std::string_view> args;
        std::string sink;      // replies are discarded
        size_t pos = 0, cmds = 0;
        bool torn_tail = false;
        while (pos < data.size()) {
            resp::ParseResult r = resp::parse_command(data.data() + pos, data.size() - pos, args);
            if (r.status == resp::Status::Incomplete) { torn_tail = true; break; }
            if (r.status == resp::Status::Error) {
                std::fprintf(stderr, "AOF corrupted at byte %zu: %s\n", pos, r.error);
                return false;  // refuse to start rather than silently lose data
            }
            pos += r.consumed;
            if (args.empty()) continue;
            sink.clear();
            cmd::execute(store, args, sink);
            cmds++;
        }
        store.loading = false;
        store.aof.clear();

        if (torn_tail) {  // crash mid-write left a partial last command: drop it
            std::fprintf(stderr, "AOF: dropping %zu trailing bytes of a partial command\n",
                         data.size() - pos);
            if (::ftruncate(fd_, static_cast<off_t>(pos)) != 0) { std::perror("ftruncate"); return false; }
        }
        std::printf("AOF: replayed %zu commands, %zu keys loaded\n", cmds, store.size());
        return true;
    }

    int fd_ = -1;
    FsyncPolicy policy_ = FsyncPolicy::EverySec;
    std::string buf_;
    bool dirty_ = false;       // written but not yet fsynced
    long long last_sync_ = 0;
};