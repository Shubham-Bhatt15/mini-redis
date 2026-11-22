#pragma once
// Keyspace (with expiry) + command table + dispatcher.
#include <cctype>
#include <chrono>
#include <climits>
#include <initializer_list>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "resp.hpp"

struct Entry {
    std::string value;
    long long expire_at = -1;  // absolute unix time in ms; -1 = never expires
};

class Store {
    using Map = std::unordered_map<std::string, Entry>;

public:
    // Commands waiting to be appended to the AOF, already RESP-encoded.
    // main.cpp drains this once per event-loop tick.
    std::string aof;

    // True while replaying the AOF. Time is "frozen": nothing expires and
    // nothing is logged, so replay reproduces the original history exactly.
    bool loading = false;

    static long long now_ms() {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    void log(std::initializer_list<std::string_view> parts) { log_range(parts.begin(), parts.size()); }
    void log_args(const std::vector<std::string_view>& parts) { log_range(parts.data(), parts.size()); }

    // ---- data operations. Every lookup is "lazy-expiring": an expired key is
    // ---- deleted on touch (and the DEL is logged) and reported as missing.

    void set(std::string_view key, std::string_view value) {  // SET clears any TTL
        auto [it, inserted] = map_.try_emplace(std::string(key));
        if (!inserted) unindex(it);
        it->second.value.assign(value.data(), value.size());
        it->second.expire_at = -1;
    }

    const std::string* get(std::string_view key) {
        auto it = live(key);
        return it == map_.end() ? nullptr : &it->second.value;
    }

    bool del(std::string_view key) {
        auto it = live(key);
        if (it == map_.end()) return false;
        unindex(it);
        map_.erase(it);
        return true;
    }

    bool exists(std::string_view key) { return live(key) != map_.end(); }

    // Missing key counts as 0; an existing key keeps its TTL.
    bool incr_by(std::string_view key, long long delta, long long& out) {
        auto it = live(key);
        long long cur = 0;
        if (it != map_.end() && !parse_i64(it->second.value, cur)) return false;
        if (__builtin_add_overflow(cur, delta, &out)) return false;
        if (it != map_.end()) it->second.value = std::to_string(out);
        else map_.emplace(std::string(key), Entry{std::to_string(out), -1});
        return true;
    }

    // ---- expiry

    // false = no such key. A deadline in the past deletes the key right away.
    bool set_expire_at(std::string_view key, long long at_ms) {
        auto it = live(key);
        if (it == map_.end()) return false;
        if (!loading && at_ms <= now_ms()) { expire_it(it); return true; }
        unindex(it);
        it->second.expire_at = at_ms;
        by_expiry_.emplace(at_ms, it->first);
        return true;
    }

    // -2 = no such key, -1 = no expiry, otherwise remaining milliseconds.
    long long pttl(std::string_view key) {
        auto it = live(key);
        if (it == map_.end()) return -2;
        if (it->second.expire_at < 0) return -1;
        long long left = it->second.expire_at - now_ms();
        return left > 0 ? left : 0;
    }

    bool persist(std::string_view key) {
        auto it = live(key);
        if (it == map_.end() || it->second.expire_at < 0) return false;
        unindex(it);
        it->second.expire_at = -1;
        return true;
    }

    // Active expiry: delete keys whose deadline passed even if nobody reads
    // them. by_expiry_ is sorted by deadline, so this is O(expired), not
    // O(all keys). `budget` caps the work per tick so a mass expiry can't
    // stall the event loop.
    size_t expire_sweep(size_t budget = 1000) {
        if (loading || by_expiry_.empty()) return 0;
        long long now = now_ms();
        size_t n = 0;
        while (n < budget && !by_expiry_.empty()) {
            auto first = by_expiry_.begin();
            if (first->first > now) break;
            auto it = map_.find(first->second);
            if (it == map_.end()) { by_expiry_.erase(first); continue; }  // defensive
            expire_it(it);
            n++;
        }
        return n;
    }

    size_t size() const { return map_.size(); }
    void clear() { map_.clear(); by_expiry_.clear(); }

private:
    bool is_expired(const Entry& e) const {
        return e.expire_at >= 0 && !loading && e.expire_at <= now_ms();
    }

    Map::iterator live(std::string_view key) {
        auto it = map_.find(std::string(key));
        if (it != map_.end() && is_expired(it->second)) {
            expire_it(it);
            return map_.end();
        }
        return it;
    }

    void expire_it(Map::iterator it) {
        log({"DEL", it->first});  // so AOF replay sees the key die
        unindex(it);
        map_.erase(it);
    }

    void unindex(Map::iterator it) {
        if (it->second.expire_at >= 0) by_expiry_.erase({it->second.expire_at, it->first});
    }

    void log_range(const std::string_view* p, size_t n) {
        if (loading) return;
        aof += '*'; resp::append_num(aof, static_cast<long long>(n)); aof += "\r\n";
        for (size_t i = 0; i < n; i++) {
            aof += '$'; resp::append_num(aof, static_cast<long long>(p[i].size())); aof += "\r\n";
            aof.append(p[i].data(), p[i].size()); aof += "\r\n";
        }
    }

    // C++17 can't look up with a string_view, so each lookup builds a
    // temporary std::string (Day 5 optimisation target).
    Map map_;
    std::set<std::pair<long long, std::string>> by_expiry_;  // (deadline, key), sorted
};

namespace cmd {

using Args = std::vector<std::string_view>;
enum class Action { Continue, Close };  // Close: flush the reply, then hang up
using Handler = Action (*)(Store&, const Args&, std::string&);

// How a command reaches the AOF:
//   None   : read-only
//   AsIs   : log the original command if it succeeded (deterministic ones)
//   Custom : the handler logs a rewritten form itself. Relative times
//            (EXPIRE, SET EX) become absolute PEXPIREAT so replaying later
//            doesn't restart the clock.
enum class Log { None, AsIs, Custom };

struct Spec { Handler fn; int arity; Log propagate; };

inline std::string lower(std::string_view s) {
    std::string r(s);
    for (char& c : r) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return r;
}
inline void wrong_args(std::string& out, std::string_view name) {
    resp::add_error(out, "ERR wrong number of arguments for '" + lower(name) + "' command");
}
constexpr const char* kNotInt = "ERR value is not an integer or out of range";
constexpr const char* kBadExpire = "ERR invalid expire time";
constexpr long long kMaxTtlMs = LLONG_MAX / 4;

// ---- handlers
inline Action ping(Store&, const Args& a, std::string& out) {
    if (a.size() > 2) { wrong_args(out, a[0]); return Action::Continue; }
    if (a.size() == 2) resp::add_bulk(out, a[1]);
    else resp::add_simple(out, "PONG");
    return Action::Continue;
}
inline Action echo(Store&, const Args& a, std::string& out) {
    resp::add_bulk(out, a[1]);
    return Action::Continue;
}

// SET key value [EX seconds | PX milliseconds]
inline Action set(Store& s, const Args& a, std::string& out) {
    long long ttl_ms = -1;
    for (size_t i = 3; i < a.size(); i += 2) {
        std::string opt = lower(a[i]);
        if ((opt != "ex" && opt != "px") || ttl_ms != -1 || i + 1 >= a.size()) {
            resp::add_error(out, "ERR syntax error");
            return Action::Continue;
        }
        long long n = 0;
        if (!parse_i64(a[i + 1], n)) { resp::add_error(out, kNotInt); return Action::Continue; }
        long long unit = (opt == "ex") ? 1000 : 1;
        if (n <= 0 || n > kMaxTtlMs / unit) {
            resp::add_error(out, "ERR invalid expire time in 'set' command");
            return Action::Continue;
        }
        ttl_ms = n * unit;
    }
    s.set(a[1], a[2]);
    s.log({"SET", a[1], a[2]});
    if (ttl_ms >= 0) {
        long long at = Store::now_ms() + ttl_ms;
        s.set_expire_at(a[1], at);
        s.log({"PEXPIREAT", a[1], std::to_string(at)});
    }
    resp::add_simple(out, "OK");
    return Action::Continue;
}

inline Action get(Store& s, const Args& a, std::string& out) {
    if (const std::string* v = s.get(a[1])) resp::add_bulk(out, *v);
    else resp::add_null(out);
    return Action::Continue;
}
inline Action del(Store& s, const Args& a, std::string& out) {
    long long n = 0;
    for (size_t i = 1; i < a.size(); i++) n += s.del(a[i]) ? 1 : 0;
    resp::add_int(out, n);
    return Action::Continue;
}
inline Action exists(Store& s, const Args& a, std::string& out) {
    long long n = 0;  // like Redis, a key named twice counts twice
    for (size_t i = 1; i < a.size(); i++) n += s.exists(a[i]) ? 1 : 0;
    resp::add_int(out, n);
    return Action::Continue;
}

inline Action incr_common(Store& s, std::string_view key, long long delta, std::string& out) {
    long long result = 0;
    if (s.incr_by(key, delta, result)) resp::add_int(out, result);
    else resp::add_error(out, kNotInt);
    return Action::Continue;
}
inline Action incr(Store& s, const Args& a, std::string& out) { return incr_common(s, a[1], 1, out); }
inline Action decr(Store& s, const Args& a, std::string& out) { return incr_common(s, a[1], -1, out); }
inline Action incrby(Store& s, const Args& a, std::string& out) {
    long long d = 0;
    if (!parse_i64(a[2], d)) { resp::add_error(out, kNotInt); return Action::Continue; }
    return incr_common(s, a[1], d, out);
}
inline Action decrby(Store& s, const Args& a, std::string& out) {
    long long d = 0;
    if (!parse_i64(a[2], d) || d == LLONG_MIN) {  // -LLONG_MIN would overflow
        resp::add_error(out, kNotInt);
        return Action::Continue;
    }
    return incr_common(s, a[1], -d, out);
}

// EXPIRE / PEXPIRE / PEXPIREAT share one body. Negative or past times delete the key.
inline Action expire_common(Store& s, const Args& a, bool seconds, bool absolute, std::string& out) {
    long long n = 0;
    if (!parse_i64(a[2], n)) { resp::add_error(out, kNotInt); return Action::Continue; }
    long long at = n;
    if ((seconds && __builtin_mul_overflow(n, 1000LL, &at)) ||
        (!absolute && __builtin_add_overflow(at, Store::now_ms(), &at))) {
        resp::add_error(out, kBadExpire);
        return Action::Continue;
    }
    if (!s.set_expire_at(a[1], at)) { resp::add_int(out, 0); return Action::Continue; }
    resp::add_int(out, 1);
    s.log({"PEXPIREAT", a[1], std::to_string(at)});
    return Action::Continue;
}
inline Action expire(Store& s, const Args& a, std::string& out)    { return expire_common(s, a, true, false, out); }
inline Action pexpire(Store& s, const Args& a, std::string& out)   { return expire_common(s, a, false, false, out); }
inline Action pexpireat(Store& s, const Args& a, std::string& out) { return expire_common(s, a, false, true, out); }

inline Action ttl(Store& s, const Args& a, std::string& out) {
    long long ms = s.pttl(a[1]);
    resp::add_int(out, ms < 0 ? ms : (ms + 500) / 1000);  // round to nearest second, like Redis
    return Action::Continue;
}
inline Action pttl(Store& s, const Args& a, std::string& out) {
    resp::add_int(out, s.pttl(a[1]));
    return Action::Continue;
}
inline Action persist(Store& s, const Args& a, std::string& out) {
    resp::add_int(out, s.persist(a[1]) ? 1 : 0);
    return Action::Continue;
}

inline Action dbsize(Store& s, const Args&, std::string& out) {
    resp::add_int(out, static_cast<long long>(s.size()));
    return Action::Continue;
}
inline Action flushall(Store& s, const Args&, std::string& out) {
    s.clear();
    resp::add_simple(out, "OK");
    return Action::Continue;
}
// redis-cli / redis-benchmark probe these at startup; empty replies satisfy them.
inline Action config(Store&, const Args&, std::string& out)  { resp::add_empty_array(out); return Action::Continue; }
inline Action command(Store&, const Args&, std::string& out) { resp::add_empty_array(out); return Action::Continue; }
inline Action quit(Store&, const Args&, std::string& out) {
    resp::add_simple(out, "OK");
    return Action::Close;
}

// ---- dispatcher
inline const std::unordered_map<std::string, Spec>& table() {
    static const std::unordered_map<std::string, Spec> t = {
        {"PING",      {ping,      -1, Log::None}},
        {"ECHO",      {echo,       2, Log::None}},
        {"SET",       {set,       -3, Log::Custom}},
        {"GET",       {get,        2, Log::None}},
        {"DEL",       {del,       -2, Log::AsIs}},
        {"EXISTS",    {exists,    -2, Log::None}},
        {"INCR",      {incr,       2, Log::AsIs}},
        {"DECR",      {decr,       2, Log::AsIs}},
        {"INCRBY",    {incrby,     3, Log::AsIs}},
        {"DECRBY",    {decrby,     3, Log::AsIs}},
        {"EXPIRE",    {expire,     3, Log::Custom}},
        {"PEXPIRE",   {pexpire,    3, Log::Custom}},
        {"PEXPIREAT", {pexpireat,  3, Log::Custom}},
        {"TTL",       {ttl,        2, Log::None}},
        {"PTTL",      {pttl,       2, Log::None}},
        {"PERSIST",   {persist,    2, Log::AsIs}},
        {"DBSIZE",    {dbsize,     1, Log::None}},
        {"FLUSHALL",  {flushall,   1, Log::AsIs}},
        {"CONFIG",    {config,    -2, Log::None}},
        {"COMMAND",   {command,   -1, Log::None}},
        {"QUIT",      {quit,      -1, Log::None}},
    };
    return t;
}

inline Action execute(Store& s, const Args& a, std::string& out) {
    std::string name(a[0]);
    for (char& c : name) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    auto it = table().find(name);
    if (it == table().end()) {
        std::string shown(a[0].substr(0, 64));
        for (char& c : shown)  // never echo CR/LF/control bytes back
            if (static_cast<unsigned char>(c) < 32) c = ' ';
        resp::add_error(out, "ERR unknown command '" + shown + "'");
        return Action::Continue;
    }
    const Spec& sp = it->second;
    int n = static_cast<int>(a.size());
    if ((sp.arity > 0 && n != sp.arity) || (sp.arity < 0 && n < -sp.arity)) {
        wrong_args(out, a[0]);
        return Action::Continue;
    }
    size_t before = out.size();
    Action act = sp.fn(s, a, out);
    // AsIs: log only if the reply isn't an error (failed commands changed nothing).
    if (sp.propagate == Log::AsIs && out.size() > before && out[before] != '-') s.log_args(a);
    return act;
}

}  // namespace cmd