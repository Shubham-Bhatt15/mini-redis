#pragma once
// RESP parser + reply writers. The parser answers Ok / Incomplete / Error.
// Parsed args are string_views INTO the caller's buffer (zero copy); they are
// valid only until that buffer is modified.
#include <charconv>
#include <cstddef>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

// Strict signed 64-bit parse (Redis rules): optional '-', no '+', no leading
// zeros ("007", "-0" rejected), no whitespace, no junk, no overflow.
inline bool parse_i64(std::string_view s, long long& out) {
    if (s.empty()) return false;
    std::string_view digits = s;
    if (digits[0] == '-') digits.remove_prefix(1);
    if (digits.empty()) return false;
    if (digits[0] == '0' && (digits.size() > 1 || s[0] == '-')) return false;
    const char* b = s.data();
    const char* e = b + s.size();
    auto [p, ec] = std::from_chars(b, e, out);
    return ec == std::errc() && p == e;
}

namespace resp {

enum class Status { Ok, Incomplete, Error };

struct ParseResult {
    Status status = Status::Incomplete;
    size_t consumed = 0;          // valid when status == Ok
    const char* error = nullptr;  // valid when status == Error
};

constexpr long long kMaxArgs = 1024 * 1024;
constexpr long long kMaxBulkLen = 512LL * 1024 * 1024;
constexpr size_t kMaxInlineLen = 64 * 1024;
constexpr size_t kMaxHeaderLine = 32;
constexpr size_t kNpos = static_cast<size_t>(-1);

// Index of the '\r' of the first "\r\n" at or after `from`, or kNpos.
inline size_t find_crlf(const char* d, size_t len, size_t from) {
    while (from + 1 < len) {
        const void* p = std::memchr(d + from, '\r', len - from - 1);
        if (!p) return kNpos;
        size_t i = static_cast<size_t>(static_cast<const char*>(p) - d);
        if (d[i + 1] == '\n') return i;
        from = i + 1;
    }
    return kNpos;
}

inline ParseResult make_error(const char* msg) {
    return ParseResult{Status::Error, 0, msg};
}

// "set a  b\r\n": the form nc/telnet users type.
inline ParseResult parse_inline(const char* d, size_t len,
                                std::vector<std::string_view>& args) {
    const void* nlp = std::memchr(d, '\n', len);
    if (!nlp) {
        if (len > kMaxInlineLen) return make_error("too big inline request");
        return ParseResult{Status::Incomplete, 0, nullptr};
    }
    size_t nl = static_cast<size_t>(static_cast<const char*>(nlp) - d);
    size_t end = nl;
    if (end > 0 && d[end - 1] == '\r') end--;

    size_t i = 0;
    while (i < end) {
        while (i < end && (d[i] == ' ' || d[i] == '\t')) i++;
        size_t start = i;
        while (i < end && d[i] != ' ' && d[i] != '\t') i++;
        if (i > start) args.emplace_back(d + start, i - start);
    }
    return ParseResult{Status::Ok, nl + 1, nullptr};
}

// "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n"
inline ParseResult parse_command(const char* d, size_t len,
                                 std::vector<std::string_view>& args) {
    args.clear();
    if (len == 0) return ParseResult{Status::Incomplete, 0, nullptr};
    if (d[0] != '*') return parse_inline(d, len, args);

    size_t eol = find_crlf(d, len, 1);
    if (eol == kNpos) {
        if (len > kMaxHeaderLine) return make_error("invalid multibulk length");
        return ParseResult{Status::Incomplete, 0, nullptr};
    }
    long long n = 0;
    if (!parse_i64(std::string_view(d + 1, eol - 1), n) || n > kMaxArgs)
        return make_error("invalid multibulk length");
    size_t pos = eol + 2;
    if (n <= 0) return ParseResult{Status::Ok, pos, nullptr};  // *0 / *-1: no-op

    args.reserve(static_cast<size_t>(n < 64 ? n : 64));  // don't trust client's count

    for (long long i = 0; i < n; i++) {
        if (pos >= len) return ParseResult{Status::Incomplete, 0, nullptr};
        if (d[pos] != '$') return make_error("expected '$'");

        eol = find_crlf(d, len, pos + 1);
        if (eol == kNpos) {
            if (len - pos > kMaxHeaderLine) return make_error("invalid bulk length");
            return ParseResult{Status::Incomplete, 0, nullptr};
        }
        long long blen = 0;
        if (!parse_i64(std::string_view(d + pos + 1, eol - pos - 1), blen) ||
            blen < 0 || blen > kMaxBulkLen)
            return make_error("invalid bulk length");

        size_t start = eol + 2;
        size_t need = start + static_cast<size_t>(blen) + 2;
        if (len < need) return ParseResult{Status::Incomplete, 0, nullptr};
        if (d[need - 2] != '\r' || d[need - 1] != '\n')
            return make_error("bad bulk terminator");

        args.emplace_back(d + start, static_cast<size_t>(blen));
        pos = need;
    }
    return ParseResult{Status::Ok, pos, nullptr};
}

// ---- reply writers: all append to `out`, so many replies batch into one send()
inline void append_num(std::string& out, long long v) {
    char buf[24];
    auto r = std::to_chars(buf, buf + sizeof(buf), v);
    out.append(buf, static_cast<size_t>(r.ptr - buf));
}
inline void add_simple(std::string& out, std::string_view s) { out += '+'; out.append(s); out += "\r\n"; }
inline void add_error(std::string& out, std::string_view s)  { out += '-'; out.append(s); out += "\r\n"; }
inline void add_int(std::string& out, long long v) { out += ':'; append_num(out, v); out += "\r\n"; }
inline void add_bulk(std::string& out, std::string_view s) {
    out += '$'; append_num(out, static_cast<long long>(s.size())); out += "\r\n";
    out.append(s); out += "\r\n";
}
inline void add_null(std::string& out) { out += "$-1\r\n"; }
inline void add_empty_array(std::string& out) { out += "*0\r\n"; }

}  // namespace resp