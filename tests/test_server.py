#!/usr/bin/env python3
"""
Integration tests for mini-redis.

Usage:   python3 tests/test_server.py ./miniredis
         python3 tests/test_server.py ./miniredis_dbg     (sanitizer build)

Each test starts the server as a separate process inside a temporary folder,
talks to it over a real TCP socket using the RESP protocol, and (for the
persistence tests) kills it with SIGKILL and restarts it on the same AOF file.
Exit code is 0 if every check passed, 1 otherwise.
"""
import os
import socket
import subprocess
import sys
import tempfile
import time
import traceback

passed = 0
failed = 0


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  PASS  {name}")
    else:
        failed += 1
        print(f"  FAIL  {name}   {detail}")


def check_eq(name, got, want):
    check(name, got == want, f"got {got!r}, want {want!r}")


# --------------------------------------------------------------------------
# RESP client
# --------------------------------------------------------------------------
class Err(str):
    """An error reply (-ERR ...)."""


def is_err(x):
    return isinstance(x, Err)


def enc(*args):
    parts = [b"*%d\r\n" % len(args)]
    for a in args:
        b = a if isinstance(a, bytes) else str(a).encode()
        parts.append(b"$%d\r\n%s\r\n" % (len(b), b))
    return b"".join(parts)


class Client:
    def __init__(self, port):
        self.s = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.buf = b""
        self.pos = 0

    def _fill(self):
        data = self.s.recv(65536)
        if not data:
            raise ConnectionError("server closed the connection")
        if self.pos > 65536:
            self.buf = self.buf[self.pos:]
            self.pos = 0
        self.buf += data

    def _line(self):
        while True:
            i = self.buf.find(b"\r\n", self.pos)
            if i >= 0:
                line = self.buf[self.pos:i]
                self.pos = i + 2
                return line
            self._fill()

    def _exact(self, n):
        while len(self.buf) - self.pos < n + 2:
            self._fill()
        data = self.buf[self.pos:self.pos + n]
        self.pos += n + 2
        return data

    def read_reply(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return Err(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n)
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self.read_reply() for _ in range(n)]
        raise ValueError(f"bad reply: {line!r}")

    def cmd(self, *args):
        self.s.sendall(enc(*args))
        return self.read_reply()

    def close(self):
        self.s.close()


# --------------------------------------------------------------------------
# Server process helper
# --------------------------------------------------------------------------
def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    live = []

    def __init__(self, binary, aof=True, policy="always"):
        self.binary = binary
        self.workdir = tempfile.mkdtemp(prefix="miniredis_test_")
        self.aof = os.path.join(self.workdir, "data.aof") if aof else "none"
        self.policy = policy
        self.log_path = os.path.join(self.workdir, "server.log")
        self.proc = None

    def start(self, wait=True):
        self.port = free_port()
        self.log = open(self.log_path, "ab")
        self.proc = subprocess.Popen(
            [self.binary, str(self.port), self.aof, self.policy],
            cwd=self.workdir, stdout=self.log, stderr=subprocess.STDOUT)
        Server.live.append(self)
        if wait:
            ok = self.wait_ready()
            if not ok:
                raise RuntimeError("server did not start:\n" + self.output())
        return self

    def wait_ready(self, timeout=5.0):
        end = time.time() + timeout
        while time.time() < end:
            if self.proc.poll() is not None:
                return False
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.5).close()
                return True
            except OSError:
                time.sleep(0.05)
        return False

    def client(self):
        return Client(self.port)

    def kill9(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
        if self.proc:
            self.proc.wait()
        self.log.close()

    def restart(self):
        self.kill9()
        return self.start()

    def output(self):
        with open(self.log_path, errors="replace") as f:
            return f.read()


def cleanup():
    for s in Server.live:
        try:
            s.kill9()
        except Exception:
            pass


# --------------------------------------------------------------------------
# Test sections
# --------------------------------------------------------------------------
def section_basics(binary):
    print("\n[basics: commands, errors, protocol]")
    srv = Server(binary, aof=False, policy="no").start()
    c = srv.client()

    check_eq("PING", c.cmd("PING"), "PONG")
    check_eq("ECHO", c.cmd("ECHO", "hello"), b"hello")
    check_eq("SET returns OK", c.cmd("SET", "a", "1"), "OK")
    check_eq("GET returns value", c.cmd("GET", "a"), b"1")
    check_eq("GET missing key is null", c.cmd("GET", "nope"), None)
    check_eq("EXISTS counts", c.cmd("EXISTS", "a", "nope"), 1)
    check_eq("STRLEN", c.cmd("STRLEN", "a"), 1)
    check_eq("DEL returns number deleted", c.cmd("DEL", "a", "nope"), 1)
    check_eq("DBSIZE after DEL", c.cmd("DBSIZE"), 0)

    check_eq("INCR creates key at 1", c.cmd("INCR", "n"), 1)
    check_eq("INCRBY", c.cmd("INCRBY", "n", 10), 11)
    check_eq("DECR", c.cmd("DECR", "n"), 10)
    check_eq("DECRBY", c.cmd("DECRBY", "n", 4), 6)
    c.cmd("SET", "word", "abc")
    check("INCR on non-number is an error", is_err(c.cmd("INCR", "word")))
    c.cmd("SET", "big", "9223372036854775807")
    check("INCR overflow is an error", is_err(c.cmd("INCR", "big")))

    check("unknown command is an error", is_err(c.cmd("NOSUCHCMD")))
    check("wrong arity is an error", is_err(c.cmd("GET")))
    check_eq("FLUSHALL", c.cmd("FLUSHALL"), "OK")
    check_eq("DBSIZE after FLUSHALL", c.cmd("DBSIZE"), 0)

    # inline command (what you type in nc / telnet)
    raw = socket.create_connection(("127.0.0.1", srv.port), timeout=5)
    raw.sendall(b"PING\r\n")
    check_eq("inline PING", raw.recv(100), b"+PONG\r\n")
    raw.close()

    # command split across two TCP packets
    c.s.sendall(b"*1\r\n$4\r\nPI")
    time.sleep(0.15)
    c.s.sendall(b"NG\r\n")
    check_eq("command split across packets", c.read_reply(), "PONG")

    # two commands in one packet (pipelining)
    c.s.sendall(enc("SET", "p", "1") + enc("GET", "p"))
    check_eq("pipelined reply 1", c.read_reply(), "OK")
    check_eq("pipelined reply 2", c.read_reply(), b"1")

    # binary-safe value
    blob = b"a\x00b\r\nc\xff"
    c.cmd("SET", "blob", blob)
    check_eq("binary-safe value", c.cmd("GET", "blob"), blob)
    srv.kill9()


def section_expiry(binary):
    print("\n[expiry semantics]")
    srv = Server(binary, aof=False, policy="no").start()
    c = srv.client()

    check("SET EX 0 is an error", is_err(c.cmd("SET", "k", "v", "EX", 0)))
    check("SET EX -5 is an error", is_err(c.cmd("SET", "k", "v", "EX", -5)))
    check("SET EX abc is an error", is_err(c.cmd("SET", "k", "v", "EX", "abc")))
    check("SET EX and PX together is an error",
          is_err(c.cmd("SET", "k", "v", "EX", 10, "PX", 100)))
    check("huge TTL is rejected",
          is_err(c.cmd("SET", "k", "v", "EX", "9223372036854775807")))

    check_eq("TTL of missing key is -2", c.cmd("TTL", "missing"), -2)
    check_eq("PTTL of missing key is -2", c.cmd("PTTL", "missing"), -2)
    c.cmd("SET", "k", "v")
    check_eq("TTL of key without expiry is -1", c.cmd("TTL", "k"), -1)
    check_eq("PTTL of key without expiry is -1", c.cmd("PTTL", "k"), -1)

    check_eq("EXPIRE returns 1", c.cmd("EXPIRE", "k", 100), 1)
    ttl = c.cmd("TTL", "k")
    check("TTL after EXPIRE 100 is 99..100", ttl in (99, 100), f"got {ttl}")
    pttl = c.cmd("PTTL", "k")
    check("PTTL is between 0 and 100000", 0 < pttl <= 100000, f"got {pttl}")
    check_eq("EXPIRE on missing key returns 0", c.cmd("EXPIRE", "missing", 10), 0)
    check_eq("PERSIST returns 1", c.cmd("PERSIST", "k"), 1)
    check_eq("TTL after PERSIST is -1", c.cmd("TTL", "k"), -1)
    check_eq("PERSIST with no TTL returns 0", c.cmd("PERSIST", "k"), 0)

    c.cmd("SET", "k", "v", "EX", 100)
    c.cmd("SET", "k", "v2")
    check_eq("plain SET clears the TTL", c.cmd("TTL", "k"), -1)

    c.cmd("SET", "n", "1", "EX", 100)
    c.cmd("INCR", "n")
    check("INCR keeps the TTL", c.cmd("TTL", "n") > 0)

    c.cmd("SET", "d", "v")
    check_eq("EXPIRE with negative time returns 1", c.cmd("EXPIRE", "d", -1), 1)
    check_eq("...and the key is gone", c.cmd("GET", "d"), None)
    c.cmd("SET", "d2", "v")
    c.cmd("PEXPIREAT", "d2", 1)
    check_eq("PEXPIREAT in the past deletes", c.cmd("EXISTS", "d2"), 0)

    # lazy expiry
    c.cmd("SET", "e", "v", "PX", 150)
    check_eq("key readable before expiry", c.cmd("GET", "e"), b"v")
    time.sleep(0.4)
    check_eq("expired key reads as null", c.cmd("GET", "e"), None)
    check_eq("TTL of expired key is -2", c.cmd("TTL", "e"), -2)

    # active expiry: nobody touches these keys, the server must sweep them
    c.cmd("FLUSHALL")
    for start in range(0, 5000, 1000):
        c.s.sendall(b"".join(enc("SET", f"x{i}", "v", "PX", 300)
                             for i in range(start, start + 1000)))
        for _ in range(1000):
            c.read_reply()
    check_eq("5000 keys exist right after writing", c.cmd("DBSIZE"), 5000)
    time.sleep(2.0)
    check_eq("active sweep removed all 5000 untouched keys", c.cmd("DBSIZE"), 0)
    srv.kill9()


def section_recovery(binary):
    print("\n[crash recovery with kill -9]")
    srv = Server(binary, aof=True, policy="always").start()
    c = srv.client()
    blob = b"bin\x00ary\r\n\xff"

    c.cmd("SET", "plain", "hello")
    c.cmd("SET", "bin", blob)
    c.cmd("SET", "cnt", "0")
    c.cmd("INCR", "cnt")
    c.cmd("INCR", "cnt")
    c.cmd("INCRBY", "cnt", 10)
    c.cmd("DECRBY", "cnt", 2)                 # 0+1+1+10-2 = 10
    c.cmd("SET", "gone", "x")
    c.cmd("DEL", "gone")
    c.cmd("SET", "ttl", "v", "EX", 100)
    c.cmd("SET", "short", "v", "PX", 200)
    c.cmd("SET", "rev", "5", "PX", 300)
    c.cmd("SET", "str", "abc")
    failed_incr = c.cmd("INCR", "str")
    check("setup: INCR on text failed", is_err(failed_incr))
    time.sleep(1.2)                           # short and rev expire, ttl ages
    check_eq("setup: short expired", c.cmd("GET", "short"), None)
    c.cmd("INCR", "rev")                      # restarts from 0 -> 1
    c.cmd("INCR", "rev")                      # -> 2
    c.cmd("SET", "down", "v", "PX", 600)      # will expire while server is down

    srv.kill9()
    time.sleep(0.9)
    srv.start()
    c = srv.client()

    check_eq("plain value recovered", c.cmd("GET", "plain"), b"hello")
    check_eq("binary-safe value recovered", c.cmd("GET", "bin"), blob)
    check_eq("INCR/INCRBY/DECRBY sum recovered", c.cmd("GET", "cnt"), b"10")
    check_eq("deleted key stays deleted", c.cmd("GET", "gone"), None)
    ttl = c.cmd("TTL", "ttl")
    check("TTL deadline preserved (not restarted)", 90 <= ttl <= 99, f"got {ttl}")
    check_eq("key that expired before the crash stays gone", c.cmd("GET", "short"), None)
    check_eq("INCRs on a since-expired key replay correctly", c.cmd("GET", "rev"), b"2")
    check_eq("value untouched by the failed INCR", c.cmd("GET", "str"), b"abc")
    check_eq("key that expired while server was down is gone", c.cmd("GET", "down"), None)
    check_eq("DBSIZE after recovery", c.cmd("DBSIZE"), 6)

    with open(srv.aof, "rb") as f:
        data = f.read()
    check_eq("failed INCR was not written to the AOF",
             data.count(b"$3\r\nstr\r\n"), 1)
    srv.kill9()


def section_torn_tail(binary):
    print("\n[torn tail repair]")
    # a) half-written command appended at the end
    srv = Server(binary, aof=True, policy="always").start()
    c = srv.client()
    for k in ("t1", "t2", "t3"):
        c.cmd("SET", k, "v-" + k)
    srv.kill9()
    good_size = os.path.getsize(srv.aof)
    with open(srv.aof, "ab") as f:
        f.write(b"*3\r\n$3\r\nSET\r\n$2\r\nt4\r\n$10\r\nabc")   # cut off mid-value
    srv.start()
    c = srv.client()
    check_eq("good commands survive a torn tail", c.cmd("GET", "t3"), b"v-t3")
    check_eq("half-written command is dropped", c.cmd("GET", "t4"), None)
    check_eq("file truncated back to last good command",
             os.path.getsize(srv.aof), good_size)
    c.cmd("SET", "t5", "after")
    srv.restart()
    c = srv.client()
    check_eq("writes after the repair survive", c.cmd("GET", "t5"), b"after")
    check_eq("older keys still there", c.cmd("GET", "t1"), b"v-t1")
    srv.kill9()

    # b) file chopped a few bytes short
    srv = Server(binary, aof=True, policy="always").start()
    c = srv.client()
    for k in ("a", "b", "c"):
        c.cmd("SET", k, "1")
    srv.kill9()
    os.truncate(srv.aof, os.path.getsize(srv.aof) - 4)
    srv.start()
    c = srv.client()
    check_eq("truncated file: earlier keys recovered", c.cmd("GET", "b"), b"1")
    check_eq("truncated file: cut command dropped", c.cmd("GET", "c"), None)
    srv.kill9()


def section_corruption(binary):
    print("\n[corrupt AOF is refused]")
    srv = Server(binary, aof=True, policy="always")
    with open(srv.aof, "wb") as f:
        f.write(enc("SET", "ok", "1") + b"*x\r\n" + enc("SET", "after", "2"))
    srv.start(wait=False)
    try:
        rc = srv.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        rc = None
    check("server exits non-zero on a corrupt middle", rc not in (None, 0), f"rc={rc}")
    check("error message says 'corrupt'", "corrupt" in srv.output().lower(),
          srv.output()[:200])
    srv.kill9()


def section_fsync_policies(binary):
    print("\n[all three fsync policies recover their writes]")
    for policy in ("always", "everysec", "no"):
        srv = Server(binary, aof=True, policy=policy).start()
        c = srv.client()
        for i in range(200):
            c.cmd("SET", f"k{i}", f"v{i}")
        srv.restart()
        c = srv.client()
        check_eq(f"policy {policy}: 200 writes recovered", c.cmd("DBSIZE"), 200)
        check_eq(f"policy {policy}: last value correct", c.cmd("GET", "k199"), b"v199")
        srv.kill9()


def section_pipelined(binary):
    print("\n[40,000 pipelined writes recovered]")
    srv = Server(binary, aof=True, policy="everysec").start()
    c = srv.client()
    all_ok = True
    for start in range(0, 40000, 1000):
        c.s.sendall(b"".join(enc("SET", f"p{i}", f"v{i}")
                             for i in range(start, start + 1000)))
        for _ in range(1000):
            if c.read_reply() != "OK":
                all_ok = False
    check("every pipelined SET was acknowledged", all_ok)
    srv.restart()
    c = srv.client()
    check_eq("DBSIZE after recovery", c.cmd("DBSIZE"), 40000)
    check_eq("spot check last key", c.cmd("GET", "p39999"), b"v39999")
    srv.kill9()


# --------------------------------------------------------------------------
def main():
    if len(sys.argv) != 2:
        print("usage: python3 tests/test_server.py ./miniredis")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print(f"binary not found: {binary}")
        return 2

    sections = [section_basics, section_expiry, section_recovery,
                section_torn_tail, section_corruption,
                section_fsync_policies, section_pipelined]
    global failed
    try:
        for sec in sections:
            try:
                sec(binary)
            except Exception:
                failed += 1
                print(f"  FAIL  {sec.__name__} crashed:")
                traceback.print_exc()
    finally:
        cleanup()

    print(f"\n{passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
