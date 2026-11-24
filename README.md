# mini-redis

A from-scratch, Redis-compatible in-memory key-value server in C++17 for Linux. It speaks the Redis wire protocol (RESP), so the real `redis-cli` and `redis-benchmark` work against it unchanged.

It is a learning and portfolio project. It handles **string values only** and is not a replacement for Redis. The goal was correct behaviour, crash-safe persistence, and honest, reproducible measurements.

## Features

- **22 commands:** `PING`, `ECHO`, `SET` (with `EX`/`PX`), `GET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `STRLEN`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `DBSIZE`, `FLUSHALL`, `QUIT`, plus `CONFIG` and `COMMAND` stubs so `redis-cli` and `redis-benchmark` connect cleanly.
- **Single-threaded, level-triggered `epoll` event loop** with non-blocking sockets and per-connection read/write buffers.
- **Pipelining and partial reads:** an incremental RESP parser that handles commands split across packets or batched in one packet. Inline commands (`nc`, `telnet`) work too.
- **Key expiry:** lazy (checked on access) plus active (a deadline-ordered index swept every loop pass).
- **Append-only-file (AOF) persistence** with three fsync policies: `always`, `everysec`, `no`.
- **Crash recovery:** the log is replayed on start. A half-written final command is repaired; corruption in the middle of the log is refused.

## Build and run

Requires Linux (uses `epoll`) and a C++17 compiler.

```bash
g++ -std=c++17 -O2 -Wall -Wextra -Wpedantic main.cpp -o miniredis
./miniredis [port=6380] [aof-file|none=appendonly.aof] [always|everysec|no=everysec]
```

Examples:

```bash
./miniredis                                  # port 6380, appendonly.aof, everysec
./miniredis 6380 none no                     # pure in-memory, no persistence
./miniredis 6380 data.aof always             # fsync before every reply batch
```

Talk to it with the standard client:

```bash
redis-cli -p 6380
127.0.0.1:6380> SET greeting hello EX 60
OK
127.0.0.1:6380> GET greeting
"hello"
127.0.0.1:6380> TTL greeting
(integer) 58
```

## Tests

An integration suite (81 checks) starts the server as a subprocess, talks RESP over a real socket, and kills it with `SIGKILL` to test recovery.

```bash
python3 tests/test_server.py ./miniredis
```

It also runs against an AddressSanitizer + UndefinedBehaviorSanitizer build:

```bash
g++ -std=c++17 -g -O1 -fsanitize=address,undefined -fno-omit-frame-pointer main.cpp -o miniredis_dbg
python3 tests/test_server.py ./miniredis_dbg
```

Covered: command semantics and error cases, inline commands, split and pipelined packets, binary-safe values, TTL rules (`-1`/`-2` codes, `SET` clears TTL, `INCR` keeps it, past deadlines delete), lazy and active expiry on 5,000 keys, `kill -9` recovery, torn-tail repair, corrupt-log refusal, all three fsync policies, and 40,000 pipelined writes.

## Architecture

```
 client sockets ──► epoll loop (single thread)
                      │
                      ├─ handle_read:  append bytes to the connection's rbuf
                      ├─ RESP parser:  Ok(consumed) | Incomplete | Error
                      ├─ execute():    command table → handler → reply in wbuf
                      │                 └─ writes also append to an in-memory AOF batch
                      │
                      └─ end of each loop pass ("tick"):
                           1. expire_sweep()          delete due keys (budget 1000/tick)
                           2. aof.append(batch)       gather this pass's write commands
                           3. aof.flush()             one write() + fdatasync per policy
                           4. flush_pending()         only now send replies to clients
```

| File | Role |
|---|---|
| `main.cpp` | epoll loop, connections, per-tick ordering, CLI arguments |
| `resp.hpp` | incremental RESP and inline parser, reply writers, strict integer parsing |
| `commands.hpp` | the store, expiry logic, command table and handlers |
| `aof.hpp` | log append, fsync policies, replay, torn-tail repair |

## Design decisions

- **Single thread.** No locks, and every command is atomic by construction. The cost is that one slow operation delays everyone.
- **Replies are sent after the log write.** Within a loop pass, replies are queued and only sent after the AOF `write()` (and `fdatasync` if the policy says so). A client never sees `OK` for a write that is not in the log.
- **Group commit.** With `always`, one `fdatasync` per loop pass is shared by every client that wrote in that pass, so throughput scales with the number of clients (see numbers below).
- **Absolute expiry times in the log.** `SET k v EX 10` is logged as `SET k v` plus `PEXPIREAT k <absolute ms>`. A relative TTL would restart its clock on every replay.
- **Replay runs with frozen time.** During replay nothing expires and nothing is logged, and expiry-deletes that happened live are logged as `DEL`. This stops a key that already expired from being resurrected by a later `INCR` in the log.
- **Failed commands are not logged.** A command that returned an error is never replayed.
- **Torn tail vs corruption.** An incomplete command at the end of the file (a crash mid-write) is truncated with a warning. An invalid command in the middle stops startup with the byte offset, because guessing would risk silent data loss.
- **Write failures are fatal.** If `write()` or `fdatasync` fails, the server exits rather than acknowledge data it cannot guarantee.
- **Backpressure.** A client with more than 64 MB of unsent output is disconnected. `EPOLLOUT` is only subscribed while output is pending.
- **Parser limits** mirror Redis-style safety: 1M arguments, 512 MB bulk strings, 64 KB inline lines.

## Benchmarks

**Setup:** Intel i5-13420H (12 logical cores), 7.6 GB RAM, WSL2 Ubuntu on Windows, g++ 15.2 `-O2`, `redis-benchmark` 8.0.5, compared against `redis-server` 8.0.5. Client and server share the same machine. Results are the **median of 3 runs**, alternating the two servers. Absolute numbers on this setup drift by roughly 30% between sessions, so compare ratios within one table, not across tables.

### vs Redis 8.0.5 (no persistence, 50 clients, 200,000 requests)

Redis was started with `--save "" --appendonly no`; mini-redis with `none no`.

| Test | mini-redis (req/s) | Redis 8.0.5 (req/s) | mini ÷ Redis |
|---|---|---|---|
| SET, single hot key | 103,520 | 113,186 | 91% |
| GET, single hot key | 98,377 | 116,077 | 85% |
| SET, random keys, pipeline 16 | 757,576 | 623,053 | 122%* |
| GET, random keys, pipeline 16 | 1,075,269 | 1,020,408 | 105%* |

\*Treat the pipelined rows as **comparable, not faster**. Run-to-run spread for mini-redis was about 40% (669k to 957k on SET), larger than the gap. Redis also does much more work per command (many data types, statistics, safety checks) while this server only handles strings.

Caveats:

- The hot-key test hits one key, the best case for the CPU cache, and is partly limited by `redis-benchmark` itself.
- In the random-key GET test, only 200,000 keys were written into a 1,000,000-key space, so most GETs are misses. It is a fair comparison between the two servers but not a full read test.

### Durability cost (SET, 50 clients, AOF on ext4)

| fsync policy | median req/s | p50 latency |
|---|---|---|
| `no` | 109,649 | 0.24 ms |
| `everysec` | 113,122 | 0.23 ms |
| `always` | 5,139 | 9.4 ms |

`always` is roughly **20x slower** than `everysec` (22x on these medians). `no` and `everysec` are indistinguishable within run-to-run noise.

### Why `always` is slow, and how group commit helps

A raw disk sync on this machine costs about 5 ms (measured separately with `dd ... oflag=dsync`: 500 synced 4 KB writes in 2.56 s).

| Clients (`always`) | req/s | p50 latency |
|---|---|---|
| 1 | 182 | 5.4 ms |
| 10 | 1,618 | 6.2 ms |
| 50 | 5,139 | 9.4 ms |

One client is limited to about one write per disk sync. More clients share each sync, so throughput grows while latency barely moves.

### Crash-recovery check

With `always`, 1,000 random-key `SET`s were written, the server was killed with `kill -9`, then restarted: `AOF: replayed 1000 commands, 1000 keys loaded`, and `DBSIZE` was 1000 before and after.

`kill -9` proves the replay logic, not fsync: the operating system keeps data already handed to `write()` even when the process dies. Only a power loss would test whether `fdatasync` truly reached the disk, and that was not tested here.

## Known limitations

- Only string values; no lists, hashes, sets or sorted sets. `SET` has no `NX`/`XX`.
- The AOF is never compacted or rewritten, so it only grows.
- `fdatasync` runs on the event-loop thread, so with `everysec` the loop stalls for one sync (about 5 to 10 ms here) once per second.
- Replay loads the whole log file into memory.
- `DBSIZE` includes keys that have expired but have not been swept yet.
- Key lookups build a temporary `std::string` (a C++17 limitation with `unordered_map`); this did not show up as a measurable cost in the benchmarks above.
- No graceful shutdown or signal handling; no replication, authentication or TLS.

## Possible next steps

AOF rewrite/compaction, a background fsync thread, heterogeneous lookup (C++20) or a flat hash map, `SET NX/XX`, more data types, graceful shutdown. Each would be measured before and after.
