# Benchmarks: goredis vs Redis 7.4

All numbers are requests per second from the real `redis-benchmark`, medians of several interleaved runs. Every table is generated from the raw CSV, not typed by hand.

## Setup

| | |
|---|---|
| Machine | Laptop, Intel Core i5-12450H (8 cores, 12 threads), 16 GB RAM |
| OS | Fedora 44, Linux 7.2.11, desktop session running |
| goredis | Go 1.26.10, `-appendonly=false` unless stated |
| Redis | 7.4.11 (`redis:7.4-alpine`, `--network host`), `--save '' --appendonly no` unless stated |
| Client | `redis-benchmark` 7.4.11 (same image, `--network host`), 50 clients, 3-byte values |
| AOF tests | Both servers write to the same btrfs filesystem on NVMe (`/tmp` is tmpfs, where fsync is free, so it was avoided) |

Reproduce with `scripts/bench.sh PORT [PIPELINE] [RUNS]`.

**How much to trust these numbers.** This is a laptop with frequency scaling and a desktop running. Even with 2M requests per test and 5 interleaved runs, Redis's own results varied by up to 79% between runs, and absolute numbers fell by about 40% over a sustained 8-minute run (thermal throttling). **Differences under about 20% are noise here.** The comparisons that hold up are the large ones, and the CPU-per-request measurement in the last section.

---

## Without persistence

### Unpipelined (`-P 1`, 300k requests, 5 runs)

| command | Redis 7.4 | goredis | goredis / Redis | p50 ms (Redis / goredis) | run-to-run spread (Redis / goredis) |
|---|---:|---:|---:|---:|---:|
| `PING_INLINE` | 88,731 | 80,710 | 91% | 0.54 / 0.53 | 21% / 52% |
| `PING_MBULK` | 90,882 | 78,247 | 86% | 0.51 / 0.55 | 33% / 44% |
| `SET` | 96,681 | 75,131 | 78% | 0.48 / 0.54 | 23% / 32% |
| `GET` | 100,067 | 77,781 | 78% | 0.47 / 0.55 | 31% / 40% |
| `INCR` | 91,547 | 73,171 | 80% | 0.51 / 0.61 | 23% / 43% |
| `LPUSH` | 103,341 | 70,755 | 68% | 0.57 / 0.66 | 24% / 45% |
| `RPUSH` | 93,779 | 64,281 | 69% | 0.50 / 0.69 | 15% / 31% |
| `LPOP` | 86,480 | 68,571 | 79% | 0.56 / 0.66 | 33% / 26% |
| `RPOP` | 87,108 | 74,147 | 85% | 0.55 / 0.60 | 35% / 10% |
| `MSET` (10 keys) | 82,781 | 67,766 | 82% | 0.75 / 0.66 | 19% / 33% |

**About 70–90% of Redis.** Every command lands in the same band, which says the cost is per request, not per command: a network round trip, a `read` and a `write` syscall, and waking a goroutine. Redis's single-threaded event loop has less of that to do per request.

### Pipelined (`-P 16`, 2M requests, 5 runs)

| command | Redis 7.4 | goredis | goredis / Redis | p50 ms (Redis / goredis) | run-to-run spread (Redis / goredis) |
|---|---:|---:|---:|---:|---:|
| `PING_INLINE` | 820,008 | 933,271 | 114% | 1.21 / 0.67 | 59% / 12% |
| `PING_MBULK` | 1,033,592 | 849,257 | 82% | 0.93 / 0.73 | 79% / 5% |
| `SET` | 762,486 | 735,024 | 96% | 1.29 / 1.01 | 75% / 9% |
| `GET` | 835,422 | 812,678 | 97% | 1.18 / 0.78 | 38% / 27% |
| `INCR` | 856,531 | 587,544 | 69% | 1.14 / 4.58 | 46% / 29% |
| `LPUSH` | 340,426 | 677,966 | 199% | 3.15 / 1.66 | 9% / 24% |
| `RPUSH` | 699,056 | 683,527 | 98% | 1.42 / 1.68 | 38% / 28% |
| `LPOP` | 556,483 | 691,802 | 124% | 1.77 / 1.34 | 28% / 35% |
| `RPOP` | 609,942 | 705,219 | 116% | 1.59 / 1.33 | 31% / 33% |
| `MSET` (10 keys) | 233,019 | 276,740 | 119% | 4.01 / 10.34 | 29% / 12% |

**Roughly on par with Redis.** Once syscalls are amortised over 16 commands, multiple cores help goredis. The two clear outliers:

- **`INCR` is slower** (69%, p50 4.6 ms). `redis-benchmark` sends every `INCR` to the same key, so all 50 connections serialise on the store's global write lock, and each `INCR` parses and formats an integer while holding it.
- **`LPUSH` is faster** (199%). The benchmark pushes millions of elements onto one list, and goredis's ring buffer appends in O(1) without the bookkeeping of Redis's compressed quicklist. A 2× gap far outside the noise, but specific to this workload.

---

## With the AOF

Write commands only, both servers fsyncing to the same btrfs filesystem.

### `appendfsync everysec` (P=1: 200k requests; P=16: 1M requests; 3 runs)

| command | Redis P=1 | goredis P=1 | Redis P=16 | goredis P=16 | goredis p50 ms at P=16 |
|---|---:|---:|---:|---:|---:|
| `SET` | 92,464 | 73,368 (79%) | 516,529 | 204,082 (40%) | 12.4 |
| `INCR` | 92,678 | 72,176 (78%) | 573,394 | 166,196 (29%) | 14.1 |
| `LPUSH` | 86,430 | 73,233 (85%) | 255,689 | 190,440 (74%) | 12.9 |
| `LPOP` | 93,809 | 80,645 (86%) | 437,445 | 184,672 (42%) | 13.1 |
| `MSET` | 60,588 | 78,339 (129%) | 138,523 | 107,411 (78%) | 22.3 |

### `appendfsync always` (5k requests; 3 runs)

| command | Redis P=1 | goredis P=1 | Redis P=16 | goredis P=16 | goredis p50 ms at P=16 |
|---|---:|---:|---:|---:|---:|
| `SET` | 18,657 | 696 (4%) | 192,615 | 746 (0.4%) | 1125.4 |
| `INCR` | 17,241 | 670 (4%) | 217,739 | 717 (0.3%) | 1159.2 |
| `LPUSH` | 16,447 | 720 (4%) | 172,690 | 736 (0.4%) | 1165.3 |
| `LPOP` | 18,182 | 708 (4%) | 208,667 | 722 (0.3%) | 1147.9 |
| `MSET` | 15,106 | 705 (5%) | 106,553 | 707 (0.7%) | 1197.1 |

**This is goredis's biggest gap, and the next thing to fix.**

| | Redis | goredis |
|---|---|---|
| AOF `write` | one per event-loop iteration, covering every command in it | one per command, under the global store lock |
| `fsync` with `always` | one per event-loop iteration (group commit) | one per command, under the global store lock |

With `everysec`, every write command makes its own `write` syscall while holding the lock, so pipelined writers queue behind each other (p50 12–14 ms). With `always`, every command waits for its own fsync while holding the lock (about 1.4 ms each, implied by the ~700/s ceiling), so throughput is capped at about 700/s *whatever the concurrency or pipelining*, and Redis is 25× to 300× faster.

The fix is **group commit**: `Append` adds to an in-memory buffer under the lock, without doing I/O; one writer goroutine writes and (for `always`) fsyncs whatever has accumulated; and a connection waits for its last entry to be written (or synced) before flushing its replies. That keeps the current guarantees (nothing acknowledged is lost on `kill -9`, nor on power loss with `always`) while paying one syscall per batch instead of one per command.

---

## The one hotspot fixed in this stage

Found with `pprof` (served on `-metrics-addr` at `/debug/pprof`) under `redis-benchmark -P 16 -t set,get,incr`, without persistence.

**Before:** `resp.(*Reader).readBulk` was **18% of all CPU**. For every argument, `io.CopyN` allocated a `LimitedReader`, `io.Copy` allocated a temporary buffer, and a `strings.Builder` allocated a third. That's three or four allocations and two copies to read a 3-byte value.

**Fix:** when the argument fits in the 4 KB `bufio` buffer, `Peek` it and copy it into a string once. Larger arguments keep the streaming path, so a client claiming `$500000000` still can't force a large allocation.

| Measure | Before | After |
|---|---:|---:|
| `BenchmarkReadCommand` (a 3-argument `SET`) | 374 ns, 296 B, 13 allocs | **157 ns, 70 B, 4 allocs** |
| `readBulk` share of CPU under load | 18.0% | 9.7% |
| `runtime.mallocgc` time in an 8 s profile | 0.99 s | 0.52 s |
| **Server CPU per million requests, P=16** (5 interleaved rounds) | 4.49 s | **3.42 s (−24%)** |
| **Server CPU per million requests, P=1** (5 interleaved rounds) | 16.63 s | **15.17 s (−9%)** |

CPU time is the reliable before/after measure here. It's read from the server's own `utime + stime` in `/proc/<pid>/stat` over a fixed workload (`SET`, `GET`, `INCR`, `LPUSH`, `LPOP`), so it isn't distorted by how much CPU the benchmark client or the desktop happened to get. At P=1, four of the five "after" rounds used less CPU than every "before" round.

Throughput, by contrast, mostly drowns the effect in noise:

| command, P=16 | before | after | change | spread before / after |
|---|---:|---:|---:|---:|
| `SET` | 625,586 | 735,024 | +17% | 2% / 9% |
| `GET` | 749,625 | 812,678 | +8% | 9% / 27% |
| `INCR` | 478,813 | 587,544 | +23% | 26% / 29% |
| `LPUSH` | 588,408 | 677,966 | +15% | 6% / 24% |
| `MSET` | 202,000 | 276,740 | +37% | 14% / 12% |
| `RPOP` | 791,139 | 705,219 | −11% | 27% / 33% |

Most commands move in the expected direction, but the change can't make `RPOP` slower (it only removes allocations), so that −11% is noise, which is the point of the warning at the top. At P=1 the request is dominated by about 12 µs of network and syscalls, so saving about 200 ns in the parser isn't visible in throughput at all.

### Remaining hotspots (not fixed)

From the same profiles, in order of size:

1. **AOF writes and fsyncs under the store lock**: see above. By far the largest.
2. **Global write lock contention**: `sync.(*Mutex).lockSlow`, including its spinning, was about 9% of CPU with 50 writers. Sharding the store by key hash is the planned experiment.
3. **`strings.ToLower` on every command name**, done twice per command (`Blocks` and `Dispatch`), about 2.5%. A case-insensitive lookup that doesn't allocate would remove it.
4. **`any` boxing of string values**: each `SET` allocates an interface box.

---

## Go micro-benchmarks

`make bench` (`go test -bench . -benchmem`), after the fix:

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| `resp.ReadCommand` (3 args) | 157 | 70 | 4 |
| `resp.WriteBulk` | 20 | 0 | 0 |
| `resp.WriteInteger` | 17 | 0 | 0 |
| `store.Get` | 64 | 0 | 0 |
| `store.Get`, parallel | 51 | 0 | 0 |
| `store.Set` | 130 | 80 | 3 |
| `store.Set`, parallel | 116 | 64 | 2 |
| `store.IncrBy` | 180 | 79 | 3 |
| `store.Push` + `Pop` | 335 | 336 | 6 |
| `command.Dispatch` `GET` | 99 | 8 | 1 |
| `command.Dispatch` `SET` | 131 | 72 | 3 |
| `command.Dispatch` `INCR` | 268 | 87 | 4 |

Every in-process operation takes well under a microsecond. Under load, the server's time goes to the network, syscalls, scheduling and the lock, not to command logic.

---

## Compatibility

`scripts/compat.sh` sends the 79 commands in `test/compat/commands.txt` through the real `redis-cli` to both servers and diffs the replies. The file's first line is a `DEL` that resets the keys it uses; it isn't compared, so the script gives the same result however many times it runs. **All 79 are byte-identical to Redis 7.4**, including every error message.

The first run also found two commands that goredis deliberately doesn't support, which are listed in the README: `SET … NX` and `INCRBY`. `redis-benchmark` prints `WARNING: Could not fetch server CONFIG`, because goredis doesn't implement `CONFIG GET`; this is harmless.
