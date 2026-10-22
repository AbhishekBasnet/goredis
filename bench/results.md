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

Write commands only. Both servers fsync to the same btrfs filesystem on NVMe. "Before" is the `stage-9` binary; "after" adds **group commit**. All three servers were interleaved, 3 runs each. The `stage-9` binary ran fewer requests under `always` (5k), because it only managed about 700/s.

### `appendfsync always`

P=1 (50k requests):

| command | Redis 7.4 | goredis before | goredis after | after / before | after / Redis | p50 ms before → after (Redis) |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 16,271 | 721 | 18,512 | 25.7× | 114% | 76.99 → 3.21 (4.20) |
| `INCR` | 19,128 | 709 | 18,182 | 25.7× | 95% | 79.04 → 3.25 (3.76) |
| `LPUSH` | 18,437 | 730 | 18,484 | 25.3× | 100% | 76.16 → 3.21 (3.91) |
| `LPOP` | 18,990 | 705 | 18,657 | 26.4× | 98% | 79.10 → 3.17 (3.76) |
| `MSET` | 16,639 | 727 | 17,403 | 23.9× | 105% | 76.73 → 3.48 (4.27) |

P=16 (500k requests):

| command | Redis 7.4 | goredis before | goredis after | after / before | after / Redis | p50 ms before → after (Redis) |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 203,004 | 759 | 287,522 | 378.9× | 142% | 1098.75 → 3.54 (5.27) |
| `INCR` | 222,321 | 554 | 240,038 | 433.0× | 108% | 2482.18 → 4.23 (4.77) |
| `LPUSH` | 169,895 | 742 | 254,194 | 342.7× | 150% | 1118.21 → 4.01 (6.22) |
| `LPOP` | 204,666 | 731 | 290,192 | 397.2× | 142% | 1131.52 → 3.25 (5.04) |
| `MSET` | 105,708 | 723 | 257,998 | 356.6× | 244% | 1213.44 → 4.42 (9.05) |

### `appendfsync everysec`

P=1 (100k requests):

| command | Redis 7.4 | goredis before | goredis after | after / before | after / Redis | p50 ms before → after (Redis) |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 114,943 | 61,387 | 85,763 | 1.4× | 75% | 0.73 → 0.54 (0.52) |
| `INCR` | 104,603 | 64,350 | 75,529 | 1.2× | 72% | 0.70 → 0.57 (0.51) |
| `LPUSH` | 93,633 | 68,918 | 62,305 | 0.9× | 67% | 0.67 → 0.70 (0.72) |
| `LPOP` | 101,112 | 64,516 | 61,387 | 1.0× | 61% | 0.67 → 0.70 (0.52) |
| `MSET` | 68,729 | 71,174 | 57,537 | 0.8× | 84% | 0.91 → 0.76 (0.96) |

P=16 (1M requests):

| command | Redis 7.4 | goredis before | goredis after | after / before | after / Redis | p50 ms before → after (Redis) |
|---|---:|---:|---:|---:|---:|---:|
| `SET` | 466,200 | 220,848 | 572,410 | 2.6× | 123% | 11.55 → 2.21 (2.13) |
| `INCR` | 583,090 | 165,044 | 422,297 | 2.6× | 72% | 14.31 → 5.74 (1.76) |
| `LPUSH` | 286,862 | 168,919 | 469,484 | 2.8× | 164% | 14.36 → 4.58 (3.65) |
| `LPOP` | 453,926 | 169,779 | 524,384 | 3.1× | 116% | 14.05 → 2.64 (2.17) |
| `MSET` | 110,779 | 105,009 | 187,688 | 1.8× | 169% | 22.77 → 12.61 (8.80) |

### What group commit changed

| | Before | After (and Redis) |
|---|---|---|
| `Append`, under the store lock | `write` syscall, plus `fsync` with `always` | encode into an in-memory buffer (about 25 ns, no allocation) |
| Who does the I/O | every writer, one at a time, holding the global lock | one flusher goroutine, with no store lock held |
| Writes per `write`/`fsync` | 1 | everything that arrived since the last one |
| When the reply is sent | after its own `write` (and `fsync`) | after the flusher reports its entry written (`no`/`everysec`) or synced (`always`) |

- **With `always`**, goredis went from about 700 writes/s, whatever the concurrency, to Redis's level: **25× unpipelined and 340–430× pipelined**. The p50 at P=16 went from 1.1 s to 3–4 ms. While one fsync is in flight, every new write joins the next batch, so the disk's fsync rate no longer limits the write rate.
- **With `everysec`, pipelined**, it's 2.6–3.1× faster. Writers no longer take turns doing a `write` syscall under the global lock.
- **Unpipelined `everysec`** is within the noise, as expected: a request there is dominated by its network round trip, not by the AOF.
- **The guarantees are unchanged.** A reply still isn't sent before its entry reached the OS (or the disk, with `always`). `TestKillDashNineUnderConcurrentWritesKeepsAcknowledgedWrites` checks this with 16 concurrent writers and `kill -9` under each policy.

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

### Remaining hotspots

From the same profiles, in order of size:

1. ~~**AOF writes and fsyncs under the store lock**~~: fixed with group commit, see above.
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
