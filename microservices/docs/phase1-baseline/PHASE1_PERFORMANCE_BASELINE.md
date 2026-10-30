# Phase 1 Performance, Availability and Correctness Baseline

> **Purpose.** This is the reference record of what the Phase 1 monolith does under load. Every microservices number will be compared against the tables here. Use it together with [MICROSERVICES_COMPARISON.md](MICROSERVICES_COMPARISON.md) (the empty comparison sheet) and [TESTING.md](TESTING.md) (how to reproduce anything below).
>
> **Run date:** 2026-10-31 · **Code:** branch `ticket-forge-monolithic`, commit `9d4e8cc` · **Raw data:** [`baseline-results/`](baseline-results/)
>
> The earlier write-up of findings and fixes is [PHASE1_BASELINE_REPORT.md](PHASE1_BASELINE_REPORT.md). This file is the numeric companion: it was produced by re-running the whole suite on a freshly started stack, and it adds resource usage, availability and the cold-start caveat.

---

## 1. Verdict in one table

| Question | Answer | Evidence |
| :--- | :--- | :--- |
| Is it correct? | **Yes.** 0 double-booked seats, 0 oversold tickets, 0 duplicate holds, 0 double charges | All six DB invariants read 0 after all 34 load runs and after the 58 automated tests |
| Is it available under contention? | **Yes, with the waiting room.** Without it the system stays correct but gets slow and sheds load | §5 |
| What is the read ceiling? | **~4,200–5,000 req/s**, then p99 passes 1 s | §4.1 |
| What is the write (hold) ceiling? | **~500–600 holds/s on one hot row; ~1,500–2,000/s with 8 shards** | §4.3 |
| What is the main bottleneck? | **The single Postgres** (3.8–3.9 of 4 cores at saturation), then the 30-connection Hikari pool | §6 |
| Biggest risk in the numbers? | Cold start. The first runs after a restart were up to 4× slower | §3 |

---

## 2. Test bed (identical for every phase; keep it identical)

| Component | Setting |
| :--- | :--- |
| Host | 12 logical CPUs (Ryzen 7 7445HS), 14 GB RAM, loopback network. Host load average was ~8.7 during this session, so expect ±10–20% noise |
| App | `taskset -c 0,1,6,7` (2 physical cores / 4 threads), `-Xmx1500m`, Java 22 runtime, Spring Boot 4.1, virtual threads, Hikari pool **30** |
| Postgres | `postgres:16-alpine`, container `tf-pg`, `--cpuset-cpus 2,3,8,9`, `--memory 2g`, `max_connections=200`, `lock_timeout 3s`, `statement_timeout 10s` |
| Load generator | `grafana/k6:1.8.2` in Docker, pinned to CPUs `4,5,10,11`, JWTs minted by k6 (bcrypt is not measured) |
| Mock payment gateway | 150 ms ± 100 ms latency, 5–10 % declines |
| Defaults | Lock strategy `ATOMIC_CONDITIONAL`, gap rule on |
| Not running | Prometheus, Grafana (the Compose plugin is not installed on this machine; Postgres was started with `docker run`) |

---

## 3. Read this first: cold start changes the numbers

Same code, same scenarios, run twice on the same machine on the same day.

| Run | Stampede (s2) ATOMIC, 1,000 buyers, hold p99 | Retry storm (s6) req/s | Queue surge (s7) p99 |
| :--- | ---: | ---: | ---: |
| Original baseline (11 days ago, warm) | 399 ms | 1,946 | 16.9 ms |
| Today, **first pass** (fresh JVM, fresh Postgres) | **1,882 ms** | 685 | 164 ms |
| Today, **second pass** (JVM warm) | **455 ms** | 1,185 | 61.5 ms |

The first pass ran straight after JVM start (JIT not compiled) on a database with no planner statistics and no autovacuum yet. The stampede scenarios last only 1–5 seconds, so they are dominated by that warm-up. Cold ATOMIC even looked slower than PESSIMISTIC, the reverse of the warm ordering.

**Rule for the microservices comparison:** run each service for a few minutes of mixed traffic first, run `ANALYZE`, then measure. Report the second pass. Comparing a cold microservice to this warm table would be unfair in the other direction. Both passes are kept in `baseline-results/` (`phase1-rerun-…json` = first pass, `phase1-warm-…json` = second pass).

---

## 4. Performance results

Column guide: **rps** = all HTTP requests per second; **p50/p99** = `http_req_duration` (every request) unless the header says *hold*; **hold p99** = latency of the hold call only; **PG cpu peak** = highest 1-second Postgres CPU in cores (limit is 4); **pool pending max** = most requests waiting for a Hikari connection at one moment.

### 4.1 Catalog reads (scenario 1), no caching

| Target rate | Requests | Achieved rps | p50 | p95 | p99 | App cpu peak | PG cpu peak | Pool pending max | Result |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 2,000 req/s | 81,996 | 1,366 avg over ramp (target met exactly) | 1.4 ms | 3.3 ms | 6.3 ms | 1.89 | 2.42 | 1 | Clean |
| 3,000 req/s | 121,996 | 2,033 avg over ramp (target met) | 1.5 ms | 4.9 ms | 9.9 ms | n/a | 3.14 | n/a | Clean (original: p99 3.9 ms) |
| 8,000 req/s (finds the knee) | 251,187 | **4,164** | 9.3 ms | 1,000 ms | **1,790 ms** | n/a | 3.85 | n/a | **Saturated** (original: 4,997 rps, p99 1,089 ms) |

The "achieved rps" for a ramp is the average over a ramp from 100 to the target, so it is lower than the target; what matters is whether requests were dropped. **Knee: ~3,000–4,000 req/s sustained clean, saturation near 4,200–5,000.** App and DB CPU both saturate because every read hits Postgres. This is the number a Redis cache or a read replica must beat.

### 4.2 Same-seat stampede (scenario 2): exactly one winner, how fast do losers hear "no"

N buyers hold the same seat in the same instant. `holds_ok` was **exactly 1** in all 18 runs.

**Warm pass (use this one), hold latency in ms, median / p99:**

| Buyers | PESSIMISTIC | OPTIMISTIC | ATOMIC_CONDITIONAL |
| ---: | :--- | :--- | :--- |
| 100 | 50 / 85 | 37 / 67 | **28 / 45** |
| 500 | 183 / 312 | **109 / 176** | 143 / 213 |
| 1,000 | 459 / 782 | **202 / 332** | 281 / 455 |

For comparison, original baseline p99: PESSIMISTIC 167 / 510 / 715, OPTIMISTIC 76 / 245 / 414, ATOMIC 47 / 228 / 399 (100 / 500 / 1,000 buyers). Today's warm ATOMIC numbers are within ~15% of the original (45 / 213 / 455 vs 47 / 228 / 399). PESSIMISTIC at 100 and 500 buyers was faster today (85 / 312 vs 167 / 510) and about the same at 1,000. OPTIMISTIC at 500 and 1,000 beat ATOMIC today, a flip from the original that is within single-run noise. PESSIMISTIC is the slowest at 1,000 buyers in both runs.

**First pass (cold), same table, p99 ms:** PESSIMISTIC 229 / 605 / 1,185 · OPTIMISTIC 106 / 305 / 693 · ATOMIC 446 / 1,812 / 1,882. Shown only to document the cold-start effect in §3.

### 4.3 Pit flash sale (scenario 3): 10,000 tickets, 12,000 buyers, 500 concurrent

| Strategy | Tickets sold | Total rps | Hold p50 | Hold p99 | 409 sold-out | 503 overloaded | App cpu peak | PG cpu peak |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| PESSIMISTIC | 10,000 | 503 | 991 ms | 1,921 ms | 1,995 | 5 | 1.88 | 1.68 |
| OPTIMISTIC | **3,065** | 619 | 731 ms | 1,311 ms | 0 | **8,935** | 2.17 | 2.17 |
| ATOMIC_CONDITIONAL | 10,000 | 503 | 979 ms | 2,001 ms | 1,994 | 6 | 2.47 | 1.72 |
| ATOMIC + **8 shards** | 10,000 | **1,512** | **206 ms** | **855 ms** | 2,000 | 0 | 2.71 | 2.80 |

Original baseline for reference: ATOMIC 590 sales/s then 1,728 with 8 shards (2.9×); today 503 → 1,512 (**3.0×**). OPTIMISTIC sold 3,174 then and 3,065 now, so the finding reproduces: **safe but not live on one hot row** (74% of buyers got a 503 while tickets remained). Sharding gives the same ~3× improvement and cuts p99 by 2.3×.

### 4.4 Retry storm (scenario 6): every request sent 3× at once with one Idempotency-Key

| Metric | Original | Today (cold) | Today (warm) |
| :--- | ---: | ---: | ---: |
| Buyers / total requests | 3,000 / 18,006 | 3,000 / 18,006 | 3,000 / 18,006 |
| Duplicate holds | **0** | **0** | **0** |
| Checkouts confirmed (no double charge) | 3,000 | 3,000 | n/a |
| Requests per second | 1,946 | 685 | 1,185 |
| http p99 | 343 ms | 485 ms | 385 ms |
| PG cpu peak | 3.66 | 3.04 | 3.45 |

Idempotency is correct in every run. Throughput varies with warm-up; correctness does not.

### 4.5 Waiting-room surge (scenario 7): 20,000 users join one queue in ~5 s

| Metric | Original | Today (cold) | Today (warm) |
| :--- | ---: | ---: | ---: |
| Joins | 20,002 | 20,006 | 20,008 |
| Requests per second | 3,202 | 2,813 | **2,825** |
| http p50 / p99 | n/a / 16.9 ms | 4.8 / 164 ms | **4.2 / 61.5 ms** |
| Errors | 0 | 0 | 0 |

Admission order and rate (strict FIFO, 0 skipped, exactly 200/s) were verified in the original run; the queue logic is unchanged and covered by `WaitingRoomIntegrationTests`.

### 4.6 Movie traffic (scenarios 4 and 10)

| Metric | s4 mix (20 shows, 300 VUs, 2 min) | s10 Friday evening (200 shows, 1,500 VUs, 3 min) |
| :--- | ---: | ---: |
| Requests | 33,172 | 270,809 |
| Requests per second | 226 (orig 249) | 1,188 (orig 1,204) |
| Holds created | 2,320 | 23,200 (**46,400 seats**, 2 per hold) |
| Checkouts confirmed / declined | 1,664 / 182 | 16,742 / 1,765 |
| Hold p99 | 22 ms (orig 15.5) | **34 ms (orig 86)** |
| http p99 | 243 ms | 257 ms |
| Pool pending max | 0 | 693 |
| PG cpu peak | 1.34 | 3.09 |
| Invariants | all 0 | all 0 |

All inventory sold in both. About 10% of payments were declined; a declined payment keeps the hold so the buyer can retry, and holds that were never paid expired and their seats were resold.

### 4.7 Concert on-sale (scenarios 8 and 9): the waiting room as load protection

| Metric | Queue **ON** (1,500 fans) | Queue **OFF** |
| :--- | ---: | ---: |
| Requests / rps | 216,156 / 1,065 | 245,032 / 1,251 |
| Holds created | 15,407 | 29,038 |
| Checkouts confirmed | 10,930 | 20,712 |
| **Hold p50 / p99** | **1.2 ms / 22.4 ms** | 316 ms / **2,754 ms** |
| http p95 / p99 | 166 / 242 ms | 1,924 / 2,721 ms |
| 503 overloaded | 0 | 1,261 |
| Pool pending max | **0** | **1,471** |
| PG cpu peak | **1.59** | **3.82** |
| App cpu peak | 2.03 | 2.67 |
| Heap max | 193 MB | 709 MB |
| Invariants | all 0 | all 0 |

Original baseline: queue on hold p99 19.5 ms, queue off 2,121 ms. **The result reproduces: the queue cut hold p99 by more than 100×, kept the pool empty and held Postgres at 40% of its CPU, while queue-off pins Postgres at 95% and queues 1,471 requests for a connection.** The system is correct either way; the queue buys latency and headroom, not correctness.

---

## 5. Availability and error budget

Definitions used here: a **conflict (409)** is a correct answer (someone else got the seat) and is not an error; **overloaded (503)** is the system refusing a request that should have been served; **error** is any unexpected 5xx. Availability = 1 − (503 + errors) / hold attempts.

| Scenario | Hold attempts | Unexpected errors | 503 overloaded | Availability |
| :--- | ---: | ---: | ---: | ---: |
| s3 pit, PESSIMISTIC | 12,000 | 0 | 5 | 99.96% |
| s3 pit, ATOMIC | 12,000 | 0 | 6 | 99.95% |
| s3 pit, ATOMIC + 8 shards | 12,000 | 0 | 0 | **100%** |
| s3 pit, OPTIMISTIC | 12,000 | 0 | 8,935 | **25.5%** (retries exhausted) |
| s8 concert, queue ON | n/a (not tallied) | 0 | 0 | **100%** |
| s8 concert, queue OFF | 223,182 | 0 | 1,261 | 99.44% |
| s4 / s10 movie | n/a | 0 | 0 | 100% |
| s7 queue surge | 20,008 | 0 | 0 | 100% |

`holds_error` (unexpected failure) was **0 in every run**. `http_req_failed` in the raw k6 output is high (up to 99%) only because k6 counts expected 409 and 4xx answers as failures; do not compare that field.

What was **not** measured in Phase 1, because a single process has nothing to fail over to: recovery time after a crash, behaviour with a dead dependency, rolling deploys. These are exactly the cases where microservices should show a *difference*; see MICROSERVICES_COMPARISON.md §4.

---

## 6. Resource usage at saturation

| Resource | Idle | Healthy peak (queue ON / movie) | Saturated (queue OFF / 8k reads) |
| :--- | ---: | ---: | ---: |
| App CPU (of 4 threads) | ~0.02 | 2.0 | 2.7–3.9 |
| Postgres CPU (of 4) | ~0.5 | 1.3–1.6 | **3.8–3.9** |
| Hikari pending (limit 30 active) | 0 | 0 | 1,471 |
| App JVM heap max | ~220 MB | 190–230 MB | 710 MB (limit 1,500 MB) |
| App RSS | 655 MB | n/a | n/a |
| Database size after run | | | 749 MB, 1.47 M `show_seats` rows |

**Conclusion: Postgres CPU is the first resource to saturate, the connection pool is the second symptom, and the app JVM is never the limit.** A distributed design that keeps one Postgres for every write will hit the same wall; the improvements must come from fewer DB round-trips (cache, Redis holds, sharding), not more app instances alone.

---

## 7. Correctness: invariants after every run

Checked by `GET /admin/invariants` after each k6 run (`run.sh`) and by `InvariantChecker` after every integration test.

| Invariant | Meaning | Result |
| :--- | :--- | :---: |
| `seat_sold_twice` | A seat in two confirmed bookings | 0 |
| `booked_seat_without_confirmed_hold` | Seat marked booked with no confirmed hold behind it | 0 |
| `confirmed_seat_not_booked` | Confirmed hold whose seat is not booked | 0 |
| `ga_counter_mismatch` | GA counter disagrees with hold records (oversell) | 0 |
| `paid_without_booking` | Money taken, nothing booked | 0 |
| `booking_without_payment` | Booking with no payment | 0 |

Result: **0 across all 34 load runs** (20 first-pass runs, 3 extra catalog ramps, 11 warm re-runs) and across all 58 tests.

---

## 8. Known weaknesses of this baseline (so nobody over-reads it)

1. **Single machine, shared cores and caches.** Absolute numbers are laptop numbers; only ratios between phases are meaningful. Noise is ±10–20%.
2. **Single run per cell.** No confidence intervals. Treat differences under ~20% as noise. For the final report, repeat each microservice scenario at least 3 times and report the median.
3. **Catalog has no cache,** so its ceiling is deliberately low. Do not credit microservices for a cache that the monolith could also have had; compare "monolith + cache" if that matters.
4. **Mock gateway** (150 ms) makes checkout latency dominated by the gateway; compare hold latency, not checkout latency.
5. **The s1 ramp's average rps is below the target** by construction (ramp from 100); the knee is read from latency, not from rps.
6. **Scenario 5 (strategy matrix)** has no separate script; it is covered by s2 and s3 run once per strategy.
