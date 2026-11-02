y# Phase 1 Baseline: Where the Monolith Breaks

TicketForge Phase 1 is one Spring Boot process and one Postgres. This report records what it sustains, where it saturates, and what broke while finding out. Every later phase is compared against these numbers.

**Headline:** across every scenario (about 1.2 million requests, including 1,000 buyers fighting over one seat and 12,000 buyers fighting over 10,000 pit tickets), **no seat or ticket was ever sold twice and the GA counters never drifted from the hold records**. All six database invariants read 0 after every run. Throughput and latency, however, hit hard limits early, and the reasons are specific and fixable.

---

## 1. Test bed

Everything ran on one laptop, so absolute numbers are modest. The comparisons between strategies are fair because each run used identical resources.

| Component | Resources | Notes |
| :--- | :--- | :--- |
| App | 2 physical cores / 4 threads (`taskset 0,1,6,7`), 1.5 GB heap | Java 22 runtime, Spring Boot 4.1, virtual threads, Hikari pool **30** |
| Postgres 16 | 2 physical cores / 4 threads (`cpuset 2,3,8,9`), 2 GB | `lock_timeout 3s`, `statement_timeout 10s` |
| k6 1.8.2 | 2 physical cores / 4 threads (`cpuset 4,5,10,11`) | JWTs minted by k6, so bcrypt is not measured |
| Host | AMD Ryzen 7 7445HS (6C/12T), 14 GB, loopback network | Shared L3 cache and thermals: expect ±10% run-to-run |

Mock payment gateway: 150 ms ± 100 ms latency, 5–10% declines. Defaults: lock strategy `ATOMIC_CONDITIONAL`, gap rule on.

---

## 2. Results at a glance

| # | Scenario | Result |
| :--- | :--- | :--- |
| 1 | Catalog reads | Clean to ~3,000 req/s (p99 3.9 ms). **Knee ≈ 5,000 req/s**: app and DB CPU both saturate, 1,964 requests queue for a connection, p99 1.08 s. No caching yet, so every read hits Postgres. |
| 2 | Same-seat stampede | **Exactly 1 winner** in all 9 runs (3 strategies × 100/500/1000 buyers). Losers get a clean 409 listing the seat. |
| 3 | Pit flash sale (10k tickets, 12k buyers) | Single counter row: 447–590 sales/s. **8 shards: 1,728/s (2.9×)**. OPTIMISTIC sold only 3,174 of 10,000. |
| 4 | Movie mix (20 shows) | All 4,640 seats sold; hold p99 15.5 ms; 10% declined payments handled. |
| 6 | Retry storm (every request sent 3× at once) | 3,000 buyers, 18,000 requests: **0 duplicate holds, 0 double charges**, 16,082 replays served. |
| 7 | Waiting-room surge | 20,000 joins in ~6 s, p99 17 ms. All 20,002 admitted strictly in join order (**0 skipped**), at exactly the configured 200/s. |
| 8 | Concert on-sale, **queue on** (1,500 fans) | Hold p99 **19 ms**, DB CPU avg 0.65 cores, 0 requests waiting for the pool. 15,152 holds; 4,317 abandoned holds expired and were resold. |
| 9 | Concert on-sale, **queue off** | Hold p99 **2.12 s**, DB CPU pinned at 3.94/4, up to 1,471 requests waiting for a connection. 29,054 holds; stadium nearly sold out. |
| 10 | Friday-evening movies (200 shows, 1,500 users) | **All 46,400 seats sold in 3 minutes**, hold p99 86 ms, pool wait max 56. |

---

## 3. Lock strategies under contention

### 3.1 Same-seat stampede (scenario 2): hold latency

| Buyers | PESSIMISTIC median / p99 | OPTIMISTIC median / p99 | ATOMIC_CONDITIONAL median / p99 |
| ---: | :--- | :--- | :--- |
| 100 | 118 / 167 ms | 37 / 76 ms | **31 / 47 ms** |
| 500 | 381 / 510 ms | 178 / 245 ms | **126 / 228 ms** |
| 1,000 | 485 / 715 ms | 267 / 414 ms | **242 / 399 ms** |

All strategies are correct: one winner every time. They differ in how long the losers wait to hear "no":

- **ATOMIC_CONDITIONAL** is fastest. The availability check *is* the `UPDATE`'s `WHERE` clause, so a loser blocks briefly on the row lock, re-checks against the winner's committed row, and gets 0 rows back.
- **PESSIMISTIC** is about 1.8× slower at p99. With the gap rule on, it locks the **whole row** (all 50 seats) to make the rule race-free, so every buyer in that row queues behind everyone else.
- **OPTIMISTIC** is close to atomic for a single seat, because losers usually just read "taken" and return without ever writing.

### 3.2 Pit flash sale (scenario 3): one hot counter row

| Strategy | Sold | Sales/s | Hold median / p99 | Turned away while tickets remained |
| :--- | ---: | ---: | :--- | ---: |
| PESSIMISTIC | 10,000 | 447 | 828 ms / 2.46 s | 241 (503, lock or pool timeout) |
| OPTIMISTIC | **3,174** | 217 | 580 ms / 938 ms | **8,826** (retries exhausted) |
| ATOMIC_CONDITIONAL | 10,000 | 590 | 743 ms / 1.46 s | 0 |
| ATOMIC + **8 shards** | 10,000 | **1,728** | **180 ms / 572 ms** | 0 |

**Finding: optimistic locking is safe but not live on a hot row.** Each of 500 concurrent buyers reads version *v*, and all but one fail the `WHERE version = v` write. After 3 attempts the buyer is told to come back later, while 6,826 tickets were still unsold. The counter never oversold (`held` always equalled the confirmed hold items), but the sale failed. The same effect appeared in the 200-thread conformance test (45 of 50 sold).

**Finding: the single counter row serializes everyone.** Splitting the 10,000-ticket pit into 8 rows of 1,250, each buyer starting at a random shard, nearly tripled throughput and cut p99 by 2.5×. The cost is fragmentation. A request never spans shards, so with multi-ticket requests the last few tickets can be stranded in shards that each hold too few. With one ticket per request everything sells.

---

## 4. What broke, and the fixes

Load testing earned its keep: three real defects were invisible to the 58 functional tests.

### 4.1 Total outage: a misplanned query pinned the whole pool

The first 100-user stampede **took the service down**: 0 winners, 60 s timeouts, even admin login failing. `pg_stat_activity` showed all 30 pooled connections *active* (not waiting on locks), each running the gap rule's "load the whole row" query.

**Root cause:** every new show bulk-inserts 50,000 `show_seats` rows. Until autovacuum re-analyzes the table, Postgres's statistics say that `show_id` has about 1 row. The planner therefore chose a nested loop for the row semi-join, which on the real data compares 50,000 × 50,000 rows. Thirty of these held the entire pool, so the sweeper, the waiting room and logins all starved.

**Fixes:**
1. The query now runs in two index-exact steps: primary key lookup, then `(show_id, section, row_label)` equality. It can't be misplanned whatever the statistics say.
2. `ANALYZE` runs right after materializing a show, which is standard after any bulk load.
3. `statement_timeout = 10s` as a safety net: no single statement can hold a pooled connection forever.

**Lesson:** a shared connection pool turns one slow query shape into a total outage. That's the motivation for bulkheads (separate pools per workload) and for Phase 3A's adaptive concurrency limiting.

### 4.2 Pool timeouts surfaced as 500s instead of 503s (two paths)

Under the no-queue concert load, 6,790 requests got **500**s. The idempotency filter runs *before* Spring MVC, so its pool timeouts never reached the `@RestControllerAdvice`. Separately, a pool timeout while *opening a transaction* surfaces as `CannotCreateTransactionException`, which isn't a `DataAccessException` and slipped past the handler (1 case in the PESSIMISTIC sell-out).

**Fix:** both now return **503 + `Retry-After`**. A 500 tells a client (or a load balancer) the server is broken; a 503 tells it to back off. Under overload that difference decides whether retries make things worse.

### 4.3 Token for a missing user became a foreign-key 500

The harness ran out of seeded users, which exposed that a validly signed JWT for a user who no longer exists (a deleted account) produced an FK-violation 500. It now returns **401 UNKNOWN_USER**.

---

## 5. Where the monolith saturates

| Load shape | First thing to run out |
| :--- | :--- |
| Uncached reads (scenario 1) | **CPU on both tiers at once** (app 3.95/4, DB 3.75/4) around 5k req/s. Every request is a DB round trip; the 30-connection pool then queues up to 1,964 requests. |
| Write stampede on hot rows (2, 3, 9) | **Row-lock serialization in Postgres.** Throughput on one row is bounded by commit latency, not CPU; DB CPU reaches 3.94/4 only when there are also thousands of distinct hot rows (scenario 9). |
| Unthrottled on-sale (9) | **The connection pool.** Up to 1,471 requests waiting for a connection; latency rises from 19 ms to 2.1 s p99. |
| Throttled on-sale (8) | Nothing: the waiting room kept the DB at about 16% CPU. The bottleneck is the **fixed 200/s admission rate**, which is too conservative. The system had headroom to admit more. |

The scenario 8 vs. 9 comparison is the key result. The waiting room turns overload into a queue the user can see, with 19 ms holds. But a **fixed** admission rate either wastes capacity (as here) or overloads the backend when set too high. Admission should follow measured backend latency, which is what Phase 3A's Little's-Law adaptive limiter is for.

---

## 6. Correctness evidence

- **Concurrency conformance suite:** 200 threads released through one latch, per strategy. Checks: one winner per seat, overlapping multi-seat claims never share a seat, the GA counter equals the winners, best-available hands out distinct seats. The database is checked directly, not just the responses.
- **Invariant checker:** six SQL invariants, each a single statement so it sees one consistent snapshot even under load. They run every 10 s (exported as a gauge that must stay 0), **after every integration test**, and after every load run. A dedicated test corrupts data on purpose to prove the checker catches it.
- **Edge cases with tests:**
  - a seat lazily taken from an expired hold is never freed by that hold's sweep
  - a payment that lands after hold expiry is refunded
  - a lost gateway response is resolved by the reconciler
  - 10 tabs checking out one hold produce 1 charge
  - a rolled-back hold is never broadcast to viewers

---

## 7. What Phase 2+ should beat

| Metric (this hardware) | Phase 1 baseline |
| :--- | :--- |
| Catalog read knee | ~5,000 req/s |
| Single-seat stampede, 1,000 buyers, p99 | 399 ms (ATOMIC) |
| Hot GA row sell-out rate | 590/s (single row), 1,728/s (8 shards) |
| Unthrottled concert on-sale, hold p99 | 2.12 s with pool saturation |
| Throttled concert on-sale, hold p99 | 19 ms at 200 admits/s |
| Correctness | 0 invariant violations |

**Reproduce:** see [`loadtest/README.md`](../loadtest/README.md). Raw k6 summaries are in [`docs/baseline-results/`](baseline-results/).
