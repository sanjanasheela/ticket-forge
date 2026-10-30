# Monolith vs Microservices: Comparison Sheet

> **How to use.** The **Phase 1** columns are filled from [PHASE1_PERFORMANCE_BASELINE.md](PHASE1_PERFORMANCE_BASELINE.md) (warm pass unless noted). The **Phase 2** columns are empty on purpose: run the same scenario against the microservices with the same test bed (see [TESTING.md §3.5](TESTING.md)), fill the cell, then compute the change. Do not edit the Phase 1 values; if the baseline is re-run, add a new dated column instead.
>
> **Change** = (Phase 2 − Phase 1) / Phase 1. For latency, negative is better. For throughput, positive is better.
>
> **Honesty rule.** If Phase 2 is worse, say so and explain why. A microservice split adds network hops, serialization and failure modes, so some rows are *expected* to get worse. The point is to know the price and what it buys.

---

## 1. Questions this comparison must answer

| # | Question | Where answered |
| :--- | :--- | :--- |
| Q1 | Does the system stay correct (0 invariant violations) when split into services? | §2 |
| Q2 | What happens to read throughput and latency (Redis cache, replicas)? | §3.1 |
| Q3 | What happens to hold throughput on a hot seat and on the GA pit? | §3.2–3.3 |
| Q4 | Does the concert on-sale still need the waiting room, or does the new design cope without it? | §3.7 |
| Q5 | What does the split cost in latency (extra network hops, Kafka)? | §3.4, §5 |
| Q6 | Is the system more available when a component dies? (the monolith has no answer) | §4 |
| Q7 | What does it cost to run and to operate? | §6 |

---

## 2. Correctness gate (must be all zero to count any other number)

| Invariant | Phase 1 | Phase 2 |
| :--- | :---: | :---: |
| `seat_sold_twice` | 0 | |
| `booked_seat_without_confirmed_hold` | 0 | |
| `confirmed_seat_not_booked` | 0 | |
| `ga_counter_mismatch` | 0 | |
| `paid_without_booking` | 0 | |
| `booking_without_payment` | 0 | |
| Duplicate holds in retry storm | 0 | |
| Double charges in retry storm | 0 | |
| Stampede winners (every run) | exactly 1 | |
| Automated tests passing | 58 / 58 | |

In a distributed system, the invariants that span services (`paid_without_booking`, `booking_without_payment`, `confirmed_seat_not_booked`) are the ones most likely to break first. Check them after chaos tests too.

---

## 3. Performance, scenario by scenario

Columns: **P1** = Phase 1 monolith (warm pass), **P2** = microservices, **Δ** = change.

### 3.1 Catalog reads (scenario 1)

| Metric | P1 | P2 | Δ |
| :--- | ---: | ---: | ---: |
| p99 at 3,000 req/s target | 9.9 ms | | |
| p99 at 8,000 req/s target | 1,790 ms | | |
| Throughput reached at 8,000 req/s target | 4,164 req/s | | |
| Rate at which p99 first exceeds 100 ms | not measured (clean at 3,000: p99 9.9 ms; saturated by 4,200) | | |
| Postgres CPU peak at 8,000 target (of 4 cores) | 3.85 | | |
| Cache hit rate | n/a (no cache) | | |
| Original baseline reached | 4,997 req/s | | |

### 3.2 Same-seat stampede (scenario 2), hold p99 in ms

| Buyers | Strategy | P1 | P2 | Δ |
| ---: | :--- | ---: | ---: | ---: |
| 100 | ATOMIC_CONDITIONAL | 45 | | |
| 500 | ATOMIC_CONDITIONAL | 213 | | |
| 1,000 | ATOMIC_CONDITIONAL | 455 | | |
| 100 | PESSIMISTIC | 85 | | |
| 500 | PESSIMISTIC | 312 | | |
| 1,000 | PESSIMISTIC | 782 | | |
| 100 | OPTIMISTIC | 67 | | |
| 500 | OPTIMISTIC | 176 | | |
| 1,000 | OPTIMISTIC | 332 | | |
| any | winners | 1 | | |

If the Phase 2 hold uses a Redis lock or Lua script, add a row for it; the three Phase 1 strategies remain the reference.

### 3.3 Pit flash sale (scenario 3): 10,000 tickets, 12,000 buyers

| Metric | P1 single counter | P1 8 shards | P2 | Δ vs best P1 |
| :--- | ---: | ---: | ---: | ---: |
| Tickets sold (must be 10,000) | 10,000 | 10,000 | | |
| Throughput (req/s) | 503 | 1,512 | | |
| Hold p50 | 979 ms | 206 ms | | |
| Hold p99 | 2,001 ms | 855 ms | | |
| 503 overloaded | 6 | 0 | | |
| Postgres CPU peak | 1.72 | 2.80 | | |
| Oversold | 0 | 0 | | |

### 3.4 Retry storm / idempotency (scenario 6)

| Metric | P1 warm | P2 | Δ |
| :--- | ---: | ---: | ---: |
| Requests | 18,006 | | |
| Requests per second | 1,185 | | |
| http p99 | 385 ms | | |
| Duplicate holds | 0 | | |
| Checkouts confirmed (of 3,000) | 3,000 | | |
| Double charges | 0 | | |

### 3.5 Waiting-room surge (scenario 7)

| Metric | P1 warm | P2 | Δ |
| :--- | ---: | ---: | ---: |
| Joins in ~5 s | 20,008 | | |
| Requests per second | 2,825 | | |
| p50 / p99 | 4.2 / 61.5 ms | | |
| FIFO violations | 0 | | |
| Admission rate | 200/s exactly | | |

### 3.6 Movie traffic (scenarios 4 and 10)

| Metric | s4 P1 | s4 P2 | s10 P1 | s10 P2 |
| :--- | ---: | ---: | ---: | ---: |
| Requests per second | 226 | | 1,188 | |
| Hold p99 | 22 ms | | 34 ms | |
| http p99 | 243 ms | | 257 ms | |
| Holds created | 2,320 | | 23,200 | |
| Checkouts confirmed | 1,664 | | 16,742 | |
| Pool / connection waits max | 0 | | 693 | |
| Postgres CPU peak | 1.34 | | 3.09 | |

### 3.7 Concert on-sale (scenarios 8 and 9)

| Metric | P1 queue ON | P2 queue ON | P1 queue OFF | P2 queue OFF |
| :--- | ---: | ---: | ---: | ---: |
| Requests per second | 1,065 | | 1,251 | |
| Hold p50 | 1.2 ms | | 316 ms | |
| Hold p99 | 22.4 ms | | 2,754 ms | |
| 503 overloaded | 0 | | 1,261 | |
| Holds created | 15,407 | | 29,038 | |
| Checkouts confirmed | 10,930 | | 20,712 | |
| Pool / connection waits max | 0 | | 1,471 | |
| Postgres CPU peak (of 4) | 1.59 | | 3.82 | |
| Invariants | all 0 | | all 0 | |

**Q4 hinge:** if P2 queue-OFF p99 gets close to P1 queue-ON p99, the new design absorbs the on-sale by itself. If not, the waiting room is still required.

---

## 4. Availability and failure behaviour (new in Phase 2)

The monolith had no way to run these experiments (one process, one database: if either dies, everything is down). Record Phase 1 as the theoretical answer and measure Phase 2.

| Experiment | Phase 1 (by design) | Phase 2 measured | Pass criteria |
| :--- | :--- | :--- | :--- |
| Kill the app instance during a stampede | Total outage until restart; in-flight holds lost | | Another instance serves; no double-sold seat |
| Kill the payment service during checkout | Not separable | | Holds survive, checkout retries, no double charge, refund if late |
| Kill Redis | n/a | | Falls back or fails safely; no oversell |
| Kill Kafka broker | n/a | | Events buffered or retried; no lost booking |
| Kill Postgres primary | Total outage | | Fails closed (no wrong data) and recovers |
| Network partition between inventory and booking | n/a | | Saga compensates; invariants hold afterwards |
| Rolling deploy under load | Restart = downtime | | Zero failed requests |
| Time to recover from a crash (seconds) | App was healthy within 30 s of start (not timed precisely) | | |
| Availability during on-sale (503 rate) | 99.44% (queue OFF), 100% (queue ON) | | |

---

## 5. Latency budget: what the split costs

Fill this from distributed traces. In the monolith, a hold is one HTTP call and one DB transaction.

| Step | P1 | P2 |
| :--- | ---: | ---: |
| Gateway → service hop | 0 (same process) | |
| Hold: DB or Redis claim | included in hold p50 (28 ms at 100 buyers) | |
| Event publish (Kafka) | 0 (in-process `AFTER_COMMIT`) | |
| Seat-map update to viewers (SSE) | in-process | |
| Hold → pay → confirm end to end (mock gateway 150 ms ± 100) | Not summarised; dominated by the gateway | |

---

## 6. Cost and complexity

| Dimension | Phase 1 | Phase 2 |
| :--- | :--- | :--- |
| Processes to run | 1 app + 1 Postgres | |
| Containers for the full stack | 2 (+ Prometheus, Grafana optional) | |
| CPU reserved for the test bed | 4 threads app + 4 threads DB | |
| Peak app memory | JVM heap 710 MB max, RSS ~655 MB idle | |
| Peak total memory | app + 2 GB Postgres | |
| Lines of application code | (count with `cloc src/main`) | |
| Deployable artefacts | 1 jar | |
| Time from clone to running system | ~1 min (build) + start | |
| Failure modes to reason about | DB down, app down | |
| Tests | 58 (unit, integration, concurrency) | |
| Distributed-only test types | none | chaos, contract, saga |

---

## 7. Summary table (fill last)

| Headline | Phase 1 | Phase 2 | Winner | Why |
| :--- | ---: | ---: | :--- | :--- |
| Max clean read rate | ≥3,000 req/s (saturated by ~4,200) | | | |
| Hot-seat hold p99 (1,000 buyers) | 455 ms | | | |
| Pit sales per second | 1,512 (sharded) | | | |
| Concert on-sale hold p99, no queue | 2,754 ms | | | |
| Concert on-sale hold p99, queue | 22.4 ms | | | |
| Availability during a component failure | 0% (single point of failure) | | | |
| Invariant violations | 0 | | | |
| Operational complexity | Low | | | |
