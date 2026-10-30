# Monolith vs Microservices: Filled Comparison Sheet

> The template is [phase1-baseline/MICROSERVICES_COMPARISON.md](phase1-baseline/MICROSERVICES_COMPARISON.md); this is the same sheet with the Phase 2 columns measured. Narrative and caveats: [PHASE2_RESULTS.md](PHASE2_RESULTS.md). **P1** = monolith (warm), **P2** = microservices (final pass). Δ = (P2 − P1) / P1, so for latency positive is slower and for throughput negative is slower. Single runs; treat differences under ~20% as noise.

## 2. Correctness gate

| Invariant | P1 | P2 |
| :--- | :---: | :---: |
| `seat_sold_twice` | 0 | 0 |
| `booked_seat_without_confirmed_hold` | 0 | 0 |
| `confirmed_seat_not_booked` | 0 | 0 |
| `ga_counter_mismatch` | 0 | 0 |
| `paid_without_booking` | 0 | 0 |
| `booking_without_payment` | 0 | 0 |
| `confirmed_hold_without_booking` (cross-service, new) | n/a | 0 once settled |
| `booking_without_confirmed_hold` (cross-service, new) | n/a | 0 |
| `sagas_stuck_in_charged_or_refunding` (new) | n/a | 0 once settled |
| Duplicate holds / double charges in the retry storm | 0 / 0 | 0 / 0 |
| Stampede winners (every run, 9 + 9) | 1 | 1 (all 9) |
| Automated tests passing | 58 / 58 | 82 / 82 (auth 6, catalog 15, reservation 42, booking 15, common 4) |

In a distributed system three invariants span services and are **eventual**: they can read non-zero for a few seconds after heavy load (up to 9,021 sagas were in flight right after the unprotected concert run in pass 2) and converge to 0.

## 3. Performance

### 3.1 Catalog reads (scenario 1)

| Metric | P1 | P2 | Δ |
| :--- | ---: | ---: | ---: |
| p99 at 3,000 req/s target (ms) | 3.9 | 55.7 | +1328% |
| p99 at 8,000 req/s target (ms) | 1088.6 | 1555.5 | +43% |
| Throughput at 8,000 target (req/s) | 4996.7 | 3466.8 | -31% |
| PostgreSQL CPU peak at 8,000 target (of 4) | 3.75 | 3.89 | +4% |
| Cache hit rate | n/a (no cache) | not measured | – |

The 8,000 req/s ramp reached 5,040, 2,997 and 3,467 req/s in the three passes: too noisy to rank. At 3,000 req/s the application tier was saturated, so the cache's benefit to PostgreSQL does not show up as lower latency.

### 3.2 Same-seat stampede (scenario 2), hold p99 in ms

| Buyers | Strategy | P1 | P2 | Δ |
| ---: | :--- | ---: | ---: | ---: |
| 100 | ATOMIC_CONDITIONAL | 45.3 | 126.6 | +179% |
| 500 | ATOMIC_CONDITIONAL | 212.6 | 667.0 | +214% |
| 1,000 | ATOMIC_CONDITIONAL | 454.7 | 903.7 | +99% |
| 100 | PESSIMISTIC | 84.8 | 100.0 | +18% |
| 500 | PESSIMISTIC | 311.7 | 551.9 | +77% |
| 1,000 | PESSIMISTIC | 781.7 | 836.7 | +7% |
| 100 | OPTIMISTIC | 67.3 | 85.1 | +26% |
| 500 | OPTIMISTIC | 175.5 | 291.1 | +66% |
| 1,000 | OPTIMISTIC | 331.9 | 562.7 | +70% |
| any | winners | 1 | 1 | – |

### 3.3 Pit flash sale (scenario 3): 10,000 tickets, 12,000 buyers

| Metric | P1 single counter | P1 8 shards | P2 single counter | P2 8 shards |
| :--- | ---: | ---: | ---: | ---: |
| Tickets sold (must be 10,000) | 10,000 | 10,000 | 10,000 | 10,000 |
| Throughput (req/s) | 503.0 | 1512.0 | 515.7 | 1383.6 |
| Hold p99 (ms) | 2001.6 | 855.1 | 2027.3 | 859.9 |
| PostgreSQL CPU peak | 1.72 | 2.8 | 2.06 | 2.72 |
| Oversold | 0 | 0 | 0 | 0 |

(P1 columns are the monolith's 2026-10-31 first-pass re-run of the same suite; the original baseline's figures are in the Phase 1 baseline document.)

### 3.4 Retry storm / idempotency (scenario 6)

| Metric | P1 warm | P2 | Δ |
| :--- | ---: | ---: | ---: |
| Requests per second | 1185.0 | 741.9 | -37% |
| http p99 (ms) | 384.7 | 1382.5 | +259% |
| Duplicate holds | 0 | 0 | – |
| Checkouts confirmed (of 3,000) | 3,000 | 3,000 | – |
| Double charges | 0 | 0 | – |

### 3.5 Waiting-room surge (scenario 7)

| Metric | P1 warm | P2 | Δ |
| :--- | ---: | ---: | ---: |
| Requests per second | 2824.6 | 2176.0 | -23% |
| http p50 / p99 (ms) | 4.2 / 61.5 | 75.3 / 801.0 | +1202% |
| Errors | 0 | 0 | – |

### 3.6 Movie traffic (scenarios 4 and 10)

| Metric | s4 P1 | s4 P2 | s10 P1 | s10 P2 |
| :--- | ---: | ---: | ---: | ---: |
| Requests per second | 248.7 | 233.0 | 1204.2 | 1095.7 |
| Hold p99 (ms) | 15.5 | 23.3 | 86.1 | 52.8 |
| http p99 (ms) | 240.8 | 824.3 | 254.4 | 741.5 |
| Holds created | 2,320 | 2,320 | 23,200 | 23,200 |
| PostgreSQL CPU peak | 1.44 | 3.11 | 3.69 | 3.69 |

### 3.7 Concert on-sale (scenarios 8 and 9)

| Metric | P1 queue ON | P2 queue ON | P1 queue OFF | P2 queue OFF |
| :--- | ---: | ---: | ---: | ---: |
| Requests per second | 1047.1 | 1023.6 | 1374.2 | 1229.4 |
| Hold p99 (ms) | 19.5 | 49.5 | 2121.4 | 2601.0 |
| Holds created | 15,152 | 14,550 | 29,054 | 29,019 |
| Pending for a connection, max | 0 | 7 | 1,471 | 1,476 |
| PostgreSQL CPU peak (of 4) | 1.63 | 2.91 | 3.94 | 3.94 |
| Invariants | all 0 | all 0 | all 0 | all 0 |

**Q4 answer:** the waiting room is still required. Without it hold p99 is 2601.0 ms (P1: 2121.4 ms), against 49.5 ms with it. The split did not absorb the on-sale on its own.

## 4. Availability and failure behaviour (new in Phase 2)

| Experiment | P1 (by design) | P2 measured | Data intact |
| :--- | :--- | :--- | :---: |
| Kill Redis | n/a | during the fault: reads 100%, holds 100%, checkouts 100% answered 200/201 | yes |
| Kill catalog-service | total outage | during the fault: reads 0%, holds 100%, checkouts 100% answered 200/201 | yes |
| Kill reservation-service | total outage | during the fault: reads 100%, holds 0%, checkouts 0% answered 200/201 | yes |
| Kill booking-service (graceful) | total outage | during the fault: reads 100%, holds 100%, checkouts 0% answered 200/201 | yes |
| `kill -9` booking-service | total outage, in-flight work lost | during the fault: reads 100%, holds 100%, checkouts 0% answered 200/201 | yes |
| Kill Kafka broker | n/a | during the fault: reads 100%, holds 100%, checkouts 8% answered 200/201 | yes |
| Kill PostgreSQL | total outage | during the fault: reads 100%, holds 0%, checkouts 0% answered 200/201 | yes |
| Kill the app instance (monolith) | total outage until restart (~30 s) | n/a | – |
| Network partition between services | n/a | **not run** | – |
| Rolling deploy under load | restart = downtime | **not run** (single instance per service) | – |
| Recover from a crash | app healthy within ~30 s of start | each service healthy within ~25 s; interrupted sagas finished by recovery | – |

## 5. Latency budget: what the split costs

From the control run of the failure harness (24 buyers, mock gateway set to 20 ms so the gateway does not dominate): browse p50 6 ms, hold p50 16 ms, **checkout p50 169 ms and p99 1256 ms**. With a 20 ms gateway, over 100 ms of every checkout is the saga's own machinery: the outbox poll (100 ms interval), two Kafka hops (`ConfirmHold` and its reply) and the verify call. The monolith did the equivalent in one local transaction. **The monolith was not measured with a 20 ms gateway, so no Δ is claimed**; the 100 ms outbox poll is the obvious first thing to tighten.

## 6. Cost and complexity

| Dimension | Phase 1 | Phase 2 |
| :--- | :--- | :--- |
| Processes to run | 1 app + 1 Postgres | 4 services + Postgres, Redis, Kafka, Kong |
| Containers | 1 (Postgres) | 4 (Postgres, Redis, Kafka, Kong) |
| Deployable artefacts | 1 jar | 4 jars + shared libraries (common, messaging, contracts) |
| Application tier CPU | 4 threads | the same 4 threads (shared by everything above) |
| Application heap | 1.5 GB | 1.5 GB total (auth 128 / catalog 384 / reservation 768 / booking 256 MB) |
| Databases | 1 | 4 on one server (logical isolation only) |
| Tests | 58 (unit, integration, concurrency) | 82 (the Phase 1 behaviours ported, plus cache, event, saga, projection and error-handling tests) + 8 failure experiments |
| Failure modes to reason about | DB down, app down | each of 4 services, Redis, Kafka, Postgres, Kong, and every pairwise interaction |
| New correctness machinery | none | transactional outbox, idempotent consumers, saga with persisted state, reconciler, recovery job, internal-API guard |

## 7. Summary

| Headline | Phase 1 | Phase 2 | Winner |
| :--- | ---: | ---: | :--- |
| Correctness (invariant violations) | 0 | 0 | tie |
| Friday-evening throughput (req/s) | 1204.2 | 1095.7 | monolith |
| Retry-storm throughput (req/s) | 1185.0 | 741.9 | monolith |
| Hot-seat hold p99, 1,000 buyers (ms) | 454.7 | 903.7 | monolith |
| Pit sales per second, sharded (req/s) | 1512.0 | 1383.6 | monolith |
| Concert hold p99 with queue (ms) | 19.5 | 49.5 | monolith |
| Browse works while checkout is down | no | yes (measured) | **microservices** |
| Holds and checkouts work while catalog is down | no | yes, 100% (measured) | **microservices** |
| Reads survive a cache outage | n/a (no cache) | yes, after the circuit breaker | **microservices** |
| Money survives a hard crash mid-checkout | in-flight work lost | yes (saga state is persisted) | **microservices** |
| Operational complexity | low | much higher | monolith |

**Bottom line:** on identical hardware the split costs throughput and tail latency and buys isolation and recoverability. The cost is mostly the distributed infrastructure sharing the same CPU, so the next experiment that matters is giving it its own.

