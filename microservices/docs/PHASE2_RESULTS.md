# Phase 2 Results: Microservices vs the Monolith

> **Run date:** 2026-10-31 · **Branch:** `ticket-forge-microservices` · **Reference:** the Phase 1 monolith numbers in [phase1-baseline/PHASE1_PERFORMANCE_BASELINE.md](phase1-baseline/PHASE1_PERFORMANCE_BASELINE.md) (warm re-run for the cold-sensitive scenarios s2/s6/s7, the original baseline for the rest).
> **Raw data:** [`phase2-results.json`](phase2-results.json) (final pass), the two earlier passes (`phase2-results-pass1-…`, `phase2-results-pass2-…`), and `../loadtest/results/` (k6 summaries, per-second samples, invariant checks, failure-experiment reports).

## 1. Verdict

| Question | Answer |
| :--- | :--- |
| Is it still correct? | **Yes.** All nine invariants read 0 after every one of the 21 scenarios (final pass), and after all 8 failure experiments. Zero double sales, zero double charges, zero lost bookings. |
| Is it as fast? | **No, not on the same compute.** With the whole application tier (4 services + Kong + Redis + Kafka) squeezed into the same 4 threads the monolith had, checkout-heavy traffic runs at roughly **9% lower throughput on the Friday scenario** (1095.7 vs 1204.2 req/s) and **37% lower on the retry storm** (741.9 vs 1185.0). The hot seat-claim path (stampede, hold p99) ranges from within noise to about **3× slower at the tail** (ATOMIC at 100 and 500 buyers), because every request now crosses Kong. |
| What did the split buy? | **Fault isolation**, measured: with the catalog down, holds and checkouts ran at 100% success; with Redis down, reads kept working; with Kafka down, browsing and holding were untouched and checkouts completed after it returned; killing booking with `kill -9` lost no money. The monolith cannot do any of these. See §5. |
| Where is the price paid? | Kong, Kafka and Redis together average about **0.4–0.7 cores of the 4-thread application tier** while busy (§4), the saga adds two Kafka hops and an outbox poll to every checkout, and cross-service invariants are **eventual**, not instantaneous. |
| What would change the speed result? | The test gave the microservices the *same* 4 threads as one process. Giving the application tier more cores, or Kong its own, is the obvious next experiment and was **not run** (the laptop has no spare cores: 4 app, 4 database, 4 load generator). |

## 2. How the numbers were produced (and the three passes)

Same machine, same pinned layout as Phase 1: application tier on CPUs `0,1,6,7`, PostgreSQL on `2,3,8,9`, k6 on `4,5,10,11`. The k6 scripts are the Phase 1 scripts, **unchanged**; only the target changed (Kong on :8080, which is where the monolith listened). The services are JVMs with a combined heap of 1.5 GB (auth 128 MB, catalog 384 MB, reservation 768 MB, booking 256 MB). A 90-second warm-up ran before measuring.

The suite was run three times, and the first two exposed real problems. They are kept because the story is the finding:

| Pass | What was wrong | Evidence | Fix |
| :--- | :--- | :--- | :--- |
| 1 | **Kong's default `worker_connections` (1024) was far too low** for thousands of concurrent clients | Kong logged **67,000** `worker_connections are not enough` errors; the Friday scenario showed 5,214 unexpected hold errors and 15,106 failed checkouts although the services logged no errors; 516 saga commands also had to be resent | `KONG_NGINX_EVENTS_WORKER_CONNECTIONS=16384`, 64k file descriptors, upstream keepalive pool |
| 2 | **The saga pipeline backed up under checkout-heavy load.** The outbox relay waited for each Kafka ack before sending the next record, and the saga listeners ran single-threaded on 3-partition topics | Right after the concert (queue-off) run ended, **9,021 sagas** were still waiting for their `ConfirmHold` reply (293 after the Friday run, and 3,655 confirmed holds without a booking yet). They all converged (invariants 0 once settled) but late, and client waits of 5 s expired into `202 PAYMENT_PENDING` | Batch-send outbox rows and wait once; listener concurrency 3 |
| 3 | Final | Invariants read 0 within 8 seconds of every run's end | — |

Comparison of the checkout-heavy scenarios across the passes (requests per second / http p99 in ms):

| Scenario | Monolith | Pass 1 (Kong limits) | Pass 2 (before saga tuning) | **Pass 3 (final)** |
| :--- | ---: | ---: | ---: | ---: |
| 4 Movie mix | 248.7 / 240.8 | 237.9 / 641.1 | 237.9 / 533.5 | **233.0 / 824.3** |
| 6 Retry storm | 1185.0 / 384.7 | 340.3 / 3236.2 | 358.0 / 2953.4 | **741.9 / 1382.5** |
| 8 Concert, queue ON | 1047.1 / 239.6 | 901.2 / 5215.1 | 967.4 / 5147.4 | **1023.6 / 424.6** |
| 9 Concert, queue OFF | 1374.2 / 2098.9 | 2634.0 / 5227.1 | 619.5 / 5728.8 | **1229.4 / 3093.5** |
| 10 Friday evening | 1204.2 / 254.4 | 898.9 / 5254.6 | 674.2 / 5256.1 | **1095.7 / 741.5** |

## 3. Performance, scenario by scenario (final pass)

`Δ` is microservices relative to the monolith: for throughput, negative is slower; for latency, positive is slower. Single runs on a shared laptop: treat differences under about 20% as noise (the monolith's own repeat runs differed by up to ±25% on the short stampede scenarios).

| Scenario | rps mono | rps ms | Δ | http p99 mono | ms | Δ | hold p99 mono | ms | holds ok mono | ms | PG cpu peak mono | ms |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 Catalog reads, 3,000 req/s target | 2032.8 | 2028.6 | -0% | 3.9 | 55.7 | +1328% | – | – | – | – | 1.22 | 3.54 |
| 1 Catalog reads, 8,000 req/s target | 4996.7 | 3466.8 | -31% | 1088.6 | 1555.5 | +43% | – | – | – | – | 3.75 | 3.89 |
| 2 Stampede ATOMIC, 100 buyers | 56.8 | 31.0 | -45% | 106.9 | 127.3 | +19% | 45.3 | 126.6 | 1 | 1 | 1.03 | 1.0 |
| 2 Stampede ATOMIC, 500 | 263.7 | 139.0 | -47% | 213.4 | 667.3 | +213% | 212.6 | 667.0 | 1 | 1 | 1.12 | 0.99 |
| 2 Stampede ATOMIC, 1,000 | 441.4 | 241.0 | -45% | 456.0 | 906.5 | +99% | 454.7 | 903.7 | 1 | 1 | 0.94 | 2.12 |
| 2 Stampede PESSIMISTIC, 100 | 60.3 | 35.5 | -41% | 88.8 | 105.4 | +19% | 84.8 | 100.0 | 1 | 1 | 0.89 | 0.97 |
| 2 Stampede PESSIMISTIC, 500 | 245.4 | 131.6 | -46% | 312.5 | 552.8 | +77% | 311.7 | 551.9 | 1 | 1 | 1.04 | 2.38 |
| 2 Stampede PESSIMISTIC, 1,000 | 372.5 | 265.7 | -29% | 782.0 | 837.1 | +7% | 781.7 | 836.7 | 1 | 1 | 1.1 | 1.01 |
| 2 Stampede OPTIMISTIC, 100 | 61.6 | 27.2 | -56% | 79.8 | 85.6 | +7% | 67.3 | 85.1 | 1 | 1 | 0.88 | 1.1 |
| 2 Stampede OPTIMISTIC, 500 | 251.0 | 146.3 | -42% | 175.5 | 291.1 | +66% | 175.5 | 291.1 | 1 | 1 | 1.11 | 2.29 |
| 2 Stampede OPTIMISTIC, 1,000 | 458.1 | 284.7 | -38% | 334.5 | 564.2 | +69% | 331.9 | 562.7 | 1 | 1 | 0.92 | 1.0 |
| 3 Pit sale ATOMIC, one counter | 707.8 | 515.7 | -27% | 1468.6 | 2030.0 | +38% | 1468.6 | 2027.3 | 10,000 | 10,000 | 1.86 | 2.06 |
| 3 Pit sale ATOMIC, 8 shards | 2074.8 | 1383.6 | -33% | 572.2 | 861.2 | +51% | 572.0 | 859.9 | 10,000 | 10,000 | 3.02 | 2.72 |
| 3 Pit sale PESSIMISTIC | 537.0 | 492.2 | -8% | 2463.6 | 1934.0 | -21% | 2463.6 | 1933.7 | 10,000 | 10,000 | 1.57 | 2.76 |
| 3 Pit sale OPTIMISTIC | 821.1 | 593.5 | -28% | 938.0 | 1331.0 | +42% | 938.1 | 1330.5 | 3,174 | 3,125 | 2.5 | 2.88 |
| 4 Movie mix (20 shows) | 248.7 | 233.0 | -6% | 240.8 | 824.3 | +242% | 15.5 | 23.3 | 2,320 | 2,320 | 1.44 | 3.11 |
| 6 Retry storm | 1185.0 | 741.9 | -37% | 384.7 | 1382.5 | +259% | – | – | – | – | 3.45 | 2.81 |
| 7 Waiting-room surge | 2824.6 | 2176.0 | -23% | 61.5 | 801.0 | +1202% | – | – | – | – | 2.1 | 3.51 |
| 8 Concert, queue ON | 1047.1 | 1023.6 | -2% | 239.6 | 424.6 | +77% | 19.5 | 49.5 | 15,152 | 14,550 | 1.63 | 2.91 |
| 9 Concert, queue OFF | 1374.2 | 1229.4 | -11% | 2098.9 | 3093.5 | +47% | 2121.4 | 2601.0 | 29,054 | 29,019 | 3.94 | 3.94 |
| 10 Friday evening | 1204.2 | 1095.7 | -9% | 254.4 | 741.5 | +191% | 86.1 | 52.8 | 23,200 | 23,200 | 3.69 | 3.69 |

Reading the table:

* **Correctness of outcomes is identical.** Every stampede has exactly one winner; the pit sells exactly 10,000 (OPTIMISTIC still sells only about 3,100: safe but not live on one hot row, the same finding as Phase 1); movie scenarios sell the same 23,200 holds.
* **Hot-row sharding still pays off** (pit sale, 8 shards: 515.7 to 1383.6 req/s, 2.7x), but both are slower than the monolith's figures. The claim path is the same SQL on the same database, so the difference is the extra hop through Kong plus the CPU taken by the infrastructure.
* **The waiting room still earns its keep.** Concert hold p99 2601.0 ms without it vs 49.5 ms with it. Without the queue the system stays correct, it just queues requests for connections (1,476 pending at peak).
* **Splitting the services did not rescue the unprotected on-sale.** Hold p99 without the queue was 2601.0 ms against the monolith's 2121.4 ms. (An earlier pass showed a better figure, 1719.8 ms, but in that pass the saga backlog was throttling checkouts and thus the load on the database, so it is not a fair number.) The waiting room remains the right defence.
* **Reads:** at 3,000 req/s the monolith answered with p99 3.9 ms; through Kong, Redis and the catalog service p99 was 55.7 ms. The application tier was saturated (see §4), so the cache cannot show its benefit when the CPU, not the database, is the limit. At the 8,000 req/s ramp the final pass reached 3466.8 req/s against the monolith's 4996.7; across the three passes this scenario gave 5,040, 2,997 and 3,467, so it is too noisy to rank. **The cache's real effect is on the database:** ask whether PostgreSQL stays out of the way during reads, not whether p99 improves.

## 4. Where the CPU went

Average CPU in cores over the seconds when the system was busy (final pass). The monolith had 4 threads for its whole application tier; here the same 4 threads hold everything in the first two columns.

| Scenario | Services (JVMs) | Kong + Kafka + Redis | **Application tier total** | PostgreSQL |
| :--- | ---: | ---: | ---: | ---: |
| 1 Catalog reads 3k | 1.47 | 0.67 | **2.14** | 1.28 |
| 4 Movie mix | 0.81 | 0.36 | **1.17** | 0.71 |
| 6 Retry storm | 1.97 | 0.48 | **2.45** | 1.68 |
| 8 Concert, queue ON | 1.36 | 0.52 | **1.88** | 1.01 |
| 9 Concert, queue OFF | 2.29 | 0.53 | **2.82** | 3.08 |
| 10 Friday evening | 1.80 | 0.65 | **2.45** | 1.97 |

The infrastructure's share is the measurable *cost of being distributed*. Because the application tier is at or near its 4-thread limit in the heavy scenarios, that cost converts directly into lost throughput.

## 5. Failure experiments (what the monolith could not do)

`scripts/chaos.py` runs 24 concurrent buyers (browse, hold, check out) through Kong for 45 seconds, injects one fault at 15 s, heals it at 30 s, waits for the system to settle, then checks the nine invariants and three money checks. Columns are the share of requests that were answered 200/201 in each phase. The "after" phase includes the time a restarted JVM needs to boot (about 25 s), so for the service-kill experiments it shows the restart, not a lingering problem.

| Fault | Reads before / during / after | Holds before / during / after | Checkouts before / during / after | Data intact? |
| :--- | :--- | :--- | :--- | :---: |
| none (control) | 100% / 100% / 100% | 100% / 100% / 100% | 100% / 100% / 100% | **yes** |
| Redis stopped | 100% / 100% / 100% | 100% / 100% / 100% | 100% / 100% / 100% | **yes** |
| catalog-service stopped | 100% / 0% / 67% | 100% / 100% / 100% | 100% / 100% / 100% | **yes** |
| reservation-service stopped | 100% / 100% / 100% | 100% / 0% / 9% | 100% / 0% / 100% | **yes** |
| booking-service stopped | 100% / 100% / 100% | 100% / 100% / 100% | 100% / 0% / 34% | **yes** |
| booking-service `kill -9` | 100% / 100% / 100% | 100% / 100% / 100% | 100% / 0% / 0% | **yes** |
| Kafka broker stopped | 100% / 100% / 100% | 100% / 100% / 100% | 100% / 8% / 97% | **yes** |
| PostgreSQL stopped | 100% / 100% / 96% | 100% / 0% / 96% | 100% / 0% / 100% | **yes** |

What each one showed:

* **Redis down:** every read still succeeded (cache falls through to PostgreSQL). Before the circuit breaker, median read latency rose from 2 ms to **403 ms** because each read paid the 200 ms client timeout twice (get, then put); after, it stayed at about 5 ms. The first run is the finding, the second the fix.
* **Catalog down:** browsing returned 502, but **holds and checkouts stayed at 100%**. In the monolith one component down means everything down.
* **Reservation down:** browsing worked, holds failed (502) until it restarted, and no checkout charged anyone for a hold it could not verify (the saga **fails closed** before charging). The 24 checkouts caught mid-flight returned `202 PAYMENT_PENDING` and completed after the restart.
* **Booking down (graceful and `kill -9`):** reads and holds were unaffected. Checkouts failed until the restart; those interrupted mid-saga were finished by the recovery job (state is in the `sagas` table). No payment was lost or doubled.
* **Kafka down:** reads and holds unaffected. Checkouts took the money and then **waited** (the confirm command sits in the outbox); buyers got `202` after 5 s, and every saga completed once Kafka returned. Nothing was dropped.
* **PostgreSQL down:** everything fails closed with 503s and recovers when the database returns. This run also found a bug: three checkouts got a raw **500** from a connection that died during commit. The error handler now maps that class of failure to 503 (with tests); the run above predates that fix.

## 6. Defects found by this phase (and fixed)

| # | Found by | Defect | Fix |
| :--- | :--- | :--- | :--- |
| 1 | Catalog tests | The shared idempotency cleanup job ran in every service and failed in those without its table | Idempotency is opt-in per service |
| 2 | Reservation tests | A text-block SQL concatenation produced `FROM showsWHERE`; the waiting room's scheduled tick swallowed the error and silently stopped admitting | Fixed; the test that caught it is kept |
| 3 | Design review of the saga participant | A caught exception inside one transaction marks it rollback-only, so the "not confirmed" reply could never be committed | Two transactions: confirm-and-reply, or (on failure) reply alone |
| 4 | Benchmark pass 1 | Kong default connection limit dropped requests | Raised and documented |
| 5 | Benchmark pass 2 | Outbox relay was sequential per record; saga listeners single-threaded | Batched sends, concurrency 3 |
| 6 | Failure experiment (Redis) | Every read paid the Redis timeout twice while Redis was down | Circuit breaker (5 s cooldown), tested |
| 7 | Failure experiment (PostgreSQL) | A connection dying during commit returned a raw 500 | Mapped to 503, with tests that other SQL errors are *not* disguised |
| 8 | Verification tooling | The invariant check once failed because a large database's parallel query exceeded the container's 64 MB `/dev/shm` | `shm_size: 256m` |
| 9 | Chaos harness | Seeded random keys reused idempotency keys across runs; the service correctly refused them | UUID keys |

## 7. Honest limitations

* **One run per cell** on a shared laptop (host load average was high). Differences under about 20% are noise; the read ramp at 8,000 req/s varied between 3.0k and 5.0k req/s across passes.
* **Strictest-fairness setup.** Same 4 threads for services plus Kong, Redis and Kafka. This is the right question for "what does distribution cost" but understates what microservices would do given more hardware. Not run: more app cores, Kong on its own cores, multiple instances per service.
* **One PostgreSQL server, four databases.** Logical isolation, shared CPU and connection limit. True per-service servers would not fit this machine.
* **One Kafka broker, one Redis.** Failure experiments cover their loss, not replication or leader election (that is Phase 3's custom mini-Kafka with Raft).
* **No Kubernetes, no cloud.** Phase 5.
* **Cross-service invariants are eventual.** The checker tolerates in-flight sagas (a 5 to 30 second settle window); the monolith's were instantaneous.

## 8. Reproduce

```bash
cd microservices
docker compose -f deploy/docker-compose.yml up -d && ./mvnw -q package -DskipTests && scripts/run-stack.sh
scripts/smoke.sh                                   # end-to-end check through Kong
loadtest/run.sh s10-movie-friday s4-movie-mix.js PROFILE=friday
scripts/chaos.py redis --seconds 45                # any of: baseline redis catalog reservation booking booking-kill9 kafka postgres
python3 scripts/verify-invariants.py              # nine invariants, all must be 0
```
