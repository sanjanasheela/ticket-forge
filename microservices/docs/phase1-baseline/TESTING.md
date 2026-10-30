# Testing Guide: automated tests and load tests

How to run every test in the monolith, what each one proves, and the problems to expect. Numbers from the last full run are in [PHASE1_PERFORMANCE_BASELINE.md](PHASE1_PERFORMANCE_BASELINE.md).

**Last full run: 2026-10-31. 58 of 58 automated tests passed (0 failures, 0 errors, 0 skipped). 34 load runs completed with 0 invariant violations.**

---

## 1. Layers of testing

| Layer | Tool | Needs | What it proves | Time |
| :--- | :--- | :--- | :--- | :--- |
| Unit + integration + concurrency | JUnit 5, Spring Boot Test, Testcontainers | Docker (Testcontainers starts its own Postgres) | Business rules, SQL, locking, API behaviour, module boundaries | ~1 min |
| Load / stress | k6 in Docker, `loadtest/run.sh` | Running app on :8080, Postgres container `tf-pg` | Throughput, latency, saturation point, and that invariants hold under pressure | ~25 min for everything |
| Invariant verification | SQL, `GET /admin/invariants`, `scripts/verify-invariants.sql` | A database | Six "this must never happen" conditions all read 0 | seconds |

Every integration test also runs the invariant checker afterwards, so a test cannot pass while leaving the data inconsistent.

---

## 2. Automated tests (58)

```bash
cd ticketforge
./mvnw verify
```

Per-class results from the last run (`target/surefire-reports/`):

| Test class | Tests | Module | What it checks |
| :--- | ---: | :--- | :--- |
| `ApplicationSmokeTests` | 2 | app | Starts against a real Postgres and reports healthy; exposes Prometheus metrics |
| `ModularityTests` | 1 | architecture | `ApplicationModules.verify()`: modules only talk through public interfaces, no cycles. This is the guard that keeps Phase 2 service extraction possible |
| `AuthIntegrationTests` | 6 | auth | Signup then login, duplicate email rejected; wrong password is 401; refresh rotation, and reuse revokes every session; logout revokes the refresh token; admin endpoints need the admin role; a queue token cannot be used as a login |
| `CatalogIntegrationTests` | 4 | catalog | Venue layout expands rows, aisles and tiers; GA tier without capacity rejected; concert vs movie show defaults (hold TTL); every tier must be priced |
| `ClaimConformanceTests` | 13 | inventory | **The core concurrency suite.** 4 tests run once per lock strategy (3 strategies = 12): same-seat stampede has exactly one winner; overlapping multi-seat claims never share a seat; GA counter never oversells; best-available gives every winner different seats. Plus 1 test that the sharded GA counter sells exactly its capacity |
| `GapRuleTests` | 7 | inventory | No single-seat gaps: middle of an empty row is fine, leaving one seat at the edge or between two taken seats is rejected, filling an existing gap is allowed, old orphans do not block others, aisles split a row. Pure logic, no database |
| `ReservationIntegrationTests` | 9 | reservation | Hold over HTTP prices seats and shows in the seat map; gap rule and max seats per hold enforced; one active concert hold per user; own expired-but-unswept hold does not block the next; queue-enabled show requires a valid access token; release returns seats and GA units; sweeper expires holds and returns GA capacity; sweeping never frees a seat someone else took |
| `CheckoutIntegrationTests` | 9 | booking, payment | Hold, pay, confirm books the seats; checkout requires an Idempotency-Key; same key replays and charges once; a key reused for a different request is rejected; retried hold request returns the same hold; declined payment keeps the hold and can be retried; lost gateway response is resolved by the reconciler; payment landing after expiry is refunded; two tabs checking out one hold charge once |
| `WaitingRoomIntegrationTests` | 3 | waitingroom | Admits strictly in join order and the token unlocks holds; a show without a waiting room rejects joining; position is streamed over SSE |
| `SeatStreamIntegrationTests` | 2 | realtime | Viewers see holds, releases and GA counts live; rolled-back holds are never broadcast (events fire after commit only) |
| `InvariantCheckerTests` | 1 | verification | The checker detects a deliberately corrupted state (a booked seat without a confirmed hold, and GA counter drift) |
| `ApiErrorHandlerTests` | 1 | shared | A pool timeout while opening a transaction becomes a 503, not a 500 |
| **Total** | **58** | | |

### Running part of the suite

```bash
./mvnw test -Dtest=ClaimConformanceTests        # concurrency only
./mvnw test -Dtest='*IntegrationTests'          # all DB-backed tests
./mvnw test -Dtest=ModularityTests              # architecture rule only
```

### Gotchas seen on this machine

| Symptom | Meaning | Action |
| :--- | :--- | :--- |
| `[ERROR] Surefire is going to kill self fork JVM. The exit has elapsed 30 seconds after System.exit(0)` | The forked test JVM did not exit within 30 s of `System.exit(0)`, so Surefire killed it. Cause not investigated (a lingering non-daemon thread is the usual suspect). All 58 reports had been written with 0 failures; Maven's own exit code was not captured | Ignore for now; investigate if it ever hides a real failure |
| Huge JSON log output | Structured ECS logs are on in tests | Use `-q` and read `target/surefire-reports/*.txt` instead |
| `docker compose` is "unknown command" | The Compose v2 plugin is not installed | `sudo apt install docker-compose-v2`, or start Postgres with `docker run` as in §3.1 |
| `mvnw ... \| tail` shows `EXIT=0` | That is `tail`'s exit code, not Maven's | Read the surefire XML, or use `set -o pipefail` |

---

## 3. Load tests

### 3.1 One-time setup (without the Compose plugin)

```bash
cd ticketforge
docker run -d --name tf-pg --cpuset-cpus 2,3,8,9 --memory 2g \
  -e POSTGRES_DB=ticketforge -e POSTGRES_USER=ticketforge -e POSTGRES_PASSWORD=ticketforge \
  -p 5432:5432 -v "$PWD/docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro" \
  postgres:16-alpine postgres -c shared_preload_libraries=pg_stat_statements -c max_connections=200

./mvnw -q -DskipTests package
taskset -c 0,1,6,7 java -Xmx1500m -jar target/ticketforge-0.1.0-SNAPSHOT.jar   # app on :8080
```

`run.sh` expects the Postgres container to be named **`tf-pg`** (override with `PG_CONTAINER`). The old `tf-pg` from the first baseline could not restart because its bind mount pointed at a moved folder; it was renamed `tf-pg-old` (data volume kept) and a new `tf-pg` was created.

### 3.2 Running one scenario

```bash
cd loadtest
./run.sh <result-name> <scenario.js> [KEY=VALUE ...]
./run.sh s2-atomic-1000 s2-stampede.js STRATEGY=ATOMIC_CONDITIONAL VUS=1000
```

Outputs in `loadtest/results/`: `<name>.json` (k6 summary), `<name>.samples.jsonl` (per-second app CPU, Postgres CPU, pool, heap), `<name>.invariants.json` (the six invariants).

### 3.3 The scenarios

| Script | Spec # | What it simulates | Knobs | Pass condition |
| :--- | :--- | :--- | :--- | :--- |
| `s1-catalog.js` | 1 | Read traffic ramp (events, show, seat map) | `RATE`, `MAX_VUS` | Finds the knee; no errors below it |
| `s2-stampede.js` | 2, 5 | N users hold the same seat at the same instant | `VUS`, `STRATEGY` | Exactly 1 winner, others get 409 |
| `s3-ga-sellout.js` | 3, 5 | 12,000 buyers for a 10,000-ticket pit | `STRATEGY`, `GA_SHARDS`, `VUS`, `ATTEMPTS` | Never more than 10,000 sold; counter matches holds |
| `s4-movie-mix.js` | 4 | Browse, hold, pay or walk away on 20 shows | `SHOWS`, `VUS`, `DURATION` | `holds_error == 0` |
| `s4-movie-mix.js PROFILE=friday` | 10 | 1,500 users, 200 shows, 3 min | same | All inventory sellable; no errors |
| `s6-retry-storm.js` | 6 | Every request sent 3× at once, same Idempotency-Key | `VUS`, `ITER` | 0 duplicate holds, 0 double charges |
| `s7-queue-surge.js` | 7 | 20,000 users join one queue in 5 s | `USERS` | FIFO, no skips, configured admission rate |
| `s8-concert-onsale.js` | 8, 9 | Concert on-sale, with and without the waiting room | `QUEUE`, `VUS`, `DURATION` | Correct either way; compare latency |

### 3.4 Re-running the whole suite

The exact batch used on 2026-10-31 (results saved as `rerun-*`):

```bash
cd loadtest
r() { n=$1; shift; ./run.sh "rerun-$n" "$@"; }
for st in ATOMIC_CONDITIONAL PESSIMISTIC OPTIMISTIC; do
  for v in 100 500 1000; do r s2-$st-$v s2-stampede.js STRATEGY=$st VUS=$v; done; done
r s3-ATOMIC_CONDITIONAL-shards0 s3-ga-sellout.js STRATEGY=ATOMIC_CONDITIONAL GA_SHARDS=0
r s3-ATOMIC_CONDITIONAL-shards8 s3-ga-sellout.js STRATEGY=ATOMIC_CONDITIONAL GA_SHARDS=8
r s3-PESSIMISTIC-shards0        s3-ga-sellout.js STRATEGY=PESSIMISTIC GA_SHARDS=0
r s3-OPTIMISTIC-shards0         s3-ga-sellout.js STRATEGY=OPTIMISTIC GA_SHARDS=0
r s6-retry-storm  s6-retry-storm.js
r s7-queue-surge  s7-queue-surge.js
r s4-movie-mix    s4-movie-mix.js
r s8-concert-queue-true  s8-concert-onsale.js QUEUE=true
r s8-concert-queue-false s8-concert-onsale.js QUEUE=false
r s1-catalog      s1-catalog.js RATE=3000
r s1-catalog-8k   s1-catalog.js RATE=8000 MAX_VUS=2000
r s10-movie-friday s4-movie-mix.js PROFILE=friday
```

Total wall time is about 25 minutes for the first pass.

### 3.5 Rules for a fair measurement (important for the microservices comparison)

1. **Warm up first.** The JVM and Postgres need a few minutes of traffic. Cold results were up to 4× slower (see the baseline doc §3). Throw away the first pass or run a short warm-up scenario.
2. **Run `ANALYZE`** after bulk seeding. Stale statistics caused the Phase 1 total-outage defect (baseline report §4.1).
3. **Same cores, same memory, same k6 settings** for every phase. The pinning in §3.1 is part of the test.
4. **Close other heavy programs.** Load average was ~8.7 during the last run. Re-run any result that looks off.
5. **Repeat 3 times and use the median** for any number you quote in the final comparison.
6. **Ignore `http_req_failed`.** k6 counts expected 409s as failures. Use `holds_error`, `holds_overloaded` and the invariants.
7. **Check the invariants after every run.** A fast run that breaks an invariant is a failed run.

---

## 4. Invariant checks by hand

```bash
TOKEN=$(curl -s -XPOST localhost:8080/auth/login -H 'Content-Type: application/json' \
  -d '{"email":"admin@ticketforge.local","password":"admin12345"}' | jq -r .accessToken)
curl -s localhost:8080/admin/invariants -H "Authorization: Bearer $TOKEN"
# every value must be 0
docker exec -i tf-pg psql -U ticketforge ticketforge < scripts/verify-invariants.sql
```

---

## 5. Raw data locations

| Path | Content |
| :--- | :--- |
| `target/surefire-reports/` | JUnit XML and text reports |
| `loadtest/results/rerun-*` | First pass of 2026-10-31 |
| `loadtest/results/warm-*` | Warm re-runs of the cold-sensitive scenarios (s2, s6, s7) |
| `loadtest/results/s*` (no prefix) | Original baseline from the first Phase 1 build |
| `docs/baseline-results/phase1-original.json` | Original baseline, extracted metrics |
| `docs/baseline-results/phase1-rerun-2026-10-31.json` | First pass, extracted metrics |
| `docs/baseline-results/phase1-warm-2026-10-31.json` | Warm pass, extracted metrics |
