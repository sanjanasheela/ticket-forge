# Phase 2 Progress

Branch: `ticket-forge-microservices`. The monolith (`ticket-forge-monolithic`, `ticketforge/`) is the frozen Phase 1 baseline and is not changed here.

* Design: [PHASE2_DESIGN.md](PHASE2_DESIGN.md)
* Results and analysis: [PHASE2_RESULTS.md](PHASE2_RESULTS.md) · the filled comparison sheet: [COMPARISON.md](COMPARISON.md)
* Phase 1 numbers every result is compared to: [phase1-baseline/](phase1-baseline/)

## Status: Phase 2 is built, tested and measured

| Piece | State | Automated tests |
| :--- | :--- | :--- |
| `common` (errors, JWT, security, internal-API guard, idempotency) | Done | 4 pass |
| `messaging` (transactional outbox, batched Kafka relay, idempotent consumer) | Done | via the service tests |
| `contracts` (event, command and API shapes) | Done | via producer and consumer tests |
| `auth-service` | Done | 6 pass |
| `catalog-service` (+ Redis cache with circuit breaker, outbox events, seeders) | Done | 15 pass |
| `reservation-service` (inventory, holds, waiting room, SSE, Kafka projection, saga participant) | Done | 42 pass (34 ported unchanged) |
| `booking-service` (checkout saga, payments, reconciler, recovery) | Done | 15 pass |
| Compose stack (Postgres x4 databases, Redis, Kafka, Kong) + run scripts | Done | smoke test passes |
| k6 suite against Kong (the 10 Phase 1 scenarios, unchanged scripts) | Done, 3 passes | all invariants 0 |
| Failure experiments (Redis, 3 services, `kill -9`, Kafka, PostgreSQL) | Done | all pass |
| Filled comparison sheet | Done | [COMPARISON.md](COMPARISON.md) |

**82 automated tests, 21 load scenarios (final pass), 8 failure experiments: no invariant violation in any of them.**

## What the work found

See [PHASE2_RESULTS.md](PHASE2_RESULTS.md) sections 1, 5 and 6. In one paragraph: the split is correct (nothing sold twice, nothing double-charged, nothing lost) and fault isolation is real and measured, but on the same 4 threads as the monolith it costs throughput on checkout-heavy traffic (about 9% on the Friday scenario, 37% on the retry storm) because Kong, Kafka and Redis take 0.4 to 0.7 cores of the same budget and the saga adds hops. Nine defects were found and fixed along the way, three of them only visible under load or failure injection.

## Not done (honest list)

* More application cores, or Kong on its own cores, or several instances per service: the experiment that would show what the split costs when it is not starved. The laptop has no spare cores.
* Network partitions between services, rolling deploys, Kafka broker replication (single broker).
* Cache hit-rate measurement, per-hop distributed tracing, Prometheus/Grafana dashboards for the new stack.
* Per-service Postgres servers (one server, four databases here), Kubernetes, cloud deployment (Phase 5).
* Saga latency: the 100 ms outbox poll is the first thing to tighten (a LISTEN/NOTIFY wake-up would remove most of it).

## Next: Phase 3

Replace each off-the-shelf piece behind the interface it already sits behind: Redis (`CatalogCache`) first, using the earlier `forgekv` prototype from git history (commit `d5fc407`), then a single-node mini-Kafka (the outbox relay and the consumers are the only touch points), then Raft, then the gateway. Each replacement is benchmarked against the numbers in COMPARISON.md.
