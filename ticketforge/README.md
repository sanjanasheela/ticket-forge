# TicketForge: Phase 1 Monolith

A high-concurrency modular monolith for movie and concert ticketing. It handles two users picking the same seat in the same millisecond, 3-minute concert holds with waiting rooms, and flash sales on a 10,000-ticket pit, with zero overselling.

- Design: [PHASE1_MONOLITH.md](../PHASE1_MONOLITH.md)
- Results: [docs/PHASE1_BASELINE_REPORT.md](docs/PHASE1_BASELINE_REPORT.md): where it breaks, three defects that only load found, strategy comparisons
- Load tests: [loadtest/README.md](loadtest/README.md)
- Numbers to beat in Phase 2: [docs/PHASE1_PERFORMANCE_BASELINE.md](docs/PHASE1_PERFORMANCE_BASELINE.md), with the empty side-by-side sheet in [docs/MICROSERVICES_COMPARISON.md](docs/MICROSERVICES_COMPARISON.md)
- How to run every test and load scenario: [docs/TESTING.md](docs/TESTING.md)

## What's inside

| Module | Responsibility |
| :--- | :--- |
| `auth` | Signup/login, HS256 JWTs, stored refresh tokens with rotation and reuse detection |
| `catalog` | Venues (rows, aisles, tiers, GA pits), events, shows with MOVIE / CONCERT defaults |
| `inventory` | The hot path: per-show seat snapshot, PESSIMISTIC / OPTIMISTIC / ATOMIC_CONDITIONAL claims, SKIP LOCKED best-available, sharded GA counter, gap rule |
| `reservation` | Holds with per-show TTL (7 min movie, 3 min concert), lazy + 1 s sweeper expiry, one active hold per concert-goer |
| `payment` / `booking` | Hold → Pay → Confirm without a transaction open during the gateway call; late-payment refunds; reconciler for lost gateway responses |
| `idempotency` | Idempotency-Key replay / in-progress / reuse detection, scoped per user |
| `waitingroom` | Token-gated FIFO queue, show-scoped access tokens, live position over SSE |
| `realtime` | Live seat maps over SSE, after-commit only |
| `verification` | Six SQL invariants, checked continuously (gauge must stay 0) |
| `seed` | Demo data (10-screen multiplex, 50k-seat stadium + 10k pit) and load-test users |

## Prerequisites
- JDK 21+ (the build targets Java 21)
- Docker; the Compose plugin for the full stack (`sudo apt install docker-compose-v2` on Ubuntu)

## Run
```bash
docker compose up -d          # postgres, prometheus (:9090), grafana (:3000, dashboard "TicketForge"), exporters
./mvnw spring-boot:run        # app on :8080
# load demo data (admin@ticketforge.local / admin12345 — dev only)
TOKEN=$(curl -s -XPOST localhost:8080/auth/login -H 'Content-Type: application/json' \
  -d '{"email":"admin@ticketforge.local","password":"admin12345"}' | jq -r .accessToken)
curl -XPOST localhost:8080/admin/seed/demo -H "Authorization: Bearer $TOKEN"
open http://localhost:8080/ui/   # open two windows on one show and click the same seat
```
Load-test mode, with the app in a CPU/RAM-limited container:
```bash
docker compose --profile app up -d --build
```

## Test
```bash
./mvnw verify   # 58 tests: unit, Testcontainers integration, concurrency conformance per strategy, module boundaries
```
Every integration test also asserts the database invariants afterwards.
