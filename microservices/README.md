# TicketForge Phase 2: Microservices

The Phase 1 [modular monolith](../ticketforge/) split into four services on the standard stack (Kong, Redis, Kafka, PostgreSQL). The monolith is the frozen baseline; every number here is compared against it. Design and reasoning: [docs/PHASE2_DESIGN.md](docs/PHASE2_DESIGN.md). Status: [docs/PHASE2_PROGRESS.md](docs/PHASE2_PROGRESS.md).

```
                          ┌──────────────────────────── Kong :8080 (public) ────────────────────────────┐
  client ──HTTP──►        │  /auth        /events /shows/{id}      /shows/{id}/{seats,holds,queue}  /holds/{id}/checkout │
                          └────┬───────────────┬──────────────────────────┬──────────────────────┬───────┘
                               ▼               ▼                          ▼                      ▼
                         auth-service    catalog-service           reservation-service     booking-service
                            :8081           :8082  ──Redis cache       :8083                   :8084
                          auth_db         catalog_db                 reservation_db           booking_db
                                              │ ShowPublished (outbox → Kafka)  ▲   ▲ ConfirmHold / reply
                                              └──────────────────────────────────┘   └── saga (booking orchestrates)
```

| Service | Owns | Notes |
| :--- | :--- | :--- |
| `auth-service` | users, JWT issue, refresh tokens | Other services verify JWTs locally with the shared key |
| `catalog-service` | venues, events, shows | Redis cache-aside on public reads; publishes `ShowPublished` through a transactional outbox |
| `reservation-service` | seat inventory, holds, waiting room, live seat maps | The hot path. Builds inventory from catalog events; answers the saga's `ConfirmHold` |
| `booking-service` | payments, bookings, the checkout saga | Charge, confirm, book or refund, with persisted state, a reconciler and a recovery job |
| `common` / `messaging` / `contracts` | shared library, outbox + Kafka relay, event shapes | |

## Run it

Needs Docker (with the Compose plugin) and JDK 21+.

```bash
cd microservices
docker compose -f deploy/docker-compose.yml up -d      # Postgres (4 DBs), Redis :6380, Kafka, Kong :8080
./mvnw -q package -DskipTests                           # builds the four service jars
scripts/run-stack.sh                                    # starts the services on the application cores
scripts/smoke.sh                                        # login -> show -> hold -> pay -> booking, through Kong
scripts/stop-stack.sh
```

Redis uses port **6380** because 6379 is commonly taken by a Redis already on the host.

## Test it

```bash
./mvnw -q verify -am   # 82 tests, needs Docker
loadtest/run.sh s2-atomic-1000 s2-stampede.js STRATEGY=ATOMIC_CONDITIONAL VUS=1000          # one k6 scenario via Kong
python3 scripts/verify-invariants.py                                                        # all nine invariants, must be 0
```

The k6 scripts in `loadtest/` are the Phase 1 scripts, unchanged. Methodology, fairness rules and the resource layout: [docs/phase1-baseline/TESTING.md](docs/phase1-baseline/TESTING.md) and PHASE2_DESIGN.md section 8.

## Documents

| File | What |
| :--- | :--- |
| [docs/PHASE2_DESIGN.md](docs/PHASE2_DESIGN.md) | Architecture, service split, data ownership, Kafka topics, the saga, resource budget |
| [docs/PHASE2_PROGRESS.md](docs/PHASE2_PROGRESS.md) | What is done, tested, and next |
| [docs/PHASE2_RESULTS.md](docs/PHASE2_RESULTS.md) | Benchmarks vs the monolith, failure experiments, defects found, limitations |
| [docs/COMPARISON.md](docs/COMPARISON.md) | The filled side-by-side sheet |
| [docs/phase1-baseline/PHASE1_PERFORMANCE_BASELINE.md](docs/phase1-baseline/PHASE1_PERFORMANCE_BASELINE.md) | The monolith numbers every result is compared to |
| [docs/phase1-baseline/MICROSERVICES_COMPARISON.md](docs/phase1-baseline/MICROSERVICES_COMPARISON.md) | The side-by-side sheet |
