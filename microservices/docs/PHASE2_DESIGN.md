# Phase 2 Design: TicketForge as Microservices (standard cloud stack)

> **Goal.** Split the Phase 1 monolith into services on the *off-the-shelf* stack (Kong, Redis, Kafka, PostgreSQL), keep every correctness guarantee, and measure what the split costs and buys. Phase 3 later replaces each off-the-shelf component with a custom one, so every external dependency here sits behind an interface.
>
> **The monolith stays untouched** in `../ticketforge/` as the frozen baseline. All Phase 2 code lives in `microservices/`.
>
> Status of this document: design, kept current with what was built. Progress: [PHASE2_PROGRESS.md](PHASE2_PROGRESS.md). Results: [PHASE2_RESULTS.md](PHASE2_RESULTS.md).
>
> Changes from the original design, found by building and measuring: the outbox relay sends a batch and waits once (a per-record wait capped checkout throughput); saga listeners run with concurrency 3 to match the 3-partition topics; the Redis cache has a circuit breaker; `/internal/shows/{id}/ready` and the catalog seeders exist so load tests can wait for the asynchronous inventory build.

---

## 1. Why these seams

Phase 1 modules already talk only through public interfaces (verified by `ModularityTests`). Their actual dependencies:

```
auth ──► shared
catalog ──► shared
inventory ──► catalog
reservation ──► catalog, inventory, waitingroom
waitingroom ──► auth, catalog
payment ──► shared
booking ──► payment, reservation
realtime ──► inventory
```

Two clusters are tightly bound by a shared database transaction and must **not** be split:

* **inventory + reservation (+ realtime)**: a hold claims seats and writes the hold row in one transaction. Splitting them would turn the hottest path into a distributed transaction for no gain.
* **booking + payment**: a payment row and a booking row must agree.

The one transaction that *does* cross a boundary today is `CheckoutService.finalizePaid`: *mark payment succeeded + confirm hold + insert booking* in one local transaction. That is the transaction the **saga** replaces (§5).

## 2. Services

| Service | Port | Owns (modules from Phase 1) | Database | Why separate |
| :--- | ---: | :--- | :--- | :--- |
| `auth-service` | 8081 | `auth` | `auth_db` | Identity is needed by all, changes rarely, scales on login bursts |
| `catalog-service` | 8082 | `catalog` | `catalog_db` | Read-heavy (the Phase 1 bottleneck); gets the Redis cache; scales on browse traffic |
| `reservation-service` | 8083 | `inventory`, `reservation`, `realtime`, `waitingroom` | `reservation_db` | The hot path: seat claims, holds, SSE, queue. Scales independently of browsing |
| `booking-service` | 8084 | `booking`, `payment`, `idempotency`, saga orchestrator | `booking_db` | Money. Slow external calls (payment gateway) must not share a pool with the hot path |
| Kong | 8080 | gateway | none (DB-less) | Single entry, routing, JWT check, rate limiting |
| Redis | 6379 | catalog cache, waiting-room counters | none | See §6 |
| Kafka (KRaft, 1 broker) | 9092 | event bus | log | See §4 |
| PostgreSQL 16 | 5432 | all four databases | | One server, **four databases** (see §8, honest caveat) |

`waitingroom` joins `reservation-service` for now because `reservation` validates queue access tokens on every hold. It can be extracted later once the token is verified statelessly (it is already a signed JWT).

## 3. Data ownership and the database-per-service rule

Each service has its own Flyway migrations, its own database and its own user. **No service reads another service's tables.** What used to be a SQL join becomes one of:

| Phase 1 cross-module access | Phase 2 replacement |
| :--- | :--- |
| `inventory` reads venue layout from `catalog` tables to materialise `show_seats` | `catalog-service` publishes `ShowCreated` (with the layout) to Kafka; `reservation-service` consumes it and materialises inventory into its own DB |
| `reservation` reads show rules (TTL, max seats, queue flag, price per tier) | Carried in `ShowCreated` and stored as a local read-model (`show_rules`) in `reservation_db` |
| `booking` reads `holds` to insert a booking | Saga asks `reservation-service` (command/reply, §5) |
| `waitingroom` reads show flags from `catalog` | Local read-model from `ShowCreated` / `ShowUpdated` |
| JWT verification | Every service verifies the HS256 JWT locally with the shared secret (no call to `auth-service`). Kong also rejects missing/invalid tokens early |

Consequence to measure: reads are now eventually consistent across services. A show created in the catalog is bookable only after `reservation-service` has consumed `ShowCreated`. Tests must wait for that.

## 4. Kafka: topics and events

One broker (laptop). Keys are chosen so events for one aggregate stay ordered.

| Topic | Key | Producer | Consumers | Events |
| :--- | :--- | :--- | :--- | :--- |
| `catalog.shows` | `showId` | catalog | reservation | `ShowCreated`, `ShowUpdated` |
| `reservation.events` | `holdId` | reservation | booking, (analytics) | `HoldCreated`, `HoldExpired`, `HoldReleased`, `HoldConfirmed` |
| `saga.reservation.commands` | `holdId` | booking | reservation | `ConfirmHold`, `ReleaseHold` |
| `saga.reservation.replies` | `holdId` | reservation | booking | `HoldConfirmedReply`, `HoldConfirmFailed(reason)` |
| `booking.events` | `bookingId` | booking | (analytics, notifications) | `BookingConfirmed`, `BookingRefunded` |

Delivery rule: **transactional outbox**. A service writes the event row in the same DB transaction as the state change; a relay publishes it to Kafka and marks it sent. This is what stops "DB committed but event lost". Consumers are **idempotent** (they record processed event ids), because delivery is at-least-once.

## 5. Checkout saga (replaces `finalizePaid`)

Orchestrated by `booking-service`. State is persisted in a `sagas` table so a crash resumes instead of losing money.

```
CheckoutRequested (HTTP, Idempotency-Key)
   │
   ▼
1. VERIFY_HOLD        booking → reservation (HTTP GET, short timeout)   fails → REJECTED (nothing to undo)
   ▼
2. CHARGE             booking → payment gateway (no DB tx open)         declined → FAILED (hold stays, buyer may retry)
   │                                                                    timeout  → CHARGE_UNKNOWN (reconciler)
   ▼
3. CONFIRM_HOLD       booking ──cmd──► reservation  (Kafka)             reply OK  → BOOKED
   │                                                                    reply FAIL (hold expired/released) → COMPENSATE
   ▼
4. BOOKED             insert booking, emit BookingConfirmed

COMPENSATE:           refund via gateway → mark payment REFUNDED → emit BookingRefunded → state REFUNDED
```

Rules:

* The charge is the only step that cannot be undone by a database rollback, so it is the step with an explicit compensation (the refund).
* `CHARGE_UNKNOWN` is resolved by the reconciler exactly like Phase 1's `PaymentReconciler`, now driving the saga forward from the persisted state.
* `ConfirmHold` is idempotent on `(holdId)`: confirming an already confirmed hold replies OK.
* The HTTP caller waits up to a bounded time (default 5 s) for the saga to reach a terminal state, then gets `202 PAYMENT_PENDING` and polls. Same response contract as Phase 1 (`CONFIRMED`, `PAYMENT_PENDING`, `PAYMENT_FAILED`, `HOLD_EXPIRED_REFUNDED`).

### The invariants that become distributed

| Invariant | Phase 1 (one DB) | Phase 2 |
| :--- | :--- | :--- |
| `seat_sold_twice` | one SQL check | inside `reservation_db`, unchanged |
| `ga_counter_mismatch` | one SQL check | inside `reservation_db`, unchanged |
| `booked_seat_without_confirmed_hold` | join `show_seats` × `holds` | within `reservation_db`, unchanged |
| `confirmed_seat_not_booked` | join `holds` × `bookings` | **cross-service**: confirmed holds in reservation vs bookings in booking |
| `paid_without_booking` | join `payments` × `bookings` | inside `booking_db`, but only true **after the saga settles** |
| `booking_without_payment` | join | inside `booking_db` |

Cross-service checks run from a `verification` job that reads both services' read-only HTTP endpoints (not their databases) and tolerates in-flight sagas: it only flags a mismatch older than the saga timeout. This is itself a Phase 2 finding: *distributed invariants are eventual, not instantaneous.*

## 6. Redis (behind an interface)

| Use | Interface | Behaviour |
| :--- | :--- | :--- |
| Catalog read cache | `CatalogCache` | Cache-aside, TTL 30 s, invalidated on `ShowUpdated`; cache failure falls through to Postgres (never an outage) |
| Waiting-room admission counters | `QueueCounters` | Atomic INCR for position; Postgres remains source of truth |

Every Redis call is wrapped so that a Redis outage degrades to Postgres. The interface exists so Phase 3 can swap in the custom mini-Redis (the earlier `forgekv` prototype in git history, commit `d5fc407`, is the starting point).

## 7. Kong gateway

DB-less declarative config (`kong.yml`): routes `/auth/**` → auth, `/events/**`, `/shows/**` (reads) → catalog or reservation by path, `/holds/**`, `/queue/**` → reservation, `/checkout/**`, `/bookings/**` → booking. JWT plugin and rate-limiting plugin enabled. The route table is the contract that Phase 3 replaces with the custom gateway.

## 8. Resource budget and fairness (read before comparing numbers)

The comparison with the monolith is only meaningful if the compute is the same.

| Tier | Monolith (Phase 1) | Microservices (Phase 2) |
| :--- | :--- | :--- |
| Application tier | 4 threads (`0,1,6,7`), 1.5 GB heap | **The same 4 threads shared by 4 services + Kong + Redis + Kafka** (`0,1,6,7`); heap split across services, total ≤ 1.5 GB |
| Database | 4 threads (`2,3,8,9`), 2 GB | Same: one Postgres server on `2,3,8,9`, 2 GB, four databases |
| Load generator | 4 threads (`4,5,10,11`) | Same |

This is deliberately the *strictest* fair test: microservices pay for their own infrastructure out of the same budget. It will make Phase 2 look worse on raw throughput. A second configuration with extra application cores is reported separately and labelled as such.

Honest caveat: one Postgres *server* with four *databases* shares CPU and the connection limit, so database-level isolation is logical, not physical. True per-service servers would not fit this machine; the document and the results say so.

## 9. Build order

1. Infrastructure up and healthy (Compose: Postgres, Redis, Kafka, Kong).
2. `catalog-service` + Redis cache (first measurable win, the Phase 1 read bottleneck).
3. `auth-service` (small, unblocks gateway JWT).
4. `reservation-service` (largest; consumes `ShowCreated`).
5. `booking-service` + saga + outbox.
6. Kong routes wired, end-to-end smoke.
7. Service-level and cross-service tests, then re-run the k6 suite against Kong and fill [MICROSERVICES_COMPARISON.md](phase1-baseline/MICROSERVICES_COMPARISON.md).
8. Failure experiments (§10).

## 10. Test plan

| Level | What | Tool |
| :--- | :--- | :--- |
| Unit / integration per service | Same cases as Phase 1, run against the service's own DB | JUnit + Testcontainers |
| Contract | Event schemas and the `ConfirmHold` command/reply | JSON-schema checks in both producer and consumer tests |
| Saga | Happy path; decline; gateway timeout then reconcile; hold expired after charge (refund); duplicate command; crash between steps (resume) | Integration with Kafka Testcontainer |
| End to end | Hold → pay → confirm through Kong | k6 smoke + script |
| Load | The ten Phase 1 scenarios, unchanged k6 scripts, pointed at Kong | `loadtest/run.sh` |
| Failure | Kill each service/Redis/Kafka mid-sale; verify invariants after recovery | Scripted `docker kill` + invariant check |

**Definition of done for Phase 2:** all Phase 1 scenarios pass with all six invariants 0 (cross-service ones after settling), the comparison sheet is filled with measured values, and at least the Redis-kill, service-kill and Kafka-kill experiments have recorded results.
