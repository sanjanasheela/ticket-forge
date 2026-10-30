# Phase 2 Progress

Branch: `ticket-forge-microservices`. The monolith (`ticket-forge-monolithic`, `ticketforge/`) is the frozen Phase 1 baseline and is not changed here. Design: [PHASE2_DESIGN.md](PHASE2_DESIGN.md). Phase 1 numbers to beat: [phase1-baseline/](phase1-baseline/).

## Status

| Piece | State | Automated tests |
| :--- | :--- | :--- |
| `common` (errors, JWT, resource-server security, internal-API guard, idempotency) | Done | exercised by every service |
| `messaging` (transactional outbox, Kafka relay, idempotent consumer) | Done | via catalog and reservation tests |
| `contracts` (event, command and API shapes) | Done | via producer and consumer tests |
| `auth-service` | Done | 6 of 6 pass (ported from Phase 1) |
| `catalog-service` (+ Redis cache, outbox events, internal layout API) | Done | 15 of 15 pass (4 ported, 11 new: cache, Kafka events, internal guard) |
| `reservation-service` (inventory, holds, waiting room, SSE, Kafka projection, saga participant) | Done, saga participant not yet tested | 42 of 42 pass (34 ported unchanged, 8 new for the projection and listener) |
| `booking-service` (checkout saga, payments, reconciler, recovery) | **Written, NOT yet compiled or tested** | none yet |
| Docker Compose stack (Postgres x4 databases, Redis, Kafka, Kong) | Not started (images pulled locally: redis:7-alpine, apache/kafka:3.8.0, kong:3.7) | |
| Kong routes and end-to-end smoke test | Not started | |
| k6 suite against Kong, failure experiments, filled comparison sheet | Not started | |

## Findings so far (worth keeping for the write-up)

* **The split follows the Phase 1 seams.** Only `catalog -> inventory` and `booking -> reservation` crossed module boundaries; everything else moved unchanged. 34 of the 34 Phase 1 reservation tests pass against the extracted service after fixing the fixture, not the logic.
* **"A show never exists without inventory" stops being true.** In the monolith inventory was built inside the show-creation transaction. Now it is built after reservation-service consumes `ShowPublished`, so there is a window where a show exists in the catalog but is not bookable.
* **The one cross-boundary transaction is the saga.** Phase 1's `finalizePaid` (mark paid + confirm hold + insert booking) cannot be one transaction any more. It became CHARGE, then a `ConfirmHold` command, then BOOK or REFUND, with persisted saga state.
* **Bugs the tests caught while building:** a shared scheduled job failing in services without its table; a Kafka test pointing at the wrong broker (`@ServiceConnection` does not set the property); a text-block SQL concatenation producing `FROM showsWHERE`, which silently stopped the waiting room (it logs a warning instead of crashing); a transaction marked rollback-only by a caught exception in the saga participant.

## Next

1. Compile and test booking-service (port the 9 Phase 1 checkout tests against a stub reservation-service).
2. Compose stack and Kong routing; end-to-end test (create show, hold, pay, confirm through Kong).
3. Point the unchanged k6 suite at Kong, run all scenarios, fill [the comparison sheet](phase1-baseline/MICROSERVICES_COMPARISON.md).
4. Failure experiments: kill Redis, a service, Kafka mid-sale; verify invariants after recovery.

## Running what exists

```bash
cd microservices
./mvnw -q verify -pl auth-service,catalog-service,reservation-service -am   # needs Docker (Testcontainers)
```
