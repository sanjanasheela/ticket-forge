# Phase 1 — TicketForge High-Concurrency Modular Monolith

> **Status: built (M0–M7 done).** Code: [ticketforge/](ticketforge/). Results and the defects load testing found: [Phase 1 baseline report](ticketforge/docs/PHASE1_BASELINE_REPORT.md).

> Part of [TicketForge Chaos-Cloud](distributed_system_project_plan.md). Phase 1 is the **baseline**: a correct, BookMyShow-style booking system in a single process on a single Postgres, instrumented so we can measure exactly where (and why) it breaks under contention. Every later phase is benchmarked against this.

---

## 1. Goals & Non-Goals

### Goals
1. **Correctness first**: zero double-booking and zero overselling under any concurrency level.
2. **Realistic flow**: Select seats → Hold (TTL depends on category: ~7 min for movies, ~3 min for concerts) → Pay → Confirm, with expiry and refund on late payment.
3. **Two traffic profiles**: everyday **movie** shows (moderate load, long holds) and high-demand **concert** on-sales (massive spikes, short holds, waiting room). The concert profile is our extreme-contention stress case.
4. **Measurable contention**: pluggable lock strategies, each benchmarked with k6 and exported via Prometheus.
5. **Clean module boundaries** so Phase 2 can split the monolith into services along existing seams.

### Non-Goals (deferred)
| Deferred | Goes to |
| :--- | :--- |
| Redis / Kafka / multiple app instances | Phase 2 |
| Saga orchestrator (we do a local, in-process compensation instead) | Phase 2 |
| Custom gateway, adaptive concurrency, Singleflight | Phase 3A |
| Search/filtering, caching of catalog | Phase 2/3 |
| Real payment provider | never (mock only) |

---

## 2. Decisions Summary

| Topic | Decision |
| :--- | :--- |
| Stack | Java 21 target (virtual threads), Spring Boot 4.1, Spring Modulith 2.1, Maven |
| Logging | Structured JSON logs (Spring Boot built-in) with `trace_id`, `hold_id`, `show_id`, `instance`, ready for Splunk/Loki (§13.1) |
| DB | PostgreSQL 16, Flyway migrations, HikariCP |
| Data access | Plain SQL everywhere (`JdbcClient`), so locking behaviour stays visible. *(Changed from JPA during build: the hot path needed hand-written SQL anyway.)* |
| Module boundaries | Spring Modulith (verified by `ApplicationModules.verify()` test) |
| Booking model | **Both** reserved seating and general admission (GA) tiers |
| Show categories | `MOVIE` (BookMyShow/Fandango-style) and `CONCERT` (Ticketmaster/flash-sale-style), see §7.1 |
| Checkout | Hold → Pay → Confirm. Hold TTL **per show**, defaulted from category: movie 7 min, concert 3 min |
| Hold storage | **Postgres only** (rows with `held_until`) |
| Live seat updates | **SSE** stream per show |
| Rules | Max seats per hold, no single-seat gaps, tiered pricing |
| Auth | Own JWT (signup/login), roles `USER`, `ADMIN` |
| Payments | Mock gateway with configurable latency & failure rate |
| Waiting room | Token-gated FIFO queue for flagged shows |
| Extras | Idempotency keys, pluggable lock strategies, Micrometer/Prometheus + k6 suite |
| Frontend | Minimal seat-map UI (plain JS or small React, served as static files) |
| Catalog | Admin APIs for venues, seat layouts, events, shows + bulk seeder |
| Local infra | `docker-compose`: app, postgres, prometheus, grafana |

---

## 3. Module Layout

```
com.ticketforge
├── auth          # signup/login, JWT issue/verify, roles
├── catalog       # venues, layouts, seat tiers, events, shows (admin + public read)
├── inventory     # show_seats, ga_inventory, lock strategies, gap rule  ← the hot path
├── reservation   # holds lifecycle, expiry sweeper, per-hold limits
├── payment       # mock gateway client, payment records
├── booking       # checkout orchestration, confirmed bookings, refunds
├── waitingroom   # queue, admission scheduler, access tokens
├── realtime      # SSE emitter registry, fan-out of seat events
├── idempotency   # Idempotency-Key filter + store
└── shared        # money, ids, error model, clock abstraction
```

Rules:
- Modules talk via **public service interfaces + domain events** only (no reaching into another module's repositories).
- Cross-module events (`SeatsHeld`, `SeatsReleased`, `BookingConfirmed`, …) are published with `@TransactionalEventListener(AFTER_COMMIT)`, so they become Kafka topics in Phase 2 almost unchanged.
- An injectable `Clock` everywhere, so TTL/expiry logic is unit-testable.

---

## 4. Data Model

```sql
-- auth
users(id uuid pk, email text unique, password_hash text, role text, created_at)

-- catalog
venues(id, name, city, created_at)
seat_tiers(id, venue_id, name, kind text check (kind in ('RESERVED','GA')),   -- Recliner, Gold, Silver, Pit
           ga_capacity int null)                                             -- GA tiers only
venue_seats(id, venue_id, tier_id, section, row_label, seat_number int,
            is_aisle_left bool, is_aisle_right bool,
            unique(venue_id, section, row_label, seat_number))
events(id, title, genre, duration_min,
       category text check (category in ('MOVIE','CONCERT')), created_at)
shows(id, event_id, venue_id, starts_at, sale_opens_at,
      hold_ttl_sec int,              -- defaulted from category (420 movie / 180 concert), admin may override
      queue_enabled bool,            -- defaults true for CONCERT
      max_seats_per_hold int,        -- defaults 10 movie / 6 concert
      max_holds_per_user int,        -- defaults unlimited movie / 1 concert
      status)
show_tier_prices(show_id, tier_id, price_minor bigint, currency, pk(show_id, tier_id))

-- inventory (hot tables)
show_seats(show_id, venue_seat_id, tier_id,
           status text check (status in ('AVAILABLE','HELD','BOOKED')),
           hold_id uuid null, held_until timestamptz null,
           version bigint not null default 0,          -- optimistic strategy
           pk(show_id, venue_seat_id))
ga_inventory(show_id, tier_id, capacity int,            -- copied from seat_tiers.ga_capacity when the show is created
             held int, sold int, version bigint,
             check (held >= 0 and sold >= 0 and held + sold <= capacity),   -- DB-level oversell guard
             pk(show_id, tier_id))
ga_inventory_shards(show_id, tier_id, shard int, capacity, held, sold, ...)  -- SHARDED_COUNTER strategy only

-- reservation
holds(id uuid pk, user_id, show_id, status text,       -- HELD | CONFIRMED | EXPIRED | RELEASED
      expires_at timestamptz, created_at)
hold_items(hold_id, show_id, venue_seat_id null, tier_id, quantity int, price_minor)

-- payment / booking
payments(id, hold_id, user_id, amount_minor, status,   -- PENDING | SUCCEEDED | FAILED | REFUNDED
         gateway_ref, created_at, updated_at)
bookings(id, hold_id unique, user_id, show_id, payment_id, status, -- CONFIRMED | REFUNDED
         created_at)

-- idempotency
idempotency_keys(user_id, key text, request_hash text, status text, -- IN_PROGRESS | COMPLETED
                 response_code int, response_body jsonb, created_at,
                 pk(user_id, key))

-- waiting room
queue_entries(show_id, user_id, position bigserial, joined_at, admitted_at null,
              pk(show_id, user_id))
```

Key indexes: `show_seats(show_id, status)`, partial index `show_seats(held_until) where status='HELD'` for the sweeper, `holds(expires_at) where status='HELD'`.

**Invariants (checked by tests and by a post-load-test verifier):**
1. A seat (`show_id, venue_seat_id`) never appears in the `hold_items` of two `CONFIRMED` holds.
2. `ga_inventory.held + sold <= capacity` (also enforced by a CHECK constraint).
3. Every `BOOKED` seat maps to exactly one `CONFIRMED` booking; every `SUCCEEDED` payment either has a `CONFIRMED` booking or was `REFUNDED`.

---

## 5. The Booking Flow

### 5.1 State machines
```
Seat:     AVAILABLE ──hold──▶ HELD ──confirm──▶ BOOKED
              ▲                │
              └──expire/release┘

Hold:     HELD ──▶ CONFIRMED
            ├───▶ EXPIRED   (TTL passed)
            └───▶ RELEASED  (user cancelled)

Payment:  PENDING ──▶ SUCCEEDED ──(hold invalid)──▶ REFUNDED
              └─────▶ FAILED
```

### 5.2 Two users, same seat, same millisecond
1. A and B both `POST /shows/{id}/holds {seats:[A12,A13]}`.
2. Seat ids are **sorted** (consistent lock order → no deadlocks between overlapping multi-seat requests).
3. The active **lock strategy** (§6) tries to claim all seats **all-or-nothing** in one transaction.
4. Exactly one wins → `201 {holdId, expiresAt}`. The loser gets `409 SEATS_UNAVAILABLE {conflicting:[A12,A13]}`.
5. After commit, `SeatsHeld` is pushed over SSE, so every open seat map greys out A12/A13.

### 5.3 Checkout (no DB transaction held across the payment call)
```
POST /holds/{id}/checkout   (Idempotency-Key required)
 tx1: verify hold is mine & HELD & not expired → insert payment(PENDING)
 ──── call mock gateway (outside tx; may be slow / fail) ────
 tx2: if payment FAILED → mark FAILED, hold stays HELD until TTL (user may retry)
      if SUCCEEDED:
         UPDATE holds SET status='CONFIRMED' WHERE id=:h AND status='HELD' AND expires_at > now()
         ├─ 1 row  → seats HELD→BOOKED (WHERE hold_id=:h AND status='HELD'), GA held→sold, insert booking  ✅
         │           (affected seat rows must equal the hold's seat count, otherwise roll back and refund)
         └─ 0 rows → hold expired/lost → payment REFUNDED (local compensation)  ↩
```
`now()` in these snippets means a `:now` parameter supplied from the injected Java `Clock`, never the database clock. That keeps expiry logic testable with a fake clock.
That last branch (payment succeeded just after the hold expired) is the edge case the Saga will take over in Phase 2.

### 5.4 Expiry
- **Lazy (reserved seats only)**: every claim treats `status='HELD' AND held_until < now()` as available, so an expired seat can be re-held *before* the sweeper reaches it.
- **Active**: `@Scheduled` sweeper every 1 s (with a 3-min concert TTL, a 5 s sweep lag would keep seats out of the pool for ~3% of their hold window), in batches of N, using `FOR UPDATE SKIP LOCKED` so it never fights live checkouts. It marks holds `EXPIRED`, frees seats / decrements GA `held`, and emits `SeatsReleased`.
- ⚠️ **Sweeper safety rule**: because of lazy expiry, a seat from expired hold A may already belong to hold B. The sweeper must only free rows that *still* point at A: `UPDATE show_seats SET status='AVAILABLE', hold_id=NULL WHERE hold_id=:a AND status='HELD'`. It must never free "the seats listed in A's hold_items".
- **GA has no lazy path**: a counter can't tell which of its `held` units are stale, so GA capacity only comes back when the sweeper decrements `held` for expired holds (in the same transaction that marks the hold `EXPIRED`). That makes the 1 s sweep interval critical for concert pits.

---

## 6. Pluggable Lock Strategies (the core experiment)

Selected via `ticketforge.inventory.strategy=...` and switchable at runtime with `PUT /admin/inventory/config`, so a k6 run sets it in `setup()`. *(Changed from a per-request header during build.)*

### Reserved seats
| Strategy | Mechanism | Expected behaviour under contention |
| :--- | :--- | :--- |
| `PESSIMISTIC` | `SELECT … FOR UPDATE ORDER BY seat_id` → check → `UPDATE` | Correct; requests queue on row locks; latency climbs, pool exhausts |
| `OPTIMISTIC` | read with `version` → `UPDATE … WHERE version=:v` → retry w/ jittered backoff (max 3) | Great at low contention, retry storms at high |
| `ATOMIC_CONDITIONAL` | single `UPDATE … WHERE status available` + affected-rows check | One round trip; likely the winner |
| `SKIP_LOCKED` | `FOR UPDATE SKIP LOCKED` (used for **"best available N seats"** in a tier) | Never waits; concurrent buyers get different seats |

### General admission (hot counter)
| Strategy | Mechanism |
| :--- | :--- |
| `PESSIMISTIC` | `SELECT … FOR UPDATE` on the `ga_inventory` row |
| `OPTIMISTIC` | version check + retry |
| `ATOMIC_CONDITIONAL` | `UPDATE ga_inventory SET held=held+:n WHERE capacity-held-sold >= :n` |
| Sharded counter | capacity split across K shard rows, pick a random shard, fall over to others when one is empty. Spreads the hot row. *(Built as a **show property** `ga_shards`, not a strategy: a show's GA capacity lives either in one row or in K shards, never both, or capacity would double.)* |

Every strategy implements one interface:
```java
interface SeatClaimStrategy {
    ClaimResult claimSeats(ShowId show, SortedSet<SeatId> seats, HoldId hold, Instant until);
    ClaimResult claimGa(ShowId show, TierId tier, int qty, HoldId hold, Instant until);
}
```
and must pass the same **concurrency conformance test suite** (N threads × same seats → exactly one winner; GA → never exceeds capacity).

---

## 7. Booking Rules

### 7.1 Show categories

| | `MOVIE` (BookMyShow / Fandango) | `CONCERT` (Ticketmaster / flash sale) |
| :--- | :--- | :--- |
| Hold TTL | **7 min** (industry range 5–8). Long enough to type card details and finish OTP / 3-D Secure | **3 min** (industry range 2–3). Unpaid seats go straight back to the pool |
| Typical venue | 150–400 reserved seats per screen | 20k–60k: reserved stands + a large **GA pit** |
| Max seats per hold | 10 | 6 |
| Holds per user per show | unlimited | **1 active** (anti-scalping) |
| Waiting room | off by default | **on by default**; opens at `sale_opens_at` |
| Traffic shape | steady, many shows in parallel | huge spike on **one** show in the first seconds of the sale |
| Contention profile | spread across many rows | everyone wants the same front sections and pit: hot rows and a hot counter |

The category only supplies **defaults**. Every value lives on the `shows` row, so an admin can create, say, a premiere movie with a 3-minute TTL and a queue.

Why short TTLs matter for the experiments: a 3-minute hold means much more **churn**. Seats cycle through held → expired → re-held several times during one on-sale. That puts load on the sweeper, the SSE fan-out and the claim path together, and it's a pattern the movie profile never shows.

### 7.2 Rules

- **Max seats per hold**: `shows.max_seats_per_hold` (category default). Validated before touching locks.
- **Holds per user**: `shows.max_holds_per_user`. Enforced with a partial unique index `holds(show_id, user_id) where status='HELD'` when the limit is 1. Before claiming, the hold path first expires the user's own stale hold for that show (if `expires_at < now`), so a not-yet-swept hold doesn't lock the user out.
- **Per-tier pricing**: price is looked up from `show_tier_prices` and snapshotted into `hold_items.price_minor` at hold time, so later price edits don't affect existing holds.
- **No single-seat gaps** (reserved tiers only): reject a selection if it would leave an AVAILABLE seat isolated between two non-available seats (or a non-available seat and an aisle/row edge) in the same row and section.
  - It's evaluated against the row's current state, *inside* the claim transaction.
  - **Deliberate trade-off**: with `ATOMIC_CONDITIONAL`, two concurrent holds on the same row can very occasionally *combine* into a gap. This is a UX rule, not a correctness invariant, so we accept it and **count it** (`gap_rule_race_total`). Only `PESSIMISTIC` (locking the whole row) prevents it fully, which is a good interview talking point.

---

## 8. Idempotency Keys

Applies to `POST /holds`, `POST /holds/{id}/checkout`, `POST /auth/signup`.

1. Client sends `Idempotency-Key: <uuid>`.
2. A filter does `INSERT … (status='IN_PROGRESS', request_hash)` in its own transaction.
   - Insert succeeds → process the request, then store the response and mark `COMPLETED`.
   - Conflict and `COMPLETED` with the same hash → **replay** the stored response.
   - Conflict and `IN_PROGRESS` → `409 REQUEST_IN_PROGRESS`.
   - Conflict with a different hash → `422 IDEMPOTENCY_KEY_REUSED`.
3. Keys expire after 24 h (cleanup job).
4. The mock gateway also takes our `payment.id` as its idempotency key, so a retry never double-charges.

---

## 9. Waiting Room (Token-Gated FIFO)

For shows with `queue_enabled = true` (the default for concerts). Users who arrive before `sale_opens_at` wait on a holding page, and admission starts when the sale opens:
1. `POST /shows/{id}/queue` → inserts into `queue_entries`, returns the `position`. Re-joining returns the existing position.
2. `GET /shows/{id}/queue/stream` (SSE) pushes `{position, aheadOfYou, estimatedWaitSec}` every couple of seconds.
3. The **admission scheduler** (every 1 s) admits the next `admitRatePerSec` users in position order and sets `admitted_at`.
4. An admitted user fetches `GET /shows/{id}/queue/access` → a **signed, show-scoped access JWT** (TTL 10 min).
5. `POST /holds` on a queue-enabled show requires a valid access token for that show and user; otherwise `403 QUEUE_REQUIRED`.

Phase 1 uses a **fixed** admission rate. Making it adaptive to downstream latency is the Phase 3A gateway's job.

---

## 10. Real-time Seat Updates (SSE)

- `GET /shows/{id}/seats/stream`: on connect, sends a `snapshot` event, then `held` / `released` / `booked` deltas with a monotonically increasing `seq` (the client re-fetches the snapshot if it detects a gap).
- `realtime` keeps a `ConcurrentHashMap<ShowId, Set<SseEmitter>>` and fans out on `AFTER_COMMIT` events, so clients never see a hold that was rolled back.
- Heartbeat comment every 15 s. Dead emitters are removed on error or timeout.
- **Known limitation** (on purpose): this only works with a single instance. Scaling out needs pub/sub, which is one of Phase 2's motivations.

---

## 11. Auth & Mock Payment

- **Auth**: `POST /auth/signup`, `POST /auth/login` → access JWT (15 min) + refresh token. BCrypt passwords. Spring Security resource server with HMAC JWTs. `ADMIN` role for catalog writes and strategy overrides.
- **Mock gateway**: an in-process module behind a `PaymentGateway` interface (so it can become a separate service later).
  - Config: `latency-ms` (fixed + jitter), `failure-rate`, `timeout-rate`.
  - Admin endpoint to change these at runtime, which gives Phase 1 a tiny chaos knob.
  - Timeouts are treated as *unknown*: the payment stays `PENDING` and a reconciler job queries the gateway by idempotency key and resolves it.

---

## 12. API Surface

| Method & Path | Auth | Notes |
| :--- | :--- | :--- |
| `POST /auth/signup`, `POST /auth/login`, `POST /auth/refresh` | — | |
| `POST /admin/venues` (+ layout: sections/rows/seats/tiers) | ADMIN | bulk layout from a compact spec |
| `POST /admin/events`, `POST /admin/shows`, `PUT /admin/shows/{id}/prices` | ADMIN | creating a show materializes `show_seats` / `ga_inventory` |
| `PUT /admin/payment-gateway/config` | ADMIN | chaos knob |
| `GET /events`, `GET /events/{id}/shows`, `GET /shows/{id}` | — | |
| `GET /shows/{id}/seats` | — | seat map snapshot |
| `GET /shows/{id}/seats/stream` | — | SSE |
| `POST /shows/{id}/queue`, `GET …/queue/stream`, `GET …/queue/access` | USER | waiting room |
| `POST /shows/{id}/holds` | USER | body: `{seats:[…]}` or `{tierId, quantity}` or `{tierId, bestAvailable:n}`; Idempotency-Key |
| `DELETE /holds/{id}` | USER | release |
| `POST /holds/{id}/checkout` | USER | Idempotency-Key |
| `GET /me/bookings` | USER | |

Errors use one model: `{code, message, details}` (`SEATS_UNAVAILABLE`, `GAP_RULE_VIOLATION`, `HOLD_EXPIRED`, `QUEUE_REQUIRED`, `MAX_SEATS_EXCEEDED`, …). OpenAPI via springdoc.

---

## 13. Observability

Micrometer → Prometheus → Grafana (dashboard JSON committed to the repo).

| Metric | Why |
| :--- | :--- |
| `holds_attempt_total{strategy,result}` (success / conflict / error) | conflict rate per strategy |
| `hold_claim_latency{strategy}` histogram (p50/p95/p99) | the headline number |
| `optimistic_retries_total`, `db_deadlocks_total`, `db_lock_timeouts_total` | contention symptoms |
| `hikaricp_connections_pending`, `…_acquire` | pool exhaustion: where the monolith dies |
| `holds_expired_total`, `late_payment_refunds_total` | flow health |
| `gap_rule_race_total` | §7 trade-off |
| `queue_depth{show}`, `queue_admitted_total` | waiting room |
| `sse_connections{type}` | fan-out load |
| `invariant_violations_total` | **must always be 0** |

Postgres side: `pg_stat_statements` + `postgres_exporter` (lock waits, tuples updated, connections).
Container side (Phase 1, docker-compose): `cAdvisor` for CPU/RAM per container.

### 13.1 Logs (groundwork for Splunk / pod-level views)

- JSON logs on stdout via Spring Boot structured logging. Every line carries `trace_id`, `span_id`, `instance` (container/pod name from an env var) and, where relevant, `user_id`, `show_id`, `hold_id`.
- **Log budget**: business events (hold created/expired, booking confirmed, refund) at INFO. Per-request access logs are **sampled** (e.g. 1%) plus all 4xx/5xx. Under load this keeps volume within Splunk's free tier (~500 MB/day).
- The logging backend (Splunk, Loki, or both through an OpenTelemetry Collector), per-pod CPU/RAM/restart dashboards, tracing and alerting are **still to be decided** before Phase 2 (§18). Phase 1 only has to emit good logs.

---

## 14. k6 Baseline Suite

| # | Scenario | Setup | We measure |
| :--- | :--- | :--- | :--- |
| 1 | **Catalog read baseline** | ramp reads to the knee | max read RPS, p99 |
| 2 | **Same-seat stampede** | 5,000 VUs all hold `A12` at once | exactly 1 winner, latency of the 4,999 losers |
| 3 | **Hot-row GA flash sale** | 10,000 VUs on 1,000 GA tickets | sold == 1000 exactly, throughput per strategy |
| 4 | **Realistic mix** | browse → hold → 70% pay / 20% abandon / 10% payment fail | end-to-end p99, refunds, expiry correctness |
| 5 | **Strategy matrix** | scenarios 2+3 × each strategy × {50, 500, 5k} VUs | the comparison table / chart for the README |
| 6 | **Retry storm** | clients retry every request 3× with the same Idempotency-Key | 0 duplicate holds / charges |
| 7 | **Waiting-room surge** | 20k users join in 5 s | queue fairness (FIFO order kept), admission rate held |
| 8 | **Concert on-sale (the big one)** | 50k-seat stadium + 10k GA pit, 3-min TTL, queue on. 0 → 50k VUs at `sale_opens_at`; 60% target the front sections/pit; 25% abandon their hold (forced expiry churn) | sell-out time, exactly-capacity sold, re-sale of expired seats, p99 under spike, which resource saturates first |
| 9 | **Concert with queue OFF** | same as #8, but everyone hits `/holds` directly | how much the waiting room protects the monolith (compare with #8) |
| 10 | **Movie Friday-evening mix** | 200 shows × 300 seats, 7-min TTL, steady 2k VUs | baseline for "normal" load; contrast with #8 |

> 50k VUs is more than one k6 machine handles comfortably. Run k6 on 2–3 machines (or k6 Operator later in Phase 5). Phase 1 can start at 10–20k on one box.

After each run, `scripts/verify-invariants.sql` checks §4 invariants and fails the run if any break.
The deliverable is a **"Monolith breaking point" report**: the throughput knee, the p99 curve, and which resource saturates first (Hikari pool? row locks? CPU?).

---

## 15. Frontend (minimal)

Static files served by Spring (`/ui`):
- Show list → show page with a **live seat map** (SSE), tier colours and prices.
- Click seats → client-side gap-rule hint → Hold → **countdown timer** → Pay (mock) → booking confirmation.
- Waiting room screen with a live position.
- An "Open 2 tabs and click the same seat" demo is the money shot for a demo video.

---

## 16. Testing Strategy

- **Unit**: gap rule, pricing, state transitions (with a fake `Clock`).
- **Integration**: Testcontainers Postgres; the **concurrency conformance suite** runs for every strategy (`@ParameterizedTest`, 200 threads via a `CountDownLatch` start gate).
- **Module boundaries**: Spring Modulith `verify()`.
- **Load**: the k6 suite in §14 against docker-compose.

---

## 17. Milestones

| # | Milestone | Done when |
| :--- | :--- | :--- |
| M0 | Scaffold | Spring Boot 4.1 + Java 21, module packages, JSON logging, docker-compose (pg, prometheus, grafana), Flyway, Modulith test, Testcontainers smoke test, CI build |
| M1 | Auth + Catalog | JWT login, admin creates venue/layout/event/show with category defaults; seeder generates a movie multiplex (10 screens) and a 50k-seat concert stadium with a GA pit |
| M2 | Holds (reserved + GA) with `ATOMIC_CONDITIONAL` | conformance suite green; expiry sweeper; max-seats + pricing |
| M3 | Checkout + mock payment + idempotency | full Hold→Pay→Confirm, late-payment refund, reconciler, retry-safe |
| M4 | Remaining lock strategies + gap rule | all 4+1 strategies pass conformance; gap-rule race metric |
| M5 | SSE + waiting room | live seat map, queue with access tokens |
| M6 | Metrics + Grafana + minimal UI | dashboards live; UI demo flow works end-to-end |
| M7 | k6 suite + baseline report | all 10 scenarios run, movie vs concert profiles compared; invariants verified; breaking-point report written |

---

## 18. Open Questions (decide later)

- Refresh tokens: stateless or stored (revocable)? Leaning stored.
- Should `bestAvailable` prefer contiguous seats (harder; needs a scoring query)? Start with "any N in tier", stretch goal contiguous.
- Concert holds: allow **one** short extension (+60 s) while a 3-D Secure / OTP page is open? Ticketmaster doesn't; leaning no.
- Concert anti-bot: add a simple CAPTCHA / proof-of-work token at queue join? Probably a Phase 3 gateway feature.
- Observability for Phase 2 pods: Splunk vs Loki vs both (via an OpenTelemetry Collector)? Tracing with Tempo or Jaeger? Alerts (pod restart / OOMKilled / p99 / invariant violations) to Discord, Telegram or email? Where should Splunk run (laptop or cloud droplet)?
