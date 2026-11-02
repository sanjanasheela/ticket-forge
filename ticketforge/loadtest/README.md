# Load tests (k6)

Ten scenarios from the Phase 1 spec (§14). Results and findings: [docs/PHASE1_BASELINE_REPORT.md](../docs/PHASE1_BASELINE_REPORT.md).

| Script | Spec # | What it does | Main knobs |
| :--- | :--- | :--- | :--- |
| `s1-catalog.js` | 1 | Ramp read traffic (events, show, seat map) | `RATE`, `MAX_VUS` |
| `s2-stampede.js` | 2, 5 | N users hold the same seat at the same instant; exactly 1 may win | `VUS`, `STRATEGY` |
| `s3-ga-sellout.js` | 3, 5 | Sell the 10,000-ticket pit one ticket per buyer | `VUS`, `ATTEMPTS`, `STRATEGY`, `GA_SHARDS` |
| `s4-movie-mix.js` | 4, 10 | Browse, hold (pick or best available), pay or walk away | `PROFILE=friday`, `VUS`, `SHOWS`, `DURATION` |
| `s6-retry-storm.js` | 6 | Every request sent 3× at once with one Idempotency-Key | `VUS`, `ITER` |
| `s7-queue-surge.js` | 7 | 20k users join one queue within 5 s | `USERS` |
| `s8-concert-onsale.js` | 8, 9 | Concert on-sale with (`QUEUE=true`) or without the waiting room | `QUEUE`, `VUS`, `DURATION` |

## Running

The app must be up on `localhost:8080` with the dev JWT secret. Each run creates a fresh show and seeds load-test users itself.

```bash
# pin the app and Postgres to their own cores (see the report's test bed)
taskset -c 0,1,6,7 java -Xmx1500m -jar target/ticketforge-0.1.0-SNAPSHOT.jar
docker update --cpuset-cpus 2,3,8,9 <postgres-container>

cd loadtest
./run.sh s2-atomic-1000 s2-stampede.js STRATEGY=ATOMIC_CONDITIONAL VUS=1000
./run.sh s3-sharded     s3-ga-sellout.js GA_SHARDS=8
./run.sh s8-queue-off   s8-concert-onsale.js QUEUE=false VUS=1500
```

`run.sh` runs k6 in Docker pinned to `K6_CPUS` (default `4,5,10,11`). While k6 runs, `sampler.py` records app and Postgres CPU, pool usage and heap every second. Afterwards the runner checks the invariants. Output goes to `results/<name>.json` (k6 summary), `.samples.jsonl` and `.invariants.json`.

Queue fairness after `s7` (0 means nobody was skipped):

```sql
SELECT count(*) FROM queue_entries w WHERE w.show_id = :show AND w.admitted_at IS NULL
  AND w.position < (SELECT max(position) FROM queue_entries a WHERE a.show_id = :show AND a.admitted_at IS NOT NULL);
```

Full invariant check: `psql ... < ../scripts/verify-invariants.sql`.
