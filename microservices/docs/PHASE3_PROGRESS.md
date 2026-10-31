# Phase 3 progress: replacing off-the-shelf parts with our own

Order: Redis (3B) first, then single-node Kafka (3C), then Raft, then the gateway (3A), then the DB (3D). Each swap is measured against a control run of the real component taken the same day.

## 3B: ForgeKV replaces Redis (done, first pass)

`custom/forgekv/` is the Java Redis-compatible server from commit `d5fc407` (RESP2, thread-per-core shards, AOF, replication, `DECRIF`/`CAS`/`CAD`). Because it speaks RESP, **catalog-service needed no code change**: `RedisCatalogCache` uses only `GET`, `SET ... PX` and `DEL`, which ForgeKV implements. The swap is one setting, `REDIS_PORT=6381`.

```bash
cd custom/forgekv && mvn -q -DskipTests package
taskset -c 0,1,6,7 ./forgekv --port 6381 --shards 1 &      # same cores Redis shared with the services
REDIS_PORT=6381 scripts/run-stack.sh && REDIS_PORT=6381 scripts/smoke.sh
```

| Check | Result |
| :--- | :--- |
| `scripts/smoke.sh` (login, show read cached, hold, pay, booking) | passes against ForgeKV |
| `redis-benchmark` GET/SET, 20 clients | 73.6k GET/s, 57.3k SET/s, p50 0.14 / 0.18 ms |
| k6 catalog reads 3k req/s (after warm-up) | Redis 2,030 req/s p99 61 ms; ForgeKV 2,031 req/s p99 43 ms; 0 failures both |
| k6 catalog reads 8k req/s (overload) | Redis 3,001 req/s, 2.5% failed; ForgeKV 2,992 req/s, 2.6% failed |
| Invariants after both runs | all nine 0 |

Reading it honestly:
* The 8k run is past saturation of the shared 4-thread app tier, so it measures the services, not the cache. Parity there means the cache is not the bottleneck.
* An earlier run of this scenario (3,467 req/s) was higher than today's control (3,001), which is why the control was re-run. Compare only runs from the same session.
* Single runs on a shared machine: treat differences under about 20% as noise.
* Not yet tested with ForgeKV: killing it mid-load (the circuit breaker is the same code), persistence/AOF on, multiple shards, the `DECRIF` flash-sale primitive wired into the waiting room.

Raw files: `loadtest/results/p3-*.json`.

## Next
1. Kill-ForgeKV experiment (reuse `scripts/chaos.py` redis case with the port changed).
2. 3C: single-node mini-Kafka behind the outbox relay and the consumers.
