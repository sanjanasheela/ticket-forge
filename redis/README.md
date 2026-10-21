# ForgeKV (Java)

A Redis-compatible in-memory data store written in Java 22. It is a port of
the Rust ForgeKV in [`../redis-rust`](../redis-rust): the same design,
on-disk format, replication protocol and command set, and the same test
suite (all 84 tests ported).

It speaks **RESP2**, so `redis-cli`, `redis-benchmark` and every standard
Redis client library work against it unchanged. On top of Redis' model it
adds thread-per-core sharding, leader–follower replication, an io_uring
persistence backend, and native flash-sale primitives.

There are no runtime dependencies: the JDK only. io_uring is reached through
the Foreign Function & Memory API (raw syscalls, no JNI and no native
library of our own).

* **Thread-per-core, shared-nothing sharding.** Each shard is one thread
  with its own NIO selector, keyspace and AOF. Nothing is locked on the data
  path.
* **Native flash-sale primitives:** `DECRIF` (decrement-if-in-stock),
  `CAS`, `CAD`. Each is atomic on its shard, with zero overselling under
  32-way contention.
* **Hand-written io_uring AOF backend** with linked `WRITE → FSYNC` chains
  and group commit.
* **Leader–follower replication**: consistent full sync without pausing the
  leader, partial resync from a backlog, `WAIT`, and leader and replicas on
  different shard counts.
* **Crash-safe AOF**: canonical time-independent log, torn-tail recovery,
  per-shard logs that can be **re-sharded across restarts**, and
  `BGREWRITEAOF`.

The AOF files and the replication stream are byte-compatible with the Rust
version. Both directions have been checked by hand: each implementation
loads (and re-shards) the other's AOF directory, and a Java replica follows
a Rust leader and vice versa.

## Quick start

Requires JDK 22+ and Maven.

```bash
mvn -q -DskipTests package          # builds target/forgekv.jar
./forgekv --port 6380                                   # Redis model: 1 thread, in-memory
./forgekv --port 6380 --shards 0                        # thread-per-core: one shard per CPU
./forgekv --port 6380 --shards 4 --appendonly \
          --appendfsync always --aof-backend uring      # durable, io_uring group commit
./forgekv --port 6381 --replicaof 127.0.0.1 6380        # read-only replica

redis-cli -p 6380 SET seat:42 alice EX 30
redis-cli -p 6380 SET stock:event:7 1000
redis-cli -p 6380 DECRIF stock:event:7 0         # 999 ... 0, then (nil): never oversells
redis-cli -p 6380 WAIT 1 100                     # block until 1 replica has it
redis-cli -p 6380 INFO shards
```

`./forgekv` is a thin wrapper around
`java --enable-native-access=ALL-UNNAMED -jar target/forgekv.jar`. Extra JVM
flags go in `FORGEKV_JAVA_OPTS`, and the log level in `FORGEKV_LOG`
(`error`, `warn`, `info`, `debug`).

| Flag | Default | |
|---|---|---|
| `--port`, `--bind` | `6380`, `127.0.0.1` | |
| `--shards N` | `1` | Shard threads. `0` = one per CPU. |
| `--appendonly` | off | Enable the AOF. |
| `--appendfilename` | `appendonly.aof` | Base name of the AOF family (see [Persistence](#persistence)). |
| `--appendfsync` | `everysec` | `always` / `everysec` / `no`, as in Redis. |
| `--aof-backend` | `auto` | `uring`, `std`, or `auto` (io_uring if the kernel allows it). |
| `--replicaof HOST PORT` | – | Start as a replica. |
| `--repl-backlog-size` | `1048576` | Bytes of stream kept for partial resyncs. |

## Architecture

```
                    acceptor thread: round-robin hand-off of new sockets
                               │
        ┌──────────────────────┼───────────────────────┐
     shard 0                shard 1        …         shard N-1      one OS thread +
   ┌──────────┐          ┌──────────┐             ┌──────────┐      one NIO Selector
   │ Db       │          │ Db       │             │ Db       │      each: nothing is
   │ AOF file │          │ AOF file │             │ AOF file │      locked inside a
   │ io_uring │          │ io_uring │             │ io_uring │      shard
   └────┬─────┘          └────┬─────┘             └────┬─────┘
        └────── mailboxes: batched cross-shard sub-commands ─────┘
                               │
             Router (the only shared state): Pub/Sub registry,
             replication hub + backlog  ──────────►  replicas
```

```
src/main/java/forgekv/
├── resp/           incremental RESP2 parser + encoder (pipelining, inline commands)
├── storage/        keyspace, lazy + active (sampled) TTL expiry, replica "passive" expiry
├── util/           Bytes (immutable byte string), ByteBuf (growable buffer), Log
└── server/
    ├── KeySlot     CRC16 key → slot → shard (Redis Cluster compatible, {hash tags})
    ├── Shard       shard thread: selector loop, mailbox, timers, group commit, durability waiters
    ├── Batch       plans a pipeline across shards, merges multi-key replies
    ├── Connection  per-client state machine, Pub/Sub mode, PSYNC hand-off
    ├── cmd/        command table: arity, write flag, key positions, handlers
    ├── aof/        canonical log, per-shard files + manifest, std & io_uring writers
    ├── uring/      hand-rolled io_uring binding over FFM (raw syscalls)
    └── repl/       replication: hub, backlog, snapshot merge, follower link, WAIT
```

### How the Rust design maps to Java

| Rust | Java |
|---|---|
| one tokio `current_thread` runtime + `LocalSet` per shard | one thread per shard running an NIO `Selector` loop |
| `async`/`.await` on other shards and on durability | continuations run on the shard's own thread: remote replies are posted back to the origin shard's mailbox, and durability waiters fire once the AOF sequence is covered |
| `mpsc::unbounded_channel` mailbox | `ConcurrentLinkedQueue` + coalesced `Selector.wakeup()` |
| bounded `mpsc` per subscriber / replica | `Outbox`: bounded queue plus a coalesced wake-up of the consumer's shard |
| two `yield_now()` passes before a group fsync | two selector passes (`selectNow`) before a group fsync |
| eventfd registered with tokio's reactor | a watcher thread blocks on the eventfd and posts "completions ready" to the shard. Only the shard thread touches the ring |
| `libc::syscall` / `mmap` | `java.lang.foreign` downcalls to `syscall`, `mmap`, `eventfd`, `open` |
| replica link task (`spawn_local`) | a daemon thread with a blocking socket that applies batches on its shard and waits for each one |

Everything else (command semantics, error strings, routing and merge rules,
the canonical AOF format, the manifest layout, snapshot merging, backlog
partial resync, `WAIT`) is a line-by-line port.

### Sharding

* **Shared-nothing.** Every key belongs to exactly one shard
  (`crc16(key or {tag}) % 16384 → slot → shard`). The shard's thread owns
  its `Db` and AOF outright, and every command is atomic on its shard,
  exactly like Redis' single event loop.
* **Batched routing.** A connection turns each pipelined read into a
  `Batch`. Commands for its own shard run **inline**. Commands for other
  shards are sent as **one mailbox message per shard per batch**. Replies are
  reassembled in request order.
* **Multi-key commands** are split and merged: `MGET` reorders,
  `DEL`/`EXISTS` sum, `KEYS`/`SCAN`/`DBSIZE` fan out, and
  `SINTER`/`SUNION`/`SDIFF` fetch the sets from their owners.
* **Atomicity contract.** A command whose keys span shards is applied shard
  by shard, like a pipeline. Keys that must change together go on one shard
  with a hash tag (`{event:7}:stock`, `{event:7}:lock`), the same rule as
  Redis Cluster. With `--shards 1` everything is atomic, as in Redis.

### Persistence

* **Canonical, time-independent log.** `SET k v EX 10` is logged as
  `SET k v PXAT <abs>`, `EXPIRE` as `PEXPIREAT`. `CAS`/`DECRIF`/`SET NX` are
  logged only if they changed something, and expiry is logged as an explicit
  `DEL`. Replay runs on a frozen clock, so it reproduces state exactly.
* **One log per shard, each self-contained.** `FLUSHALL` is logged as
  `FLUSHSHARD <shard> <n>`. Logs replay correctly in any order, so
  `--shards` can change between restarts: the data is re-partitioned into a
  new generation and the manifest is swapped atomically
  (`appendonly.manifest` → `appendonly.g<gen>.s<i>.aof`).
* **Group commit.** Replies (reads included) wait until their AOF sequence is
  durable. One `fdatasync` (or io_uring chain) covers every connection served
  in that event-loop pass, and the commit waits two loop passes so
  stragglers join the group.
* **io_uring backend.** SQ/CQ rings are mmap'd through FFM with
  acquire/release `VarHandle` access to the ring indices. Each group is a
  linked `WRITE → FSYNC` chain from a native staging buffer, with only one
  chain in flight, so a crash can only tear the tail. If io_uring is
  unavailable (old kernel, seccomp), `auto` falls back to the std
  (`FileChannel`) backend.
* `BGREWRITEAOF` compacts every shard's log (temp file + fsync + rename).

### Replication

* The replication stream is the canonical AOF stream. A replica applies it
  on a frozen clock and never expires keys itself: expired keys are hidden
  until the leader's `DEL` arrives.
* **Consistent full sync without stopping the world.** Each leader shard
  snapshots itself between two of its own commands. The hub drops a shard's
  writes that predate its snapshot and appends the ones that come after it.
* **Partial resync** with `PSYNC <replid> <offset>` from a 1 MB backlog.
* **`WAIT numreplicas timeout`**, `REPLICAOF host port | NO ONE` at runtime,
  `INFO replication`, `CLIENT KILL TYPE replica`.

### Flash-sale primitives

```bash
SET lock:event:7 <token> NX PX 5000   # acquire
CAD lock:event:7 <token>              # release only if we still own it
SET stock:event:7 1000
DECRIF stock:event:7 0                # -> 999 ... -> 0 -> (nil)  never oversells
DECRIF stock:event:7 0 4              # take 4 seats or none
CAS key expected new                  # compare-and-swap, keeps the TTL
```

## Commands

| Group | Commands |
|---|---|
| Connection / server | `PING ECHO QUIT SELECT DBSIZE FLUSHALL FLUSHDB INFO TIME COMMAND CONFIG CLIENT BGREWRITEAOF` |
| Strings | `GET SET(NX/XX/GET/EX/PX/EXAT/PXAT/KEEPTTL) SETNX GETDEL MGET MSET INCR DECR INCRBY DECRBY APPEND STRLEN` |
| Keys / TTL | `DEL UNLINK EXISTS TYPE KEYS SCAN EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST` |
| Lists | `LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX` |
| Hashes | `HSET HMSET HGET HMGET HDEL HGETALL HKEYS HVALS HLEN HEXISTS HINCRBY` |
| Sets | `SADD SREM SMEMBERS SISMEMBER SCARD SINTER SUNION SDIFF` |
| Pub/Sub | `SUBSCRIBE UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE PUBLISH PUBSUB` |
| Replication | `REPLICAOF SLAVEOF WAIT PSYNC SYNC REPLCONF` · `CLIENT KILL TYPE replica` |
| **Flash-sale (ForgeKV)** | `CAS key expected new` · `CAD key expected` · `DECRIF key floor [n]` |

`INFO` sections: `server clients stats replication persistence keyspace shards`.

## Testing

```bash
mvn test        # 84 tests, ~10 s
```

The suite is a one-to-one port of the Rust tests: the RESP parser, the
keyspace and expiry, CRC16 slots, glob matching, every command, Pub/Sub
queues, AOF replay/rewrite/re-sharding, the io_uring ring, the replication
hub, and the end-to-end integration and replication tests. The end-to-end
tests use a small RESP client in `src/test/java/forgekv/RespClient.java` in
place of the Rust `redis` crate. Highlights:

* `flashSaleNeverOversells`: 32 clients × 100 attempts race for 1,000 seats
  on 1 and 4 shards. Exactly 1,000 are sold.
* `fullSyncUnderConcurrentWritesIsExact`: a replica attaches while 4 writers
  pipeline `RPUSH`/`INCR`. Every list and counter must match.
* `dataSurvivesRestartAndReshardWithAof`: write on 4 shards, restart on 4,
  then 3 (re-partition + `BGREWRITEAOF`), then 1.
* `uringGroupCommitAndDurabilitySequencing`: 500 io_uring flushes give a
  byte-exact file with no holes.
* `reconnectResumesFromBacklog`: killed replica links resume with
  `+CONTINUE` and no second full sync.

## Benchmarks

`bench/bench.py` is the Rust suite adapted to launch the jar (it builds with
`mvn package` and warms the JIT up before measuring):

```bash
bench/bench.py --quick        # ~1 minute per suite
bench/bench.py --only throughput
```

One quick, unrepeated head-to-head on the same laptop as the Rust numbers
(Ryzen 7 7445HS; both servers with 4 shards pinned to cores 0-3,
`redis-benchmark --threads 4 -c 50 -r 1000000` pinned to 4,5,10,11):

| | Java, 4 shards | Rust, 4 shards |
|---|---:|---:|
| SET, `-P 1` | 250 k · p99 0.26 ms | 250 k · p99 0.21 ms |
| GET, `-P 1` | 267 k · p99 0.21 ms | 267 k · p99 0.19 ms |
| SET, `-P 16` | 1.82 M · p99 0.83 ms | 1.67 M · p99 0.82 ms |
| GET, `-P 16` | 2.22 M · p99 0.63 ms | 2.00 M · p99 0.66 ms |

Throughput is on par: without pipelining both hit the same ceiling, which
is probably the client. The difference is in the tail: the Java server's
worst-case latency in these runs was 30–90 ms (GC and JIT pauses) against
3–60 ms for Rust. Run the full suite before relying on any of these
numbers.

## Trade-offs and limitations

Same as the Rust version, plus a few that come from the JVM:

* **Tail latency.** GC and JIT can pause a shard; p99 is close to Rust but
  the maximum is higher. A low-pause collector (`FORGEKV_JAVA_OPTS=-XX:+UseZGC`)
  is an option for latency-sensitive deployments.
* **io_uring completions** are noticed by a small watcher thread per shard
  (blocking on the eventfd) rather than by the selector itself, since NIO
  selectors only accept JDK channels. That adds one cross-thread hand-off per
  completed chain.
* **Durable multi-shard writes are slower** than single-shard ones: each shard
  fsyncs its own file.
* **Cross-shard multi-key commands are not atomic.** Use hash tags.
* `BGREWRITEAOF` and replication snapshots run on the shard's own thread
  (there is no `fork`), so that shard pauses in proportion to its data size.
* Replication is ForgeKV ↔ ForgeKV only. Pub/Sub messages are not replicated.
* No `MULTI/EXEC`, Lua, sorted sets, streams, RESP3, ACLs or eviction
  policies yet.
