# ForgeLog

A custom Kafka: a distributed, replicated commit-log broker written in Java
22. It is the "Mini-Kafka" of the **TicketForge Chaos-Cloud** project
(Phase 3C), the sibling of ForgeKV in [`../redis`](../redis).

Brokers form a cluster and every topic partition is replicated with
**Raft**. A record is acknowledged only once a quorum has it on disk, so
killing the leader during a write storm loses nothing that was
acknowledged. Producers are **idempotent**, so their retries never
duplicate. Consumers form **groups** with rebalancing and committed offsets
that are themselves replicated through Raft. No dependencies beyond the JDK.

* **Raft per partition**, written from scratch: pre-vote, check-quorum
  (a partitioned leader steps down), leader stickiness, fast log
  backtracking, pipelined replication. Leader failover after `kill -9`:
  **median 151 ms, max 240 ms** over 20 runs (3 brokers on one machine).
* **The partition log is the Raft log**: offset == Raft index, so data is
  stored once. The high watermark is the commit index, so consumers only
  see quorum-committed records.
* **Storage**: segmented log, CRC32C on every record, a **dense mmap'd offset
  index** (O(1) offset → file position), torn-tail recovery, and fetches sent
  straight from the page cache with `transferTo` (**sendfile, zero-copy**).
* **Group commit**: one loop pass covers every request served in it. All
  written logs and Raft vote state are fsynced in parallel. Append acks and
  votes go out only after the fsync.
* **Idempotent producer**: `(producerId, sequence)` per partition. A retry of
  a record already in the log (even an uncommitted one inherited by a new
  leader) is answered with its original offset.
* **Consumer groups**: eager rebalance with a join barrier, session timeouts,
  generation-fenced offset commits, committed offsets in a replicated
  `__offsets` group.
* **Fault injection built in**: `forgelog chaos` cuts brokers off from each
  other (the split-brain experiment) without `iptables`.

## Quick start

Requires JDK 22+ and Maven.

```bash
mvn -q -DskipTests package                 # builds target/forgelog.jar

./forgelog cluster                         # 3 brokers in one process, 127.0.0.1:9091-9093
# or as three real processes (so one can be kill -9'd):
scripts/cluster.sh start                   # logs and data in ./run

./forgelog topic create orders --partitions 3
./forgelog topic list                      # orders  partitions=3  leaders=[2, 3, 2]
printf 'alice A1\nbob B7\n' | ./forgelog produce orders
./forgelog consume orders --group billing
./forgelog status                          # every broker's view of every Raft group
./forgelog bench --partitions 3 --records 300000

scripts/cluster.sh kill 2                  # kill -9 a broker...
scripts/cluster.sh restart 2               # ...and bring it back; it catches up
./forgelog chaos 1 --block 2,3             # broker 1 drops all Raft traffic with 2 and 3
./forgelog chaos 1 --heal
```

Client commands take `--bootstrap host:port,...` (default
`127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093`).

| `broker` flag | Default | |
|---|---|---|
| `--id N` | required | This broker's id |
| `--cluster 1=host:port,...` | required | Every broker of the cluster, including this one |
| `--data-dir DIR` | required | Where logs and Raft state live |
| `--no-fsync` | fsync on | Rely on replication alone for durability (Kafka's default) |
| `--segment-mb` | `256` | Segment roll size |
| `--election-ms` | `150-300` | Randomized election timeout |
| `--heartbeat-ms` | `50` | Leader heartbeat interval |

Logging: `FORGELOG_LOG` (or `-Dforgelog.log`) = `error`, `warn`, `info`,
`debug`. JVM flags go in `FORGELOG_JAVA_OPTS`.

## Using it from Java

```java
try (Admin admin = new Admin("127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093")) {
    admin.createTopic("seat.reserved", 6);   // waits until every partition has a leader
}

try (Producer p = new Producer(bootstrap)) {
    p.send("seat.reserved", "event-7", "seat 42 held for alice")
     .thenAccept(m -> System.out.println("committed at " + m.partition() + "@" + m.offset()));
}

try (Consumer c = new Consumer(bootstrap, "saga-orchestrator", List.of("seat.reserved"))) {
    while (running) {
        for (ConsumerRecord r : c.poll(Duration.ofMillis(500))) {
            handle(r);                           // at-least-once: make it idempotent
        }
        c.commitSync();                          // replicated before it returns
    }
}
```

## Architecture

```
             producers / consumers / admin  (binary protocol over TCP)
                               │
     ┌─────────────────────────┼─────────────────────────┐
  broker 1                  broker 2                  broker 3
  one event loop            one event loop            one event loop
  ├ __metadata  (group 0) ◄──── Raft ────►  controller = its leader
  ├ __offsets   (group 1) ◄──── Raft ────►  group coordinator = its leader
  ├ orders-0    (group 2) ◄──── Raft ────►  each partition: its own leader
  ├ orders-1    (group 3)        ...
  └ orders-2    (group 4)
       │
  data/topics/orders-0/00000000000000000000.log    records (CRC32C)
                       00000000000000000000.index  dense mmap'd int32 positions
                       raft.state                  term + vote (fsynced)
```

```
src/main/java/forgelog/
├── log/       Record format, Segment (dense mmap index, buffered appends), CommitLog (recovery, term index)
├── raft/      RaftNode: pure Raft (no I/O), driven by tick/receive/propose/flush/onPersisted
├── protocol/  framing, request/response messages, Raft message codec
├── broker/    Broker (event loop), Replica + Metadata/Offsets/Partition replicas,
│              GroupCoordinator, ClientConn (zero-copy writes), PeerLink (broker-to-broker)
└── client/    ClusterClient, Admin, Producer, Consumer
```

### One event loop per broker

Everything a broker does runs on one thread: client and peer sockets
(NIO), every Raft group's timers and messages, the logs, the coordinator.
There are no locks and every state change happens in a defined order. Each
pass of the loop:

1. handles ready sockets; requests may propose entries to Raft groups;
2. ticks each Raft group **at its own deadline** (exact randomized election
   timeouts: quantizing them made followers time out together and split
   the vote);
3. sends each group's pending entries to its followers, one message per
   follower however many records arrived;
4. **group commit**: writes every dirty log, then fsyncs all of them and
   every changed `raft.state` in parallel, then tells Raft what is durable.
   This may commit entries and complete produce requests;
5. only then sends append acknowledgements, vote requests and vote grants.
   A broker never promises something it could lose in a crash.

### Replication

* **Raft per partition.** The partition's commit log is its Raft log. A
  record is appended by the leader, replicated, and committed once a majority
  has persisted it. Produce responses wait for the commit, and consumers read
  only below the high watermark (= commit index + 1).
* **Leader election** uses randomized 150–300 ms timeouts. **Pre-vote**
  means a node that was cut off can't bump terms and depose a healthy leader
  when it returns. **Check-quorum** means a leader that hasn't heard from a
  majority within the election timeout steps down, so a partitioned leader
  stops accepting writes it could never commit.
* A new leader appends a no-op entry for its term. Once that commits it is
  *ready* and serves fetches and coordinator requests. Until then clients get
  `LEADER_NOT_AVAILABLE` and retry, so a stale high watermark is never
  served.
* **Metadata** (topics) and **consumer offsets** are two more Raft groups on
  every broker. Applying `CREATE_TOPIC` in log order on every broker gives
  each partition the same Raft group id everywhere, with no extra
  coordination.

### Storage

* Records: `length, crc32c, offset, term, type, timestamp, producerId,
  sequence, key, value`. The same bytes are used on disk, in replication, and
  on the wire to consumers.
* Offsets are dense, so the index is an mmap'd array of int32 file positions.
  A lookup is `index[offset - base]`.
* Appends are buffered and written in large writes. Reads drain the buffer
  first.
* Recovery: rolled segments were fsynced (log and index) when they rolled and
  are only sanity-checked. The active segment is scanned record by record
  (CRC, offset continuity) and a torn tail is truncated. Term boundaries are
  rebuilt by bisecting each segment between its first and last term, with
  no full scan.
* Fetch responses carry `FileRegion`s that `ClientConn` writes with
  `FileChannel.transferTo` (sendfile). Record bytes never enter the JVM heap.

### Exactly-once into the log, at-least-once out of it

* The producer gives each partition queue a random producer id and numbers
  its records. A record leaves the head of its queue only when acknowledged,
  so every retry (timeout, broken connection, `NOT_LEADER` after a failover)
  resends the same sequence numbers. Each replica tracks the latest
  `(sequence, offset)` per producer as its log changes, including on
  truncation. A new leader therefore already knows which retried records it
  holds and answers them with their original offsets. Records are never
  written twice or reordered.
* Consumers deliver at-least-once: records after the last commit are
  redelivered after a crash or a rebalance. Commits are fenced by group
  generation, so a member that lost its partitions can't move their offsets.

### Consumer groups

The coordinator is the leader of `__offsets`. It uses Kafka's eager
protocol, with the assignment computed by the coordinator:
1. A join, leave or expired session starts a rebalance, and heartbeats then
   answer `REBALANCE_IN_PROGRESS`.
2. `JOIN_GROUP` is a long poll, answered when every member has rejoined or
   the rebalance timeout runs out. Members that didn't rejoin by then are
   dropped.
3. The generation is bumped and partitions are dealt round-robin.

Membership is soft state. If the coordinator moves, members rejoin the new
one; committed offsets are in the Raft log.

## Measurements

One laptop (Ryzen 7 7445HS, NVMe), **3 broker processes on the same
machine**, fsync on, every partition replicated 3×, 100-byte records, from
`forgelog bench` and `scripts/cluster.sh`. These are single runs on
loopback, so treat them as rough numbers.

| | |
|---|---|
| Leader failover after `kill -9` (link loss → new leader), 20 runs | median **151 ms**, max **240 ms**, 0 split votes |
| Worst produce stall while the leader is `kill -9`'d mid-stream | **288 ms** |
| …records acknowledged / lost / duplicated (200,000 sent) | 200,000 / **0** / **0** |
| Produce, unbounded pipelining, 3 partitions | **~330,000 records/s** (33 MB/s) |
| Produce, ≤1024 in flight | ~99,000 records/s, ack p50 10.6 ms, p99 16.3 ms |
| Produce, ≤64 in flight | ~7,800 records/s, ack p50 7.4 ms, p99 13.6 ms |
| Produce, 1 at a time (send → quorum fsync → ack) | p50 **2.65 ms**, p99 4.05 ms |
| Consume (zero-copy fetch), 300,000 records | ~2.2 M records/s |

The Raft simulation tests give the same failover picture: over 500 seeded
runs the time to a *ready* new leader was p50 184 ms and p90 252 ms. About
2% of runs need a second election after a split vote.

Two things I fixed while measuring, which are worth knowing about:
* Raft was first ticked every 10 ms, which quantized the election timeouts:
  57% of real failovers split the vote.
* `raft.state` was first fsynced synchronously inside each election. A
  broker taking over a dozen partitions at once stalled for tens of
  milliseconds, which caused 30% splits.

Both are fixed (exact deadlines, group-committed votes).

## Testing

```bash
mvn test        # 27 tests, ~15 s
```

* **Raft in deterministic simulation** (`RaftNodeTest`, `SimCluster`):
  virtual clock, message delay and loss, partitions, crash/restart. Election
  safety (one leader per term) and state-machine safety (a committed index
  never holds two different entries) are checked on **every simulated
  millisecond**. Scenarios: election, replication, failover, partitioned
  leader stepping down, minority can't commit, pre-vote keeping a rejoining
  node harmless, repair of divergent logs, and 6 seeds × 300 random chaos
  steps (crashes, partitions, 5% loss) followed by convergence checks.
* **Storage** (`CommitLogTest`): segment rolls and reopen, torn tail, a
  corrupt record in the active segment, truncation across segments, term
  index rebuilt by bisection, zero-copy regions byte-exact.
* **Real 3-broker clusters** (`ClusterTest`):
  - produce/consume with per-key order;
  - **kill the leader mid-stream: no acknowledged record lost, none
    duplicated**;
  - consumer group split, then resume from committed offsets;
  - a crashed consumer's partitions moving after its session times out;
  - **partitioned leader steps down while the majority keeps writing, then
    the logs converge**;
  - full cluster restart keeps data and committed offsets;
  - duplicate and out-of-order sequences;
  - long-poll wake-up, validation, and a single-broker cluster.

## Trade-offs and limitations

* **Replication factor = cluster size.** Every broker replicates every
  partition, and leaders spread across brokers by election. There is no
  replica placement, reassignment, or cluster membership change.
* **No retention or compaction.** Logs grow forever, including `__offsets`,
  which is replayed in full at startup. Raft snapshots and log cleaning are
  the missing pieces.
* **Throughput under modest pipelining is limited by the producer**, which
  keeps one request in flight per broker. Allowing several (Kafka allows 5
  with idempotence) would close most of the gap between the "64 in flight"
  and "unbounded" rows.
* Producer dedupe state is rebuilt at startup from the last 100,000 records
  of each partition. A retry of something older than that would be
  accepted again.
* Delivery to consumers is at-least-once. There are no transactions, so no
  exactly-once consume-process-produce.
* The protocol is ForgeLog's own binary protocol, not Kafka's: Kafka
  clients can't talk to it.
* No TLS, auth or ACLs, and no metrics endpoint yet (STATUS covers Raft state
  for a dashboard).
* io_uring isn't used here. The zero-copy story is mmap'd indexes plus
  sendfile. ForgeKV's FFM io_uring binding could be reused for appends.
