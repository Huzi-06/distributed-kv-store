# Distributed Key-Value Store (Java)

A key-value store built from scratch in Java that runs across multiple
nodes, replicates data for fault tolerance, detects and routes around
failed nodes, and lets you tune the consistency/availability tradeoff
via quorum parameters — implementing the core mechanisms behind
production systems like **DynamoDB**, **Cassandra**, and **Riak**.

Built as a systems-programming portfolio project to demonstrate
distributed systems fundamentals: consistent hashing, replication,
failure detection, and quorum consensus — with measured performance
data, not just a working demo.

## Features

- **Durable single-node storage** — write-ahead log (WAL) survives crashes/restarts
- **Consistent hashing** — sharding with virtual nodes for even key distribution
- **Replication** — configurable replication factor (N) across nodes
- **Quorum reads/writes** — tunable W/R consistency (Dynamo-style)
- **Failure detection** — heartbeat-based, automatically routes around dead nodes
- **Read repair** — stale replicas are patched up lazily on read
- **Benchmark tooling** — measures real throughput and p50/p95/p99 latency

## Quick start

**1. Build:**
```bash
mvn package
```

**2. Start a 3-node cluster** (one command per terminal):
```bash
java -jar target/kvstore.jar node1 8081 data/node1-wal.log "node1=localhost:8081,node2=localhost:8082,node3=localhost:8083"
java -jar target/kvstore.jar node2 8082 data/node2-wal.log "node1=localhost:8081,node2=localhost:8082,node3=localhost:8083"
java -jar target/kvstore.jar node3 8083 data/node3-wal.log "node1=localhost:8081,node2=localhost:8082,node3=localhost:8083"
```

**3. Talk to it from any node — writes replicate automatically:**
```bash
curl -X PUT -d "hello" http://localhost:8081/key/foo
curl http://localhost:8082/key/foo   # -> hello, from a different node
curl http://localhost:8083/key/foo   # -> hello
```

**4. Kill a node and watch the cluster keep working** (quorum = 2 of 3):
```bash
# Ctrl+C the node3 terminal, then:
curl -X PUT -d "still works" http://localhost:8081/key/bar   # still succeeds
```

**5. Run the benchmark:**
```bash
java -cp target/classes kvstore.Benchmark http://localhost:8081 2000 20
```

## Architecture

**Single node.** An in-memory map (`ConcurrentHashMap`) backed by a
write-ahead log on disk, so a single node's data survives a restart.

**Sharding (Consistent Hashing).** Keys are distributed across nodes
using a hash ring with virtual nodes (100 per physical node) for even
load distribution. Adding/removing a node only reshuffles a small
fraction of keys, not the whole dataset.

**Replication.** Each key is stored on N nodes (the "preference list"
from the ring), not just one. Writes fan out to all N in parallel.

**Failure Detection.** Every node heartbeats every other node every
second; a node that hasn't responded in 3 seconds is marked dead and
skipped when computing preference lists, so the cluster keeps serving
requests around a failure.

**Quorum (W/R).** Writes only need W of N nodes to acknowledge; reads
only need R of N to respond. As long as W + R > N, every read is
guaranteed to see the most recent write, even though no single request
talks to every replica. Default: N=3, W=2, R=2.

## Consistency model

This system provides **tunable eventual consistency**, not strong
consistency. Concretely:

- With W+R > N (e.g. W=2, R=2, N=3), reads are guaranteed to overlap
  with the most recent write's acknowledgment set — this gives you
  **read-your-writes** consistency in the common case.
- Conflicting/stale replica data is resolved with **last-write-wins**
  based on wall-clock timestamps, and lazily repaired via **read
  repair** (a stale replica gets patched up the next time it's read).
- This is NOT linearizable — under network partitions, it's possible
  (though bounded by the quorum overlap) for a read shortly after a
  write to miss it if the timing/failure pattern is unlucky. This is a
  deliberate, well-known tradeoff (the same one DynamoDB makes), worth
  stating explicitly rather than overclaiming strong consistency.

## Known simplifications

- **Static cluster membership** — nodes are configured via a fixed list
  at startup, not discovered dynamically. Real systems use gossip
  protocols for membership.
- **Tombstones are never garbage collected** — deleted keys leave a
  permanent marker. Real systems expire tombstones after a delay.
- **Clock-based last-write-wins** — relies on wall-clock timestamps,
  which can be wrong if node clocks drift. Real systems often use
  vector clocks or hybrid logical clocks for a more rigorous ordering.
- **No live rebalancing** — adding a node requires restarting the
  cluster with a new config, rather than a runtime ring change.

## Measured results

N=3, W=2, R=2; 2000 requests, concurrency=20:

| Scenario            | Alive nodes | Failures    | Throughput     | p50  | p95  | p99  |
|----------------------|-------------|-------------|----------------|------|------|------|
| Healthy cluster      | 3/3         | 0/2000      | 1118.6 req/s   | 15ms | 30ms | 50ms |
| One node down        | 2/3         | 0/2000      | 2059.7 req/s   | 7ms  | 19ms | 30ms |
| Two nodes down       | 1/3         | 2000/2000   | (all rejected) | —    | —    | —    |

**Takeaways:**
- The cluster stayed fully available with 1 node down (2/3 still meets
  W=2), and correctly rejected 100% of writes once quorum became
  unreachable (1/3, below W=2) — the availability/consistency tradeoff
  enforcing itself under real load, not just in a single manual test.
- Counterintuitively, throughput was *higher* with one node down. Each
  write fans out to N targets in parallel and waits on all of those
  responses before returning, even though only W acks are required —
  so fewer replication targets means less coordination overhead per
  write. More replicas improve durability, but add coordination cost
  to every write.

## Tech stack

Java 17, built-in `HttpServer`/`HttpClient` (no external networking
dependencies), Maven.

## Project structure

```
src/main/java/kvstore/
  VersionedValue.java       # value + timestamp + tombstone (conflict resolution)
  WriteAheadLog.java        # durability layer
  KVStore.java              # core map + WAL-backed read/write logic
  NodeInfo.java              # node identity (id, host, port)
  ConsistentHashRing.java   # sharding
  ClusterConfig.java        # cluster membership parsing
  HttpUtil.java              # node-to-node HTTP client
  FailureDetector.java      # heartbeat-based failure detection
  ClusterCoordinator.java   # replication + quorum + read repair
  Server.java                 # HTTP API (client-facing + internal)
  Main.java                   # entry point
  Benchmark.java             # load-testing tool
```
