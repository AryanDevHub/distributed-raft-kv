# Distributed Raft Key-Value Store (Java 17, JDK only)

![CI](https://github.com/AryanDevHub/distributed-raft-kv/actions/workflows/ci.yml/badge.svg)

A replicated key-value store built on the Raft consensus algorithm, written from scratch with no external
dependencies: only the JDK. A cluster elects a leader, replicates writes through a durable log, and keeps serving
after a node is killed.

**Highlights**

- Leader election with pre-vote and check-quorum, optional leader stickiness
- Raft safety rules: log up-to-date voting (5.4.1) and no commit of old-term entries by counting replicas (5.4.2)
- CRC32-framed binary write-ahead log with fsync and torn-write recovery; atomic term and vote persistence
- Pure consensus core (no I/O, no threads, no timers) separated from the network and disk runtime
- HTTP API with automatic 307 redirect from followers to the leader
- Verified by 52 directed checks, a fault-injection cluster simulation (crashes, partitions, message loss and
  reordering), and 14 deliberately planted bugs that the tests all catch

**Quick start** (needs JDK 17+, bash and curl)

    cd raft-java
    chmod +x *.sh
    ./run-cluster.sh --fresh
    ./test-cluster.sh
    ./stop-cluster.sh

Full documentation, API, design notes, measured failover times and limitations:
[raft-java/README.md](raft-java/README.md)
