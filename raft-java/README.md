# Distributed Raft Key-Value Store (Java 17, JDK only)

![CI](https://github.com/AryanDevHub/distributed-raft-kv/actions/workflows/ci.yml/badge.svg)

A replicated key-value store built on the Raft consensus algorithm, written from scratch with **no external
dependencies**: only the JDK (`java.nio`, `java.util.concurrent`, `java.net.http`, `com.sun.net.httpserver`,
`java.util.zip.CRC32`). A cluster of nodes elects a leader, replicates writes through a durable log, and keeps
serving after a node is killed.

## Features

- **Leader election** with randomized election timeouts (150-350 ms) and 50 ms heartbeats.
- **Pre-vote**: before a node raises its term it polls the cluster. A node that cannot reach a majority never
  inflates its term, so a briefly disconnected node cannot force a healthy leader to step down.
- **Check-quorum**: a leader that has not heard from a majority within an election-timeout window steps down instead
  of believing it still leads.
- **Optional leader stickiness** (`-sticky true`): followers that recently heard from a live leader refuse pre-votes,
  so a rejoining node cannot depose a healthy leader at all. It costs failover latency, so it is off by default.
- **Strict-majority quorum**: `((peers + 1) / 2) + 1`, where `peers` excludes the node itself.
- **§5.4.1 vote safety**: a candidate's log must be at least as up to date as the voter's.
- **§5.4.2 commit safety**: a leader never commits an older-term entry by counting replicas. Older entries commit
  only indirectly, when an entry from the leader's current term reaches a quorum (each new leader appends a no-op
  entry to make that happen).
- **Conflict resolution**: uncommitted follower entries that disagree with the leader are truncated and overwritten.
- **Binary write-ahead log** (`BinaryWAL`): `FileChannel` + `ByteBuffer`, fsync after every append, torn or
  corrupt tails detected by magic header and CRC32 and truncated on recovery.
- **Atomic hard state** (`StateStore`): `currentTerm` and `votedFor` are written to a temp file, fsynced and
  renamed with `ATOMIC_MOVE`, so a restarted node cannot vote twice in one term.
- **Pure consensus core**: `RaftCore` does no I/O, owns no threads or timers. It takes events and returns a list
  of `Action`s that the runtime executes in order.
- **HTTP API** with automatic redirect of writes from followers to the leader (`307`).

## Requirements

- A full **JDK 17 or newer** (a JRE is not enough, you need `javac`; `jps` is used by the scripts).
- `bash` and `curl` for the scripts (Git Bash on Windows works).

## Project layout

```
raft-java/
├── src/main/java/raftkv/
│   ├── RaftCore.java        pure state machine: elections, pre-vote, replication, commit rules
│   ├── Action.java          side effects requested by the core (persist, append, send, apply, timers)
│   ├── RaftNodeServer.java  runtime: timers, HTTP server, outbound RPCs, executes actions, main()
│   ├── BinaryWAL.java       framed, checksummed write-ahead log
│   ├── StateStore.java      atomic persistence of currentTerm / votedFor
│   ├── KVStore.java         in-memory map guarded by a ReentrantReadWriteLock
│   ├── Messages.java        RPC messages and their binary encoding
│   ├── LogEntry.java        replicated log entry
│   └── Command.java         PUT / DELETE / NOOP commands
├── src/test/java/raftkv/
│   ├── SelfCheck.java            52 directed checks
│   ├── ClusterSimulation.java    fault-injection simulation of whole clusters
│   └── ProtocolScenarios.java    scripted scenarios for pre-vote, check-quorum and stickiness
├── run-cluster.sh           compile and start a 3-node cluster on ports 8051-8053
├── test-cluster.sh          end-to-end demo (redirect, replication, leader failover)
├── stop-cluster.sh          stop the nodes
└── LICENSE
```

## Quick start

```bash
chmod +x *.sh
./run-cluster.sh --fresh     # compiles, starts node1/node2/node3, waits for a leader
./test-cluster.sh            # runs the demo described below
./stop-cluster.sh
```

`--fresh` wipes `./data` first. Without it, nodes recover their term, vote and log from disk.

### Starting nodes by hand

```bash
javac --release 17 -d out $(find src/main/java -name '*.java')

java -cp out raftkv.RaftNodeServer -id node1 -port 8051 \
     -peers node2=localhost:8052,node3=localhost:8053 -data data/node1
java -cp out raftkv.RaftNodeServer -id node2 -port 8052 \
     -peers node1=localhost:8051,node3=localhost:8053 -data data/node2
java -cp out raftkv.RaftNodeServer -id node3 -port 8053 \
     -peers node1=localhost:8051,node2=localhost:8052 -data data/node3
```

| Flag | Meaning |
|---|---|
| `-id` | Node identifier, for example `node1` |
| `-port` | HTTP port to listen on |
| `-peers` | The **other** nodes, as `id=host:port`, comma separated (do not list the node itself) |
| `-data` | Directory for the WAL and hard state |
| `-sticky` | Optional, `true` or `false` (default `false`): leader stickiness, see Features |

## HTTP API

| Endpoint | Behavior |
|---|---|
| `PUT /v1/keys/{key}` | The request body is the value. On the leader: replicate, wait up to 2 s for quorum commit, apply, return `200`. On a follower: `307` with `Location` pointing at the leader. |
| `GET /v1/keys/{key}` | `200` with the value, or `404`. Served from the local applied state. |
| `GET /v1/keys/{key}?consistent=true` | Linearizable read: goes to the leader, which commits a no-op through the log first. Followers answer `307`. |
| `DELETE /v1/keys/{key}` | Same path as `PUT`. |
| `GET /v1/status` | One line: id, state (`FOLLOWER`, `PRE_CANDIDATE`, `CANDIDATE`, `LEADER`), term, leader, commit index, last log index, key count. |
| `POST /raft/pre-vote`, `POST /raft/request-vote`, `POST /raft/append-entries` | Internal peer RPCs with binary bodies. |

Status codes for writes: `200` committed, `307` not the leader, `503` no leader known yet, timed out waiting for
quorum, or leadership changed before commit.

```bash
# write through a follower and see the redirect
curl -i -X PUT --data-binary 'world' http://localhost:8052/v1/keys/hello

# same request, letting curl follow the redirect to the leader
curl -L -X PUT --data-binary 'world' http://localhost:8052/v1/keys/hello

# read from any replica
curl http://localhost:8051/v1/keys/hello
curl http://localhost:8052/v1/keys/hello
curl http://localhost:8053/v1/keys/hello

# which node is the leader?
curl http://localhost:8051/v1/status
```

A follower can trail the leader by up to one heartbeat (about 50 ms), so a plain `GET` on a follower right after a
write may briefly miss it. Use `?consistent=true` when you need to see every committed write.

A `503` on a write does not prove the write failed: if the leader timed out while waiting, the entry may still
commit later. Treat it as "unknown" and retry idempotent operations.

## Testing

```bash
mkdir -p out out-test
javac -d out src/main/java/raftkv/*.java
javac -cp out -d out-test src/test/java/raftkv/*.java
java -cp "out;out-test" raftkv.SelfCheck               # Linux/macOS: use : instead of ;
java -cp "out;out-test" raftkv.ClusterSimulation 300   # runs per scenario
java -cp "out;out-test" raftkv.ProtocolScenarios
```

- **`SelfCheck`** (52 checks): WAL torn-write and CRC recovery, atomic hard state, the quorum formula, vote safety
  (§5.4.1), commit safety (§5.4.2), conflict resolution, pre-vote, check-quorum and leader stickiness.
- **`ClusterSimulation`**: runs whole clusters of real `RaftCore` instances on a virtual clock with a fault-injecting
  network (loss, duplication, reordering, delay), partitions, and node crash/restart that keeps only what was
  persisted. After every step it checks election safety, log matching, leader completeness, state-machine safety
  and that term, vote and log are durable before any message leaves a node; after healing it checks that the
  cluster converges and keeps committing. Ten scenarios cover 1 to 5 nodes, with and without leader stickiness.
  A failing run prints its seed and the last events so it can be replayed.
- **`ProtocolScenarios`**: scripted cases that show what pre-vote and check-quorum do, and what stickiness costs.
- CI (`.github/workflows/ci.yml`) runs all three on every push.

### Do the tests catch bugs?

14 classic Raft bugs were planted one at a time into a copy of `RaftCore` (quorum one too small, vote not
persisted, log up-to-date check removed, old-term entries committed by counting replicas, follower never
overwriting conflicting entries, pre-vote skipped, check-quorum disabled, and others). Every one was caught by at
least one of the three layers. No layer catches all of them: for example the §5.4.2 bug is caught only by the
directed check, and "pre-vote ignores the candidate's log" only by `SelfCheck`.

Over 15,000 simulated cluster lifetimes (about 100 million events) the real `RaftCore` held every invariant.

### What pre-vote and check-quorum buy (from `ProtocolScenarios`)

- A follower cut off for 10 s keeps its term (20 of 20 runs). Without pre-vote the same node's term climbs from 1
  to 43 in that time.
- A leader cut off from the majority steps down within 580 ms, and the majority elects a replacement.
- On rejoin of the isolated follower, the healthy leader was deposed in 1 of 20 runs with stickiness off, and in
  0 of 20 with stickiness on.

## Demo output

`./test-cluster.sh` on a 3-node cluster:

```
== 1. PUT to a FOLLOWER without following redirects (expect HTTP 307 + Location) ==
HTTP/1.1 307
Location: http://localhost:8051/v1/keys/hello

== 3. Read the key back from every replica ==
:8051 -> world (HTTP 200)
:8052 -> world (HTTP 200)
:8053 -> world (HTTP 200)

== 4. Kill the leader and time the failover ==
old leader node1 (:8051) killed; new leader on :8053 after 302 ms
```

### About failover time

Failover is the time for the survivors to notice the missing heartbeat and elect a new leader. It is **not
guaranteed to be under 300 ms**: the election window itself runs from 150 to 350 ms, and a split vote costs
another round. Measured in the simulation (1000 leader crashes per row, network latency 1-20 ms):

| Election mode | median | mean | p95 |
|---|---|---|---|
| classic Raft (no pre-vote) | 221 ms | 248 ms | 483 ms |
| pre-vote (default) | 243 ms | 262 ms | 426 ms |
| pre-vote + `-sticky true` | 329 ms | 328 ms | 421 ms |

Pre-vote costs about one extra round trip at the median and trims the slow tail. Stickiness adds roughly another
90 ms because the first node to time out is refused and the second timer decides. Runs against real processes on
a development machine, including `curl` polling overhead, ranged from about 180 ms to 400 ms with occasional
outliers above 1 s.

## On-disk format

`data/<node>/raft.wal`: a sequence of frames, all integers big-endian:

```
4 bytes   magic 0xDEADCAFE
4 bytes   payload length N
N bytes   payload (serialized log entry)
4 bytes   CRC32 of the payload
```

`data/<node>/hardstate.bin`: magic, `currentTerm`, `votedFor`, CRC32. It is replaced atomically on every change.

On startup, `BinaryWAL.recoverAll()` reads from offset 0, stops at the first frame with a bad magic, a short
length or a wrong checksum, and truncates the file there.

The commit index and the key-value contents are **not** stored separately. After a restart the node replays its log
once it learns the commit index from the current leader, which commits a no-op on election to cover earlier entries.

## Design notes

- `RaftCore` is not thread-safe by design. `RaftNodeServer` calls it under a single lock and executes the returned
  actions under the same lock, so state is fsynced **before** any reply or request leaves the node.
- A pre-vote is a poll: it never changes the receiver's term, vote, role or timers and persists nothing.
- The leader reuses its election timer as the check-quorum tick.
- If a disk write fails, the node halts instead of continuing without durability.
- Network messages travel over the JDK `HttpClient` and `HttpServer`; the Raft RPCs use a compact binary encoding.
- The simulation tests the protocol logic, not the HTTP layer or real disk behaviour.

## Limitations

This is a correct, tested Raft core, not a hardened production system. It does **not** include:

- snapshots or log compaction (the log grows without bound)
- cluster membership changes (the node set is fixed at startup)
- authentication or TLS on the HTTP endpoints
- a persistent key-value store (state is rebuilt from the log)
- measured throughput or latency numbers (every write fsyncs under one lock, so expect modest throughput)

It has been tested on a single machine, including `kill -9` of leaders and followers and restarts from disk. It has
not been tested across multiple hosts.

## Windows notes

Run the scripts from Git Bash. They look up each node's real process ID with `jps` and stop nodes with
`taskkill`, because on Git Bash the shell's own `$!` is not the Java process ID. If the ports are still busy after a
crashed run:

```bash
netstat -ano | findstr LISTENING | findstr ":8051 :8052 :8053"
taskkill //F //PID <pid from the last column>
```

`test-cluster.sh` uses GNU `date +%s%N`, which works in Git Bash and Linux but not in the stock macOS `date`.

## License

Apache License 2.0, see [LICENSE](LICENSE).