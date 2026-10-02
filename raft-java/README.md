# Distributed Raft Key-Value Store (Java 17, JDK only)

A replicated key-value store built on the Raft consensus algorithm, written from scratch with **no external
dependencies**: only the JDK (`java.nio`, `java.util.concurrent`, `java.net.http`, `com.sun.net.httpserver`,
`java.util.zip.CRC32`). A cluster of nodes elects a leader, replicates writes through a durable log, and keeps
serving after a node is killed.

## Features

- **Leader election** with randomized election timeouts (150-350 ms) and 50 ms heartbeats.
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
│   ├── RaftCore.java        pure state machine: elections, replication, commit rules
│   ├── Action.java          side effects requested by the core (persist, append, send, apply, timers)
│   ├── RaftNodeServer.java  runtime: timers, HTTP server, outbound RPCs, executes actions, main()
│   ├── BinaryWAL.java       framed, checksummed write-ahead log
│   ├── StateStore.java      atomic persistence of currentTerm / votedFor
│   ├── KVStore.java         in-memory map guarded by a ReentrantReadWriteLock
│   ├── Messages.java        RPC messages and their binary encoding
│   ├── LogEntry.java        replicated log entry
│   └── Command.java         PUT / DELETE / NOOP commands
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

## HTTP API

| Endpoint | Behavior |
|---|---|
| `PUT /v1/keys/{key}` | The request body is the value. On the leader: replicate, wait up to 2 s for quorum commit, apply, return `200`. On a follower: `307` with `Location` pointing at the leader. |
| `GET /v1/keys/{key}` | `200` with the value, or `404`. Served from the local applied state. |
| `GET /v1/keys/{key}?consistent=true` | Linearizable read: goes to the leader, which commits a no-op through the log first. Followers answer `307`. |
| `DELETE /v1/keys/{key}` | Same path as `PUT`. |
| `GET /v1/status` | One line: id, state, term, leader, commit index, last log index, key count. |
| `POST /raft/request-vote`, `POST /raft/append-entries` | Internal peer RPCs with binary bodies. |

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

Failover is the time for the survivors to notice the missing heartbeat and elect a new leader. With election
timeouts drawn from 150-350 ms plus a vote round trip, it is typically between 180 and 400 ms and varies from run
to run. It is **not guaranteed to be under 300 ms**: the election window itself extends to 350 ms, and a split vote
costs another round. The figure printed by the script also includes `curl` polling overhead.

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
- If a disk write fails, the node halts instead of continuing without durability.
- Network messages travel over the JDK `HttpClient` and `HttpServer`; the Raft RPCs use a compact binary encoding.

## Limitations

This is a correct, tested Raft core, not a hardened production system. It does **not** include:

- snapshots or log compaction (the log grows without bound)
- cluster membership changes (the node set is fixed at startup)
- pre-vote
- authentication or TLS on the HTTP endpoints
- a persistent key-value store (state is rebuilt from the log)

It has been tested on a single machine, including `kill -9` of leaders and followers and restarts from disk. It has
not been tested across multiple hosts or under network partitions.

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