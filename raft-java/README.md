Distributed Raft Key-Value Store (Java)A high-performance, fault-tolerant distributed consensus library and replicated key-value datastore implemented in Java 17, adhering to the formal Raft Distributed Consensus Algorithm specification and inspired by Diego Ongaro's LogCabin.Supported CapabilitiesLeader Election: Automated democratic election mechanism featuring randomized election timers (150ms–350ms), pre-vote checks to prevent disruptive elections from partitioned nodes, and candidate term synchronization.Log Replication & Quorum Commit: Append-only log replication across active cluster peers; commits require agreement from a strict majority quorum ($\lfloor N/2 \rfloor + 1$).Historical Commit Safety (§5.4.2): Enforces the invariant where leaders never commit log entries from previous terms by counting replicas directly; older terms commit indirectly through current-term replication.Segmented Write-Ahead Logging (WAL): High-throughput disk storage with sequential segment files, CRC32 checksum framing, and atomic crash recovery.Snapshotting & Log Compaction: Automated periodic snapshots of the state machine to disk, truncating obsolete log segments to conserve storage and enable fast bootstrap for lagging followers.Dynamic Membership Changes: Runtime cluster reconfiguration allowing nodes to be dynamically added or removed without stopping the cluster.Transparent Client Routing: Built-in RPC client routing that tracks cluster topology and forwards write requests directly to the active leader.Pluggable State Machine Engine: Clean StateMachine interface decoupled from the consensus core, implemented with an embedded RocksDB key-value backend in the example application.Quick Start (Local 3-Node Cluster)You can launch and test a complete 3-instance Raft cluster on your local machine using the deployment scripts provided in raft-java-example.1. Build and DeployFrom the raft-java directory in Git Bash:bashcd raft-java-example./deploy.sh
This script performs the following:
1. Compiles the project and generates runtime tarballs in `raft-java-example/env/`.
2. Spins up three independent server instances:
   * **Node 1**: `127.0.0.1:8051` (Server ID: `1`)
   * **Node 2**: `127.0.0.1:8052` (Server ID: `2`)
   * **Node 3**: `127.0.0.1:8053` (Server ID: `3`)
3. Prepares a client test environment in `raft-java-example/env/client/`.

---

### 2. Test Write and Read Operations

#### Execute a Write Operation (`SET`):
```bash
cd env/client
./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello world
Execute a Read Operation (GET):Bash./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello
LicenseThis project is licensed under the Apache License 2.0.
---

### Step 2: Run `deploy.sh` in Git Bash (Not PowerShell)

In your VS Code terminal window, look at the terminal tabs on the right side:
* You have a **`powershell`** tab and a **`bash` (MINGW64)** tab.
* Click on the **`bash`** tab (or press the `+` dropdown and choose **Git Bash**).

Inside the **Git Bash** terminal, execute:

```bash
cd "/f/Code Diary/Project/Raft Distributed KV Store/raft-java/raft-java-example"
./deploy.sh
deploy.sh will run Maven, extract the runtime into raft-java-example/env/, and launch the three cluster servers in the background.Step 3: Run the Client to Verify the ClusterStill inside the Git Bash terminal:Navigate into the generated client folder:Bashcd env/client
Test Writing Data (SET):Bash./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello world
Expected output: set request, key=hello value=world response={"success":true}Test Reading Data (GET):Bash./bin/run_client.sh "list://127.0.0.1:8051,127.0.0.1:8052,127.0.0.1:8053" hello
Expected output: get request, key=hello, response={"value":"world"}Step 4: Commit and Push Everything to GitHubNow that the build succeeds, the cluster runs, and the README is in place, push everything:Bashcd "/f/Code Diary/Project/Raft Distributed KV Store/raft-java"
git add .
git commit -m "docs: finalize english README and verify cluster deployment"
git push origin main