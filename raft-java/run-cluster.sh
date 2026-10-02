
set -uo pipefail
cd "$(dirname "$0")"

PORTS=(8051 8052 8053)

# Kill by REAL OS pid: taskkill on Windows/Git Bash (plain kill cannot stop a native java.exe), kill -9 elsewhere.
kill_pid() {
  if command -v taskkill >/dev/null 2>&1; then
    taskkill //F //PID "$1" >/dev/null 2>&1
  else
    kill -9 "$1" 2>/dev/null
  fi
}

# Real OS pid of the node started with "-port $1". jps (shipped with the JDK) reports native pids,
# which on Git Bash differ from the shell's $!. Prints nothing if jps is unavailable.
node_pid() {
  jps -m 2>/dev/null | awk -v p="$1" 'index($0, "-port " p " ") {print $1; exit}'
}

# 1. stop a cluster we started earlier
if [[ -f .pids ]]; then
  while read -r _ pid; do kill_pid "$pid" || true; done < .pids
  rm -f .pids
  sleep 1
fi

# 2. refuse to start on top of nodes that are still alive (they would keep the ports and the old state)
for port in "${PORTS[@]}"; do
  if curl -s --max-time 0.5 "http://localhost:$port/v1/status" >/dev/null 2>&1; then
    echo "Port $port is already served by a running node (probably left over from an earlier run)." >&2
    echo "Run ./stop-cluster.sh. On Windows, if it still answers: netstat -ano | findstr :$port" >&2
    echo "then: taskkill //F //PID <the pid in the last column>" >&2
    exit 1
  fi
done

if [[ "${1:-}" == "--fresh" ]]; then
  rm -rf data
fi

# 3. build
rm -rf out && mkdir -p out logs data
if ! javac --release 17 -d out $(find src/main/java -name '*.java'); then
  echo "Compilation failed (a full JDK 17+ is required, not just a JRE)." >&2
  exit 1
fi

# 4. launch
start_node() {
  local id=$1 port=$2 peers=$3
  nohup java -cp out raftkv.RaftNodeServer \
      -id "$id" -port "$port" -peers "$peers" -data "data/$id" \
      > "logs/$id.log" 2>&1 &
  echo "$id $!" >> .pids   # provisional; replaced by the real OS pid below when jps is available
}

start_node node1 8051 "node2=localhost:8052,node3=localhost:8053"
start_node node2 8052 "node1=localhost:8051,node3=localhost:8053"
start_node node3 8053 "node1=localhost:8051,node2=localhost:8052"

echo "Started 3 nodes (logs in ./logs). Waiting for a leader..."
for _ in $(seq 1 50); do
  for port in "${PORTS[@]}"; do
    if curl -s --max-time 0.3 "http://localhost:$port/v1/status" | grep -q "state=LEADER"; then
      # record the real OS pids so stop-cluster.sh and test-cluster.sh can really kill the processes
      i=0; : > .pids.new
      while read -r id pid; do
        real=$(node_pid "${PORTS[$i]}")
        echo "$id ${real:-$pid}" >> .pids.new
        i=$((i + 1))
      done < .pids
      mv .pids.new .pids
      curl -s "http://localhost:$port/v1/status"
      echo "Cluster ready. Try: ./test-cluster.sh"
      exit 0
    fi
  done
  sleep 0.1
done
echo "No leader elected within 5s; check logs/*.log (a 'BindException' means the port is taken)." >&2
exit 1
