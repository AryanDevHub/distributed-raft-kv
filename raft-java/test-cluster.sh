#!/usr/bin/env bash
# End-to-end demo: write via a follower (307 redirect), read from every replica, kill the leader,
# measure failover, and verify the data survived. Requires GNU date (nanosecond timestamps).
cd "$(dirname "$0")"
PORTS=(8051 8052 8053)

status()      { curl -s --max-time 0.3 "http://localhost:$1/v1/status"; }
port_with()   { for p in "${PORTS[@]}"; do [[ "$(status "$p")" == *"state=$1"* ]] && { echo "$p"; return 0; }; done; return 1; }
now_ms()      { echo $(( $(date +%s%N) / 1000000 )); }
kill_pid() {
  if command -v taskkill >/dev/null 2>&1; then taskkill //F //PID "$1" >/dev/null 2>&1; else kill -9 "$1" 2>/dev/null; fi
}

LEADER_PORT=$(port_with LEADER) || { echo "no leader; run ./run-cluster.sh first"; exit 1; }
FOLLOWER_PORT=$(port_with FOLLOWER) || { echo "no follower found"; exit 1; }
echo "leader=:$LEADER_PORT follower=:$FOLLOWER_PORT"

echo; echo "== 1. PUT to a FOLLOWER without following redirects (expect HTTP 307 + Location) =="
curl -i -s -X PUT --data-binary 'world' "http://localhost:$FOLLOWER_PORT/v1/keys/hello"

echo; echo "== 2. PUT to the same follower WITH -L (curl follows the 307 to the leader) =="
curl -s -L -X PUT --data-binary 'world' "http://localhost:$FOLLOWER_PORT/v1/keys/hello" -w 'HTTP %{http_code} after %{num_redirects} redirect(s)\n'

sleep 0.3   # followers learn the new commitIndex on the next heartbeat (<=50ms)
echo; echo "== 3. Read the key back from every replica =="
for p in "${PORTS[@]}"; do printf ':%s -> ' "$p"; curl -s -w ' (HTTP %{http_code})\n' "http://localhost:$p/v1/keys/hello"; done
echo "linearizable read via leader:"; curl -s -L -w ' (HTTP %{http_code})\n' "http://localhost:$FOLLOWER_PORT/v1/keys/hello?consistent=true"
echo "missing key:"; curl -s -w ' (HTTP %{http_code})\n' "http://localhost:$FOLLOWER_PORT/v1/keys/nope"

echo; echo "== 4. Kill the leader and time the failover =="
LEADER_ID=$(status "$LEADER_PORT" | sed -n 's/.*id=\([^ ]*\).*/\1/p')
LEADER_PID=$(awk -v id="$LEADER_ID" '$1==id {print $2}' .pids)
kill_pid "$LEADER_PID"
T0=$(now_ms)
NEW_PORT=""
while [[ -z "$NEW_PORT" ]]; do
  for p in "${PORTS[@]}"; do
    [[ "$p" == "$LEADER_PORT" ]] && continue
    if [[ "$(status "$p")" == *"state=LEADER"* ]]; then NEW_PORT=$p; break; fi
  done
  if [[ $(( $(now_ms) - T0 )) -gt 5000 ]]; then
    echo "no new leader within 5s"
    if [[ -n "$(status "$LEADER_PORT")" ]]; then
      echo "The old leader ($LEADER_ID, pid $LEADER_PID) is STILL RUNNING, so the kill did not work." >&2
      echo "Kill it manually (netstat -ano | findstr :$LEADER_PORT, then taskkill //F //PID <pid>) and re-run." >&2
    fi
    exit 1
  fi
done
echo "old leader $LEADER_ID (:$LEADER_PORT) killed; new leader on :$NEW_PORT after $(( $(now_ms) - T0 )) ms"
status "$NEW_PORT"

echo; echo "== 5. Data survived, and the new leader accepts writes =="
for p in "${PORTS[@]}"; do [[ "$p" == "$LEADER_PORT" ]] && continue; printf ':%s -> ' "$p"; curl -s -w ' (HTTP %{http_code})\n' "http://localhost:$p/v1/keys/hello"; done
curl -s -X PUT --data-binary 'after-failover' "http://localhost:$NEW_PORT/v1/keys/post" -w 'PUT HTTP %{http_code}\n'
