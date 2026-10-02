#!/usr/bin/env bash
# Stops the nodes started by run-cluster.sh and verifies the ports are really free.
cd "$(dirname "$0")"

kill_pid() {
  if command -v taskkill >/dev/null 2>&1; then
    taskkill //F //PID "$1" >/dev/null 2>&1
  else
    kill -9 "$1" 2>/dev/null
  fi
}

if [[ -f .pids ]]; then
  while read -r id pid; do
    if kill_pid "$pid"; then echo "stopped $id ($pid)"; else echo "$id ($pid) was not running"; fi
  done < .pids
  rm -f .pids
fi

sleep 0.5
for port in 8051 8052 8053; do
  if curl -s --max-time 0.5 "http://localhost:$port/v1/status" >/dev/null 2>&1; then
    echo "WARNING: something still answers on port $port." >&2
    echo "Find it: netstat -ano | findstr :$port   then: taskkill //F //PID <the pid in the last column>" >&2
  fi
done
