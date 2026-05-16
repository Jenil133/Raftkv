#!/usr/bin/env bash
# Start a 5-node local cluster on ports 7001-7005. Ctrl-C stops all nodes.
# Data lives in ./data/node<N>; delete it for a fresh cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

N=${N:-5}
PEERS=""
for i in $(seq 1 "$N"); do
  PEERS+="${i}=127.0.0.1:$((7000 + i)),"
done
PEERS=${PEERS%,}

go build -o bin/raftkvd ./cmd/raftkvd
go build -o bin/raftkvctl ./cmd/raftkvctl

pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM
for i in $(seq 1 "$N"); do
  ./bin/raftkvd -id "$i" -peers "$PEERS" >"data-node$i.log" 2>&1 &
  pids+=($!)
done
echo "cluster up: $PEERS"
echo "try: ./bin/raftkvctl -endpoints $PEERS put hello world"
wait
