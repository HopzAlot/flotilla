#!/usr/bin/env bash
# Launches the orchestrator (spawns the real 3-node fleet + control API)
# and the React dashboard dev server together. Ctrl-C stops both.
set -euo pipefail
cd "$(dirname "$0")/.."

cleanup() {
	echo
	echo "[dashboard] stopping fleet + dev server"
	kill 0
}
trap cleanup EXIT INT TERM

go run ./cmd/orchestrator &
( cd web && npm install --silent && npm run dev ) &
wait
