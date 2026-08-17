#!/usr/bin/env bash
# Checkpoint C: kill the leader mid-write, verify a new leader is elected
# and every write that was actually acked (Success:true) survives.
#
# Only writes that got Success:true go on the "must survive" list — per
# the durability fix in handleSubmit/waitForCommit, that's now a genuine
# promise. Writes that errored out (leader died before replying) are
# deliberately NOT checked either way: they may or may not have
# committed, and Raft only promises "acked implies durable," never
# "unacked implies lost."
set -uo pipefail

cd "$(dirname "$0")/.."
RUN_DIR="chaos-run"
NODE_IDS=(node1 node2 node3)
NODE_ADDRS=(":8001" ":8002" ":8003")
CURL_TIMEOUT=1

log() { printf '\033[1;36m[chaos]\033[0m %s\n' "$1"; }
pass() { printf '\033[1;32m[ pass ]\033[0m %s\n' "$1"; }
fail() { printf '\033[1;31m[ FAIL ]\033[0m %s\n' "$1"; }

cleanup() {
	log "cleaning up node processes"
	for id in "${NODE_IDS[@]}"; do
		if [[ -f "$RUN_DIR/$id.pid" ]]; then
			kill -9 "$(cat "$RUN_DIR/$id.pid")" 2>/dev/null
		fi
	done
}
trap cleanup EXIT

rm -rf "$RUN_DIR"
mkdir -p "$RUN_DIR"

log "building flotillanode"
go build -o "$RUN_DIR/flotillanode" ./cmd/flotillanode

peers_for() {
	local self="$1" peers=""
	for i in "${!NODE_IDS[@]}"; do
		[[ "${NODE_IDS[$i]}" == "$self" ]] && continue
		peers+="${NODE_IDS[$i]}=localhost${NODE_ADDRS[$i]},"
	done
	echo "${peers%,}"
}

addr_for() {
	local id="$1"
	for i in "${!NODE_IDS[@]}"; do
		[[ "${NODE_IDS[$i]}" == "$id" ]] && echo "localhost${NODE_ADDRS[$i]}" && return
	done
}

log "starting 3-node cluster"
for i in "${!NODE_IDS[@]}"; do
	id="${NODE_IDS[$i]}"
	(
		cd "$RUN_DIR"
		./flotillanode -id="$id" -addr="${NODE_ADDRS[$i]}" -peers="$(peers_for "$id")" \
			> "$id.log" 2>&1 &
		echo $! > "$id.pid"
	)
done
sleep 1

# submit_one ADDR KEY VALUE -> prints raw JSON reply, empty string on connection error
submit_one() {
	curl -s --max-time "$CURL_TIMEOUT" -X POST "http://$1/submit" \
		-d "{\"Op\":\"PUT\",\"Key\":\"$2\",\"Value\":\"$3\"}" 2>/dev/null
}

get_one() {
	curl -s --max-time "$CURL_TIMEOUT" "http://$1/debug/get?key=$2" 2>/dev/null
}

CURRENT_LEADER=""

# find_leader: probe every node with a harmless write until one accepts.
find_leader() {
	for attempt in $(seq 1 20); do
		for id in "${NODE_IDS[@]}"; do
			addr="$(addr_for "$id")"
			reply="$(submit_one "$addr" "__probe__" "$attempt")"
			[[ -z "$reply" ]] && continue
			if [[ "$(echo "$reply" | jq -r '.Success')" == "true" ]]; then
				CURRENT_LEADER="$addr"
				return 0
			fi
		done
		sleep 0.2
	done
	return 1
}

# submit_with_retry KEY VALUE -> "true"/"false"/"error", tries CURRENT_LEADER
# first, follows LeaderAddr redirects, re-discovers leader if needed.
submit_with_retry() {
	local key="$1" value="$2" reply success leader_addr
	for attempt in $(seq 1 15); do
		if [[ -z "$CURRENT_LEADER" ]]; then
			find_leader || { echo "error"; return; }
		fi
		reply="$(submit_one "$CURRENT_LEADER" "$key" "$value")"
		if [[ -z "$reply" ]]; then
			CURRENT_LEADER=""
			sleep 0.2
			continue
		fi
		success="$(echo "$reply" | jq -r '.Success')"
		if [[ "$success" == "true" ]]; then
			echo "true"
			return
		fi
		leader_addr="$(echo "$reply" | jq -r '.LeaderAddr')"
		if [[ -n "$leader_addr" && "$leader_addr" != "null" ]]; then
			CURRENT_LEADER="$leader_addr"
		else
			CURRENT_LEADER=""
		fi
		sleep 0.2
	done
	echo "error"
}

log "waiting for initial leader election"
find_leader || { fail "no leader elected in time"; exit 1; }
log "leader is $CURRENT_LEADER"

declare -A MUST_SURVIVE
INDETERMINATE=0

write_batch() {
	local prefix="$1" count="$2"
	for i in $(seq 1 "$count"); do
		key="${prefix}_${i}"
		value="val_${prefix}_${i}"
		result="$(submit_with_retry "$key" "$value")"
		if [[ "$result" == "true" ]]; then
			MUST_SURVIVE["$key"]="$value"
			log "write $key=$value -> committed (acked)"
		else
			INDETERMINATE=$((INDETERMINATE + 1))
			log "write $key=$value -> connection error (indeterminate, not required to survive)"
		fi
	done
}

log "--- writing batch 1 (before kill) ---"
write_batch "pre" 5

leader_id=""
for id in "${NODE_IDS[@]}"; do
	[[ "$(addr_for "$id")" == "$CURRENT_LEADER" ]] && leader_id="$id"
done

log "*** KILLING LEADER $leader_id (pid=$(cat "$RUN_DIR/$leader_id.pid")) ***"
kill -9 "$(cat "$RUN_DIR/$leader_id.pid")"
CURRENT_LEADER=""

log "--- writing batch 2 (during/after re-election, script retries through it) ---"
write_batch "post" 5

log "waiting for cluster to settle"
find_leader || { fail "no new leader elected after kill"; exit 1; }
log "new leader is $CURRENT_LEADER"

log "issuing flush write (drags any same-old-term-only entries to commit under new leader)"
submit_with_retry "__flush__" "flush" > /dev/null

sleep 0.5

log "--- verifying all acked writes survived ---"
FAILURES=0
for key in "${!MUST_SURVIVE[@]}"; do
	expected="${MUST_SURVIVE[$key]}"
	reply="$(get_one "$CURRENT_LEADER" "$key")"
	found="$(echo "$reply" | jq -r '.Found' 2>/dev/null)"
	value="$(echo "$reply" | jq -r '.Value' 2>/dev/null)"
	if [[ "$found" == "true" && "$value" == "$expected" ]]; then
		pass "$key=$value"
	else
		fail "$key expected=$expected got found=$found value=$value"
		FAILURES=$((FAILURES + 1))
	fi
done

echo
log "=== RESULT ==="
log "$(( ${#MUST_SURVIVE[@]} - FAILURES ))/${#MUST_SURVIVE[@]} acked writes survived, $FAILURES failed, $INDETERMINATE indeterminate (correctly skipped)"
if [[ "$FAILURES" -eq 0 ]]; then
	pass "CHECKPOINT C PASSED"
	exit 0
else
	fail "CHECKPOINT C FAILED"
	exit 1
fi
