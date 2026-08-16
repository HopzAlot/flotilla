package raft

import (
	"encoding/json"
	"log"
	"sync"
)

// snapshotThreshold is how many log entries accumulate before the next
// applyCommitted() call triggers a compaction. Real systems use numbers
// in the thousands+; this is kept small so a snapshot can actually be
// demoed without generating a huge batch of writes first.
const snapshotThreshold = 10

// NodeState is which of the three Raft roles this node currently plays.
// Behavior for every RPC depends on this — a Follower and a Leader handle
// the same AppendEntries call completely differently.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Node holds one Raft node's full state. Field names mirror Figure 2 of
// the Raft paper so they're easy to cross-reference later.
type Node struct {
	// mu guards every field below. Election-timeout, RPC-handler, and
	// heartbeat goroutines all read/write this state concurrently now —
	// this is the mutex flagged as a heads-up back in Step 3.
	mu sync.Mutex

	id    string
	state NodeState

	// Persistent state — must survive a restart (BoltDB, added in Step 6).
	currentTerm int
	votedFor    string // "" means no vote cast yet this term
	log         []LogEntry

	// lastIncludedIndex/lastIncludedTerm describe the highest Raft index
	// and its term folded into the most recent snapshot (Step 6.5). Once
	// compaction starts discarding entries, n.log[0] no longer holds Raft
	// index 1 — it holds index lastIncludedIndex+1. Every conversion
	// between a Raft log index and a position in n.log must go through
	// posForIndex, which is why these two fields exist even before
	// anything actually gets discarded.
	lastIncludedIndex int
	lastIncludedTerm  int

	// Volatile state — rebuilt from the log/persisted state on every restart.
	commitIndex int
	lastApplied int

	// leaderID isn't part of Figure 2 — it's a convenience hint, the last
	// LeaderID a follower has seen in an accepted AppendEntries, used only
	// to tell a misdirected client where to retry. Never trusted for
	// anything safety-related.
	leaderID string

	// kv is the state machine every committed entry eventually reaches.
	// Every node — leader or follower — runs the same log through the
	// same Apply calls in the same order, which is what makes them all
	// converge on identical state.
	kv *KVStore

	// storage is this node's own on-disk file — never shared with any
	// other node. Every write to currentTerm/votedFor/log must go through
	// persist() before any RPC reply that depended on it is sent.
	storage *Storage
}

// NewNode constructs a Node, restoring currentTerm/votedFor/log from
// storage if this id has persisted state from a previous run — a fresh
// file's Load() returns the same zero values a brand new node would use
// anyway, so no separate "first ever boot" branch is needed. Every node
// still starts life in the Follower state regardless of what its old term
// was — no node is trusted to resume as Leader just because it remembers
// once being one; it has to win an election again like everyone else.
func NewNode(id string, storage *Storage) *Node {
	currentTerm, votedFor, storedLog, lastIncludedIndex, lastIncludedTerm, snapshotBytes, err := storage.Load()
	if err != nil {
		log.Fatalf("[%s] failed to load persisted state: %v", id, err)
	}

	kv := NewKVStore()
	if len(snapshotBytes) > 0 {
		var data map[string]string
		if err := json.Unmarshal(snapshotBytes, &data); err != nil {
			log.Fatalf("[%s] failed to decode persisted snapshot: %v", id, err)
		}
		kv.Restore(data)
	}

	return &Node{
		id:                id,
		state:             Follower,
		currentTerm:       currentTerm,
		votedFor:          votedFor,
		log:               storedLog,
		lastIncludedIndex: lastIncludedIndex,
		lastIncludedTerm:  lastIncludedTerm,
		// Anything folded into the snapshot was, by definition, already
		// committed and applied — restoring these to 0 instead would let
		// applyCommitted/advanceCommitIndex walk into indices the log no
		// longer has (posForIndex would go negative), and would falsely
		// re-litigate entries that are already durably settled.
		commitIndex: lastIncludedIndex,
		lastApplied: lastIncludedIndex,
		kv:          kv,
		storage:     storage,
	}
}

// persist writes the current currentTerm/votedFor/log triple to disk as
// one atomic transaction. Callers must hold n.mu, and must call this
// before sending any RPC reply that depends on the change being durable —
// otherwise a crash between "reply sent" and "state saved" can make a
// node forget a vote it already promised, and grant it again after
// restart. See Storage.Save for the atomicity guarantee this relies on.
func (n *Node) persist() {
	if err := n.storage.Save(n.currentTerm, n.votedFor, n.log); err != nil {
		log.Printf("[%s] failed to persist state: %v", n.id, err)
	}
}

// lastLogInfo returns the index/term of the most recent log entry. If
// n.log is empty, that no longer means "nothing to report" once
// compaction exists — it can mean every entry this node ever had is now
// folded into a snapshot, in which case lastIncludedIndex/Term is the
// real answer. Reporting (0, 0) here for a fully-snapshotted node would
// wrongly make it look less up-to-date than it is in the RequestVote
// check. Callers must hold n.mu.
func (n *Node) lastLogInfo() (index, term int) {
	if len(n.log) == 0 {
		return n.lastIncludedIndex, n.lastIncludedTerm
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// currentLeaderHint returns this node's own best current belief of who's
// leader: itself, if it is one, or whatever it last learned via leaderID
// otherwise (possibly "" if it's never accepted a call from any leader).
// Callers must hold n.mu.
func (n *Node) currentLeaderHint() string {
	if n.state == Leader {
		return n.id
	}
	return n.leaderID
}

// posForIndex converts a Raft log index into a position in n.log. Only
// valid for index > n.lastIncludedIndex — anything at or before that
// boundary has no position in n.log at all (see termAtIndex). Callers
// must hold n.mu.
func (n *Node) posForIndex(index int) int {
	return index - n.lastIncludedIndex - 1
}

// termAtIndex returns the term of the log entry at the given Raft index.
// index == n.lastIncludedIndex is the boundary case — that entry's term
// is remembered directly on Node rather than in n.log, since compaction
// may have discarded the entry itself while still needing its term for
// future consistency checks. For a fresh node, lastIncludedIndex is 0 and
// this collapses to the original "before the log even starts" sentinel.
// Callers must hold n.mu, and must never ask about an index below
// lastIncludedIndex — Step 6.5's InstallSnapshot path exists specifically
// to guarantee no caller needs to.
func (n *Node) termAtIndex(index int) int {
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	return n.log[n.posForIndex(index)].Term
}

// applyCommitted feeds every entry between lastApplied and commitIndex
// into the state machine, in order, one at a time. Callers must hold
// n.mu — safe to do inline like this because KVStore.Apply guards its
// own separate lock and only ever does a fast map write, so it's not
// worth handing this off to a dedicated goroutine for this project's
// scope. This is what turns "safely committed" into "actually visible
// on a read" — before this runs, commitIndex moving is invisible to
// anyone actually querying the store.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.posForIndex(n.lastApplied)]
		n.kv.Apply(entry)
	}
	n.maybeSnapshot()
}

// maybeSnapshot takes a snapshot and discards the log entries it covers,
// once the log has grown past snapshotThreshold. Callers must hold n.mu.
// The boundary is always lastApplied, never commitIndex — the snapshot is
// a copy of the KV map, which only reflects entries that have actually
// been applied, not merely committed-but-not-yet-applied.
func (n *Node) maybeSnapshot() {
	if len(n.log) <= snapshotThreshold {
		return
	}

	snapshotIndex := n.lastApplied
	snapshotTerm := n.termAtIndex(snapshotIndex)                        // uses the OLD lastIncludedIndex
	remainingLog := append([]LogEntry(nil), n.log[n.posForIndex(snapshotIndex+1):]...) // also OLD lastIncludedIndex

	kvBytes, err := json.Marshal(n.kv.Snapshot())
	if err != nil {
		log.Printf("[%s] failed to serialize snapshot: %v", n.id, err)
		return
	}

	n.lastIncludedIndex = snapshotIndex
	n.lastIncludedTerm = snapshotTerm
	n.log = remainingLog

	if err := n.storage.SaveSnapshot(n.lastIncludedIndex, n.lastIncludedTerm, n.log, kvBytes); err != nil {
		log.Printf("[%s] failed to persist snapshot: %v", n.id, err)
	}
	log.Printf("[%s] snapshot taken at index %d (term %d), log truncated to %d entries", n.id, snapshotIndex, snapshotTerm, len(n.log))
}
