package raft

import "sync"

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
}

// NewNode constructs a Node in the initial Follower state every Raft node
// starts in — nobody begins as a leader.
func NewNode(id string) *Node {
	return &Node{
		id:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    "",
		log:         []LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		kv:          NewKVStore(),
	}
}

// lastLogInfo returns the index/term of the most recent log entry, or
// (0, 0) for an empty log. Callers must hold n.mu. Used by the
// RequestVote log-recency check — with no log yet (Step 5 adds real
// entries), this trivially returns (0, 0) for every node, which is
// correct and forward-compatible: it becomes meaningful once logs
// actually diverge.
func (n *Node) lastLogInfo() (index, term int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// termAtIndex returns the term of the log entry at the given 1-based
// index, or 0 for index 0 (the "before the log even starts" sentinel used
// as PrevLogTerm when PrevLogIndex is 0). Callers must hold n.mu. Relies
// on n.log never having gaps — index i is always at slice position i-1 —
// which holds until Step 6.5 adds compaction.
func (n *Node) termAtIndex(index int) int {
	if index == 0 {
		return 0
	}
	return n.log[index-1].Term
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
		entry := n.log[n.lastApplied-1]
		n.kv.Apply(entry)
	}
}
