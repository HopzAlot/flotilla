package raft

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
	id    string
	state NodeState

	// Persistent state — must survive a restart (BoltDB, added in Step 6).
	currentTerm int
	votedFor    string // "" means no vote cast yet this term
	log         []LogEntry

	// Volatile state — rebuilt from the log/persisted state on every restart.
	commitIndex int
	lastApplied int
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
	}
}
