package raft

// RequestVoteArgs is sent by a candidate canvassing for votes.
// LastLogIndex/LastLogTerm let the receiver refuse to vote for a
// candidate whose log is less up-to-date than its own — the core
// mechanism that keeps a node with missing writes from ever becoming
// leader and silently losing committed data.
type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader. When Entries is empty this is
// purely a heartbeat ("I'm still leader, don't start an election").
// PrevLogIndex/PrevLogTerm are the consistency check: the receiver
// rejects this call outright if its own log doesn't already match at
// that point, which is what surfaces log conflicts for Step 6 to fix.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// LeaderHint is only meaningful when this reply is a stale-term
	// rejection (Term > the caller's own term, Success false because the
	// caller never even got past the term check). It's the responder's
	// own best current belief of who's leader — itself, if it is one, or
	// whatever it last learned via its own leaderID otherwise. Lets a
	// rejected caller redirect a client without needing a separate RPC.
	LeaderHint string
}

// InstallSnapshotArgs is sent by the leader when a peer's nextIndex has
// fallen at or behind the leader's own lastIncludedIndex — the entries
// that peer needs no longer exist anywhere in the leader's log, only in
// this snapshot. Data is the serialized KV map (same JSON shape as what
// Storage.SaveSnapshot persists), sent as one unchunked blob — real
// systems stream this in chunks for large state, out of scope here.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          string
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}
