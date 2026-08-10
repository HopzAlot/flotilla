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
}
