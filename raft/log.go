package raft

// LogEntry is one command in the replicated log.
//
// Term is the leader's term when this entry was created. Two entries
// sharing (Index, Term) implies every earlier entry in both logs is
// identical too (the Log Matching Property) — this is what lets nodes
// resolve conflicts by comparing a handful of fields instead of diffing
// entire logs.
type LogEntry struct {
	Term    int
	Index   int
	Command Command
}

// Command is a single KV operation applied to the state machine once its
// containing LogEntry is committed.
type Command struct {
	Op    string // "PUT", "DELETE", etc.
	Key   string
	Value string
}
