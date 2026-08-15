package raft

import (
	"encoding/json"
	"log"
	"net/http"
)

// Server wraps a Node with an HTTP transport so other nodes can reach it
// over the network, plus the peer addresses it needs to reach them back.
type Server struct {
	node    *Node
	peers   map[string]string // peer id -> HTTP address, excludes self
	resetCh chan struct{}

	// Leader-only volatile state (Raft Figure 2): reinitialized every time
	// this node becomes leader, meaningless otherwise. Guarded by node.mu
	// rather than a separate lock, since advancing commitIndex has to read
	// matchIndex and node.log together consistently.
	nextIndex  map[string]int // peer id -> next log index leader will send them
	matchIndex map[string]int // peer id -> highest log index leader has confirmed on them
}

func NewServer(node *Node, peers map[string]string) *Server {
	return &Server{
		node:    node,
		peers:   peers,
		resetCh: make(chan struct{}, 1),
	}
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	// Stale term proof: anything this candidate knows is older than what
	// we already know. Reject outright, term comparison alone decides it.
	if args.Term < s.node.currentTerm {
		reply := RequestVoteReply{Term: s.node.currentTerm, VoteGranted: false}
		log.Printf("[%s] RequestVote from %s (term=%d) -> rejected: stale term", s.node.id, args.CandidateID, args.Term)
		json.NewEncoder(w).Encode(reply)
		return
	}

	// Newer term proof: our own view is stale. Catch up and forget any
	// vote already cast this (now-old) term before deciding this request.
	if args.Term > s.node.currentTerm {
		s.node.currentTerm = args.Term
		s.node.state = Follower
		s.node.votedFor = ""
	}

	lastLogIndex, lastLogTerm := s.node.lastLogInfo()
	logIsUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	haveNotVotedYet := s.node.votedFor == "" || s.node.votedFor == args.CandidateID

	reply := RequestVoteReply{Term: s.node.currentTerm}
	if haveNotVotedYet && logIsUpToDate {
		s.node.votedFor = args.CandidateID
		reply.VoteGranted = true
		s.resetElectionTimer() // granting a vote counts as "heard from a peer"
	}

	s.node.persist() // term may have bumped and/or a vote may have been granted above
	log.Printf("[%s] RequestVote from %s (term=%d) -> granted=%v", s.node.id, args.CandidateID, args.Term, reply.VoteGranted)
	json.NewEncoder(w).Encode(reply)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	if args.Term < s.node.currentTerm {
		reply := AppendEntriesReply{Term: s.node.currentTerm, Success: false}
		log.Printf("[%s] AppendEntries from %s (term=%d) -> rejected: stale term", s.node.id, args.LeaderID, args.Term)
		json.NewEncoder(w).Encode(reply)
		return
	}

	if args.Term > s.node.currentTerm {
		s.node.currentTerm = args.Term
		s.node.votedFor = ""
	}
	// Any AppendEntries from a current-or-newer-term leader means that
	// leader is legitimate — step down/stay down and reset our clock.
	s.node.state = Follower
	s.node.leaderID = args.LeaderID
	s.resetElectionTimer()

	// Consistency check: our log must already agree with the leader's up
	// to PrevLogIndex, or we refuse the entries outright. PrevLogIndex==0
	// always agrees trivially — there's nothing before entry 1 to disagree
	// on. Otherwise we need an entry at that index, with that exact term.
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(s.node.log) || s.node.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			s.node.persist() // term may have just bumped above; must be durable before this reply goes out
			reply := AppendEntriesReply{Term: s.node.currentTerm, Success: false}
			log.Printf("[%s] AppendEntries from %s (term=%d) -> rejected: log mismatch at index %d", s.node.id, args.LeaderID, args.Term, args.PrevLogIndex)
			json.NewEncoder(w).Encode(reply)
			return
		}
	}

	// Check passed — merge in the new entries. Walk them in order: skip
	// any we already have that match exactly (a retransmitted call we've
	// already applied), truncate-and-overwrite on the first mismatch
	// (leftover entries from a dead/former leader that never committed),
	// or append past the end of what we've got.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= len(s.node.log) {
			if s.node.log[idx-1].Term == entry.Term {
				continue
			}
			s.node.log = s.node.log[:idx-1] // conflict: discard this entry onward
		}
		s.node.log = append(s.node.log, args.Entries[i:]...)
		break
	}

	// The leader may know about commits we haven't heard of yet, but we
	// can never claim to have committed further than what we actually
	// hold locally — hence the min. We'll catch up on a later call once
	// more entries have actually landed in our own log.
	if args.LeaderCommit > s.node.commitIndex {
		s.node.commitIndex = min(args.LeaderCommit, len(s.node.log))
		log.Printf("[%s] commitIndex advanced to %d (leaderCommit=%d)", s.node.id, s.node.commitIndex, args.LeaderCommit)
		s.node.applyCommitted()
	}

	s.node.persist() // term may have bumped above and/or the merge loop just changed the log
	reply := AppendEntriesReply{Term: s.node.currentTerm, Success: true}
	log.Printf("[%s] AppendEntries from %s (term=%d, entries=%d) -> accepted, log len=%d", s.node.id, args.LeaderID, args.Term, len(args.Entries), len(s.node.log))
	json.NewEncoder(w).Encode(reply)
}

// SubmitReply tells a client whether its command was accepted. LeaderHint
// is only a best-known guess — it's the last leaderID this node observed,
// so it can be empty (this node has never seen a leader) or stale (that
// node has since crashed or lost an election). Good enough to retry with,
// never something to trust blindly.
type SubmitReply struct {
	Success    bool
	LeaderHint string
}

// handleSubmit is how a client actually gets a command into the cluster.
// Only the leader may append to its own log here — every other node
// rejects outright rather than forwarding, so the client is the one doing
// the retry, not some hidden hop-through-the-cluster relay.
func (s *Server) handleSubmit(w http.ResponseWriter, r *http.Request) {
	var cmd Command
	if err := json.NewDecoder(r.Body).Decode(&cmd); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.node.mu.Lock()
	if s.node.state != Leader {
		hint := s.node.leaderID
		s.node.mu.Unlock()
		log.Printf("[%s] Submit %+v -> rejected: not leader (hint=%q)", s.node.id, cmd, hint)
		json.NewEncoder(w).Encode(SubmitReply{Success: false, LeaderHint: hint})
		return
	}

	lastLogIndex, _ := s.node.lastLogInfo()
	entry := LogEntry{Term: s.node.currentTerm, Index: lastLogIndex + 1, Command: cmd}
	s.node.log = append(s.node.log, entry)
	s.node.persist()
	s.node.mu.Unlock()

	log.Printf("[%s] Submit %+v -> appended at index %d", s.node.id, cmd, entry.Index)
	json.NewEncoder(w).Encode(SubmitReply{Success: true})
}

// handleDebugGet is a raw, unguarded peek at this node's own local state
// machine — NOT the real client read path. It can return stale data on a
// lagging follower, or even on a leader that's been silently partitioned
// away. Making reads actually safe is Step 7.5's job; this exists only so
// Step 5's apply logic can be observed while it's being built.
func (s *Server) handleDebugGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	val, ok := s.node.kv.Get(key)
	json.NewEncoder(w).Encode(map[string]any{"value": val, "found": ok})
}

// Run starts both the election-timeout goroutine and the HTTP server.
// ListenAndServe blocks, so the timer has to be started first.
func (s *Server) Run(addr string) error {
	go s.runElectionTimer()

	mux := http.NewServeMux()
	mux.HandleFunc("/requestvote", s.handleRequestVote)
	mux.HandleFunc("/appendentries", s.handleAppendEntries)
	mux.HandleFunc("/submit", s.handleSubmit)
	mux.HandleFunc("/debug/get", s.handleDebugGet)
	log.Printf("[%s] listening on %s", s.node.id, addr)
	return http.ListenAndServe(addr, mux)
}
