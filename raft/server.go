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
	if args.PrevLogIndex > s.node.lastIncludedIndex {
		if args.PrevLogIndex > s.node.lastIncludedIndex+len(s.node.log) || s.node.termAtIndex(args.PrevLogIndex) != args.PrevLogTerm {
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
		if idx <= s.node.lastIncludedIndex+len(s.node.log) {
			if s.node.log[s.node.posForIndex(idx)].Term == entry.Term {
				continue
			}
			s.node.log = s.node.log[:s.node.posForIndex(idx)] // conflict: discard this entry onward
		}
		s.node.log = append(s.node.log, args.Entries[i:]...)
		break
	}

	// The leader may know about commits we haven't heard of yet, but we
	// can never claim to have committed further than what we actually
	// hold locally — hence the min. We'll catch up on a later call once
	// more entries have actually landed in our own log.
	if args.LeaderCommit > s.node.commitIndex {
		s.node.commitIndex = min(args.LeaderCommit, s.node.lastIncludedIndex+len(s.node.log))
		log.Printf("[%s] commitIndex advanced to %d (leaderCommit=%d)", s.node.id, s.node.commitIndex, args.LeaderCommit)
		s.node.applyCommitted()
	}

	s.node.persist() // term may have bumped above and/or the merge loop just changed the log
	reply := AppendEntriesReply{Term: s.node.currentTerm, Success: true}
	log.Printf("[%s] AppendEntries from %s (term=%d, entries=%d) -> accepted, log len=%d", s.node.id, args.LeaderID, args.Term, len(args.Entries), len(s.node.log))
	json.NewEncoder(w).Encode(reply)
}

// SubmitReply tells a client whether its command was accepted. LeaderAddr
// is only a best-known guess — the network address of the last leaderID
// this node observed, translated from Raft id to something a client can
// actually reach (peers only ever stores id->addr, never anything a
// client understands on its own). Can be empty (this node has never seen
// a leader) or stale (that node has since crashed or lost an election).
// Good enough to retry with, never something to trust blindly.
type SubmitReply struct {
	Success    bool
	LeaderAddr string
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
		leaderAddr := s.peers[s.node.leaderID] // "" if leaderID is "" or otherwise unknown
		s.node.mu.Unlock()
		log.Printf("[%s] Submit %+v -> rejected: not leader (leaderAddr=%q)", s.node.id, cmd, leaderAddr)
		json.NewEncoder(w).Encode(SubmitReply{Success: false, LeaderAddr: leaderAddr})
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

// handleInstallSnapshot receives a leader's snapshot when this node has
// fallen too far behind for ordinary AppendEntries to catch it up — the
// entries it needs no longer exist in the leader's log at all. Always
// wholesale-replaces the local log rather than trying to keep a matching
// suffix (a valid Raft optimization the paper calls out as optional, not
// a correctness requirement) — simpler, and always safe, since a
// currently-valid leader's snapshot is authoritative regardless of what
// this node had locally.
func (s *Server) handleInstallSnapshot(w http.ResponseWriter, r *http.Request) {
	var args InstallSnapshotArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	if args.Term < s.node.currentTerm {
		reply := InstallSnapshotReply{Term: s.node.currentTerm}
		log.Printf("[%s] InstallSnapshot from %s (term=%d) -> rejected: stale term", s.node.id, args.LeaderID, args.Term)
		json.NewEncoder(w).Encode(reply)
		return
	}

	if args.Term > s.node.currentTerm {
		s.node.currentTerm = args.Term
		s.node.votedFor = ""
	}
	s.node.state = Follower
	s.node.leaderID = args.LeaderID
	s.resetElectionTimer()

	// Idempotency/staleness guard: a retried or reordered call for a
	// boundary we've already adopted (or moved past) must not roll our
	// own state backward.
	if args.LastIncludedIndex <= s.node.lastIncludedIndex {
		s.node.persist()
		reply := InstallSnapshotReply{Term: s.node.currentTerm}
		log.Printf("[%s] InstallSnapshot from %s (term=%d) -> ignored: already have index %d", s.node.id, args.LeaderID, args.Term, args.LastIncludedIndex)
		json.NewEncoder(w).Encode(reply)
		return
	}

	var data map[string]string
	if err := json.Unmarshal(args.Data, &data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.node.kv.Restore(data)

	s.node.log = []LogEntry{}
	s.node.lastIncludedIndex = args.LastIncludedIndex
	s.node.lastIncludedTerm = args.LastIncludedTerm
	// Everything up to a leader-supplied snapshot boundary is, by
	// definition, already committed and applied — this node just adopted
	// that exact state wholesale via kv.Restore above.
	s.node.commitIndex = args.LastIncludedIndex
	s.node.lastApplied = args.LastIncludedIndex

	if err := s.node.storage.SaveSnapshot(s.node.lastIncludedIndex, s.node.lastIncludedTerm, s.node.log, args.Data); err != nil {
		log.Printf("[%s] failed to persist installed snapshot: %v", s.node.id, err)
	}

	reply := InstallSnapshotReply{Term: s.node.currentTerm}
	log.Printf("[%s] InstallSnapshot from %s (term=%d) -> installed snapshot at index %d", s.node.id, args.LeaderID, args.Term, args.LastIncludedIndex)
	json.NewEncoder(w).Encode(reply)
}

// GetReply mirrors SubmitReply's shape for the same reason: a client
// pointed at the wrong node needs an address to retry against, not just a
// bare failure.
type GetReply struct {
	Success    bool
	Value      string
	Found      bool
	LeaderAddr string
}

// handleDebugGet is the linearizable client read path (Step 7.5). Unlike a
// write, a read never touches the log — but it still must not answer from
// a leader that only *thinks* it's still leader. confirmLeadership proves
// a live majority still acks this node/term right now before anything is
// read; waitForApply then ensures this node's own kv actually reflects
// everything up through that proof point. Only after both does kv.Get run.
func (s *Server) handleDebugGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")

	readIndex, ok := s.confirmLeadership()
	if !ok {
		s.node.mu.Lock()
		leaderAddr := s.peers[s.node.leaderID]
		s.node.mu.Unlock()
		log.Printf("[%s] Get %q -> rejected: not leader (leaderAddr=%q)", s.node.id, key, leaderAddr)
		json.NewEncoder(w).Encode(GetReply{Success: false, LeaderAddr: leaderAddr})
		return
	}

	s.waitForApply(readIndex)

	val, found := s.node.kv.Get(key)
	json.NewEncoder(w).Encode(GetReply{Success: true, Value: val, Found: found})
}

// Run starts both the election-timeout goroutine and the HTTP server.
// ListenAndServe blocks, so the timer has to be started first.
func (s *Server) Run(addr string) error {
	go s.runElectionTimer()

	mux := http.NewServeMux()
	mux.HandleFunc("/requestvote", s.handleRequestVote)
	mux.HandleFunc("/appendentries", s.handleAppendEntries)
	mux.HandleFunc("/installsnapshot", s.handleInstallSnapshot)
	mux.HandleFunc("/submit", s.handleSubmit)
	mux.HandleFunc("/debug/get", s.handleDebugGet)
	log.Printf("[%s] listening on %s", s.node.id, addr)
	return http.ListenAndServe(addr, mux)
}
