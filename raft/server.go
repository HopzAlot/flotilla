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
	s.resetElectionTimer()

	// NOTE: PrevLogIndex/PrevLogTerm consistency check and actually
	// appending Entries land in Step 5/6. For now, any accepted call
	// (heartbeat or otherwise) just proves the leader is alive.
	reply := AppendEntriesReply{Term: s.node.currentTerm, Success: true}
	log.Printf("[%s] AppendEntries from %s (term=%d, entries=%d) -> accepted", s.node.id, args.LeaderID, args.Term, len(args.Entries))
	json.NewEncoder(w).Encode(reply)
}

// Run starts both the election-timeout goroutine and the HTTP server.
// ListenAndServe blocks, so the timer has to be started first.
func (s *Server) Run(addr string) error {
	go s.runElectionTimer()

	mux := http.NewServeMux()
	mux.HandleFunc("/requestvote", s.handleRequestVote)
	mux.HandleFunc("/appendentries", s.handleAppendEntries)
	log.Printf("[%s] listening on %s", s.node.id, addr)
	return http.ListenAndServe(addr, mux)
}
