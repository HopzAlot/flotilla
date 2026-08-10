package raft

import (
	"encoding/json"
	"log"
	"net/http"
)

// Server wraps a Node with an HTTP transport so other nodes can reach it
// over the network. Handlers here are deliberately stubbed: they decode
// the request and log it, but the actual decision (grant vote? accept
// entries?) is hardcoded. This step only proves nodes can talk — real
// Raft rules land in Step 4 (election) and Step 5 (replication).
type Server struct {
	node *Node
}

func NewServer(node *Node) *Server {
	return &Server{node: node}
}

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[%s] RequestVote from %s (term=%d, lastLogIndex=%d, lastLogTerm=%d)",
		s.node.id, args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)

	// Stub: always refuse. Real vote-granting (term + log-recency checks,
	// one-vote-per-term) arrives in Step 4.
	reply := RequestVoteReply{
		Term:        s.node.currentTerm,
		VoteGranted: false,
	}

	json.NewEncoder(w).Encode(reply)
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("[%s] AppendEntries from %s (term=%d, prevLogIndex=%d, entries=%d)",
		s.node.id, args.LeaderID, args.Term, args.PrevLogIndex, len(args.Entries))

	// Stub: always reject. Real consistency-check/replication logic
	// arrives in Step 5; heartbeat handling in Step 4.
	reply := AppendEntriesReply{
		Term:    s.node.currentTerm,
		Success: false,
	}

	json.NewEncoder(w).Encode(reply)
}

func (s *Server) ListenAndServe(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/requestvote", s.handleRequestVote)
	mux.HandleFunc("/appendentries", s.handleAppendEntries)
	log.Printf("[%s] listening on %s", s.node.id, addr)
	return http.ListenAndServe(addr, mux)
}
