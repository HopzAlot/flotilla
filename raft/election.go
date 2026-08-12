package raft

import (
	"log"
	"math/rand"
	"time"
)

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// resetElectionTimer signals the timer loop to restart its countdown.
// Non-blocking: if a reset is already pending, this one is redundant.
func (s *Server) resetElectionTimer() {
	select {
	case s.resetCh <- struct{}{}:
	default:
	}
}

// runElectionTimer is the heart of Follower->Candidate transitions. It
// runs for the lifetime of the process, in its own goroutine, independent
// of whatever the RPC handlers or an in-flight election are doing — that
// independence is what lets a stuck election naturally retry with a new
// random timeout instead of hanging forever.
func (s *Server) runElectionTimer() {
	for {
		timeout := randomElectionTimeout()
		select {
		case <-s.resetCh:
			// Heard from a leader or granted a vote — the countdown
			// starts over. This is the mechanism that keeps a healthy
			// cluster from ever holding an election at all.
			continue
		case <-time.After(timeout):
			s.node.mu.Lock()
			alreadyLeader := s.node.state == Leader
			s.node.mu.Unlock()
			if !alreadyLeader {
				go s.startElection()
			}
		}
	}
}

// startElection runs as its own goroutine so the election timer above
// keeps ticking independently — if this election is inconclusive (e.g. a
// split vote), the timer will fire again on its own and trigger a fresh
// one with a new term, exactly like a real Raft candidate.
func (s *Server) startElection() {
	s.node.mu.Lock()
	s.node.currentTerm++
	s.node.state = Candidate
	s.node.votedFor = s.node.id
	term := s.node.currentTerm
	lastLogIndex, lastLogTerm := s.node.lastLogInfo()
	s.node.mu.Unlock()

	log.Printf("[%s] election timeout -> Candidate for term %d", s.node.id, term)

	if len(s.peers) == 0 {
		// Single-node cluster: no votes to wait for, majority of one.
		s.becomeLeader(term)
		return
	}

	votes := 1 // counted for self, per the majority-math note above
	majority := (len(s.peers)+1)/2 + 1

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  s.node.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	repliesCh := make(chan RequestVoteReply, len(s.peers))
	for peerID, addr := range s.peers {
		go func(peerID, addr string) {
			reply, err := s.sendRequestVote(addr, args)
			if err != nil {
				log.Printf("[%s] RequestVote -> %s failed: %v", s.node.id, peerID, err)
				return
			}
			repliesCh <- reply
		}(peerID, addr)
	}

	for i := 0; i < len(s.peers); i++ {
		select {
		case reply := <-repliesCh:
			s.node.mu.Lock()
			if reply.Term > s.node.currentTerm {
				log.Printf("[%s] saw higher term %d while campaigning, stepping down", s.node.id, reply.Term)
				s.node.currentTerm = reply.Term
				s.node.state = Follower
				s.node.votedFor = ""
				s.node.mu.Unlock()
				return
			}
			stillCandidate := s.node.state == Candidate && s.node.currentTerm == term
			s.node.mu.Unlock()
			if !stillCandidate {
				return // already resolved elsewhere (won, or stepped down)
			}
			if reply.VoteGranted {
				votes++
				if votes >= majority {
					s.becomeLeader(term)
					return
				}
			}
		case <-time.After(electionTimeoutMax):
			log.Printf("[%s] election for term %d timed out waiting for votes", s.node.id, term)
			return
		}
	}
}

func (s *Server) becomeLeader(term int) {
	s.node.mu.Lock()
	if s.node.state != Candidate || s.node.currentTerm != term {
		s.node.mu.Unlock()
		return
	}
	s.node.state = Leader
	s.node.mu.Unlock()

	log.Printf("[%s] *** elected Leader for term %d ***", s.node.id, term)
	go s.runHeartbeats(term)
}

// runHeartbeats keeps this node's leadership alive by repeatedly proving
// it's reachable to a majority. It stops the moment this node is no
// longer leader of this specific term — checked every tick rather than
// assumed, since another goroutine could have stepped it down concurrently.
func (s *Server) runHeartbeats(term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		s.node.mu.Lock()
		stillLeader := s.node.state == Leader && s.node.currentTerm == term
		s.node.mu.Unlock()
		if !stillLeader {
			return
		}
		s.sendHeartbeats(term)
		<-ticker.C
	}
}

func (s *Server) sendHeartbeats(term int) {
	for peerID, addr := range s.peers {
		go func(peerID, addr string) {
			args := AppendEntriesArgs{
				Term:     term,
				LeaderID: s.node.id,
			}
			reply, err := s.sendAppendEntries(addr, args)
			if err != nil {
				return
			}
			s.node.mu.Lock()
			if reply.Term > s.node.currentTerm {
				log.Printf("[%s] saw higher term %d in heartbeat reply, stepping down", s.node.id, reply.Term)
				s.node.currentTerm = reply.Term
				s.node.state = Follower
				s.node.votedFor = ""
			}
			s.node.mu.Unlock()
		}(peerID, addr)
	}
}
