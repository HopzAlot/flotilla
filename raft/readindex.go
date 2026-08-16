package raft

import "time"

// readApplyPollInterval is how often waitForApply rechecks lastApplied.
// Kept short since, per the invariant traced in node.go, this almost never
// actually loops more than once.
const readApplyPollInterval = 2 * time.Millisecond

// confirmLeadership proves this node is still leader of a live majority
// right this instant, by sending one round of empty AppendEntries (a plain
// heartbeat) and waiting for acks. What proves leadership is NOT
// reply.Success — a perfectly loyal follower can fail the PrevLogIndex/
// PrevLogTerm consistency check while still recognizing this leader's
// term. What proves it is simply getting a reply back that didn't carry a
// higher term forcing a stepdown: only a peer that still considers this
// node its leader for this term would reply at all instead of rejecting.
//
// On success, returns the commitIndex captured at the start of the round
// as readIndex: since a real majority just acknowledged this leader/term,
// no election could have completed elsewhere, so nothing after this
// captured commitIndex could have been silently overwritten by a
// different leader. That's what makes it safe to serve any state up to
// and including readIndex once this node has actually applied that far
// (see waitForApply).
func (s *Server) confirmLeadership() (readIndex int, ok bool) {
	s.node.mu.Lock()
	if s.node.state != Leader {
		s.node.mu.Unlock()
		return 0, false
	}
	term := s.node.currentTerm
	readIndex = s.node.commitIndex
	s.node.mu.Unlock()

	if len(s.peers) == 0 {
		return readIndex, true // single-node cluster: majority of one, always confirmed
	}

	votes := 1 // counted for self, same majority math as an election
	majority := (len(s.peers)+1)/2 + 1
	repliesCh := make(chan bool, len(s.peers))

	for peerID, addr := range s.peers {
		go func(peerID, addr string) {
			s.node.mu.Lock()
			prevLogIndex := s.nextIndex[peerID] - 1
			if prevLogIndex < s.node.lastIncludedIndex {
				// Peer is behind the snapshot boundary — this round isn't
				// the place to fix that (sendHeartbeats' normal tick will
				// send InstallSnapshot). Just don't count it this time.
				s.node.mu.Unlock()
				repliesCh <- false
				return
			}
			prevLogTerm := s.node.termAtIndex(prevLogIndex)
			s.node.mu.Unlock()

			args := AppendEntriesArgs{
				Term:         term,
				LeaderID:     s.node.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil,
				LeaderCommit: readIndex,
			}
			reply, err := s.sendAppendEntries(addr, args)
			if err != nil {
				repliesCh <- false
				return
			}

			s.node.mu.Lock()
			if reply.Term > s.node.currentTerm {
				s.node.currentTerm = reply.Term
				s.node.state = Follower
				s.node.votedFor = ""
				s.node.persist()
			}
			stillLeader := s.node.state == Leader && s.node.currentTerm == term
			s.node.mu.Unlock()

			// The ack itself (reply.Term == term, no error) is the proof —
			// Success is irrelevant here, per the quiz above.
			repliesCh <- stillLeader && reply.Term == term
		}(peerID, addr)
	}

	for i := 0; i < len(s.peers); i++ {
		if <-repliesCh {
			votes++
			if votes >= majority {
				return readIndex, true
			}
		}
	}
	return 0, false
}

// waitForApply blocks until lastApplied has reached index, so that a
// kv.Get performed right after this returns is guaranteed to reflect
// everything committed up through index. Not waiting on anything that
// hasn't happened yet — index was already <= commitIndex at the moment
// confirmLeadership captured it, and applyCommitted() is what always
// drives lastApplied up to match commitIndex (see node.go). In this
// codebase that catch-up already happened synchronously before this is
// even called, so this loop runs its check exactly once and returns.
func (s *Server) waitForApply(index int) {
	for {
		s.node.mu.Lock()
		caughtUp := s.node.lastApplied >= index
		s.node.mu.Unlock()
		if caughtUp {
			return
		}
		time.Sleep(readApplyPollInterval)
	}
}
