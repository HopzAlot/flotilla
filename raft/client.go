package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// rpcClient has a short timeout because an unreachable peer must not be
// allowed to stall an election or a heartbeat round — a hung network call
// here would look indistinguishable from a real outage to the rest of
// the cluster's logic.
var rpcClient = &http.Client{Timeout: 100 * time.Millisecond}

func (s *Server) sendRequestVote(addr string, args RequestVoteArgs) (RequestVoteReply, error) {
	var reply RequestVoteReply
	body, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}
	resp, err := rpcClient.Post(fmt.Sprintf("http://%s/requestvote", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&reply)
	return reply, err
}

func (s *Server) sendAppendEntries(addr string, args AppendEntriesArgs) (AppendEntriesReply, error) {
	var reply AppendEntriesReply
	body, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}
	resp, err := rpcClient.Post(fmt.Sprintf("http://%s/appendentries", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&reply)
	return reply, err
}

func (s *Server) sendInstallSnapshot(addr string, args InstallSnapshotArgs) (InstallSnapshotReply, error) {
	var reply InstallSnapshotReply
	body, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}
	resp, err := rpcClient.Post(fmt.Sprintf("http://%s/installsnapshot", addr), "application/json", bytes.NewReader(body))
	if err != nil {
		return reply, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&reply)
	return reply, err
}
