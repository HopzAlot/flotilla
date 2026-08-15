package raft

import (
	"encoding/json"
	"strconv"

	bolt "go.etcd.io/bbolt"
)

// stateBucket is the single bucket holding the three Figure 2 persistent
// fields. One bucket is enough — we're not indexing anything, just storing
// three keys that always get written together.
var stateBucket = []byte("raft-state")

const (
	keyCurrentTerm = "currentTerm"
	keyVotedFor    = "votedFor"
	keyLog         = "log"
)

// Storage is one node's on-disk persistent state, backed by its own BoltDB
// file. Every Node gets its own Storage, opened against a file named after
// that node's id — never shared between nodes.
type Storage struct {
	db *bolt.DB
}

// NewStorage opens (creating if necessary) the BoltDB file at path and
// ensures the state bucket exists, so Save/Load never have to check for it.
func NewStorage(path string) (*Storage, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(stateBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Storage{db: db}, nil
}

// Save writes currentTerm, votedFor, and log as one BoltDB transaction —
// all three land together or, on a crash mid-write, none of them do. That
// atomicity is what prevents votedFor ever being read back paired with the
// wrong term.
func (s *Storage) Save(currentTerm int, votedFor string, log []LogEntry) error {
	logBytes, err := json.Marshal(log)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(stateBucket)
		if err := b.Put([]byte(keyCurrentTerm), []byte(strconv.Itoa(currentTerm))); err != nil {
			return err
		}
		if err := b.Put([]byte(keyVotedFor), []byte(votedFor)); err != nil {
			return err
		}
		return b.Put([]byte(keyLog), logBytes)
	})
}

// Load reads back currentTerm, votedFor, and log. On a brand new file (no
// node has ever saved here before), it returns the same zero values a
// fresh NewNode would use — so callers don't need a separate "first ever
// boot" case.
func (s *Storage) Load() (currentTerm int, votedFor string, log []LogEntry, err error) {
	err = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(stateBucket)

		if v := b.Get([]byte(keyCurrentTerm)); v != nil {
			currentTerm, err = strconv.Atoi(string(v))
			if err != nil {
				return err
			}
		}
		if v := b.Get([]byte(keyVotedFor)); v != nil {
			votedFor = string(v)
		}
		if v := b.Get([]byte(keyLog)); v != nil {
			if err := json.Unmarshal(v, &log); err != nil {
				return err
			}
		}
		return nil
	})
	if log == nil {
		log = []LogEntry{}
	}
	return currentTerm, votedFor, log, err
}

func (s *Storage) Close() error {
	return s.db.Close()
}
