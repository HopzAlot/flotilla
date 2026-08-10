package raft

import "sync"

// KVStore is the state machine Raft replicates. It has no idea a
// consensus algorithm exists above it — it only knows how to apply one
// command at a time, in the order it's handed. All ordering and
// durability guarantees are the Raft layer's problem, not this one.
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply is the single seam between consensus and storage: called once,
// in log order, for every entry the Raft layer has decided is committed.
// It must never be called out of order or more than once for the same
// index — that ordering guarantee is Raft's job, not this method's.
func (kv *KVStore) Apply(entry LogEntry) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch entry.Command.Op {
	case "PUT":
		kv.data[entry.Command.Key] = entry.Command.Value
	case "DELETE":
		delete(kv.data, entry.Command.Key)
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}
