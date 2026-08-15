package main

import (
	"flag"
	"log"
	"strings"

	"flotilla/raft"
)

// parsePeers turns "node2=localhost:8002,node3=localhost:8003" into a map.
func parsePeers(s string) map[string]string {
	peers := make(map[string]string)
	if s == "" {
		return peers
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid -peers entry %q, expected id=addr", pair)
		}
		peers[parts[0]] = parts[1]
	}
	return peers
}

func main() {
	id := flag.String("id", "node1", "unique id for this node")
	addr := flag.String("addr", ":8001", "address to listen on")
	peersFlag := flag.String("peers", "", "comma-separated peer list, e.g. node2=localhost:8002,node3=localhost:8003")
	flag.Parse()

	storage, err := raft.NewStorage(*id + ".db")
	if err != nil {
		log.Fatalf("failed to open storage for %s: %v", *id, err)
	}

	node := raft.NewNode(*id, storage)
	server := raft.NewServer(node, parsePeers(*peersFlag))

	if err := server.Run(*addr); err != nil {
		log.Fatal(err)
	}
}
