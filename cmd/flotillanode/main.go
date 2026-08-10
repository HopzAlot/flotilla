package main

import (
	"flag"
	"log"

	"flotilla/raft"
)

func main() {
	id := flag.String("id", "node1", "unique id for this node")
	addr := flag.String("addr", ":8001", "address to listen on")
	flag.Parse()

	node := raft.NewNode(*id)
	server := raft.NewServer(node)

	if err := server.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
