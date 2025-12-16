package main

import (
	"log"
	"net"

	"github.com/tiagomorais/simple-message-broker/internal/broker"
	"github.com/tiagomorais/simple-message-broker/internal/storage"
	"github.com/tiagomorais/simple-message-broker/internal/wal"
)

const (
	walDir      = "./wal/"
	offsetsFile = "offsets.json"
	listenAddr  = ":8080"
)

func main() {
	// Initialize WAL
	w, err := wal.NewWAL(walDir)
	if err != nil {
		log.Fatalf("Error creating WAL: %v\n", err)
	}

	// Initialize offset store
	offsetStore := storage.NewOffsetStore(offsetsFile)
	if err := offsetStore.Load(); err != nil {
		log.Printf("Error loading offsets: %v\n", err)
	}

	// Create broker
	b := broker.NewBroker(w, offsetStore)

	// Start server
	//nolint:gosec // G102: Intentionally bind to all interfaces for message broker accessibility
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("Error starting server: %v\n", err)
	}
	defer listener.Close()

	log.Println("Server started on port 8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go b.HandleConnection(conn)
	}
}
