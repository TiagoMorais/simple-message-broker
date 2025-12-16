package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/tiagomorais/simple-message-broker/internal/protocol"
)

// WAL represents a Write-Ahead Log for message persistence
type WAL struct {
	dir string
	mu  sync.Mutex
}

// NewWAL creates a new WAL instance with the specified directory
func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return nil, err
	}
	return &WAL{dir: dir}, nil
}

// Append writes a message to the WAL and returns the assigned ID
func (w *WAL) Append(msg protocol.Message) (uint32, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	walPath := filepath.Join(w.dir, msg.Topic+".log")

	// Count existing messages to determine the next ID
	nextID, err := w.countMessages(walPath)
	if err != nil {
		return 0, err
	}

	// Assign the auto-generated ID to the message
	msg.ID = nextID

	// Open the file for appending, creating it if it doesn't exist
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	line, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return 0, err
	}

	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		return 0, err
	}

	return nextID, nil
}

// ReadAt reads a message from the WAL at the specified offset
func (w *WAL) ReadAt(topic string, offset int64) (*protocol.Message, error) {
	walPath := filepath.Join(w.dir, topic+".log")
	file, err := os.Open(walPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var currentLine int64
	for scanner.Scan() {
		if currentLine == offset {
			var msg protocol.Message
			if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
				return nil, err
			}
			return &msg, nil
		}
		currentLine++
	}

	return nil, nil // No message at this offset
}

// countMessages counts the number of messages in a WAL file
func (w *WAL) countMessages(walPath string) (uint32, error) {
	file, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	var count uint32
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	return count, scanner.Err()
}
