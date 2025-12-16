package storage

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

// OffsetStore manages topic offsets with persistence
type OffsetStore struct {
	path    string
	offsets map[string]int64
	mu      sync.RWMutex
}

// NewOffsetStore creates a new OffsetStore instance
func NewOffsetStore(path string) *OffsetStore {
	return &OffsetStore{
		path:    path,
		offsets: make(map[string]int64),
	}
}

// Get returns the current offset for a topic
func (s *OffsetStore) Get(topic string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.offsets[topic]
}

// Set sets the offset for a topic
func (s *OffsetStore) Set(topic string, offset int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[topic] = offset
}

// Increment increments the offset for a topic and returns the new value
func (s *OffsetStore) Increment(topic string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offsets[topic]++
	return s.offsets[topic]
}

// InitTopic initializes a topic offset to 0 if it doesn't exist
func (s *OffsetStore) InitTopic(topic string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.offsets[topic]; !exists {
		s.offsets[topic] = 0
	}
}

// Save persists the offsets to disk
func (s *OffsetStore) Save() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	file, err := os.Create(s.path)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	if err := enc.Encode(s.offsets); err != nil {
		return err
	}

	return file.Sync()
}

// Load reads offsets from disk
func (s *OffsetStore) Load() error {
	file, err := os.Open(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("Offsets file not found, starting with empty offsets.")
			return nil
		}
		return err
	}
	defer file.Close()

	var snapshot map[string]int64
	dec := json.NewDecoder(file)
	if err := dec.Decode(&snapshot); err != nil {
		return err
	}

	s.mu.Lock()
	for topic, offset := range snapshot {
		s.offsets[topic] = offset
	}
	s.mu.Unlock()

	log.Printf("Topic offsets loaded from disk: %v\n", snapshot)
	return nil
}
