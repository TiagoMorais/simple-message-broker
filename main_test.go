package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/tiagomorais/simple-message-broker/internal/protocol"
	"github.com/tiagomorais/simple-message-broker/internal/storage"
)

func TestOffsetStore(t *testing.T) {
	testFile := "test_offsets.json"
	defer os.Remove(testFile)

	// Create store and set values
	store := storage.NewOffsetStore(testFile)
	store.Set("topic1", 10)
	store.Set("topic2", 25)

	// Save to disk
	if err := store.Save(); err != nil {
		t.Fatalf("Error saving offsets: %v", err)
	}

	// Create new store and load
	store2 := storage.NewOffsetStore(testFile)
	if err := store2.Load(); err != nil {
		t.Fatalf("Error loading offsets: %v", err)
	}

	// Compare
	expected := map[string]int64{"topic1": 10, "topic2": 25}
	actual := map[string]int64{
		"topic1": store2.Get("topic1"),
		"topic2": store2.Get("topic2"),
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Loaded offsets do not match. Expected %v, got %v", expected, actual)
	}
}

func TestOffsetIncrement(t *testing.T) {
	store := storage.NewOffsetStore("")

	store.Set("test", 5)
	newOffset := store.Increment("test")

	if newOffset != 6 {
		t.Errorf("Expected offset 6, got %d", newOffset)
	}
}

func BenchmarkPublish(b *testing.B) {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		b.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	msg := protocol.Message{
		Topic:   "test_topic",
		Message: "hello world",
	}
	body, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("Error marshalling message: %v", err)
	}

	header := make([]byte, 5)
	header[0] = protocol.MessageTypePublish
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := conn.Write(header)
		if err != nil {
			b.Fatalf("Error writing header: %v", err)
		}
		_, err = conn.Write(body)
		if err != nil {
			b.Fatalf("Error writing body: %v", err)
		}
	}
}

func BenchmarkPublishSubscribe(b *testing.B) {
	// Publisher connection
	pubConn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		b.Fatalf("Error connecting publisher: %v", err)
	}
	defer pubConn.Close()

	// Subscriber connection
	subConn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		b.Fatalf("Error connecting subscriber: %v", err)
	}
	defer subConn.Close()

	topic := fmt.Sprintf("benchmark_topic_%d", time.Now().UnixNano())

	// Subscribe
	subMsg := protocol.Message{Topic: topic}
	subBody, _ := json.Marshal(subMsg)
	subHeader := make([]byte, 5)
	subHeader[0] = protocol.MessageTypeSubscribe
	binary.BigEndian.PutUint32(subHeader[1:], uint32(len(subBody)))
	if _, err := subConn.Write(subHeader); err != nil {
		b.Fatalf("Error writing subscribe header: %v", err)
	}
	if _, err := subConn.Write(subBody); err != nil {
		b.Fatalf("Error writing subscribe body: %v", err)
	}

	// Reader for subscriber
	subReader := bufio.NewReader(subConn)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Publish
		pubMsg := protocol.Message{Topic: topic, Message: "benchmark message"}
		pubBody, _ := json.Marshal(pubMsg)
		pubHeader := make([]byte, 5)
		pubHeader[0] = protocol.MessageTypePublish
		binary.BigEndian.PutUint32(pubHeader[1:], uint32(len(pubBody)))
		if _, err := pubConn.Write(pubHeader); err != nil {
			b.Fatalf("Error writing publish header: %v", err)
		}
		if _, err := pubConn.Write(pubBody); err != nil {
			b.Fatalf("Error writing publish body: %v", err)
		}

		// Read message from server
		header := make([]byte, 5)
		_, err := subReader.Read(header)
		if err != nil {
			b.Fatalf("Error reading header from subscriber: %v", err)
		}
		bodyLength := binary.BigEndian.Uint32(header[1:])
		body := make([]byte, bodyLength)
		_, err = subReader.Read(body)
		if err != nil {
			b.Fatalf("Error reading body from subscriber: %v", err)
		}

		// Send ACK
		var receivedMsg protocol.Message
		if err := json.Unmarshal(body, &receivedMsg); err != nil {
			b.Fatalf("Error unmarshalling message: %v", err)
		}
		ack := protocol.Ack{Topic: topic, Offset: int64(receivedMsg.ID)}
		ackBody, _ := json.Marshal(ack)
		ackHeader := make([]byte, 5)
		ackHeader[0] = protocol.MessageTypeAck
		binary.BigEndian.PutUint32(ackHeader[1:], uint32(len(ackBody)))
		if _, err := subConn.Write(ackHeader); err != nil {
			b.Fatalf("Error writing ACK header: %v", err)
		}
		if _, err := subConn.Write(ackBody); err != nil {
			b.Fatalf("Error writing ACK body: %v", err)
		}
	}
}
