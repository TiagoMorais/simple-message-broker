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
)

func TestSubscribe(t *testing.T) {
	// Clean up before and after test
	subscriptions.m = make(map[string][]net.Conn)
	defer func() {
		subscriptions.m = make(map[string][]net.Conn)
	}()

	topic := "test_subscribe_topic"
	conn1, _ := net.Pipe()
	defer conn1.Close()

	sub1 := Subscription{
		Topic: topic,
		Conn:  conn1,
	}

	// First subscription should succeed
	if ok := subscribe(sub1); !ok {
		t.Errorf("Expected first subscription to succeed, but it failed")
	}

	if len(subscriptions.m[topic]) != 1 {
		t.Errorf("Expected 1 subscriber for topic %s, got %d", topic, len(subscriptions.m[topic]))
	}

	// Second subscription should fail
	conn2, _ := net.Pipe()
	defer conn2.Close()
	sub2 := Subscription{
		Topic: topic,
		Conn:  conn2,
	}
	if ok := subscribe(sub2); ok {
		t.Errorf("Expected second subscription to fail, but it succeeded")
	}

	if len(subscriptions.m[topic]) != 1 {
		t.Errorf("Expected 1 subscriber for topic %s after failed attempt, got %d", topic, len(subscriptions.m[topic]))
	}
}

func TestSaveAndLoadTopicOffsets(t *testing.T) {
	// Clean up before and after test
	defer func() {
		os.Remove(OffsetsFile)
		topicOffsets.offsets = make(map[string]int64)
	}()

	// 1. Save some offsets
	expectedOffsets := map[string]int64{
		"topic1": 10,
		"topic2": 25,
	}
	topicOffsets.offsets = expectedOffsets
	saveTopicOffsets()

	// 2. Clear in-memory offsets
	topicOffsets.offsets = make(map[string]int64)

	// 3. Load from file
	loadTopicOffsets()

	// 4. Compare
	if !reflect.DeepEqual(expectedOffsets, topicOffsets.offsets) {
		t.Errorf("Loaded offsets do not match saved offsets. Expected %v, got %v", expectedOffsets, topicOffsets.offsets)
	}
}


func BenchmarkPublish(b *testing.B) {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		b.Fatalf("Error connecting to server: %v", err)
	}
	defer conn.Close()

	msg := Message{
		Topic:   "test_topic",
		Message: "hello world",
	}
	body, err := json.Marshal(msg)
	if err != nil {
		b.Fatalf("Error marshalling message: %v", err)
	}

	header := make([]byte, 5)
	header[0] = 0x01 // PUBLISH
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
	subMsg := Message{Topic: topic}
	subBody, _ := json.Marshal(subMsg)
	subHeader := make([]byte, 5)
	subHeader[0] = 0x02 // SUBSCRIBE
	binary.BigEndian.PutUint32(subHeader[1:], uint32(len(subBody)))
	subConn.Write(subHeader)
	subConn.Write(subBody)

	// Reader for subscriber
	subReader := bufio.NewReader(subConn)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Publish
		pubMsg := Message{Topic: topic, Message: "benchmark message"}
		pubBody, _ := json.Marshal(pubMsg)
		pubHeader := make([]byte, 5)
		pubHeader[0] = 0x01 // PUBLISH
		binary.BigEndian.PutUint32(pubHeader[1:], uint32(len(pubBody)))
		pubConn.Write(pubHeader)
		pubConn.Write(pubBody)

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
		var receivedMsg Message
		json.Unmarshal(body, &receivedMsg)
		ack := Ack{Topic: topic, Offset: int64(receivedMsg.Id)}
		ackBody, _ := json.Marshal(ack)
		ackHeader := make([]byte, 5)
		ackHeader[0] = 0x03 // ACK
		binary.BigEndian.PutUint32(ackHeader[1:], uint32(len(ackBody)))
		subConn.Write(ackHeader)
		subConn.Write(ackBody)
	}
}
