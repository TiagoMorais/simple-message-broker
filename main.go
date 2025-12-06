package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

// Message types
const (
	MessageTypePublish   = 0x01
	MessageTypeSubscribe = 0x02
	MessageTypeAck       = 0x03
	MessageTypeMessage   = 0x03
	MessageTypeError     = 0xFF
)

type Message struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
	Id      uint32 `json:"id"`
}

type Subscription struct {
	Topic string
	Conn  net.Conn
}

type Ack struct {
	Topic  string `json:"topic"`
	Offset int64  `json:"offset"`
}

var subscriptions = struct {
	sync.RWMutex
	m map[string][]net.Conn
}{m: make(map[string][]net.Conn)}

const MaxBodySize = 1024 * 1024

// const WriteAheadLogDirectory = "/etc/simple_message_broker/wal/"
const WriteAheadLogDirectory = "./wal/"
const ConfigFile = "config.json"
const OffsetsFile = "offsets.json"

// Track each topic's offset (only one consumer per topic)
var topicOffsets = struct {
	sync.RWMutex
	offsets map[string]int64
}{offsets: make(map[string]int64)}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Ler o cabeçalho
		header := make([]byte, 5)
		_, err := io.ReadFull(reader, header)
		if err != nil {
			if err == io.EOF {
				// Connection closed by the client
				return
			}
			log.Println("Error reading header:", err)
			return
		}

		messageType := header[0]
		bodyLength := binary.BigEndian.Uint32(header[1:])

		// Verificar se o tamanho do body passa 1MB
		if bodyLength > MaxBodySize {
			log.Println("Body size exceeds 1MB limit.")
			return
		}

		// Ler o corpo da mensagem
		body := make([]byte, bodyLength)
		_, err = io.ReadFull(reader, body)
		if err != nil {
			if err == io.EOF {
				// Connection closed by the client
				return
			}
			log.Println("Error reading body:", err)
			return
		}

		// Processar a mensagem
		switch messageType {
		case MessageTypePublish:
			var msg Message
			err = json.Unmarshal(body, &msg)
			if err != nil {
				log.Printf("Error decoding PUBLISH message: %v\n", err)
				return
			}
			if err := writeOnWriteAheadLog(msg); err != nil {
				log.Printf("Error writing to WAL for topic %s: %v\n", msg.Topic, err)
				return
			}
			// Do NOT send to consumers directly here
		case MessageTypeSubscribe:
			var sub Subscription
			err = json.Unmarshal(body, &sub)
			if err != nil {
				log.Printf("Error decoding SUBSCRIBE message: %v\n", err)
				return
			}
			sub.Conn = conn
			if !subscribe(sub) {
				// Send error to client: only one consumer allowed
				log.Printf("Subscription rejected for topic %s: already has a consumer\n", sub.Topic)
				sendErrorToClient(conn, "Topic already has a consumer")
				return
			}
			log.Printf("New subscription for topic: %s\n", sub.Topic)
			// Start sending messages from WAL at current topic offset (default 0)
			topicOffsets.Lock()
			if _, exists := topicOffsets.offsets[sub.Topic]; !exists {
				topicOffsets.offsets[sub.Topic] = 0
			}
			offset := topicOffsets.offsets[sub.Topic]
			topicOffsets.Unlock()
			sendMessageFromWALAtOffset(conn, sub.Topic, offset)
			saveTopicOffsets()
		case MessageTypeAck:
			var ack Ack
			err = json.Unmarshal(body, &ack)
			if err != nil {
				log.Printf("Error decoding ACK message: %v\n", err)
				return
			}
			log.Printf("ACK received for topic %s, offset %d\n", ack.Topic, ack.Offset)
			handleAckAndSendNextTopic(ack.Topic, conn)
			saveTopicOffsets()
		default:
			log.Println("Unknown message type:", messageType)
			return
		}
	}
}

func writeOnWriteAheadLog(msg Message) error {
	walPath := WriteAheadLogDirectory + msg.Topic + ".log"

	// Ensure the directory exists.
	if err := os.MkdirAll(WriteAheadLogDirectory, os.ModePerm); err != nil {
		return err
	}

	// Open the file for appending, creating it if it doesn't exist.
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	line, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(line, '\n')); err != nil {
		return err
	}

	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		return err
	}

	// Notify subscriber if one exists and is waiting
	subscriptions.RLock()
	defer subscriptions.RUnlock()
	if conns, ok := subscriptions.m[msg.Topic]; ok && len(conns) > 0 {
		topicOffsets.RLock()
		offset := topicOffsets.offsets[msg.Topic]
		topicOffsets.RUnlock()
		sendMessageFromWALAtOffset(conns[0], msg.Topic, offset)
	}

	return nil
}

// Send the next message from WAL to the consumer based on topic offset
func sendMessageFromWALAtOffset(conn net.Conn, topic string, offset int64) {
	walPath := WriteAheadLogDirectory + topic + ".log"
	file, err := os.Open(walPath)
	if err != nil {
		// This can happen if a client subscribes before any messages are published.
		// It's not a server error, so we just return.
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var currentLine int64
	for scanner.Scan() {
		if currentLine == offset {
			var msg Message
			if err := json.Unmarshal(scanner.Bytes(), &msg); err == nil {
				// Ensure the message has a unique ID (use offset as ID if not set)
				if msg.Id == 0 {
					msg.Id = uint32(offset)
				}
				body, _ := json.Marshal(msg)
				header := make([]byte, 5)
				header[0] = MessageTypeMessage
				binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
				conn.Write(header)
				conn.Write(body)
			}
			break
		}
		currentLine++
	}
}

// On ACK, advance topic offset and send next message
func handleAckAndSendNextTopic(topic string, conn net.Conn) {
	topicOffsets.Lock()
	topicOffsets.offsets[topic]++
	offset := topicOffsets.offsets[topic]
	topicOffsets.Unlock()
	sendMessageFromWALAtOffset(conn, topic, offset)
}

func subscribe(sub Subscription) bool {
	subscriptions.Lock()
	defer subscriptions.Unlock()
	if len(subscriptions.m[sub.Topic]) > 0 {
		// Already has a consumer
		return false
	}
	subscriptions.m[sub.Topic] = []net.Conn{sub.Conn}
	return true
}

func sendErrorToClient(conn net.Conn, errMsg string) {
	header := make([]byte, 5)
	header[0] = MessageTypeError
	msg := []byte(errMsg)
	binary.BigEndian.PutUint32(header[1:], uint32(len(msg)))
	conn.Write(header)
	conn.Write(msg)
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Error starting server: %v\n", err)
	}
	defer listener.Close()

	log.Println("Server started on port 8080")
	loadTopicOffsets()
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v\n", err)
			continue
		}
		go handleConnection(conn)
	}
}



// Save topic offsets to disk
func saveTopicOffsets() {
	topicOffsets.RLock()
	defer topicOffsets.RUnlock()
	file, err := os.Create(OffsetsFile)
	if err != nil {
		log.Printf("Error creating offsets file: %v\n", err)
		return
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	if err := enc.Encode(topicOffsets.offsets); err != nil {
		log.Printf("Error saving offsets: %v\n", err)
		return
	}
	// Sync to ensure durability
	if err := file.Sync(); err != nil {
		log.Printf("Error syncing offsets file: %v\n", err)
	}
}

// Load topic offsets from disk
func loadTopicOffsets() {
	file, err := os.Open(OffsetsFile)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("Offsets file not found, starting with empty offsets.")
		} else {
			log.Printf("Error opening offsets file: %v\n", err)
		}
		return
	}
	defer file.Close()
	var snapshot map[string]int64
	dec := json.NewDecoder(file)
	if err := dec.Decode(&snapshot); err != nil {
		log.Printf("Error loading offsets: %v\n", err)
		return
	}
	topicOffsets.Lock()
	for topic, offset := range snapshot {
		topicOffsets.offsets[topic] = offset
	}
	topicOffsets.Unlock()
	log.Printf("Topic offsets loaded from disk: %v\n", snapshot)
}
