package broker

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"sync"

	"github.com/tiagomorais/simple-message-broker/internal/protocol"
	"github.com/tiagomorais/simple-message-broker/internal/storage"
	"github.com/tiagomorais/simple-message-broker/internal/wal"
)

// Broker handles message routing and subscription management
type Broker struct {
	wal           *wal.WAL
	offsetStore   *storage.OffsetStore
	subscriptions struct {
		sync.RWMutex
		m map[string][]net.Conn
	}
}

// NewBroker creates a new Broker instance
func NewBroker(w *wal.WAL, store *storage.OffsetStore) *Broker {
	b := &Broker{
		wal:         w,
		offsetStore: store,
	}
	b.subscriptions.m = make(map[string][]net.Conn)
	return b
}

// readHeader reads and parses the message header from a connection.
// Returns the message type, body length, and any error encountered.
func readHeader(reader *bufio.Reader) (messageType byte, bodyLength uint32, err error) {
	header := make([]byte, 5)
	_, err = io.ReadFull(reader, header)
	if err != nil {
		return 0, 0, err
	}

	messageType = header[0]
	bodyLength = binary.BigEndian.Uint32(header[1:])
	return messageType, bodyLength, nil
}

// HandleConnection handles a client connection
func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		messageType, bodyLength, err := readHeader(reader)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Error reading header:", err)
			return
		}

		// Check body size limit
		if bodyLength > protocol.MaxBodySize {
			log.Println("Body size exceeds 1MB limit.")
			return
		}

		// Read body
		body := make([]byte, bodyLength)
		_, err = io.ReadFull(reader, body)
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Error reading body:", err)
			return
		}

		// Process message
		switch messageType {
		case protocol.MessageTypePublish:
			b.handlePublish(body)
		case protocol.MessageTypeSubscribe:
			if !b.handleSubscribe(body, conn) {
				return
			}
		case protocol.MessageTypeAck:
			b.handleAck(body, conn)
		default:
			log.Println("Unknown message type:", messageType)
			return
		}
	}
}

func (b *Broker) handlePublish(body []byte) {
	var msg protocol.Message
	if err := json.Unmarshal(body, &msg); err != nil {
		log.Printf("Error decoding PUBLISH message: %v\n", err)
		return
	}

	if _, err := b.wal.Append(msg); err != nil {
		log.Printf("Error writing to WAL for topic %s: %v\n", msg.Topic, err)
		return
	}

	// Notify subscriber if one exists and is waiting
	b.subscriptions.RLock()
	defer b.subscriptions.RUnlock()
	if conns, ok := b.subscriptions.m[msg.Topic]; ok && len(conns) > 0 {
		offset := b.offsetStore.Get(msg.Topic)
		b.sendMessageFromWALAtOffset(conns[0], msg.Topic, offset)
	}
}

func (b *Broker) handleSubscribe(body []byte, conn net.Conn) bool {
	var sub protocol.Subscription
	if err := json.Unmarshal(body, &sub); err != nil {
		log.Printf("Error decoding SUBSCRIBE message: %v\n", err)
		return false
	}
	sub.Conn = conn

	if !b.subscribe(sub) {
		log.Printf("Subscription rejected for topic %s: already has a consumer\n", sub.Topic)
		b.sendErrorToClient(conn, "Topic already has a consumer")
		return false
	}

	log.Printf("New subscription for topic: %s\n", sub.Topic)

	// Initialize topic offset if needed
	b.offsetStore.InitTopic(sub.Topic)
	offset := b.offsetStore.Get(sub.Topic)
	b.sendMessageFromWALAtOffset(conn, sub.Topic, offset)
	if err := b.offsetStore.Save(); err != nil {
		log.Printf("Error saving offsets: %v\n", err)
	}

	return true
}

func (b *Broker) handleAck(body []byte, conn net.Conn) {
	var ack protocol.Ack
	if err := json.Unmarshal(body, &ack); err != nil {
		log.Printf("Error decoding ACK message: %v\n", err)
		return
	}

	log.Printf("ACK received for topic %s, offset %d\n", ack.Topic, ack.Offset)

	// Advance offset and send next message
	offset := b.offsetStore.Increment(ack.Topic)
	b.sendMessageFromWALAtOffset(conn, ack.Topic, offset)
	if err := b.offsetStore.Save(); err != nil {
		log.Printf("Error saving offsets: %v\n", err)
	}
}

func (b *Broker) subscribe(sub protocol.Subscription) bool {
	b.subscriptions.Lock()
	defer b.subscriptions.Unlock()
	if len(b.subscriptions.m[sub.Topic]) > 0 {
		return false
	}
	b.subscriptions.m[sub.Topic] = []net.Conn{sub.Conn}
	return true
}

func (b *Broker) sendMessageFromWALAtOffset(conn net.Conn, topic string, offset int64) {
	msg, err := b.wal.ReadAt(topic, offset)
	if err != nil || msg == nil {
		return
	}

	body, _ := json.Marshal(msg)
	header := make([]byte, 5)
	header[0] = protocol.MessageTypeMessage
	binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

	if _, err := conn.Write(header); err != nil {
		log.Printf("Error writing message header: %v\n", err)
		return
	}
	if _, err := conn.Write(body); err != nil {
		log.Printf("Error writing message body: %v\n", err)
	}
}

func (b *Broker) sendErrorToClient(conn net.Conn, errMsg string) {
	header := make([]byte, 5)
	header[0] = protocol.MessageTypeError
	msg := []byte(errMsg)
	binary.BigEndian.PutUint32(header[1:], uint32(len(msg)))

	if _, err := conn.Write(header); err != nil {
		log.Printf("Error writing error header: %v\n", err)
		return
	}
	if _, err := conn.Write(msg); err != nil {
		log.Printf("Error writing error message: %v\n", err)
	}
}
