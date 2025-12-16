package protocol

import "net"

// Message types
const (
	MessageTypePublish   = 0x01
	MessageTypeSubscribe = 0x02
	MessageTypeAck       = 0x03
	MessageTypeMessage   = 0x03
	MessageTypeError     = 0xFF
)

// MaxBodySize is the maximum allowed message body size (1MB)
const MaxBodySize = 1024 * 1024

// Message represents a pub/sub message
type Message struct {
	Topic   string `json:"topic"`
	Message string `json:"message"`
	ID      uint32 `json:"id"`
}

// Subscription represents a topic subscription request
type Subscription struct {
	Topic string
	Conn  net.Conn
}

// Ack represents an acknowledgment from a consumer
type Ack struct {
	Topic  string `json:"topic"`
	Offset int64  `json:"offset"`
}
