package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
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

type Topic struct {
	Name   string `json:"name"`
	Offset int64  `json:"offset"`
}

type Ack struct {
	Topic  string `json:"topic"`
	Offset int64  `json:"offset"`
}

var topic_subscribers = struct {
	sync.RWMutex
	subs map[Topic]net.Conn
}{subs: make(map[Topic]net.Conn)}

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

func getTopicByName(name string) (*Topic, error) {
        for k := range topic_subscribers.subs {
		if k.Name == name {
                        return &k, nil
                }
	}
	return nil, fmt.Errorf("tópico %s não encontrado", name) 
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		// Ler o cabeçalho
		header := make([]byte, 5)
		_, err := reader.Read(header)
		if err != nil {
			fmt.Println("Erro ao ler o cabeçalho:", err)
			return
		}

		messageType := header[0]
		bodyLength := binary.BigEndian.Uint32(header[1:])

		// Verificar se o tamanho do body passa 1MB
		if bodyLength > MaxBodySize {
			fmt.Println("Tamanho do body excede o limite 1MB.")
			return
		}

		// Ler o corpo da mensagem
		body := make([]byte, bodyLength)
		_, err = reader.Read(body)
		if err != nil {
			fmt.Println("Erro ao ler o corpo:", err)
			return
		}

		// Processar a mensagem
		switch messageType {
		case 0x01: // PUBLISH
			var msg Message
			err = json.Unmarshal(body, &msg)
			if err != nil {
				fmt.Println("Erro ao decodificar a mensagem PUBLISH:", err)
				return
			}
			writeOnWriteAheadLog(msg)
			// Do NOT send to consumers directly here
		case 0x02: // SUBSCRIBE
			var sub Subscription
			err = json.Unmarshal(body, &sub)
			if err != nil {
				fmt.Println("Erro ao decodificar a mensagem SUBSCRIBE:", err)
				return
			}
			sub.Conn = conn
			if !subscribe(sub) {
				// Send error to client: only one consumer allowed
				header := make([]byte, 5)
				header[0] = 0xFF // Custom error type
				msg := []byte("Topic already has a consumer")
				binary.BigEndian.PutUint32(header[1:], uint32(len(msg)))
				conn.Write(header)
				conn.Write(msg)
				return
			}
			// Start sending messages from WAL at current topic offset (default 0)
			topicOffsets.Lock()
			if _, exists := topicOffsets.offsets[sub.Topic]; !exists {
				topicOffsets.offsets[sub.Topic] = 0
			}
			offset := topicOffsets.offsets[sub.Topic]
			topicOffsets.Unlock()
			sendMessageFromWALAtOffset(conn, sub.Topic, offset)
			saveTopicOffsets()
		case 0x03: // ACK
			var ack Ack
			err = json.Unmarshal(body, &ack)
			if err != nil {
				fmt.Println("Erro ao decodificar a mensagem ACK:", err)
				return
			}
			fmt.Printf("ACK recebido para o tópico %s, offset %d\n", ack.Topic, ack.Offset)
			handleAckAndSendNextTopic(ack.Topic, conn)
			saveTopicOffsets()
		default:
			fmt.Println("Tipo de mensagem desconhecido:", messageType)
			return
		}
	}
}

func writeOnWriteAheadLog(msg Message) {
	wal_path := WriteAheadLogDirectory + msg.Topic + ".log"
	var _, err = os.Stat(wal_path)
	var file *os.File = nil
	defer file.Close()
	if os.IsNotExist(err) {
		err = os.MkdirAll(WriteAheadLogDirectory, os.ModePerm)
		if err != nil {
			fmt.Println(err)
		}
		file, err = os.Create(wal_path)
		if err != nil {
			fmt.Println(err)
			return
		}
	} else if err == nil {
		file, err = os.OpenFile(wal_path, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println("Erro ao abrir o WAL:", err)
			return
		}
	}

	//line, err := json.Marshal(msg)
	cenas, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("Erro ao serializar mensagem para WAL:", err)
		return
	}
	if _, err := file.Write(append(cenas, '\n')); err != nil {
		fmt.Println("Erro ao escrever no WAL:", err)
	}
}

// Send the next message from WAL to the consumer based on topic offset
func sendMessageFromWALAtOffset(conn net.Conn, topic string, offset int64) {
	walPath := WriteAheadLogDirectory + topic + ".log"
	file, err := os.Open(walPath)
	if err != nil {
		fmt.Println("Erro ao abrir WAL:", err)
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
				header[0] = 0x03 // MESSAGE
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

func publishMessage(msg Message) {
        topic_subscribers.RLock()
        topic, err := getTopicByName(msg.Topic)
        if err != nil {
                fmt.Println("Erro ao obter tópico:", err)
                topic_subscribers.RUnlock()
                return
        }
        sub_connection := topic_subscribers.subs[*topic]
        body, err := json.Marshal(msg)
        if err != nil {
                fmt.Println("Erro ao codificar a mensagem:", err)
                return
        }
        header := make([]byte, 5)
        header[0] = 0x03 // MESSAGE
        binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
        if sub_connection != nil {
                sub_connection.Write(header)
                sub_connection.Write(body)
        }


	subscriptions.RLock()
	for _, conn := range subscriptions.m[msg.Topic] {
		// Enviar a mensagem para todos os subscritores do tópico
		body, err := json.Marshal(msg)
		if err != nil {
			fmt.Println("Erro ao codificar a mensagem:", err)
			continue
		}
		header := make([]byte, 5)
		header[0] = 0x03 // MESSAGE
		binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
		conn.Write(header)
		conn.Write(body)
	}
	subscriptions.RUnlock()
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

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Erro ao iniciar o servidor:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Servidor iniciado na porta 8080")
	loadTopicOffsets()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar a conexão:", err)
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
		fmt.Println("Erro ao criar arquivo de offsets:", err)
		return
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	if err := enc.Encode(topicOffsets.offsets); err != nil {
		fmt.Println("Erro ao salvar offsets:", err)
	}
}

// Load topic offsets from disk
func loadTopicOffsets() {
	file, err := os.Open(OffsetsFile)
	if err != nil {
		fmt.Println("Arquivo de offsets não encontrado, iniciando vazio.")
		return
	}
	defer file.Close()
	var snapshot map[string]int64
	dec := json.NewDecoder(file)
	if err := dec.Decode(&snapshot); err != nil {
		fmt.Println("Erro ao carregar offsets:", err)
		return
	}
	topicOffsets.Lock()
	for topic, offset := range snapshot {
		topicOffsets.offsets[topic] = offset
	}
	topicOffsets.Unlock()
	fmt.Println("Offsets de tópicos carregados do disco:", snapshot)
}
