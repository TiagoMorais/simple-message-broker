package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type Message struct {
        Topic   string      `json:"topic"`
        Message string      `json:"message"`
        Id      uint32      `json:"id"`
}

type Subscription struct {
        Topic   string
        Conn    net.Conn
}

var subscriptions = struct {
        sync.RWMutex
        m map[string][]net.Conn
}{m: make(map[string][]net.Conn)}

const MaxBodySize = 1024 * 1024

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
                if bodyLength > MaxBodySize{
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
                        publishMessage(msg)
                case 0x02: // SUBSCRIBE
                        var sub Subscription
                        err = json.Unmarshal(body, &sub)
                        if err != nil {
                                fmt.Println("Erro ao decodificar a mensagem SUBSCRIBE:", err)
                                return
                        }
                        sub.Conn = conn
                        subscribe(sub)
                case 0x03: // ACK
                        var ack Message
                        err = json.Unmarshal(body, &ack)
                        if err != nil {
                                fmt.Println("Erro ao decodificar a mensagem ACK:", err)
                                return
                        }
                        fmt.Println("ACK recebido para a mensagem:", ack.Id)
                default:
                        fmt.Println("Tipo de mensagem desconhecido:", messageType)
                        return
                }
        }
}

func publishMessage(msg Message) {
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

func subscribe(sub Subscription) {
        subscriptions.Lock()
        subscriptions.m[sub.Topic] = append(subscriptions.m[sub.Topic], sub.Conn)
        subscriptions.Unlock()
}

func main() {
        listener, err := net.Listen("tcp", ":8080")
        if err != nil {
                fmt.Println("Erro ao iniciar o servidor:", err)
                return
        }
        defer listener.Close()

        fmt.Println("Servidor iniciado na porta 8080")

        for {
                conn, err := listener.Accept()
                if err != nil {
                        fmt.Println("Erro ao aceitar a conexão:", err)
                        continue
                }
                go handleConnection(conn)
        }
}