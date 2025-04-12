package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Message struct {
        Topic   string      `json:"topic"`
        Message string      `json:"message"`
        Id      uint32      `json:"id"`
}

func main() {
        if len(os.Args) < 2 {
                fmt.Println("Uso: cli <endereço do servidor>")
                return
        }

        serverAddr := os.Args[1]

        conn, err := net.Dial("tcp", serverAddr)
        if err != nil {
                fmt.Println("Erro ao conectar ao servidor:", err)
                return
        }
        defer conn.Close()

        reader := bufio.NewReader(os.Stdin)
        go readMessages(conn) // Inicia goroutine para ler mensagens

        for {
                fmt.Print("> ")
                input, _ := reader.ReadString('\n')
                input = strings.TrimSpace(input)

                parts := strings.Split(input, " ")
                if len(parts) == 0 {
                        continue
                }

                command := parts[0]

                switch command {
                case "publish":
                        if len(parts) < 3 {
                                fmt.Println("Uso: publish <tópico> <mensagem>")
                                continue
                        }
                        topic := parts[1]
                        message := strings.Join(parts[2:], " ")

                        msg := Message{Topic: topic, Message: message}
                        body, err := json.Marshal(msg)
                        if err != nil {
                                fmt.Println("Erro ao codificar a mensagem:", err)
                                continue
                        }

                        header := make([]byte, 5)
                        header[0] = 0x01 // PUBLISH
                        binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

                        conn.Write(header)
                        conn.Write(body)

                case "subscribe":
                        if len(parts) < 2 {
                                fmt.Println("Uso: subscribe <tópico>")
                                continue
                        }
                        topic := parts[1]

                        msg := Message{Topic: topic}
                        body, err := json.Marshal(msg)
                        if err != nil {
                                fmt.Println("Erro ao codificar a mensagem:", err)
                                continue
                        }

                        header := make([]byte, 5)
                        header[0] = 0x02 // SUBSCRIBE
                        binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

                        conn.Write(header)
                        conn.Write(body)
                case "ack":
                        if len(parts) < 2 {
                                fmt.Println("Uso: ack <id>")
                                continue
                        }

                        id, err := strconv.Atoi(parts[1])

                        if err != nil{
                                fmt.Println("id tem de ser um número")
                                continue
                        }

                        msg := Message{Id: uint32(id)}
                        body, err := json.Marshal(msg)

                        if err != nil {
                                fmt.Println("Erro ao codificar a mensagem:", err)
                                continue
                        }

                        header := make([]byte, 5)
                        header[0] = 0x04 // ACK
                        binary.BigEndian.PutUint32(header[1:], uint32(len(body)))

                        conn.Write(header)
                        conn.Write(body)

                case "exit":
                        return
                default:
                        fmt.Println("Comando desconhecido:", command)
                }
        }
}

func readMessages(conn net.Conn) {
        reader := bufio.NewReader(conn)
        for {
                header := make([]byte, 5)
                _, err := reader.Read(header)
                if err != nil {
                        fmt.Println("Erro ao ler o cabeçalho:", err)
                        return
                }

                messageType := header[0]
                bodyLength := binary.BigEndian.Uint32(header[1:])

                body := make([]byte, bodyLength)
                _, err = reader.Read(body)
                if err != nil {
                        fmt.Println("Erro ao ler o corpo:", err)
                        return
                }

                if messageType == 0x03 { // MESSAGE
                        var msg Message
                        err = json.Unmarshal(body, &msg)
                        if err != nil {
                                fmt.Println("Erro ao decodificar a mensagem:", err)
                                return
                        }
                        fmt.Printf("Mensagem recebida do tópico '%s': %s\n", msg.Topic, msg.Message)
                }
        }
}