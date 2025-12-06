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
	Topic   string `json:"topic"`
	Message string `json:"message,omitempty"`
	ID      uint32 `json:"id,omitempty"`
}

type Ack struct {
	Topic  string `json:"topic"`
	Offset int64  `json:"offset"`
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

			if _, err := conn.Write(header); err != nil {
				fmt.Println("Erro ao enviar cabeçalho:", err)
				continue
			}
			if _, err := conn.Write(body); err != nil {
				fmt.Println("Erro ao enviar corpo:", err)
				continue
			}

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

			if _, err := conn.Write(header); err != nil {
				fmt.Println("Erro ao enviar cabeçalho:", err)
				continue
			}
			if _, err := conn.Write(body); err != nil {
				fmt.Println("Erro ao enviar corpo:", err)
				continue
			}

		case "ack":
			if len(parts) < 3 {
				fmt.Println("Uso: ack <tópico> <offset>")
				continue
			}
			topic := parts[1]
			offset, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				fmt.Println("offset tem de ser um número inteiro")
				continue
			}
			ack := Ack{Topic: topic, Offset: offset}
			body, err := json.Marshal(ack)
			if err != nil {
				fmt.Println("Erro ao codificar o ACK:", err)
				continue
			}
			header := make([]byte, 5)
			header[0] = 0x03 // ACK
			binary.BigEndian.PutUint32(header[1:], uint32(len(body)))
			if _, err := conn.Write(header); err != nil {
				fmt.Println("Erro ao enviar cabeçalho:", err)
				continue
			}
			if _, err := conn.Write(body); err != nil {
				fmt.Println("Erro ao enviar corpo:", err)
				continue
			}

		case "exit":
			return
		default:
			fmt.Println("Command desconhecido:", command)
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

		switch messageType {
		case 0x03: // MESSAGE
			var msg Message
			err = json.Unmarshal(body, &msg)
			if err != nil {
				fmt.Println("Erro ao decodificar a mensagem:", err)
				return
			}
			fmt.Printf("Mensagem recebida do tópico '%s' (id=%d): %s\n", msg.Topic, msg.ID, msg.Message)
		case 0xFF: // Error
			fmt.Printf("Erro do servidor: %s\n", string(body))
		default:
			fmt.Printf("Mensagem desconhecida do servidor (tipo %d): %s\n", messageType, string(body))
		}
	}
}