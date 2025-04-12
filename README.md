# Simple Message Protocol (SMP)

## Descrição

O Simple Message Protocol (SMP) é um protocolo de mensagens personalizado projetado para um sistema de mensagens distribuído. Ele opera sobre TCP/IP e define um formato de mensagem e um conjunto de tipos de mensagens para facilitar a comunicação entre produtores, consumidores e o servidor de mensagens.

## Formato da Mensagem

Cada mensagem SMP é composta por um cabeçalho e um corpo.

* **Cabeçalho:**
    * `Tipo de Mensagem` (1 byte): Indica o tipo da mensagem (PUBLISH, SUBSCRIBE, MESSAGE, ACK).
    * `Comprimento do Corpo` (4 bytes): Indica o comprimento do corpo da mensagem em bytes.
* **Corpo:**
    * Os dados da mensagem, cujo formato varia dependendo do tipo da mensagem.

## Tipos de Mensagem

### PUBLISH (Tipo 0x01)

* Usado para publicar uma mensagem em um tópico.
* Corpo: `Tópico` (string), `Mensagem` (string).

### SUBSCRIBE (Tipo 0x02)

* Usado para se inscrever num tópico.
* Corpo: `Tópico` (string).

### MESSAGE (Tipo 0x03)

* Usado para enviar uma mensagem a um consumidor.
* Corpo: `Tópico` (string), `Mensagem` (string).

### ACK (Tipo 0x04)

* Usado para confirmar o recebimento de uma mensagem.
* Corpo: `ID da mensagem` (4 bytes).

## Exemplos de Mensagens

### PUBLISH

* `0x01`: Tipo de mensagem PUBLISH.
* `0x00 0x00 0x00 0x1A`: Comprimento do corpo (26 bytes).
* `"meu_topico" "Olá, mundo!"`: Corpo da mensagem (tópico e mensagem).

### SUBSCRIBE

* `0x02`: Tipo de mensagem SUBSCRIBE.
* `0x00 0x00 0x00 0x0A`: Comprimento do corpo (10 bytes).
* `"meu_topico"`: Corpo da mensagem (tópico).

### ACK

* `0x04`: Tipo de mensagem ACK.
* `0x00 0x00 0x00 0x04`: Comprimento do corpo (4 bytes).
* `0x00 0x00 0x30 0x39`: ID da mensagem recebida.

## Funcionamento do ACK

* Quando um produtor publica uma mensagem (PUBLISH) ou um servidor envia uma mensagem para um assinante (MESSAGE), ele atribui um ID único a essa mensagem.
* O ID é incluído no corpo da mensagem.
* Quando um consumidor ou servidor recebe a mensagem, ele verifica se a mensagem foi recebida corretamente.
* Se a mensagem foi recebida com sucesso, o consumidor ou servidor envia uma mensagem ACK de volta para o remetente, contendo o ID da mensagem original.
* Se o remetente não receber um ACK dentro de um determinado período de tempo (timeout), ele considera que a mensagem foi perdida e a retransmite.

## Implementação em Go

O protocolo SMP pode ser implementado em Go usando o pacote `net` para comunicação TCP/IP e o pacote `encoding/json` para serialização/desserialização de mensagens.

## Considerações

* Este é um protocolo simplificado e pode ser expandido para atender a requisitos específicos.
* A implementação do tratamento de erros e do controle de fluxo é essencial para garantir a confiabilidade do protocolo.
* A escolha de um formato de serialização eficiente, como Protocol Buffers, pode melhorar o desempenho.
