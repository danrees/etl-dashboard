package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randomString(l int) string {
	bytes := make([]byte, l)

	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}

	return string(bytes)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	var rabbitUser = flag.String("user", "guest", "RabbitMQ user name")
	var rabbitPassword = flag.String("password", "guest", "RabbitMQ password")
	var rabbitHost = flag.String("host", "localhost", "RabbitMQ host")
	var rabbitPort = flag.String("port", "5672", "RabbitMQ port")
	var sendKey = flag.String("routingKey", "", "Routing that is sent on")

	flag.Parse()

	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s:%s", *rabbitUser, *rabbitPassword, *rabbitHost, *rabbitPort))

	if err != nil {
		log.Fatal(fmt.Sprintf("Unable to retrieve connection to rabbitmq server at %v:%v using provided credentials", rabbitHost, rabbitPort))
		panic(err)

	}
	defer conn.Close()
	exchangeName := "etl_exchange"
	//routingKey := "test.key"

	publishChannel, err := conn.Channel()
	if err != nil {
		log.Fatal("Unable to create publish channel for rabbitmq", err)
		panic(err)
	}
	defer publishChannel.Close()
	var publisher Sender = NewRabbitMessenger(publishChannel, exchangeName)

	subscribeChannel, err := conn.Channel()
	if err != nil {
		log.Fatal("Unable to create subscribe channel for rabbitmq", err)
		panic(err)
	}
	defer subscribeChannel.Close()
	var subscriber Watcher = NewRabbitMessenger(subscribeChannel, exchangeName)

	//Start up the watcher ... I hope
	go subscriber.Watch("#")

	log.Println("[*] Watcher starting, waiting for messages")

	http.HandleFunc("/message", func(writer http.ResponseWriter, request *http.Request) {
		var msg Message
		err := json.NewDecoder(request.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), 400)
			return
		}
		err = publisher.Send(msg, *sendKey, randomString(32))
	})

	http.ListenAndServe(":8002", nil)
}
