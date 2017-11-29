package messaging

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
	"time"
)

const queueName string = "etl-dashboard"

type Message struct {
	Env map[string]string `json:"env"`
}

type Sender interface {
	Send(msg Message, routingKey string, correlationId string) error
}

type Watcher interface {
	Watch(routingKey string) error
}

type Messenger interface {
	Watcher
	Sender
}

type RabbitMessenger struct {
	channel      *amqp.Channel
	exchangeName string
}

func NewRabbitMessenger(channel *amqp.Channel, exchangeName string) Messenger {
	return RabbitMessenger{channel: channel, exchangeName: exchangeName}
}

func (rm RabbitMessenger) Send(msg Message, routingKey string, correlationId string) error {
	//referencing the channel from the owning struct to avoid some refactoring
	ch := rm.channel
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return ch.Publish(
		rm.exchangeName,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: correlationId,
			Body:          body,
			Timestamp:     time.Now(),
		},
	)
}

func (rm RabbitMessenger) Watch(routingKey string) error {
	ch := rm.channel

	err := ch.ExchangeDeclare(
		rm.exchangeName,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	q, err := ch.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = ch.QueueBind(
		q.Name,
		routingKey,
		rm.exchangeName,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for d := range msgs {

		//TODO: I feel like I should manually ack ... but I couldn't get that to work
		log.Printf("Received message at [%s]: %s on %s with key %s -> %s", d.Timestamp, d.Body, d.Exchange, d.RoutingKey, d.CorrelationId)
	}

	return nil
}
