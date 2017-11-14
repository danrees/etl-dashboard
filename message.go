package main

import (
	"encoding/json"
	"github.com/streadway/amqp"
	"log"
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
	//TODO: Consider here, you create and dispose of a channel each send ... figure out how to keep the publish channel alive
	ch := rm.channel
	//defer ch.Close()
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
		"#",
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
		log.Printf("Received message: %s on %s with key %s -> %s", d.Body, d.Exchange, d.RoutingKey, d.CorrelationId)
	}

	return nil
}
