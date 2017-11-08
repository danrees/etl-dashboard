package main

import (
	"encoding/json"
	"github.com/streadway/amqp"
)

type Message struct {
	Env map[string]string `json:"env"`
}

type Messenger interface {
	Send(msg Message, routingKey string, correlationId string) error
}

type RabbitMessenger struct {
	connection   *amqp.Connection
	exchangeName string
}

func NewRabbitMessenger(conn *amqp.Connection, exchangeName string) Messenger {
	return RabbitMessenger{connection: conn, exchangeName: exchangeName}
}

func (rm RabbitMessenger) Send(msg Message, routingKey string, correlationId string) error {
	ch, err := rm.connection.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
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
