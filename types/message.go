package types

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageHandler interface {
	Handle(amqp.Delivery)
}
