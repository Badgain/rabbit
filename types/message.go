package types

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageHandler func(amqp.Delivery)
