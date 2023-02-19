package types

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MessageHandler interface {
	Handle(amqp.Delivery)
}

type MessageParams struct {
	Exchange    string
	RoutingKey  string
	Mandatory   bool
	Immediate   bool
	ContentType ContentType
}

type RabbitMessage struct {
	ch          *amqp.Channel
	msg         []byte
	contentType ContentType
	exchange    string
	routingKey  string
	mandatory   bool
	immediate   bool
}

func NewMessage(ch *amqp.Channel, msg []byte, messageParams MessageParams) *RabbitMessage {
	return &RabbitMessage{
		ch:          ch,
		msg:         msg,
		contentType: messageParams.ContentType,
		exchange:    messageParams.Exchange,
		routingKey:  messageParams.RoutingKey,
		mandatory:   messageParams.Mandatory,
		immediate:   messageParams.Immediate,
	}
}

func (m *RabbitMessage) Publish(ctx context.Context) error {
	err := m.ch.PublishWithContext(ctx,
		m.exchange,
		m.routingKey,
		m.mandatory,
		m.immediate,
		amqp.Publishing{
			ContentType: string(m.contentType),
			Body:        m.msg,
		})
	return err
}
