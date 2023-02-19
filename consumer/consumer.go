package consumer

import (
	"errors"
	"log"
	"rabbit/types"
	"rabbit/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumerParams struct {
	Name             string
	Durable          bool
	DeleteWhenUnused bool
	Exclusive        bool
	NoWait           bool
	Args             amqp.Table
}

type QueueOptions struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type Consumer interface {
	Consume() error
	SetHandler(types.MessageHandler)
	Kill() error
}

type consumer struct {
	ch      *amqp.Channel
	queue   amqp.Queue
	opts    QueueOptions
	msgs    <-chan amqp.Delivery
	ready   bool
	handler types.MessageHandler
}

func NewConsumer(ch *amqp.Channel, params ConsumerParams, queueOpts QueueOptions) Consumer {
	q, err := ch.QueueDeclare(
		params.Name,
		params.Durable,
		params.DeleteWhenUnused,
		params.Exclusive,
		params.NoWait,
		params.Args,
	)
	utils.FailHandler(err, "Failed to declare a queue")
	return &consumer{
		ch:    ch,
		queue: q,
		opts:  queueOpts,
		ready: false,
	}
}

func (c *consumer) SetHandler(h types.MessageHandler) {
	c.handler = h
}

func (c *consumer) Consume() error {
	if c.handler == nil {
		return errors.New("Missing handler")
	}

	msgs, err := c.ch.Consume(
		c.opts.Queue,
		c.opts.Consumer,
		c.opts.AutoAck,
		c.opts.Exclusive,
		c.opts.NoLocal,
		c.opts.NoWait,
		c.opts.Args,
	)
	if err != nil {
		return err
	}

	go func() {
		for d := range msgs {
			c.handler.Handle(d)
		}
	}()

	return nil
}

func (c *consumer) Kill() error {
	log.Printf("Killing Consumer \n")
	return c.ch.Close()
}
