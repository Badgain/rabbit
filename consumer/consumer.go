package consumer

import (
	"errors"
	"github.com/Badgain/rabbit/config"
	"github.com/Badgain/rabbit/types"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"log"
)

type Consumer interface {
	Consume() error
	Subscribe(types.MessageHandler) *consumer
	Kill() error
}

type consumer struct {
	ch      *amqp.Channel
	queue   amqp.Queue
	opts    *config.QueueConfig
	msgs    <-chan amqp.Delivery
	ready   bool
	handler types.MessageHandler
	logger  *zap.Logger
}

func NewConsumer(ch *amqp.Channel, consumerConf *config.ConsumerConfig, queueConf *config.QueueConfig, logger *zap.Logger) (Consumer, error) {
	q, err := ch.QueueDeclare(
		consumerConf.Name,
		consumerConf.Durable,
		consumerConf.DeleteWhenUnused,
		consumerConf.IsExclusive,
		consumerConf.NoWait,
		nil,
	)
	if err != nil {
		logger.Sugar().Errorw("Failed to declare a queue", err)
		return nil, err
	}

	return &consumer{
		ch:     ch,
		queue:  q,
		opts:   queueConf,
		ready:  false,
		logger: logger,
	}, nil
}

func (c *consumer) Subscribe(h types.MessageHandler) *consumer {
	c.handler = h
	return c
}

func (c *consumer) Consume() error {
	if c.handler == nil {
		return errors.New("Missing handler")
	}

	msgs, err := c.ch.Consume(
		c.opts.Queue,
		c.opts.Consumer,
		c.opts.AutoAck,
		c.opts.IsExclusive,
		c.opts.NoLocal,
		c.opts.NoWait,
		nil,
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
