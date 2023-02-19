package producer

import (
	"context"
	"rabbit/types"
	"rabbit/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer interface {
	Produce(context.Context, []byte, types.MessageParams) error
	Kill() error
}

type producer struct {
	ch *amqp.Channel
}

func NewProducer(ch *amqp.Channel) Producer {
	return &producer{
		ch: ch,
	}
}

func (p *producer) Produce(ctx context.Context, msg []byte, options types.MessageParams) error {
	mqMessage := types.NewMessage(p.ch, msg, options)
	err := mqMessage.Publish(ctx)
	if err != nil {
		utils.FailHandler(err, "Failed to publish message")
		return err
	}
	return nil
}

func (p *producer) Kill() error {
	return p.ch.Close()
}
