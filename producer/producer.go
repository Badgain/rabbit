package producer

import (
	"context"
	"github.com/Badgain/rabbit/config"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Producer interface {
	Produce(context.Context, []byte) error
	Kill() error
}

type producer struct {
	ch             *amqp.Channel
	logger         *zap.Logger
	exchangeParams *config.ExchangeConfig
	messageParams  *config.MessageTypeConfig
}

func NewProducer(ch *amqp.Channel, exParams *config.ExchangeConfig, mParams *config.MessageTypeConfig, logger *zap.Logger) (Producer, error) {
	err := ch.ExchangeDeclare(
		exParams.Name,
		exParams.Kind,
		exParams.IsDurable,
		exParams.AutoDelete,
		exParams.Internal,
		exParams.NoWait,
		nil,
	)
	if err != nil {
		logger.Sugar().Errorw("Error during exchange declaration.", err)
		return nil, err
	}
	return &producer{
		ch:             ch,
		logger:         logger,
		exchangeParams: exParams,
		messageParams:  mParams,
	}, nil
}

func (p *producer) Produce(ctx context.Context, msg []byte) error {
	err := p.ch.PublishWithContext(ctx,
		p.messageParams.Exchange,
		p.messageParams.RoutingKey,
		p.messageParams.Mandatory,
		p.messageParams.Immediate,
		amqp.Publishing{
			ContentType: p.messageParams.ContentType,
			Body:        msg,
		})
	if err != nil {
		p.logger.Sugar().Errorw("Error during message producing", err)
		return err
	}
	return nil
}

func (p *producer) Kill() error {
	return p.ch.Close()
}
