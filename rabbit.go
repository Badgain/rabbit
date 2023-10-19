package rabbit

import (
	"errors"
	"fmt"
	"github.com/Badgain/rabbit/config"
	rconf "github.com/Badgain/rabbit/config"
	"github.com/Badgain/rabbit/consumer"
	"github.com/Badgain/rabbit/producer"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient interface {
	Start() error
	Init() error
	Consumer(string) consumer.Consumer
	NewConsumer(consumerParams *rconf.ConsumerConfig, queueOptions *rconf.QueueConfig) (consumer.Consumer, error)
	NewProducer(exchangeParams *rconf.ExchangeConfig, messageTypeParams *rconf.MessageTypeConfig) (producer.Producer, error)
	Producer(string, string) producer.Producer
	Shutdown() error
}

type client struct {
	fx.In

	conn      *amqp.Connection
	config    config.RabbitConfig
	rwmutex   sync.RWMutex
	consumers map[string]consumer.Consumer
	producers map[string]producer.Producer
	connStr   string
	logger    *zap.Logger
}

func NewRabbitClient(cf config.RabbitConfig, logger *zap.Logger) RabbitClient {
	return &client{
		consumers: map[string]consumer.Consumer{},
		producers: map[string]producer.Producer{},
		config:    cf,
		rwmutex:   sync.RWMutex{},
		logger:    logger,
	}
}

func (c *client) Init() error {
	srvConf := c.config.ServerConfig
	c.connStr = fmt.Sprintf("amqp://%s:%s@%s:%d/",
		srvConf.User,
		srvConf.Password,
		srvConf.Host,
		srvConf.Port)
	return nil
}

func (c *client) Start() error {

	if c.connStr == "" {
		return errors.New("connection string not provided")
	}

	conn, err := amqp.Dial(c.connStr)
	if err != nil {
		return err
	}

	c.conn = conn
	return nil
}

func (c *client) channel() (*amqp.Channel, error) {
	return c.conn.Channel()
}

func (c *client) Consumer(topic string) consumer.Consumer {

	var (
		csm consumer.Consumer
		ok  bool
	)

	c.rwmutex.RLock()

	if csm, ok = c.consumers[topic]; !ok {
		return nil
	}

	c.rwmutex.RUnlock()

	return csm
}

func (c *client) Producer(exchange, route string) producer.Producer {
	var (
		p  producer.Producer
		ok bool
	)

	c.rwmutex.RLock()

	key := fmt.Sprintf("%s.%s", exchange, route)

	if p, ok = c.producers[key]; !ok {
		return nil
	}

	c.rwmutex.RUnlock()

	return p
}

func (c *client) NewConsumer(consumerParams *rconf.ConsumerConfig, queueOptions *rconf.QueueConfig) (consumer.Consumer, error) {
	queue := consumerParams.Name
	if queue == "" {
		return nil, errors.New("can not create a consumer. details:queue value is empty")
	}
	csm := c.Consumer(queue)
	if csm != nil {
		return csm, nil
	}

	ch, err := c.channel()
	if err != nil {
		return nil, err
	}

	return consumer.NewConsumer(ch, consumerParams, queueOptions, c.logger)
}

func (c *client) NewProducer(exchangeParams *rconf.ExchangeConfig, messageTypeParams *rconf.MessageTypeConfig) (producer.Producer, error) {
	route := messageTypeParams.RoutingKey
	if route == "" {
		return nil, errors.New("can not create a producer. details:route key is an empty value")
	}
	p := c.Producer(messageTypeParams.Exchange, messageTypeParams.RoutingKey)
	if p != nil {
		return p, nil
	}

	ch, err := c.channel()
	if err != nil {
		return nil, err
	}
	p, err = producer.NewProducer(ch, exchangeParams, messageTypeParams, c.logger)
	return p, nil
}

func (c *client) Shutdown() error {
	c.rwmutex.RLock()
	defer c.rwmutex.RUnlock()

	for k, v := range c.consumers {
		err := v.Kill()
		if err != nil {
			log.Printf("Error during shutdown. Reson: can not to kill consumer")
			return err
		}
		delete(c.consumers, k)
	}
	for k, v := range c.producers {
		err := v.Kill()
		if err != nil {
			log.Printf("Error during shutdown. Reson: can not to kill producer")
			return err
		}
		delete(c.producers, k)
	}
	return c.conn.Close()
}
