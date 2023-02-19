package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"rabbit/consumer"
	"rabbit/producer"
	"rabbit/types"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Client interface {
	Start() error
	Consumer(string) consumer.Consumer
	Producer(string) producer.Producer
	Shutdown() error
}

type client struct {
	conn      *amqp.Connection
	config    types.YamlConfig
	consumers map[string]consumer.Consumer
	producers map[string]producer.Producer
}

func New(cf types.YamlConfigProvider) Client {
	config, _ := cf.ProvideConfig()
	return &client{
		consumers: map[string]consumer.Consumer{},
		producers: map[string]producer.Producer{},
		config:    config,
	}
}

func (c *client) Start() error {
	if c.config == nil {
		return errors.New("Missing config error")
	}

	connStr := fmt.Sprintf("amqp://guest:guest@%s:%s/", c.config.GetHost(), c.config.GetPort())
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return err
	}

	exit := make(chan os.Signal)

	signal.Notify(exit, os.Interrupt)
	go func() {
		<-exit
		err = c.Shutdown()
		if err != nil {
			log.Println(err.Error())
		}
	}()

	c.conn = conn
	return nil
}

func (c *client) channel() (*amqp.Channel, error) {
	return c.conn.Channel()
}

func (c *client) Consumer(topic string) consumer.Consumer {
	var csm consumer.Consumer
	var ok bool

	if csm, ok = c.consumers[topic]; !ok {
		ch, _ := c.channel()
		params := consumer.ConsumerParams{
			Name: topic,
		}
		queueOpts := consumer.QueueOptions{
			Queue:   topic,
			AutoAck: true,
		}
		csm = consumer.NewConsumer(ch, params, queueOpts)
		c.consumers[topic] = csm
	}

	return csm
}

func (c *client) Producer(topic string) producer.Producer {
	var p producer.Producer
	var ok bool

	if p, ok = c.producers[topic]; !ok {
		ch, _ := c.channel()
		p = producer.NewProducer(ch)
		c.producers[topic] = p
	}

	return p
}

func FailHandler(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func (c *client) Shutdown() error {
	for _, v := range c.consumers {
		err := v.Kill()
		if err != nil {
			log.Printf("Error during shutdown. Reson: can not to kill consumer")
			return err
		}
	}
	for _, v := range c.producers {
		err := v.Kill()
		if err != nil {
			log.Printf("Error during shutdown. Reson: can not to kill producer")
			return err
		}
	}
	return c.conn.Close()
}
