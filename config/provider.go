package config

import (
	"sync"
)

type ServerConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
}

type RabbitConfig struct {
	ServerConfig     ServerConfig        `json:"server_config"`
	ConsumersConfigs []ConsumerConfig    `json:"consumers"`
	QueueConfigs     []QueueConfig       `json:"queues"`
	Exchanges        []ExchangeConfig    `json:"exchanges"`
	MessageTypes     []MessageTypeConfig `json:"message_types"`
	mu               *sync.RWMutex
}

func NewRabbitConfig() *RabbitConfig {
	return &RabbitConfig{
		mu: &sync.RWMutex{},
	}
}

func (c *RabbitConfig) QueueConfig(queue string) *QueueConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, v := range c.QueueConfigs {
		if v.Queue == queue {
			return &v
		}
	}

	return nil
}

func (c *RabbitConfig) ConsumerConfig(name string) *ConsumerConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, v := range c.ConsumersConfigs {
		if v.Name == name {
			return &v
		}
	}

	return nil
}

func (c *RabbitConfig) Exchange(name string) *ExchangeConfig {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, v := range c.Exchanges {
		if v.Name == name {
			return &v
		}
	}
	return nil
}

type ConsumerConfig struct {
	Name             string `json:"queue_name"`
	Durable          bool   `json:"durable"`
	DeleteWhenUnused bool   `json:"delete_when_unused"`
	IsExclusive      bool   `json:"is_exclusive"`
	NoWait           bool   `json:"no_wait"`
}

type QueueConfig struct {
	Queue       string `json:"queue"`
	AutoAck     bool   `json:"auto_ack"`
	Consumer    string `json:"consumer"`
	IsExclusive bool   `json:"is_exclusive"`
	NoLocal     bool   `json:"no_local"`
	NoWait      bool   `json:"no_wait"`
}

type ExchangeConfig struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	IsDurable  bool   `json:"is_durable"`
	AutoDelete bool   `json:"auto_delete"`
	Internal   bool   `json:"is_internal"`
	NoWait     bool   `json:"no_wait"`
}

type MessageTypeConfig struct {
	Exchange    string `json:"exchange"`
	RoutingKey  string `json:"routing_key"`
	Mandatory   bool   `json:"mandatory"`
	Immediate   bool   `json:"immediate"`
	ContentType string `json:"content_type"`
}

func (q *QueueConfig) FromMap(m map[string]interface{}) {
	if queue, ok := m["queue"]; ok {
		q.Queue = queue.(string)
	}

	if consumer, ok := m["consumer"]; ok {
		q.Consumer = consumer.(string)
	}

	if autoAck, ok := m["auto-ack"]; ok {
		q.AutoAck = autoAck.(bool)
	}

	if exclusive, ok := m["is-exclusive"]; ok {
		q.IsExclusive = exclusive.(bool)
	}

	if noWait, ok := m["no-wait"]; ok {
		q.NoWait = noWait.(bool)
	}

	if noLocal, ok := m["no-local"]; ok {
		q.NoLocal = noLocal.(bool)
	}
}

func (e *ExchangeConfig) FromMap(m map[string]interface{}) {
	if name, ok := m["name"]; ok {
		e.Name = name.(string)
	}

	if t, ok := m["kind"]; ok {
		e.Kind = t.(string)
	}

	if durable, ok := m["is_durable"]; ok {
		e.IsDurable = durable.(bool)
	}

	if autoDelete, ok := m["auto_delete"]; ok {
		e.AutoDelete = autoDelete.(bool)
	}

	if isInternal, ok := m["is_internal"]; ok {
		e.Internal = isInternal.(bool)
	}

	if noWait, ok := m["no_wait"]; ok {
		e.NoWait = noWait.(bool)
	}
}

func (m *MessageTypeConfig) FromMap(mp map[string]interface{}) {
	if exchange, ok := mp["exchange"]; ok {
		m.Exchange = exchange.(string)
	}

	if routingKey, ok := mp["routing-key"]; ok {
		m.RoutingKey = routingKey.(string)
	}

	if mandatory, ok := mp["mandatory"]; ok {
		m.Mandatory = mandatory.(bool)
	}

	if immediate, ok := mp["immediate"]; ok {
		m.Immediate = immediate.(bool)
	}

	if ct, ok := mp["content-type"]; ok {
		m.ContentType = ct.(string)
	}
}
