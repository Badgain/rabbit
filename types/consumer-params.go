package types

import amqp "github.com/rabbitmq/amqp091-go"

type ConsumerParams struct {
	Name             string
	Durable          bool
	DeleteWhenUnused bool
	Exclusive        bool
	NoWait           bool
	Args             amqp.Table
}

func (c *ConsumerParams) FromMap(m map[string]interface{}) {
	if name, ok := m["name"]; ok {
		c.Name = name.(string)
	}

	if durable, ok := m["is_durable"]; ok {
		c.Durable = durable.(bool)
	}

	if deleteWhenUnused, ok := m["delete_when_unused"]; ok {
		c.DeleteWhenUnused = deleteWhenUnused.(bool)
	}

	if exclusive, ok := m["is_exclusive"]; ok {
		c.Exclusive = exclusive.(bool)
	}

	if noWait, ok := m["no_wait"]; ok {
		c.NoWait = noWait.(bool)
	}
}
