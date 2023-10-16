package consumer

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Consumer struct {
	client   pulsar.Client
	consumer pulsar.Consumer
}

func Create(client pulsar.Client, consumerOptions pulsar.ConsumerOptions) (Consumer, error) {
	consumer, err := client.Subscribe(consumerOptions)

	if err != nil {
		return Consumer{}, err
	}

	return Consumer{
		client,
		consumer,
	}, nil
}

func (c *Consumer) ConsumeMessage(ctx context.Context) (pulsar.Message, error) {
	msg, err := c.consumer.Receive(ctx)
	if err != nil {
		return nil, err
	}

	c.consumer.Ack(msg)
	return msg, err
}

func (c *Consumer) Close() {
	c.consumer.Close()
}
