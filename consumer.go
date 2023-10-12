package main

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

func CreateConsumer(topic string, subscriptionName string, client pulsar.Client) (pulsar.Consumer, error) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	})

	if err != nil {
		return nil, err
	}

	return consumer, nil
}

func ConsumeMessage(consumer pulsar.Consumer) (pulsar.Message, error) {
	ctx := context.Background()
	msg, err := consumer.Receive(ctx)
	if err != nil {
		return nil, err
	}

	consumer.Ack(msg)
	return msg, err
}
