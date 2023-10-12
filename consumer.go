package main

import (
	"context"
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
)

func CreateConsumer(topic string, subscriptionName string, name string, client pulsar.Client) (pulsar.Consumer, error) {
	if topic == "" {
		return nil, errors.New("empty topic")
	}
	if subscriptionName == "" {
		return nil, errors.New("empty topic")
	}
	consumerOptions := pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	}
	if name != "" {
		consumerOptions.Name = name
	}
	consumer, err := client.Subscribe(consumerOptions)

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
