package main

import (
	"context"
	"errors"

	"github.com/apache/pulsar-client-go/pulsar"
)

func CreateProducer(topic string, name string, client pulsar.Client) (pulsar.Producer, error) {
	if topic == "" {
		return nil, errors.New("empty topic")
	}
	producerOptions := pulsar.ProducerOptions{
		Topic: topic,
	}
	if name != "" {
		producerOptions.Name = name
	}
	producer, err := client.CreateProducer(producerOptions)

	if err != nil {
		return nil, err
	}

	return producer, nil
}

func SendMessage(producer pulsar.Producer, ctx context.Context, payload []byte) (pulsar.MessageID, error) {
	producerMessage := pulsar.ProducerMessage{
		Payload: payload,
	}
	return producer.Send(ctx, &producerMessage)

}
