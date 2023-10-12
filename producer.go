package main

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

func CreateProducer(topic string, client pulsar.Client) (pulsar.Producer, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

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
