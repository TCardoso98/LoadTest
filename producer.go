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

func SendNMessages(quantity int, producer pulsar.Producer, ctx context.Context, payload []byte, ignoreError bool) ([]pulsar.MessageID, error) {
	var messages []pulsar.MessageID = make([]pulsar.MessageID, quantity)
	for i := 0; i < quantity; i++ {
		messageId, err := SendMessage(producer, ctx, payload)
		if err != nil {
			if !ignoreError {
				return nil, err
			}
		}
		messages[i] = messageId
	}
	return messages, nil
}
