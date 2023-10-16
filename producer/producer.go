package producer

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Producer struct {
	client   pulsar.Client
	producer pulsar.Producer
}

func Create(client pulsar.Client, producerOptions pulsar.ProducerOptions) (Producer, error) {
	producer, err := client.CreateProducer(producerOptions)

	if err != nil {
		return Producer{}, err
	}

	return Producer{
		client:   client,
		producer: producer,
	}, nil
}

func (p *Producer) SendMessage(ctx context.Context, payload []byte) (pulsar.MessageID, error) {
	producerMessage := pulsar.ProducerMessage{
		Payload: payload,
	}
	return p.producer.Send(ctx, &producerMessage)

}

func (p *Producer) SendMessageAsync(ctx context.Context,
	payload []byte,
	f func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {

	producerMessage := pulsar.ProducerMessage{
		Payload: payload,
	}
	p.producer.SendAsync(ctx, &producerMessage, f)
}

func (p *Producer) Close() {
	p.producer.Close()
}
