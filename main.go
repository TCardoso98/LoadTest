package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type LoadTestResults struct {
	TimeElapsed             float64
	SuccessfulPayloads      int
	FailedPayloads          int
	AccumulativePayloadSize int
	SecondsPerPayload       float64
}

const DEFAULT_CONFIG_FILEPATH = "config_test_file.yml"

func main() {
	var config ConfigFile
	configFilePath := DEFAULT_CONFIG_FILEPATH
	if len(os.Args) > 1 {
		configFilePath = os.Args[1]
	}
	err := LoadYaml(configFilePath, &config)
	if err != nil {
		log.Fatal(err)
	}
	resultsProducer, resultsConsumer, err := LoadTest(config)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Printf("Producer Results:\n%+v\n\n", resultsProducer)
		fmt.Printf("Consumer Results:\n%+v\n", resultsConsumer)
	}
}

func LoadTest(config ConfigFile) (LoadTestResults, LoadTestResults, error) {

	client, consumer, producer, err := LoadTestPreparation(config)
	if err != nil {
		return LoadTestResults{}, LoadTestResults{}, err
	}
	defer client.Close()
	defer consumer.Close()
	defer producer.Close()

	payload := []byte(fmt.Sprintf(config.ConfigOptions.TestParameters.ProducerParameters.Payload))
	log.Println("START TEST")
	if config.ConfigOptions.TestParameters.MultithreadTest {
		producerResults, consumerResults := LoadTestParallelMultiThread(config.ConfigOptions.TestParameters.ProducerParameters.ProduceQuantity,
			config.ConfigOptions.TestParameters.ConsumerParameters.ConsumeQuantity,
			payload,
			producer,
			consumer)
		return producerResults, consumerResults, nil
	} else if config.ConfigOptions.TestParameters.ParallelTest {
		producerResults, consumerResults := LoadTestParallel(config.ConfigOptions.TestParameters.ProducerParameters.ProduceQuantity,
			config.ConfigOptions.TestParameters.ConsumerParameters.ConsumeQuantity,
			payload,
			producer,
			consumer)
		return producerResults, consumerResults, nil

	} else {
		results, err := LoadTestSingleThread(config.ConfigOptions.TestParameters.ProducerParameters.ProduceQuantity,
			payload, producer, consumer)
		if err != nil {
			return LoadTestResults{}, LoadTestResults{}, err
		}
		return results, results, nil
	}

}

func LoadTestPreparation(config ConfigFile) (pulsar.Client, pulsar.Consumer, pulsar.Producer, error) {
	clientOptionsTest := pulsar.ClientOptions{
		URL:                        config.ConfigOptions.PulsarConnection.URL,
		TLSValidateHostname:        false,
		TLSAllowInsecureConnection: true,
		Authentication: pulsar.NewAuthenticationTLS(
			config.ConfigOptions.PulsarConnection.AuthCertificate,
			config.ConfigOptions.PulsarConnection.AuthKey),
	}
	client, err := CreateClient(clientOptionsTest)

	if err != nil {
		return nil, nil, nil, err
	}

	producer, err := CreateProducer(
		config.ConfigOptions.TestParameters.ProducerParameters.ProducerTopic,
		config.ConfigOptions.TestParameters.ProducerParameters.ProducerName,
		client)
	if err != nil {
		return nil, nil, nil, err
	}
	consumer, err := CreateConsumer(
		config.ConfigOptions.TestParameters.ConsumerParameters.ConsumerTopic,
		config.ConfigOptions.TestParameters.ConsumerParameters.SubscriptionName,
		config.ConfigOptions.TestParameters.ConsumerParameters.ConsumerName,
		client)
	if err != nil {
		return nil, nil, nil, err
	}
	return client, consumer, producer, nil
}

func LoadTestSingleThread(quantity int, payload []byte, producer pulsar.Producer, consumer pulsar.Consumer) (LoadTestResults, error) {
	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0
	for i := 0; i < quantity; i++ {
		start := time.Now()
		_, err := SendMessage(producer, context.Background(), payload)

		if err != nil {
			_, err = ConsumeMessage(consumer)
		}
		timeElapsed += time.Since(start).Seconds()
		if err != nil {

			failedPayloads++
		} else {
			payloadPerSecond += timeElapsed
			successfulPayloads++
		}
	}
	return LoadTestResults{
		TimeElapsed:             timeElapsed,
		SuccessfulPayloads:      successfulPayloads,
		FailedPayloads:          failedPayloads,
		AccumulativePayloadSize: quantity * len(payload) / 8,
		SecondsPerPayload:       timeElapsed / float64(successfulPayloads),
	}, nil
}

func LoadTestParallel(produceQuantity int, consumeQuantity int, payload []byte,
	producer pulsar.Producer,
	consumer pulsar.Consumer) (LoadTestResults, LoadTestResults) {
	producerResults := LoadTestSendMessages(produceQuantity, payload, producer)
	consumerResults := LoadTestConsumeMessages(consumeQuantity, consumer)

	return producerResults, consumerResults
}

func LoadTestParallelMultiThread(produceQuantity int, consumeQuantity int, payload []byte,
	producer pulsar.Producer,
	consumer pulsar.Consumer) (LoadTestResults, LoadTestResults) {
	c := make(chan LoadTestResults)
	go func(c chan LoadTestResults) {
		log.Println("SENDING MESSAGE PROCESS START")
		c <- LoadTestSendMessages(produceQuantity, payload, producer)
		log.Println("SENDING MESSAGE PROCESS FINISH")

	}(c)
	go func(c chan LoadTestResults) {
		log.Println("RECEIVING MESSAGE PROCESS START")
		c <- LoadTestConsumeMessages(consumeQuantity, consumer)
		log.Println("RECEIVING MESSAGE PROCESS FINISH")

	}(c)
	producerResults := <-c
	consumerResults := <-c

	return producerResults, consumerResults
}

func LoadTestSendMessages(quantity int, payload []byte, producer pulsar.Producer) LoadTestResults {
	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0
	for i := 0; i < quantity; i++ {
		start := time.Now()
		_, err := SendMessage(producer, context.Background(), payload)
		time := time.Since(start).Seconds()
		timeElapsed += time

		if err != nil {
			failedPayloads++
		} else {
			payloadPerSecond += time
			successfulPayloads++
		}
	}
	return LoadTestResults{
		TimeElapsed:             timeElapsed,
		SuccessfulPayloads:      successfulPayloads,
		FailedPayloads:          failedPayloads,
		AccumulativePayloadSize: successfulPayloads * len(payload) / 8,
		SecondsPerPayload:       payloadPerSecond / float64(successfulPayloads),
	}
}

func LoadTestConsumeMessages(quantity int, consumer pulsar.Consumer) LoadTestResults {
	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0
	var payloadSize = 0
	for i := 0; i < quantity; i++ {
		start := time.Now()
		message, err := ConsumeMessage(consumer)
		time := time.Since(start).Seconds()
		timeElapsed += time
		if err != nil {
			failedPayloads++
		} else {
			if payloadSize == 0 {
				payloadSize = len(message.Payload())
			}
			payloadPerSecond += time
			successfulPayloads++
		}
	}
	return LoadTestResults{
		TimeElapsed:             timeElapsed,
		SuccessfulPayloads:      successfulPayloads,
		FailedPayloads:          failedPayloads,
		AccumulativePayloadSize: successfulPayloads * payloadSize / 8,
		SecondsPerPayload:       payloadPerSecond / float64(successfulPayloads),
	}
}
