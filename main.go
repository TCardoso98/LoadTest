package main

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

type LoadTestResults struct {
	timeElapsed             float64
	successfulPayloads      int
	failedPayloads          int
	accumulativePayloadSize int64
	payloadPerSecond        float64
}

func LoadTest(config ConfigFile, payload []byte) (LoadTestResults, error) {
	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0
	clientOptionsTest := pulsar.ClientOptions{
		URL:                        config.ConfigOptions.PulsarConnection.URL,
		TLSValidateHostname:        false,
		TLSAllowInsecureConnection: true,
		Authentication: pulsar.NewAuthenticationTLS(
			config.ConfigOptions.PulsarConnection.AuthCertificate,
			config.ConfigOptions.PulsarConnection.AuthKey),
	}
	client, err := CreateClient(clientOptionsTest)
	defer client.Close()
	if err != nil {
		return LoadTestResults{}, err
	}

	producer, err := CreateProducer(config.ConfigOptions.TestParameters.Topic, client)
	defer producer.Close()
	if err != nil {
		return LoadTestResults{}, err
	}

	consumer, err := CreateConsumer(
		config.ConfigOptions.TestParameters.Topic,
		config.ConfigOptions.TestParameters.SubscriptionName,
		client)
	defer consumer.Close()

	for i := 0; i < config.ConfigOptions.TestParameters.NMessages; i++ {
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
		timeElapsed:             timeElapsed,
		successfulPayloads:      successfulPayloads,
		failedPayloads:          failedPayloads,
		accumulativePayloadSize: int64(config.ConfigOptions.TestParameters.NMessages*len(payload)) / 8,
		payloadPerSecond:        timeElapsed / float64(successfulPayloads),
	}, nil
}
