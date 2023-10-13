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
	AccumulativePayloadSize int64
	PayloadPerSecond        float64
}

const DEFAULT_CONFIG_FILEPATH = "config_test_file.yml"

func LoadTest(config ConfigFile) (LoadTestResults, error) {
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

	producer, err := CreateProducer(
		config.ConfigOptions.TestParameters.ProducerParameters.ProducerTopic,
		config.ConfigOptions.TestParameters.ProducerParameters.ProducerName,
		client)
	defer producer.Close()
	if err != nil {
		return LoadTestResults{}, err
	}

	consumer, err := CreateConsumer(
		config.ConfigOptions.TestParameters.ConsumerParameters.ConsumerTopic,
		config.ConfigOptions.TestParameters.ConsumerParameters.SubscriptionName,
		config.ConfigOptions.TestParameters.ConsumerParameters.ConsumerName,
		client)
	defer consumer.Close()

	for i := 0; i < config.ConfigOptions.TestParameters.NMessages; i++ {
		start := time.Now()
		_, err := SendMessage(
			producer,
			context.Background(),
			[]byte(fmt.Sprintf(
				config.ConfigOptions.TestParameters.ProducerParameters.Payload)))

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
		TimeElapsed:        timeElapsed,
		SuccessfulPayloads: successfulPayloads,
		FailedPayloads:     failedPayloads,
		AccumulativePayloadSize: int64(config.ConfigOptions.TestParameters.NMessages*
			len(config.ConfigOptions.TestParameters.ProducerParameters.Payload)) / 8,
		PayloadPerSecond: timeElapsed / float64(successfulPayloads),
	}, nil
}

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
	results, err := LoadTest(config)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Printf("%+v\n", results)
	}
}
