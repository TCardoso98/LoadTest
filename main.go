package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"sibs.com/loadtest/config"
	"sibs.com/loadtest/consumer"
	"sibs.com/loadtest/loadtest"
	"sibs.com/loadtest/producer"
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
	configFilePath := DEFAULT_CONFIG_FILEPATH
	if len(os.Args) > 1 {
		configFilePath = os.Args[1]
	}
	c, err := config.LoadConfigFile(configFilePath)
	if err != nil {
		log.Fatal(err)
	}

	resultsProducer, resultsConsumer, err := LoadTest(c)
	if err != nil {
		log.Println(err)
	} else {
		fmt.Printf("Producer Results:\n%+v\n\n", resultsProducer)
		fmt.Printf("Consumer Results:\n%+v\n", resultsConsumer)
	}
}

var EMPTY_LOADTEST_RESULT = loadtest.LoadTestResults{}

func LoadTest(cfg config.ConfigOptions) (loadtest.LoadTestResults, loadtest.LoadTestResults, error) {
	cliOpt, err := cfg.RetrieveClientOptions()
	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}

	client, err := pulsar.NewClient(cliOpt)
	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}

	prodOpt, err := cfg.RetrieveProducerOptions()
	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}

	producer, err := producer.Create(client, prodOpt)

	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}

	consumerOpt, err := cfg.RetrieveConsumerOptions()

	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}
	consumer, err := consumer.Create(client, consumerOpt)
	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}
	defer client.Close()
	defer consumer.Close()
	defer producer.Close()

	loadTest := loadtest.Create(cfg)
	consumerResults, producerResults, err := loadTest.PerformTest(producer, consumer)
	if err != nil {
		return EMPTY_LOADTEST_RESULT, EMPTY_LOADTEST_RESULT, err
	}
	return consumerResults, producerResults, nil
}
