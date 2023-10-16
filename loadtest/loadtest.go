package loadtest

import (
	"context"
	"log"
	"time"

	"sibs.com/loadtest/config"
	"sibs.com/loadtest/consumer"
	"sibs.com/loadtest/producer"
)

type LoadTest struct {
	ProduceQuantity int
	ConsumeQuantity int
	Payload         []byte
	ParallelTest    bool
	MultithreadTest bool
}

type LoadTestResults struct {
	TimeElapsed             float64
	SuccessfulPayloads      int
	FailedPayloads          int
	AccumulativePayloadSize int
	SecondsPerPayload       float64
}

func Create(cfg config.ConfigOptions) *LoadTest {
	return &LoadTest{
		ProduceQuantity: cfg.ProducerParameters.ProduceQuantity,
		ConsumeQuantity: cfg.ConsumerParameters.ConsumeQuantity,
		Payload:         []byte(cfg.ProducerParameters.Payload),
		ParallelTest:    cfg.TestParameters.ParallelTest,
		MultithreadTest: cfg.TestParameters.MultithreadTest,
	}
}

func (t *LoadTest) PerformTest(producer producer.Producer,
	consumer consumer.Consumer) (LoadTestResults, LoadTestResults, error) {
	if t.MultithreadTest {
		producerResults, consumerResults := t.LoadTestParallelMultiThread(producer, consumer)
		return producerResults, consumerResults, nil
	} else if t.ParallelTest {
		producerResults, consumerResults := t.LoadTestParallel(producer, consumer)
		return producerResults, consumerResults, nil
	} else {
		results, err := t.LoadTestSingleThread(producer, consumer)
		if err != nil {
			return LoadTestResults{}, LoadTestResults{}, err
		}
		return results, results, nil
	}
}
func (t *LoadTest) LoadTestParallel(producer producer.Producer, consumer consumer.Consumer) (LoadTestResults, LoadTestResults) {
	producerResults := t.loadTestSendMessages(producer)
	consumerResults := t.loadTestConsumeMessages(consumer)
	return producerResults, consumerResults
}

func (t *LoadTest) LoadTestParallelMultiThread(producer producer.Producer,
	consumer consumer.Consumer) (LoadTestResults, LoadTestResults) {
	c := make(chan LoadTestResults)
	go func(c chan LoadTestResults) {
		log.Println("SENDING MESSAGE PROCESS START")
		c <- t.loadTestSendMessages(producer)
		log.Println("SENDING MESSAGE PROCESS FINISH")

	}(c)
	go func(c chan LoadTestResults) {
		log.Println("RECEIVING MESSAGE PROCESS START")
		c <- t.loadTestConsumeMessages(consumer)
		log.Println("RECEIVING MESSAGE PROCESS FINISH")

	}(c)
	producerResults := <-c
	consumerResults := <-c
	return producerResults, consumerResults
}

func (t *LoadTest) LoadTestSingleThread(producer producer.Producer, consumer consumer.Consumer) (LoadTestResults, error) {
	result := LoadTestResults{}
	for i := 0; i < t.ProduceQuantity; i++ {
		start := time.Now()
		_, err := producer.SendMessage(context.Background(), t.Payload)

		if err != nil {
			_, err = consumer.ConsumeMessage(context.Background())
		}
		elapsed := time.Since(start).Seconds()
		result.TimeElapsed += elapsed
		if err != nil {
			result.FailedPayloads++
		} else {
			result.SecondsPerPayload += elapsed
			result.SuccessfulPayloads++
		}
	}
	result.AccumulativePayloadSize = result.SuccessfulPayloads * len(t.Payload) / 8
	result.SecondsPerPayload = result.TimeElapsed / result.SecondsPerPayload
	return result, nil
}

func (t *LoadTest) loadTestSendMessages(producer producer.Producer) LoadTestResults {

	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0
	for i := 0; i < t.ProduceQuantity; i++ {
		start := time.Now()
		_, err := producer.SendMessage(context.Background(), t.Payload)
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
		AccumulativePayloadSize: successfulPayloads * len(t.Payload) / 8,
		SecondsPerPayload:       payloadPerSecond / float64(successfulPayloads),
	}
}

func (t *LoadTest) loadTestConsumeMessages(consumer consumer.Consumer) LoadTestResults {
	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0
	for i := 0; i < t.ConsumeQuantity; i++ {
		start := time.Now()
		_, err := consumer.ConsumeMessage(context.Background())
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
		AccumulativePayloadSize: successfulPayloads * len(t.Payload) / 8,
		SecondsPerPayload:       payloadPerSecond / float64(successfulPayloads),
	}
}
