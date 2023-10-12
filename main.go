package main

import (
	"context"
	"log"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"gopkg.in/yaml.v3"
)

var ClientOptions = pulsar.ClientOptions{
	URL:                        "pulsar+ssl://localhost:6651",
	TLSValidateHostname:        false,
	TLSAllowInsecureConnection: true,
	Authentication:             pulsar.NewAuthenticationTLS("PULSAR_BROKER_CLIENT.cert.pem", "PULSAR_BROKER_CLIENT.key.p8"),
}

type ConfigOptions struct {
	PulsarConnection struct {
		Url             string `yaml:"Url"`
		AuthCertificate string `yaml:"Auth_Certificate"`
		AuthKey         string `yaml:"Auth_Key"`
	} `yaml:"Pulsar_Connection"`
	TestParameters struct {
		Topic            string  `yaml:"Topic"`
		MessageQuantity  int     `yaml:"N_Messages"`
		FlowFrequency    float32 `yaml:"Flow_Frequency"`
		ProducerName     string  `yaml:"Producer_Name"`
		ConsumerName     string  `yaml:"Consumer_Name"`
		SubscriptionName string  `yaml:"Subscription_Name"`
	} `yaml:"Test_Parameters"`
}

func CreateClient(clientOptions pulsar.ClientOptions) (pulsar.Client, error) {
	client, err := pulsar.NewClient(clientOptions)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return client, nil
}

func CreateProducer(topic string, client pulsar.Client) (pulsar.Producer, error) {
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})

	if err != nil {
		return nil, err
	}

	return producer, nil
}

func CreateConsumer(topic string, subscriptionName string, client pulsar.Client) (pulsar.Consumer, error) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subscriptionName,
	})

	if err != nil {
		return nil, err
	}

	return consumer, nil

}

func SendMessage(producer pulsar.Producer, ctx context.Context, payload []byte) (pulsar.MessageID, error) {
	producerMessage := pulsar.ProducerMessage{
		Payload: payload,
	}
	return producer.Send(ctx, &producerMessage)

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

func LoadYaml(filePath string, out interface{}) error {
	f, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// Unmarshal our input YAML file into empty E (var c)
	if err := yaml.Unmarshal(f, out); err != nil {
		return err
	}

	return nil
}

type LoadTestResults struct {
	timeElapsed             float64
	successfulPayloads      int
	failedPayloads          int
	accumulativePayloadSize int64
	payloadPerSecond        float64
}

// func loadConfigs()
/*func loadTestFunc(producer pulsar.Producer, ctx context.Context, iterations int, payload []byte) *LoadTestResults {
	producerMessage := pulsar.ProducerMessage{
		Payload: payload,
	}
	var failedPayloads = 0
	var successfulPayloads = 0
	var timeElapsed = 0.0
	var payloadPerSecond = 0.0

	for i := 0; i < iterations; i++ {
		start := time.Now()
		_, err := producer.Send(ctx, &producerMessage)
		timeElapsed += time.Since(start).Seconds()
		if err != nil {
			payloadPerSecond += timeElapsed
			failedPayloads++
		} else {
			successfulPayloads++
		}
	}
	return &LoadTestResults{
		timeElapsed,
		successfulPayloads,
		failedPayloads,
		int64(iterations * (len(payload) / 8)),
		float64(successfulPayloads) / payloadPerSecond,
	}
}

func main() {
	client, err := CreateClient(clientOptions)
	if err != nil {
		log.Println(err)
		return
	}
	defer client.Close()

	producer, err := CreateProducer("TOPIC-A", client)
	if err != nil {
		log.Println(err)
		return
	}
	defer producer.Close()

	ctx := context.Background()
	loadTestFunc(producer, ctx, 10, []byte(fmt.Sprintf("hello world")))

}
*/
