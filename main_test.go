package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
)

var clientOptionsTest = pulsar.ClientOptions{
	URL:                        "pulsar+ssl://localhost:6651",
	TLSValidateHostname:        false,
	TLSAllowInsecureConnection: true,
	Authentication:             pulsar.NewAuthenticationTLS("PULSAR_BROKER_CLIENT.cert.pem", "PULSAR_BROKER_CLIENT.key.p8"),
}

type YamlTest struct {
	P1 int     `yaml:"p1"`
	P2 string  `yaml:"p2"`
	P3 float32 `yaml:"p3"`
}

const TEST_TOPIC string = "TOPIC-A"
const TEST_SUBSCRIPTION string = "sub_test"
const TEST_YAML_FILEPATH string = "yaml_test_file.yml"
const TEST_CONFIG_FILEPATH string = "config_test_file.yml"

func TestClientCreation(t *testing.T) {
	client, err := CreateClient(ClientOptions)
	defer client.Close()
	assert.Nil(t, err)
	assert.NotNil(t, client)
}

func TestProducerCreation(t *testing.T) {
	client, err := CreateClient(clientOptionsTest)
	defer client.Close()

	producer, err := CreateProducer(TEST_TOPIC, client)
	defer producer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, producer)
}

func TestConsumerCreation(t *testing.T) {
	client, err := CreateClient(clientOptionsTest)
	defer client.Close()

	consumer, err := CreateConsumer(TEST_TOPIC, TEST_SUBSCRIPTION, client)
	defer consumer.Close()
	assert.Nil(t, err)
	assert.NotNil(t, consumer)
}

func TestSendMessage(t *testing.T) {
	client, err := CreateClient(clientOptionsTest)
	defer client.Close()

	producer, err := CreateProducer(TEST_TOPIC, client)
	defer producer.Close()

	ctx := context.Background()
	assert.NotNil(t, ctx)

	messageId, err := SendMessage(producer, ctx, []byte(fmt.Sprintf("hello world")))
	assert.Nil(t, err)
	assert.NotNil(t, messageId)
}

func TestConsumeMessage(t *testing.T) {
	TestSendMessage(t)
	client, _ := CreateClient(clientOptionsTest)
	defer client.Close()

	consumer, _ := CreateConsumer(TEST_TOPIC, TEST_SUBSCRIPTION, client)
	defer consumer.Close()
	message, err := ConsumeMessage(consumer)

	assert.Nil(t, err)
	assert.NotNil(t, message)
}

func TestLoadYaml(t *testing.T) {
	expectedValue := YamlTest{
		P1: 123,
		P2: "hello",
		P3: 1.2,
	}
	var value YamlTest
	err := LoadYaml(TEST_YAML_FILEPATH, &value)
	assert.Nil(t, err)
	assert.EqualValues(t, expectedValue, value)
}

func TestLoadConfig(t *testing.T) {
	/*expectedValue := ConfigFile{
		ConfigOptions {
			PulsarConnection {
				Url:             "pulsar+ssl://localhost:6651",
				AuthCertificate: "PULSAR_BROKER_CLIENT.cert.pem",
				AuthKey:         "PULSAR_BROKER_CLIENT.key.p8",
			},
			TestParameters{
				Topic:            "TOPIC-A",
				MessageQuantity:  10,
				FlowFrequency:    0,
				ProducerName:     "",
				ConsumerName:     "",
				SubscriptionName: "sub_test",
			},
		},
	}*/

	var value ConfigFile
	//value := make(map[string]ConfigOptions)
	err := LoadYaml(TEST_CONFIG_FILEPATH, &value)
	assert.Nil(t, err)
	//assert.EqualValues(t, expectedValue, value)
}

func TestLoadTest(t *testing.T) {

}
