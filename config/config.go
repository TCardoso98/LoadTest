package config

import (
	"errors"
	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	"gopkg.in/yaml.v3"
)

type ConfigFile struct {
	ConfigOptions ConfigOptions `yaml:"Config"`
}

type ConfigOptions struct {
	PulsarConnection struct {
		URL             string `yaml:"Url"`
		AuthCertificate string `yaml:"Auth_Certificate"`
		AuthKey         string `yaml:"Auth_Key"`
	} `yaml:"Pulsar_Connection"`
	TestParameters     TestOptions `yaml:"Test_Parameters"`
	ProducerParameters struct {
		ProducerTopic   string `yaml:"Producer_Topic"`
		ProducerName    string `yaml:"Producer_Name"`
		ProduceQuantity int    `yaml:"Produce_Quantity"`
		Payload         string `yaml:"Payload"`
	} `yaml:"Producer_Parameters"`
	ConsumerParameters struct {
		ConsumerTopic    string `yaml:"Consumer_Topic"`
		SubscriptionName string `yaml:"Subscription_Name"`
		ConsumerName     string `yaml:"Consumer_Name"`
		ConsumeQuantity  int    `yaml:"Consume_Quantity"`
	} `yaml:"Consumer_Parameters"`
}

type TestOptions struct {
	ParallelTest    bool `yaml:"Parallel_Test"`
	MultithreadTest bool `yaml:"Multithread_Test"`
}

func LoadConfigFile(filePath string) (ConfigOptions, error) {
	err := validateFilePath(filePath)
	if err != nil {
		return ConfigOptions{}, err
	}
	var configFile ConfigFile
	err = loadYaml(filePath, &configFile)
	if err != nil {
		return ConfigOptions{}, err
	}
	return configFile.ConfigOptions, nil
}

func validateFilePath(path string) error {
	_, err := os.Stat(path)
	return err
}

func loadYaml(filePath string, out interface{}) error {
	f, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(f, out); err != nil {
		return err
	}

	return nil
}

func (c *ConfigOptions) RetrieveProducerOptions() (pulsar.ProducerOptions, error) {
	if c.ConsumerParameters.ConsumerTopic == "" {
		return pulsar.ProducerOptions{}, errors.New("empty topic")
	}
	return pulsar.ProducerOptions{
		Topic: c.ProducerParameters.ProducerTopic,
		Name:  c.ProducerParameters.ProducerName,
	}, nil
}

func (c *ConfigOptions) RetrieveConsumerOptions() (pulsar.ConsumerOptions, error) {
	if c.ConsumerParameters.ConsumerTopic == "" {
		return pulsar.ConsumerOptions{}, errors.New("empty topic")
	}
	if c.ConsumerParameters.SubscriptionName == "" {
		return pulsar.ConsumerOptions{}, errors.New("empty subscription name")
	}
	return pulsar.ConsumerOptions{
		Topic:            c.ConsumerParameters.ConsumerTopic,
		Name:             c.ConsumerParameters.ConsumerName,
		SubscriptionName: c.ConsumerParameters.SubscriptionName,
	}, nil
}

func (c *ConfigOptions) RetrieveClientOptions() (pulsar.ClientOptions, error) {
	if c.PulsarConnection.URL == "" {
		return pulsar.ClientOptions{}, errors.New("empty URL")
	}
	return pulsar.ClientOptions{
		URL:                        c.PulsarConnection.URL,
		TLSValidateHostname:        false,
		TLSAllowInsecureConnection: true,
		Authentication: pulsar.NewAuthenticationTLS(
			c.PulsarConnection.AuthCertificate,
			c.PulsarConnection.AuthKey),
	}, nil
}
