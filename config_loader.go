package main

import (
	"os"

	"gopkg.in/yaml.v3"
)

type ConfigFile struct {
	ConfigOptions struct {
		PulsarConnection struct {
			URL             string `yaml:"Url"`
			AuthCertificate string `yaml:"Auth_Certificate"`
			AuthKey         string `yaml:"Auth_Key"`
		} `yaml:"Pulsar_Connection"`
		TestParameters struct {
			NMessages          int  `yaml:"N_Messages"`
			ParallelTest       bool `yaml:"Parallel_Test"`
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
		} `yaml:"Test_Parameters"`
	} `yaml:"Config"`
}

func LoadYaml(filePath string, out interface{}) error {
	f, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(f, out); err != nil {
		return err
	}

	return nil
}
