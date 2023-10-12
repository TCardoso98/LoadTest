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
			Topic            string `yaml:"Topic"`
			NMessages        int    `yaml:"N_Messages"`
			FlowFrequency    int    `yaml:"Flow_Frequency"`
			ProducerName     string `yaml:"Producer_Name"`
			ConsumerName     string `yaml:"Consumer_Name"`
			SubscriptionName string `yaml:"Subscription_Name"`
		} `yaml:"Test_Parameters"`
	} `yaml:"Config"`
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

func main() {
	var value ConfigFile
	//value := make(map[string]ConfigOptions)
	LoadYaml("config_test_file.yml", &value)

}
