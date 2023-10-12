package main

import (
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

var ClientOptions = pulsar.ClientOptions{
	URL:                        "pulsar+ssl://localhost:6651",
	TLSValidateHostname:        false,
	TLSAllowInsecureConnection: true,
	Authentication:             pulsar.NewAuthenticationTLS("PULSAR_BROKER_CLIENT.cert.pem", "PULSAR_BROKER_CLIENT.key.p8"),
}

func CreateClient(clientOptions pulsar.ClientOptions) (pulsar.Client, error) {
	client, err := pulsar.NewClient(clientOptions)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return client, nil
}
