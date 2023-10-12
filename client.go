package main

import (
	"log"

	"github.com/apache/pulsar-client-go/pulsar"
)

func CreateClient(clientOptions pulsar.ClientOptions) (pulsar.Client, error) {
	client, err := pulsar.NewClient(clientOptions)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	return client, nil
}
