package client

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

type Client struct {
	Client pulsar.Client
}

func Create(clientOptions pulsar.ClientOptions) (*Client, error) {
	client, err := pulsar.NewClient(clientOptions)

	if err != nil {
		return nil, err
	}

	return &Client{
		client,
	}, nil
}

func (c *Client) Close() {
	c.Client.Close()
}
