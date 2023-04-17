package kafka

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

var (
	client      *sarama.Client
	clientMutex = &sync.Mutex{}
)

func getClient() (*sarama.Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	var c sarama.Client
	var err error
	if client == nil {
		if len(Brokers) == 0 {
			return nil, errors.New("cannot create new client, Brokers must be specified")
		}
		c, err = sarama.NewClient(Brokers, Config)
		if err != nil {
			return nil, err
		}

		client = &c
	}

	return client, nil
}
