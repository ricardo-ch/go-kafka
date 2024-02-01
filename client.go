package kafka

import (
	"errors"
	"sync"

	"github.com/IBM/sarama"
)

var (
	client      *sarama.Client
	clientMutex = &sync.Mutex{}
)

func getClient() (*sarama.Client, error) {
	if client != nil {
		return client, nil
	}

	clientMutex.Lock()
	defer clientMutex.Unlock()

	if client == nil {
		var c sarama.Client
		var err error

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
