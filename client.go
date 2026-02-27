package kafka

import (
	"errors"
	"sync"

	"github.com/IBM/sarama"
)

var (
	client     *sarama.Client
	clientErr  error
	clientOnce sync.Once
)

func getClient() (*sarama.Client, error) {
	clientOnce.Do(func() {
		if len(Brokers) == 0 {
			clientErr = errors.New("cannot create new client, Brokers must be specified")
			return
		}

		var c sarama.Client
		c, clientErr = sarama.NewClient(Brokers, Config)
		if clientErr == nil {
			client = &c
		}
	})

	// If Brokers is empty, we should return the error even if clientOnce has already run
	// This is needed for tests that change Brokers dynamically
	if len(Brokers) == 0 {
		return nil, errors.New("cannot create new client, Brokers must be specified")
	}

	return client, clientErr
}

// resetClient is used for testing purposes only
func resetClient() {
	clientOnce = sync.Once{}
	client = nil
	clientErr = nil
}
