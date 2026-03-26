package kafka

import (
	"errors"
	"sync"

	"github.com/IBM/sarama"
)

var (
	client     sarama.Client
	clientErr  error
	clientOnce sync.Once
)

func getClient() (sarama.Client, error) {
	clientOnce.Do(func() {
		if len(Brokers) == 0 {
			clientErr = errors.New("cannot create new client, Brokers must be specified")
			return
		}

		client, clientErr = sarama.NewClient(Brokers, Config)
	})

	return client, clientErr
}
