package kafka

import (
	"github.com/IBM/sarama"
)

type Producer interface {
	Produce(msg *sarama.ProducerMessage) error
	Close() error
}

// ProducerHandler is a function that handles the production of a message. It is exposed to allow for easy middleware building.
type ProducerHandler func(p *producer, msg *sarama.ProducerMessage) error

type producer struct {
	handler       ProducerHandler
	producer      sarama.SyncProducer
	instrumenting *ProducerMetricsService
}

// NewProducer creates a new producer that uses the default sarama client.
func NewProducer(options ...ProducerOption) (Producer, error) {
	client, err := getClient()
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewSyncProducerFromClient(*client)
	if err != nil {
		return nil, err
	}

	producer := &producer{
		producer: p,
		handler:  produce,
	}

	for _, option := range options {
		option(producer)
	}

	return producer, nil
}

// Produce sends a message to the kafka cluster.
func (p *producer) Produce(msg *sarama.ProducerMessage) error {
	return p.handler(p, msg)
}

// Close closes the producer.
func (p *producer) Close() error {
	return p.producer.Close()
}

func produce(p *producer, msg *sarama.ProducerMessage) error {
	_, _, err := p.producer.SendMessage(msg)
	return err
}
