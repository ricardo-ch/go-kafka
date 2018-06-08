package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

// producer object represents kafka producer
type producer struct {
	syncProducer sarama.SyncProducer
}

// Producer interface used to send messages
type Producer interface {
	SendMessage(key []byte, msg []byte, topic string) (partition int32, offset int64, err error)
	Close() error
}

// NewProducer creates a new instance of Producer
func NewProducer(brokers []string) (Producer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("cannot create new producer, brokers cannot be empty")
	}

	p, err := sarama.NewSyncProducer(brokers, &Config.Config)
	if err != nil {
		return nil, err
	}

	return producer{p}, nil
}

// newProducerFromClient creates a new instance of Producer from an existing client
func newProducerFromClient(client sarama.Client) (Producer, error) {
	if client == nil {
		return nil, errors.New("cannot create new producer from client, client cannot be nil")
	}

	p, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return producer{p}, nil
}

// SendMessage is used to send the meassage to kafka
func (p producer) SendMessage(key []byte, msg []byte, topic string) (partition int32, offset int64, err error) {
	if p.syncProducer == nil {
		return 0, 0, errors.New("cannot send message. producer is nil")
	}

	message := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(msg),
	}

	return p.syncProducer.SendMessage(message)
}

// Close the producer and dependencies
func (p producer) Close() error {
	if p.syncProducer != nil {
		return p.syncProducer.Close()
	}
	return nil
}
