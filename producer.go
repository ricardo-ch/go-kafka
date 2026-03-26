package kafka

import (
	"context"
	"log/slog"
	"sync"

	"github.com/IBM/sarama"
)

type Producer interface {
	Produce(ctx context.Context, msg *sarama.ProducerMessage) error
	Close()
}

// producerHandler is a function that handles the production of a message.
type producerHandler func(ctx context.Context, p *producer, msg *sarama.ProducerMessage) error

type producer struct {
	handler       producerHandler
	producer      sarama.SyncProducer
	instrumenting *ProducerMetricsService
	closeOnce     sync.Once
}

// NewProducer creates a new producer that uses the default sarama client.
func NewProducer(options ...ProducerOption) (Producer, error) {
	c, err := getClient()
	if err != nil {
		return nil, err
	}

	p, err := sarama.NewSyncProducerFromClient(c)
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
func (p *producer) Produce(ctx context.Context, msg *sarama.ProducerMessage) error {
	return p.handler(ctx, p, msg)
}

// Close closes the producer.
func (p *producer) Close() {
	p.closeOnce.Do(func() {
		if err := p.producer.Close(); err != nil {
			slog.Warn("failed to close producer", "error", err)
		}
	})
}

// produce sends the message via sarama. ctx is part of the producerHandler signature
// so that middleware (instrumenting, tracing) can access it; it is not used here directly.
func produce(_ context.Context, p *producer, msg *sarama.ProducerMessage) error {
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return err
	}
	slog.Debug("message produced", "topic", msg.Topic, "partition", partition, "offset", offset)
	return nil
}
