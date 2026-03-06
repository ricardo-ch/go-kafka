package kafka

import (
	"context"
	"log/slog"

	"github.com/IBM/sarama"
)

type Producer interface {
	Produce(ctx context.Context, msg *sarama.ProducerMessage) error
	Close() error
}

// producerHandler is a function that handles the production of a message.
type producerHandler func(ctx context.Context, p *producer, msg *sarama.ProducerMessage) error

type producer struct {
	handler       producerHandler
	producer      sarama.SyncProducer
	instrumenting *ProducerMetricsService
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
func (p *producer) Close() error {
	err := p.producer.Close()
	if err != nil {
		slog.Default().Error("failed to close producer", "error", err)
	} else {
		slog.Default().Info("producer closed")
	}
	return err
}

func produce(ctx context.Context, p *producer, msg *sarama.ProducerMessage) error {
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		slog.Default().ErrorContext(ctx, "failed to produce message", "error", err, "topic", msg.Topic)
		return err
	}
	slog.Default().DebugContext(ctx, "message produced", "topic", msg.Topic, "partition", partition, "offset", offset)
	return nil
}
