package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/ricardo-ch/go-tracing"
)

//WithInstrumenting add a instance of Prometheus metrics
func WithInstrumenting() ListenerOption {
	return func(l *listener) {
		l.instrumenting = NewConsumerMetricsService(l.groupID)
	}
}

// ContextFunc is used to create tracing and/or propagate the tracing context from the each messages to the go context.
type ContextFunc func(ctx context.Context) (opentracing.Span, context.Context)

// WithTracing accept a ContextFunc to execute before each message
func WithTracing(tracer ContextFunc) ListenerOption {
	return func(l *listener) {
		l.tracer = tracer
	}
}

func DefaultTracing(ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	return tracing.ExtractFromCarrier(ctx, carrier, fmt.Sprintf("message from %s", msg.Topic),
		&map[string]interface{}{"offset": msg.Offset, "partition": msg.Partition, "key": string(msg.Key)},
	)
}
