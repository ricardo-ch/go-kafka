package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/ricardo-ch/go-tracing"
)

// WithInstrumenting adds an instance of Prometheus metrics
func WithInstrumenting() ListenerOption {
	return func(l *listener) {
		l.instrumenting = NewConsumerMetricsService(l.groupID)
	}
}

// TracingFunc is used to create tracing and/or propagate the tracing context from each messages to the go context.
type TracingFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context)

// WithTracing accepts a TracingFunc to execute before each message
func WithTracing(tracer TracingFunc) ListenerOption {
	return func(l *listener) {
		l.tracer = tracer
	}
}

// DefaultTracing implement TracingFunc
// It fetch opentracing headers from the kafka message headers, then create a span using the opentracing.GlobalTracer()
// usage: `listener, err = kafka.NewListener(brokers, appName, handlers, kafka.WithTracing(kafka.DefaultTracing))`
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
