package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/ricardo-ch/go-tracing"
)

// WithInstrumenting adds the instrumenting layer on a listener.
func WithInstrumenting() ListenerOption {
	return func(l *listener) {
		l.instrumenting = NewConsumerMetricsService(l.groupID)
	}
}

// ProducerOption is a function that is passed to the producer constructor to configure it.
type ProducerOption func(p *producer)

// WithProducerInstrumenting adds the instrumenting layer on a producer.
func WithProducerInstrumenting() ProducerOption {
	return func(p *producer) {
		p.instrumenting = NewProducerMetricsService()
		p.handler = p.instrumenting.Instrumentation(p.handler)
	}
}

// WithDeadletterProducerInstrumenting adds the instrumenting layer on a deadletter producer.
func WithDeadletterProducerInstrumenting() ProducerOption {
	return func(p *producer) {
		p.instrumenting = NewDeadletterProducerMetricsService()
		p.handler = p.instrumenting.DeadletterInstrumentation(p.handler)
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

// DefaultTracing implements TracingFunc
// It fetches opentracing headers from the kafka message headers, then creates a span using the opentracing.GlobalTracer()
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

// GetKafkaHeadersFromContext fetch tracing metadata from context and returns them in format []RecordHeader
func GetKafkaHeadersFromContext(ctx context.Context) []sarama.RecordHeader {
	carrier := tracing.InjectIntoCarrier(ctx)

	recordHeaders := make([]sarama.RecordHeader, 0, len(carrier))
	for headerKey, headerValue := range carrier {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{Key: []byte(headerKey), Value: []byte(headerValue)})
	}
	return recordHeaders
}

// GetContextFromKafkaMessage fetches tracing headers from the kafka message
func GetContextFromKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	return tracing.ExtractFromCarrier(ctx, carrier, fmt.Sprintf("message from %s", msg.Topic), nil)
}

// SerializeKafkaHeadersFromContext fetches tracing metadata from context and serialize it into a json map[string]string
func SerializeKafkaHeadersFromContext(ctx context.Context) (string, error) {
	kafkaHeaders := tracing.InjectIntoCarrier(ctx)
	kafkaHeadersJSON, err := json.Marshal(kafkaHeaders)

	return string(kafkaHeadersJSON), err
}

// DeserializeContextFromKafkaHeaders fetches tracing headers from json encoded carrier and returns the context
func DeserializeContextFromKafkaHeaders(ctx context.Context, kafkaheaders string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rawHeaders map[string]string
	if err := json.Unmarshal([]byte(kafkaheaders), &rawHeaders); err != nil {
		return nil, err
	}

	_, ctx = tracing.ExtractFromCarrier(ctx, rawHeaders, "", nil)

	return ctx, nil
}
