package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/ricardo-ch/go-kafka"

// TracingFunc is used to create tracing and/or propagate the tracing context from each message to the go context.
// The returned span must be ended by the caller (e.g. defer span.End()).
type TracingFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context)

// WithTracing accepts a TracingFunc to execute before each message
func WithTracing(tracer TracingFunc) ListenerOption {
	return func(l *listener) {
		l.tracer = tracer
	}
}

// DefaultTracing implements TracingFunc using OpenTelemetry.
// It extracts W3C Trace Context headers from the Kafka message, then creates a child span.
// Usage: `listener, err = kafka.NewListener(brokers, appName, handlers, kafka.WithTracing(kafka.DefaultTracing))`
func DefaultTracing(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := make(propagation.MapCarrier, len(msg.Headers))
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	tracer := otel.Tracer(tracerName)
	spanName := fmt.Sprintf("message from %s", msg.Topic)
	attrs := []attribute.KeyValue{
		attribute.Int64("messaging.kafka.offset", msg.Offset),
		attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
		attribute.String("messaging.kafka.message_key", string(msg.Key)),
	}
	ctx, span := tracer.Start(ctx, spanName, trace.WithAttributes(attrs...))
	return span, ctx
}

// GetKafkaHeadersFromContext fetches tracing metadata from context and returns them in format []RecordHeader.
// Uses W3C Trace Context format (traceparent, tracestate).
func GetKafkaHeadersFromContext(ctx context.Context) []sarama.RecordHeader {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	recordHeaders := make([]sarama.RecordHeader, 0, len(carrier))
	for headerKey, headerValue := range carrier {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{Key: []byte(headerKey), Value: []byte(headerValue)})
	}
	return recordHeaders
}

// GetContextFromKafkaMessage fetches tracing headers from the Kafka message and creates a span.
// The returned span must be ended by the caller (e.g. defer span.End()).
func GetContextFromKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := make(propagation.MapCarrier, len(msg.Headers))
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	tracer := otel.Tracer(tracerName)
	spanName := fmt.Sprintf("message from %s", msg.Topic)
	ctx, span := tracer.Start(ctx, spanName)
	return span, ctx
}

// SerializeKafkaHeadersFromContext fetches tracing metadata from context and serializes it into a JSON map[string]string
func SerializeKafkaHeadersFromContext(ctx context.Context) (string, error) {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	kafkaHeadersJSON, err := json.Marshal(map[string]string(carrier))
	return string(kafkaHeadersJSON), err
}

// DeserializeContextFromKafkaHeaders fetches tracing headers from JSON encoded carrier and returns the context
func DeserializeContextFromKafkaHeaders(ctx context.Context, kafkaheaders string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rawHeaders map[string]string
	if err := json.Unmarshal([]byte(kafkaheaders), &rawHeaders); err != nil {
		return nil, err
	}

	carrier := propagation.MapCarrier(rawHeaders)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	return ctx, nil
}
