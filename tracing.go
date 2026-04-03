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

// startMessageSpan starts a tracing span for the given message using the listener's TracingFunc.
// It returns the enriched context and a finish function that must be deferred by the caller.
// If no tracer is configured, ctx is returned unchanged and finish is a no-op.
func (l *listener) startMessageSpan(ctx context.Context, msg *sarama.ConsumerMessage) (context.Context, func()) {
	if l.tracer == nil {
		return ctx, func() {}
	}
	span, ctx := l.tracer(ctx, msg)
	if span == nil {
		return ctx, func() {}
	}

	return ctx, func() { span.End() }
}

// extractCarrierFromMessage extracts propagation headers from a Kafka message into a MapCarrier.
func extractCarrierFromMessage(ctx context.Context, msg *sarama.ConsumerMessage) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if msg == nil {
		return ctx
	}
	carrier := make(propagation.MapCarrier, len(msg.Headers))
	for _, h := range msg.Headers {
		carrier[string(h.Key)] = string(h.Value)
	}

	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// DefaultTracing implements TracingFunc using OpenTelemetry.
// It extracts W3C Trace Context headers from the Kafka message, then creates a child span
// with messaging-specific attributes.
// Usage: `listener, err = kafka.NewListener(appName, handlers, kafka.WithTracing(kafka.DefaultTracing))`
func DefaultTracing(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context) {
	ctx = extractCarrierFromMessage(ctx, msg)

	spanName := "message from " + msg.Topic
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName, trace.WithAttributes(
		attribute.Int64("messaging.kafka.offset", msg.Offset),
		attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
		attribute.String("messaging.kafka.message_key", string(msg.Key)),
	))

	return span, ctx
}

// GetKafkaHeadersFromContext fetches tracing metadata from context and returns them as []RecordHeader.
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

// GetContextFromKafkaMessage extracts tracing headers from a Kafka message and creates a span.
// The returned span must be ended by the caller (e.g. defer span.End()).
func GetContextFromKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context) {
	ctx = extractCarrierFromMessage(ctx, msg)

	spanName := "message from " + msg.Topic
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName)

	return span, ctx
}

// SerializeKafkaHeadersFromContext fetches tracing metadata from context and serializes it into a JSON map[string]string.
func SerializeKafkaHeadersFromContext(ctx context.Context) (string, error) {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	kafkaHeadersJSON, err := json.Marshal(map[string]string(carrier))

	return string(kafkaHeadersJSON), err
}

// DeserializeContextFromKafkaHeaders extracts tracing headers from a JSON-encoded carrier and returns the enriched context.
// On error, the original context is returned alongside the error.
func DeserializeContextFromKafkaHeaders(ctx context.Context, kafkaHeaders string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rawHeaders map[string]string
	err := json.Unmarshal([]byte(kafkaHeaders), &rawHeaders)
	if err != nil {
		return ctx, err
	}

	carrier := propagation.MapCarrier(rawHeaders)

	return otel.GetTextMapPropagator().Extract(ctx, carrier), nil
}
