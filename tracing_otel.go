package kafka

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// OTELTracingFunc is the OpenTelemetry hook used to create tracing
// and/or propagate the tracing context from each message to the Go context.
// The returned span must be ended by the caller (e.g. defer span.End()).
type OTELTracingFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context)

// WithOTELTracing accepts an OpenTelemetry tracing hook to execute before each message.
func WithOTELTracing(tracer OTELTracingFunc) ListenerOption {
	return func(l *listener) {
		l.otelTracer = tracer
		l.tracer = nil
	}
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

// DefaultOTELTracing implements OpenTelemetry tracing with Kafka-specific attributes.
func DefaultOTELTracing(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context) {
	ctx = extractCarrierFromMessage(ctx, msg)

	spanName := "message from " + msg.Topic
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName, trace.WithAttributes(
		attribute.Int64("messaging.kafka.offset", msg.Offset),
		attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
		attribute.String("messaging.kafka.message_key", string(msg.Key)),
	))

	return span, ctx
}

// GetOTELContextFromKafkaMessage extracts W3C Trace Context headers from a Kafka message
// and creates a span. The returned span must be ended by the caller.
func GetOTELContextFromKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage) (trace.Span, context.Context) {
	ctx = extractCarrierFromMessage(ctx, msg)

	spanName := "message from " + msg.Topic
	ctx, span := otel.Tracer(tracerName).Start(ctx, spanName)

	return span, ctx
}

// GetOTELKafkaHeadersFromContext fetches OpenTelemetry metadata from context and returns them as []RecordHeader.
func GetOTELKafkaHeadersFromContext(ctx context.Context) []sarama.RecordHeader {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	recordHeaders := make([]sarama.RecordHeader, 0, len(carrier))
	for headerKey, headerValue := range carrier {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{Key: []byte(headerKey), Value: []byte(headerValue)})
	}

	return recordHeaders
}

// SerializeOTELKafkaHeadersFromContext fetches OpenTelemetry metadata from context and serializes it into a JSON map[string]string.
func SerializeOTELKafkaHeadersFromContext(ctx context.Context) (string, error) {
	carrier := make(propagation.MapCarrier)
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	kafkaHeadersJSON, err := json.Marshal(map[string]string(carrier))
	return string(kafkaHeadersJSON), err
}

// DeserializeOTELContextFromKafkaHeaders extracts OpenTelemetry headers from a JSON-encoded carrier and returns the enriched context.
// On error, the original context is returned alongside the error.
func DeserializeOTELContextFromKafkaHeaders(ctx context.Context, kafkaHeaders string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rawHeaders map[string]string
	if err := json.Unmarshal([]byte(kafkaHeaders), &rawHeaders); err != nil {
		return ctx, err
	}

	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(rawHeaders)), nil
}
