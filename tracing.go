package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

const tracerName = "github.com/ricardo-ch/go-kafka"

// Deprecated: use OTELTracingFunc with WithOTELTracing instead.
// TracingFunc is the legacy OpenTracing hook used to create tracing and/or
// propagate the tracing context from each message to the Go context.
// The returned span must be finished by the caller (e.g. defer span.Finish()).
type TracingFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context)

// Deprecated: use WithOTELTracing.
// WithTracing accepts the legacy OpenTracing TracingFunc to execute before each message.
func WithTracing(tracer TracingFunc) ListenerOption {
	return func(l *listener) {
		l.tracer = tracer
		l.otelTracer = nil
	}
}

// Deprecated: use DefaultOTELTracing.
// DefaultTracing implements the legacy OpenTracing TracingFunc.
// It fetches tracing headers from the Kafka message and creates a child span
// with Kafka metadata using the global OpenTracing tracer.
func DefaultTracing(ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context) {
	return extractOpenTracingSpanFromKafkaMessage(ctx, msg, true)
}

// Deprecated: use GetOTELContextFromKafkaMessage or GetOTELContextFromKafkaMessageWithKafkaAttributes.
// GetContextFromKafkaMessage extracts OpenTracing headers from a Kafka message
// and creates a span. The returned span must be finished by the caller.
func GetContextFromKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage) (opentracing.Span, context.Context) {
	return extractOpenTracingSpanFromKafkaMessage(ctx, msg, false)
}

func extractOpenTracingSpanFromKafkaMessage(ctx context.Context, msg *sarama.ConsumerMessage, withKafkaAttributes bool) (opentracing.Span, context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	carrier := make(map[string]string)
	spanName := "message from kafka"
	if msg != nil {
		carrier = make(map[string]string, len(msg.Headers))
		for _, h := range msg.Headers {
			carrier[string(h.Key)] = string(h.Value)
		}
		spanName = fmt.Sprintf("message from %s", msg.Topic)
	}

	var fields *map[string]interface{}
	if withKafkaAttributes && msg != nil {
		fields = &map[string]interface{}{
			"offset":    msg.Offset,
			"partition": msg.Partition,
			"key":       string(msg.Key),
		}
	}

	return extractOpenTracingContextFromCarrier(ctx, carrier, spanName, fields)
}

// Deprecated: use GetOTELKafkaHeadersFromContext.
// GetKafkaHeadersFromContext fetches OpenTracing metadata from context and returns them as []RecordHeader.
func GetKafkaHeadersFromContext(ctx context.Context) []sarama.RecordHeader {
	carrier := injectOpenTracingContextIntoCarrier(ctx)

	recordHeaders := make([]sarama.RecordHeader, 0, len(carrier))
	for headerKey, headerValue := range carrier {
		recordHeaders = append(recordHeaders, sarama.RecordHeader{Key: []byte(headerKey), Value: []byte(headerValue)})
	}

	return recordHeaders
}

// Deprecated: use SerializeOTELKafkaHeadersFromContext.
// SerializeKafkaHeadersFromContext fetches OpenTracing metadata from context and serializes it into a JSON map[string]string.
func SerializeKafkaHeadersFromContext(ctx context.Context) (string, error) {
	kafkaHeadersJSON, err := json.Marshal(injectOpenTracingContextIntoCarrier(ctx))
	return string(kafkaHeadersJSON), err
}

// Deprecated: use DeserializeOTELContextFromKafkaHeaders.
// DeserializeContextFromKafkaHeaders extracts OpenTracing headers from a JSON-encoded carrier and returns the enriched context.
// On error, the original context is returned alongside the error.
func DeserializeContextFromKafkaHeaders(ctx context.Context, kafkaHeaders string) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	var rawHeaders map[string]string
	if err := json.Unmarshal([]byte(kafkaHeaders), &rawHeaders); err != nil {
		return ctx, err
	}

	_, extractedCtx := extractOpenTracingContextFromCarrier(ctx, opentracing.TextMapCarrier(rawHeaders), "", nil)
	return extractedCtx, nil
}

func injectOpenTracingContextIntoCarrier(ctx context.Context) opentracing.TextMapCarrier {
	carrier := opentracing.TextMapCarrier{}

	span := opentracing.SpanFromContext(ctx)
	if span == nil {
		return carrier
	}

	ext.SpanKindProducer.Set(span)
	_ = opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, carrier)

	return carrier
}

func extractOpenTracingContextFromCarrier(ctx context.Context, carrier opentracing.TextMapCarrier, spanName string, tags *map[string]interface{}) (opentracing.Span, context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	wireContext, _ := opentracing.GlobalTracer().Extract(opentracing.TextMap, carrier)
	span := opentracing.GlobalTracer().StartSpan(spanName, ext.RPCServerOption(wireContext))
	if tags != nil {
		for k, v := range *tags {
			span.SetTag(k, v)
		}
	}

	return span, opentracing.ContextWithSpan(ctx, span)
}
