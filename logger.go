package kafka

import (
	"context"
	"log/slog"
)

type loggerKey struct{}

// loggerFromContext returns the enriched logger stored in the context during
// message processing, or slog.Default() if no logger is present.
func loggerFromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(loggerKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

type kafkaMessageInfo struct {
	Topic         string `json:"topic"`
	Partition     int32  `json:"partition"`
	Offset        int64  `json:"offset"`
	Key           string `json:"key"`
	ConsumerGroup string `json:"consumer_group"`
}

func (i kafkaMessageInfo) LogValue() slog.Value {
	attrs := make([]slog.Attr, 0, 5)

	if i.Topic != "" {
		attrs = append(attrs, slog.String("topic", i.Topic))
	}
	if i.ConsumerGroup != "" {
		attrs = append(attrs, slog.String(logFieldName("consumerGroup", "consumer_group"), i.ConsumerGroup))
	}
	if i.Partition != 0 || i.Topic != "" {
		attrs = append(attrs, slog.Int("partition", int(i.Partition)))
	}
	if i.Offset != 0 || i.Topic != "" {
		attrs = append(attrs, slog.Int64("offset", i.Offset))
	}
	if i.Key != "" {
		attrs = append(attrs, slog.String("key", i.Key))
	}

	return slog.GroupValue(attrs...)
}

func logFieldName(camelCase, snakeCase string) string {
	if LogFormat == LogFieldFormatSnakeCase {
		return snakeCase
	}
	return camelCase
}

func logForwardTopicField(kind string) string {
	switch kind {
	case "retry":
		return logFieldName("retryTopic", "retry_topic")
	case "deadletter":
		return logFieldName("deadletterTopic", "deadletter_topic")
	default:
		return kind + "Topic"
	}
}

func WithLogContextStorer(storer LogContextStorer) ListenerOption {
	return func(l *listener) {
		l.logContextStorer = storer
	}
}
