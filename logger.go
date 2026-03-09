package kafka

import (
	"context"
	"log/slog"
)

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
		attrs = append(attrs, slog.String("consumer_group", i.ConsumerGroup))
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

func WithLogContextStorer(storer LogContextStorer) ListenerOption {
	return func(l *listener) {
		l.logContextStorer = storer
	}
}

func logContextOrBackground(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

// ContextWithMessageInfo enriches a context with Kafka message metadata.
// When used with a ContextHandler, metadata is automatically added to all log records.
//func ContextWithMessageInfo(ctx context.Context, msg *sarama.ConsumerMessage, consumerGroup string) context.Context {
//	info := kafkaMessageInfo{ConsumerGroup: consumerGroup}
//	if msg != nil {
//		info.Topic = msg.Topic
//		info.Partition = msg.Partition
//		info.Offset = msg.Offset
//		info.Key = string(msg.Key)
//	}
//	return context.WithValue(ctx, contextKey{}, info)
//}

// MessageAttrsFromContext extracts Kafka message metadata stored in the context
// and returns them as slog-compatible key-value pairs.
// Returns nil if no metadata is present.
//func MessageAttrsFromContext(ctx context.Context) []any {
//	attrs := kafkaAttrs(ctx)
//	if len(attrs) == 0 {
//		return nil
//	}
//	result := make([]any, 0, len(attrs)*2)
//	for _, a := range attrs {
//		result = append(result, a.Key, a.Value.Any())
//	}
//	return result
//}

//func kafkaAttrs(ctx context.Context) []slog.Attr {
//	info, ok := ctx.Value(contextKey{}).(kafkaMessageInfo)
//	if !ok {
//		return nil
//	}
//	var attrs []slog.Attr
//	if info.Topic != "" {
//		attrs = append(attrs, slog.String("topic", info.Topic))
//	}
//	if info.ConsumerGroup != "" {
//		attrs = append(attrs, slog.String("consumer_group", info.ConsumerGroup))
//	}
//	if info.Partition != 0 || info.Topic != "" {
//		attrs = append(attrs, slog.Int("partition", int(info.Partition)))
//	}
//	if info.Offset != 0 || info.Topic != "" {
//		attrs = append(attrs, slog.Int64("offset", info.Offset))
//	}
//	if info.Key != "" {
//		attrs = append(attrs, slog.String("key", info.Key))
//	}
//	return attrs
//}

// ContextHandler is a slog.Handler that automatically extracts Kafka message
// metadata from the context and adds it to every log record.
//
// Usage:
//
//	baseHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
//	slog.SetDefault(slog.New(kafka.NewContextHandler(baseHandler)))
//type ContextHandler struct {
//	next slog.Handler
//}

// NewContextHandler wraps a slog.Handler to automatically enrich log records
// with Kafka message metadata stored in the context via ContextWithMessageInfo.
//func NewContextHandler(next slog.Handler) *ContextHandler {
//	return &ContextHandler{next: next}
//}
//
//func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
//	return h.next.Enabled(ctx, level)
//}
//
//func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
//	if attrs := kafkaAttrs(ctx); len(attrs) > 0 {
//		record.AddAttrs(attrs...)
//	}
//	return h.next.Handle(ctx, record)
//}
//
//func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
//	return &ContextHandler{next: h.next.WithAttrs(attrs)}
//}
//
//func (h *ContextHandler) WithGroup(name string) slog.Handler {
//	return &ContextHandler{next: h.next.WithGroup(name)}
//}
