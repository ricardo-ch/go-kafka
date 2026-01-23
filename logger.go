package kafka

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// DefaultLogger is the default slog.Logger instance used by go-kafka.
// By default, it logs at Info level to stderr with text format.
// You can replace it with your own logger using SetLogger().
var DefaultLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// SetLogger sets a custom slog.Logger for go-kafka.
// This allows you to use your own logging configuration, handlers, and format.
//
// Example with JSON handler:
//
//	kafka.SetLogger(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
//	    Level: slog.LevelDebug,
//	})))
//
// Example with custom handler:
//
//	kafka.SetLogger(slog.New(myCustomHandler))
func SetLogger(logger *slog.Logger) {
	DefaultLogger = logger
}

// SetLogLevel sets the minimum log level for the default logger.
// This creates a new TextHandler with the specified level.
// For more control, use SetLogger() with a custom handler.
func SetLogLevel(level slog.Level) {
	DefaultLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
}

// MessageContext contains contextual information about a Kafka message for logging.
type MessageContext struct {
	Topic         string
	Partition     int32
	Offset        int64
	Key           string
	ConsumerGroup string
	Timestamp     time.Time
}

// NewMessageContext creates a MessageContext from a sarama.ConsumerMessage.
func NewMessageContext(msg *sarama.ConsumerMessage, consumerGroup string) MessageContext {
	if msg == nil {
		return MessageContext{ConsumerGroup: consumerGroup}
	}
	return MessageContext{
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Key:           string(msg.Key),
		ConsumerGroup: consumerGroup,
		Timestamp:     msg.Timestamp,
	}
}

// LogAttrs returns the message context as slog attributes.
func (mc MessageContext) LogAttrs() []slog.Attr {
	attrs := []slog.Attr{}
	if mc.Topic != "" {
		attrs = append(attrs, slog.String("topic", mc.Topic))
	}
	if mc.ConsumerGroup != "" {
		attrs = append(attrs, slog.String("consumer_group", mc.ConsumerGroup))
	}
	if mc.Partition != 0 || mc.Topic != "" {
		attrs = append(attrs, slog.Int("partition", int(mc.Partition)))
	}
	if mc.Offset != 0 || mc.Topic != "" {
		attrs = append(attrs, slog.Int64("offset", mc.Offset))
	}
	if mc.Key != "" {
		attrs = append(attrs, slog.String("key", mc.Key))
	}
	return attrs
}

// --- Convenience functions using the default logger ---

// LogDebug logs a debug message.
func LogDebug(msg string, args ...any) {
	DefaultLogger.Debug(msg, args...)
}

// LogInfo logs an info message.
func LogInfo(msg string, args ...any) {
	DefaultLogger.Info(msg, args...)
}

// LogWarn logs a warning message.
func LogWarn(msg string, args ...any) {
	DefaultLogger.Warn(msg, args...)
}

// LogError logs an error message.
func LogError(msg string, args ...any) {
	DefaultLogger.Error(msg, args...)
}

// LogDebugContext logs a debug message with context.
func LogDebugContext(ctx context.Context, msg string, args ...any) {
	DefaultLogger.DebugContext(ctx, msg, args...)
}

// LogInfoContext logs an info message with context.
func LogInfoContext(ctx context.Context, msg string, args ...any) {
	DefaultLogger.InfoContext(ctx, msg, args...)
}

// LogWarnContext logs a warning message with context.
func LogWarnContext(ctx context.Context, msg string, args ...any) {
	DefaultLogger.WarnContext(ctx, msg, args...)
}

// LogErrorContext logs an error message with context.
func LogErrorContext(ctx context.Context, msg string, args ...any) {
	DefaultLogger.ErrorContext(ctx, msg, args...)
}

// --- Message-aware logging functions ---

// logWithMessageContext is a helper that combines message context with additional args.
func logWithMessageContext(mc MessageContext, args ...any) []any {
	result := make([]any, 0, len(mc.LogAttrs())*2+len(args))
	for _, attr := range mc.LogAttrs() {
		result = append(result, attr.Key, attr.Value.Any())
	}
	result = append(result, args...)
	return result
}

// LogMessageDebug logs a debug message with message context.
func LogMessageDebug(msg string, mc MessageContext, args ...any) {
	DefaultLogger.Debug(msg, logWithMessageContext(mc, args...)...)
}

// LogMessageInfo logs an info message with message context.
func LogMessageInfo(msg string, mc MessageContext, args ...any) {
	DefaultLogger.Info(msg, logWithMessageContext(mc, args...)...)
}

// LogMessageWarn logs a warning message with message context.
func LogMessageWarn(msg string, mc MessageContext, args ...any) {
	DefaultLogger.Warn(msg, logWithMessageContext(mc, args...)...)
}

// LogMessageError logs an error message with message context.
func LogMessageError(msg string, mc MessageContext, args ...any) {
	DefaultLogger.Error(msg, logWithMessageContext(mc, args...)...)
}
