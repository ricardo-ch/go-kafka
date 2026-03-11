## go-kafka v4.0.0

### Summary

Major release introducing modern Go patterns, improved observability, and better error handling flexibility.

### Breaking Changes

- **Minimum Go version**: Go 1.26+ required (for `log/slog` support)
- **Module path**: `github.com/ricardo-ch/go-kafka/v3` (to be updated to `/v4` before release)
- **Producer API**: `Produce(msg)` → `Produce(ctx context.Context, msg)` — context is now required for trace propagation and context-aware logging
- **Error handling**: `ErrEventUnretriable` and `ErrEventOmitted` are now interface-based (see migration guide below)
- **Logging**: Removed custom `Logger`/`ErrorLogger` and all wrapper functions (`SetLogger`, `SetLogLevel`, `LowercaseLevelAttr`). The library now calls `slog.Default()` directly — configure via `slog.SetDefault()` in your application.
- **Tracing**: Replaced OpenTracing (`github.com/opentracing/opentracing-go`, `github.com/ricardo-ch/go-tracing`) with OpenTelemetry (`go.opentelemetry.io/otel`). Propagation format is now W3C Trace Context (`traceparent`, `tracestate`).
- **Removed**: `func Ptr[T any](v T)` (now standard in Go 1.26)

### New Features

#### Structured Logging with `slog`

The library uses `slog.Default()` directly with no wrapper functions. Configure logging in your application:

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
})))
```

#### Kafka Message Context in Logs (`WithLogContextStorer`)

When processing a message, the library can enrich the `context.Context` with a `*slog.Logger` that carries Kafka metadata as a structured `"kafka"` group. This is opt-in via the `WithLogContextStorer` listener option.

The library:
1. Builds a `*slog.Logger` via `slog.With("kafka", kafkaMessageInfo{...})` containing the message metadata
2. Calls your `LogContextStorer` function to store it in the context
3. Your handler retrieves it from context and logs with full Kafka context automatically

```go
type LogContextStorer func(ctx context.Context, logger *slog.Logger) context.Context
```

Example setup:

```go
listener, err := kafka.NewListener(appName, handlers,
    kafka.WithLogContextStorer(myContextStorer),
)
```

Example handler:

```go
func (s service) OnUserEvent(ctx context.Context, msg UserEvent) error {
    loggerFromCtx(ctx).Info("received user event", "content", msg.Content)
    return nil
}
```

Output (JSON):

```json
{"level":"INFO","msg":"received user event","kafka":{"topic":"test-users","consumer_group":"example-kafka","partition":0,"offset":42},"content":"hello"}
```

`kafkaMessageInfo` implements `slog.LogValuer` for structured nested output under the `"kafka"` key. The `LogContextStorer` function type is agnostic — you provide your own `ToContext`/`FromContext` helpers (or use a library like `slogr`).

#### Interface-based Error Handling

Errors are now detected via interfaces, allowing custom error types:

```go
// Option 1: Use wrapper functions (recommended)
return kafka.NewUnretriableError(errors.New("validation failed"))
return kafka.NewOmittedError(errors.New("duplicate message"))

// Option 2: Implement interfaces on custom types
type ValidationError struct { Field, Message string }
func (e ValidationError) Error() string       { return e.Message }
func (e ValidationError) IsUnretriable() bool { return true }
```

#### Native Sarama Exponential Backoff

Replaced custom exponential backoff with `sarama.NewExponentialBackoff` (KIP-580):
- Includes jitter to avoid thundering herd
- Configurable via `MaxBackoffDuration` (default: 1 minute)
- Per-handler `BackoffFunc` option available

#### Modern Kafka Version

Default Kafka version updated to `sarama.MaxVersion` to enable all available features.

#### Observability Modernization (Tracing)

- Replaced OpenTracing with OpenTelemetry, the current industry standard
- Standard helpers to inject and extract trace context from Kafka headers using OTel propagators
- **Retry/deadletter trace propagation**: forwarded messages now carry the current processing span's trace context (not the original producer's), giving a complete trace: producer → consumer (failure) → retry/deadletter

### Bug Fixes

- **Partition instrumentation**: Fixed `string(msg.Partition)` → `strconv.FormatInt` that produced incorrect Unicode characters instead of numeric partition IDs
- **Consumer error channel**: Fixed to continuously read from the error channel (`for err := range`) instead of consuming a single message
- **Retry/deadletter headers**: Forwarded messages now correctly preserve original application headers while injecting the current trace context
- **Graceful shutdown**: Fixed blocking during retry backoff when context is cancelled — the consumer now exits promptly via `select` on `ctx.Done()`

### Improvements

- **Logging**: No wrapper functions — library uses `slog.Default()` directly. Client configures via `slog.SetDefault()` and optionally `WithLogContextStorer` for per-message context
- **Panic recovery**: Added `debug.Stack()` for full stack traces in error logs, isolated into `safeProcess`
- **Client initialization**: Replaced custom Mutex with `sync.Once` for thread-safe, idiomatic singleton pattern. Added `resetClient()` for test isolation
- **Metrics initialization**: Replaced double-checked locking with `sync.Once` for all Prometheus metric registrations (consumer and producer)
- **Retry loop**: Rewrote `handleMessageWithRetry` from recursive to iterative, eliminating stack overflow risk with `InfiniteRetries`
- **Producer context**: `Produce(ctx, msg)` now accepts context, enabling trace propagation and context-aware logging through the producer middleware chain

### Migration Guide

#### Error Handling

**Before (v3):**
```go
return fmt.Errorf("error: %w", kafka.ErrEventUnretriable)
```

**After (v4):**
```go
// Recommended
return kafka.NewUnretriableError(err)

// Or implement the interface
type MyError struct{}
func (e MyError) Error() string       { return "my error" }
func (e MyError) IsUnretriable() bool { return true }
```

#### Logging

**Before (v3):**
```go
kafka.Logger = myLogger
kafka.ErrorLogger = myErrorLogger
```

**After (v4):**
```go
// Configure the global slog logger
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
})))

// Optionally, enrich handler context with Kafka metadata
listener, _ := kafka.NewListener(appName, handlers,
    kafka.WithLogContextStorer(slogr.ToContext),
)
```

#### Producer

**Before (v3):**
```go
producer.Produce(msg)
```

**After (v4):**
```go
producer.Produce(ctx, msg)
```

#### Tracing

**Before (v3):**
```go
import "github.com/ricardo-ch/go-tracing"
// OpenTracing-based tracing
```

**After (v4):**
```go
// Configure OpenTelemetry in your application
listener, _ := kafka.NewListener(appName, handlers,
    kafka.WithTracing(kafka.DefaultTracing),
)

// Propagate trace context when producing
headers := kafka.GetKafkaHeadersFromContext(ctx)
```
