# Migration Guide: v3 → v4

This document covers all breaking changes and how to adapt your code when upgrading from `go-kafka/v3` to `go-kafka/v4`.

## Requirements

- **Go 1.26+** (for `log/slog` support)
- **Module path**: update `github.com/ricardo-ch/go-kafka/v3` → `github.com/ricardo-ch/go-kafka/v4` in your `go.mod` and all import statements

## Producer API

The `Produce` method now requires a `context.Context` as first argument, enabling trace propagation and middleware compatibility.

**v3:**
```go
err := producer.Produce(msg)
```

**v4:**
```go
err := producer.Produce(ctx, msg)
```

If you have no context available, use `context.Background()`:
```go
err := producer.Produce(context.Background(), msg)
```

The `Producer` interface has changed accordingly:
```go
// v3
type Producer interface {
    Produce(msg *sarama.ProducerMessage) error
    Close() error
}

// v4
type Producer interface {
    Produce(ctx context.Context, msg *sarama.ProducerMessage) error
    Close() error
}
```

Update any custom implementations or mocks of the `Producer` interface.

## Tracing: OpenTracing → OpenTelemetry

OpenTracing and `github.com/ricardo-ch/go-tracing` have been replaced by OpenTelemetry (`go.opentelemetry.io/otel`). Propagation format is now W3C Trace Context (`traceparent`, `tracestate`) instead of Jaeger/OpenTracing.

### Dependencies to remove

```
github.com/opentracing/opentracing-go
github.com/ricardo-ch/go-tracing
```

### Dependencies to add

Configure OpenTelemetry in your application (TracerProvider, exporter, propagator). See https://opentelemetry.io/docs/languages/go/getting-started/

### Code changes

**v3:**
```go
import "github.com/ricardo-ch/go-tracing"

// Tracing was initialized via go-tracing
```

**v4:**
```go
// 1. Configure OTel in your application startup
// 2. Enable tracing on the listener
listener, _ := kafka.NewListener(appName, handlers,
    kafka.WithTracing(kafka.DefaultTracing),
)

// 3. Propagate trace context when producing
headers := kafka.GetKafkaHeadersFromContext(ctx)
msg := &sarama.ProducerMessage{
    Topic:   "my-topic",
    Value:   sarama.StringEncoder("payload"),
    Headers: headers,
}
producer.Produce(ctx, msg)
```

### Trace context in retry/deadletter

Forwarded messages now carry the **current processing span's** trace context (not the original producer's). This gives a complete trace: producer → consumer (failure) → retry/deadletter.

## Logging

Custom logger types and all wrapper functions have been removed. The library now calls `slog.Default()` directly.

### Removed API

| v3 | v4 replacement |
|---|---|
| `kafka.Logger` | `slog.SetDefault(...)` |
| `kafka.ErrorLogger` | `slog.SetDefault(...)` |
| `kafka.SetLogger(logger)` | `slog.SetDefault(logger)` |
| `kafka.SetLogLevel(level)` | Set level in `slog.HandlerOptions` |
| `kafka.LowercaseLevelAttr` | Configure via custom `slog.Handler` |
| `kafka.LogDebug(...)` | Library uses `slog.Debug(...)` internally |
| `kafka.LogInfo(...)` | Library uses `slog.Info(...)` internally |
| `kafka.LogWarn(...)` | Library uses `slog.Warn(...)` internally |
| `kafka.LogError(...)` | Library uses `slog.Error(...)` internally |
| `kafka.LogMessageDebug(...)` | Library uses `loggerFromContext(ctx).Debug(...)` internally |
| `kafka.LogMessageInfo(...)` | Library uses `loggerFromContext(ctx).Info(...)` internally |
| `kafka.LogMessageWarn(...)` | Library uses `loggerFromContext(ctx).Warn(...)` internally |
| `kafka.LogMessageError(...)` | Library uses `loggerFromContext(ctx).Error(...)` internally |

### Configuration

**v3:**
```go
kafka.Logger = myLogger
kafka.ErrorLogger = myErrorLogger
```

**v4:**
```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
})))
```

### Context-aware logging (new, optional)

v4 introduces `WithLogContextStorer` to enrich your handler's context with a `*slog.Logger` carrying Kafka metadata:

```go
listener, _ := kafka.NewListener(appName, handlers,
    kafka.WithLogContextStorer(myToContext),
)
```

In your handler:
```go
func myHandler(ctx context.Context, msg *sarama.ConsumerMessage) error {
    myFromContext(ctx).Info("processing order", "order_id", orderID)
    return nil
}
```

All library-internal logs automatically include Kafka metadata (topic, partition, offset, consumer_group) as a structured `"kafka"` group.

## Error Handling

Error detection is now interface-based instead of relying solely on sentinel errors with `errors.Is()`.

### Wrapper functions (recommended)

**v3:**
```go
return fmt.Errorf("error: %w", kafka.ErrEventUnretriable)
return kafka.ErrEventOmitted
```

**v4:**
```go
return kafka.NewUnretriableError(errors.New("validation failed"))
return kafka.NewUnretriableError(fmt.Errorf("bad format: %w", err))

return kafka.NewOmittedError(errors.New("duplicate message"))
return kafka.NewOmittedError(fmt.Errorf("outdated: %w", err))
```

The original error is preserved and can be unwrapped with `errors.Is()` / `errors.As()`.

### Custom error types (new)

Implement the `UnretriableError` or `OmittedError` interfaces on your own types:

```go
type ValidationError struct { Field, Message string }
func (e ValidationError) Error() string       { return e.Message }
func (e ValidationError) IsUnretriable() bool { return true }

type OutdatedEventError struct{}
func (e OutdatedEventError) Error() string { return "outdated" }
func (e OutdatedEventError) IsOmitted() bool { return true }
```

### Sentinel errors

`ErrEventUnretriable` and `ErrEventOmitted` still exist and can be used directly or wrapped:

```go
return kafka.ErrEventUnretriable
return fmt.Errorf("bad payload: %w", kafka.ErrEventUnretriable)
```

### Error logging

Errors are now logged with an `error_type` field (`"retriable"`, `"unretriable"`, `"omitted"`). Retriable errors additionally include a `stack` trace for debugging.

## Exponential Backoff

`ExponentialBackoffFunc` is now `nil` by default. It is evaluated lazily using the current `DurationBeforeRetry` and `MaxBackoffDuration` via `sarama.NewExponentialBackoff` (KIP-580 with jitter).

**v3:**
```go
kafka.ExponentialBackoffFunc = sarama.NewExponentialBackoff(base, max)
```

**v4:**
```go
// Option 1: just configure the base values (lazy evaluation)
kafka.DurationBeforeRetry = 2 * time.Second
kafka.MaxBackoffDuration = 10 * time.Minute

// Option 2: override with a custom global function
kafka.ExponentialBackoffFunc = func(retries, maxRetries int) time.Duration {
    return time.Duration(retries+1) * time.Second
}

// Option 3: per-handler override
handler := kafka.Handler{
    Processor: myFunc,
    Config: kafka.HandlerConfig{
        ExponentialBackoff: true,
        BackoffFunc: func(retries, maxRetries int) time.Duration {
            return time.Duration(retries+1) * 500 * time.Millisecond
        },
    },
}
```

Priority: handler `BackoffFunc` > global `ExponentialBackoffFunc` > lazy `sarama.NewExponentialBackoff`.

## Global Variables Changes

| Variable | v3 default | v4 default | Notes |
|---|---|---|---|
| `MaxBackoffDuration` | `1m` | **`10m`** | Caps both fixed and exponential backoff |
| `ForwardMaxBackoffDuration` | *(new)* | `30s` | Caps retry backoff when forwarding to retry/deadletter topics |
| `ExponentialBackoffFunc` | `sarama.NewExponentialBackoff(...)` | `nil` | Evaluated lazily on first use |

## Retry/Deadletter Forwarding

Forwarding to retry/deadletter topics is now **guaranteed**: on producer failure, the library retries with exponential backoff (capped by `ForwardMaxBackoffDuration`) until the message is published or the context is cancelled.

In v3, a failed forward was fire-and-forget — the message was lost. In v4, no code change is required; this is an automatic behavior improvement.

To tune the forward retry cap:
```go
kafka.ForwardMaxBackoffDuration = 1 * time.Minute
```

## Removed Utilities

| Removed | Replacement |
|---|---|
| `func Ptr[T any](v T) *T` | Use Go 1.26 built-in equivalent |

## Checklist

- [ ] Update `go.mod`: `go-kafka/v3` → `go-kafka/v4`
- [ ] Update all import paths: `go-kafka/v3` → `go-kafka/v4`
- [ ] Update `producer.Produce(msg)` → `producer.Produce(ctx, msg)`
- [ ] Update `Producer` interface mocks (add `context.Context` parameter)
- [ ] Remove `kafka.Logger`, `kafka.ErrorLogger`, `kafka.SetLogger(...)`, `kafka.SetLogLevel(...)` calls
- [ ] Configure logging via `slog.SetDefault(...)` instead
- [ ] Optionally add `kafka.WithLogContextStorer(...)` for context-enriched logging
- [ ] Replace OpenTracing imports with OpenTelemetry configuration
- [ ] Remove `github.com/ricardo-ch/go-tracing` dependency
- [ ] Replace `fmt.Errorf("...: %w", kafka.ErrEventUnretriable)` with `kafka.NewUnretriableError(err)`
- [ ] Replace `fmt.Errorf("...: %w", kafka.ErrEventOmitted)` with `kafka.NewOmittedError(err)`
- [ ] Review `MaxBackoffDuration` — default changed from 1m to 10m
- [ ] Review `ExponentialBackoffFunc` — now lazy, remove explicit initialization if using defaults
- [ ] Run tests to verify compatibility
