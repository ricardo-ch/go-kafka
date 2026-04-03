## How to run these examples

These examples use the same basic consumer setup but highlight different features:

- `consumer-tracing` shows OpenTelemetry tracing with `WithTracing(kafka.DefaultTracing)`.
- `consumer-instrumenting-slog` shows Prometheus metrics and contextual `slog` logging with `WithInstrumenting()` and `WithLogContextStorer(...)`.
- `consumer-noretry-nodeadletter` shows the minimal setup with retries and dead-letter forwarding disabled.
- `consumer-retry-cancel` shows a retriable handler error followed by a context cancellation during the library retry backoff.

From the **`examples/`** directory:

```bash
docker-compose up -d
```

Wait until Kafka is healthy, then go to the directory containing the desired example and run `go run .` in another terminal.

To send a test message, run `make send_msg`.

### Test messages and flows

The example `Makefile` includes a few test messages to exercise different flows:

- `make send_msg` sends `{"Content":"hello"}` and exercises the success path.
- `make send_msg_test_retry` sends `{"Content":"retry"}` and exercises the retriable error flow.
- `make send_msg_test_omit` sends `{"Content":"omit"}` and exercises the omitted error flow.
- `make send_msg_test_deadletter` sends `{"Content":"deadletter"}` and exercises the unretriable/dead-letter flow.
- `make send_msg_test_context_canceled` sends `{"Content":"context_canceled"}` and exercises a direct handler context-canceled flow. The message is not committed.
- `make send_msg_test_retry_cancel` sends `{"Content":"retry-cancel"}` and cancels the listener context during the library retry backoff. The message is not committed.

What you observe depends on the example you run:

- In `consumer-tracing`, the focus is tracing. `retry` triggers retries, `deadletter` goes directly to dead-letter, and `omit` is treated like a normal success because this example does not return an omitted error for that payload.
- In `consumer-instrumenting-slog`, all four flows are implemented so you can see the effect in logs and metrics.
- In `consumer-noretry-nodeadletter`, handler errors are still produced, but forwarding to retry and dead-letter topics is disabled globally.
- In `consumer-retry-cancel`, the first handler failure triggers the library retry wait, then the example cancels the listener context before the retry fires so you can verify the offset is not committed.


### OpenTelemetry tracing

By default, spans are exported to **stdout** as pretty-printed JSON ([`stdouttrace`](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/stdout/stdouttrace)), so you can confirm tracing without an OTLP collector.

To use **OTLP gRPC** instead, set:

```bash
export EXAMPLE_TRACE_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317   # your collector
go run .
```
