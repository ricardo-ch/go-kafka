## How to run these examples

From the **`example/`** directory:

```bash
docker-compose up -d
```

Wait until Kafka is healthy, then run go to the directory containing the desired example and execute`go run .` in another terminal.

to send a test message, run `make send_msg`

### OpenTelemetry tracing

By default, spans are exported to **stdout** as pretty-printed JSON ([`stdouttrace`](https://pkg.go.dev/go.opentelemetry.io/otel/exporters/stdout/stdouttrace)), so you can confirm tracing without an OTLP collector.

To use **OTLP gRPC** instead, set:

```bash
export EXAMPLE_TRACE_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317   # your collector
go run .
```
