# AGENTS.md — go-kafka

## Domain

- Internal Go library for Kafka consumer/producer workflows on top of `github.com/IBM/sarama`
- Focus: listener abstraction, retry/deadletter forwarding, Prometheus metrics, OpenTelemetry tracing, `slog` context enrichment
- Module: `github.com/ricardo-ch/go-kafka/v4`

## HTTP API Surface

- None in this repository
- Examples may expose `/metrics` in example apps; the library itself does not ship an HTTP server

## Asynchronous Operations

- `Listener.Listen(ctx)` — consumer-group loop for one or more Kafka topics
- Handler retry loop — blocking retry with per-handler/global backoff configuration
- Retry-topic forwarding — publishes failed retriable messages to retry topics
- Deadletter forwarding — publishes unretriable or exhausted messages to deadletter topics
- Consumer error monitor — background goroutine logging `consumerGroup.Errors()`

## Database Business Objects

- None
- This repository is a library and does not persist business entities

## External Service Dependencies

- Kafka via `github.com/IBM/sarama` — core consumer group and sync producer client
- Prometheus via `github.com/prometheus/client_golang` — consumer and producer metrics
- OpenTelemetry via `go.opentelemetry.io/otel` — trace extraction/injection for Kafka headers

## Internal Tooling & Libraries

- No organization-specific runtime libraries such as `go-utils` or `go-clients`
- `github.com/stretchr/testify` — test assertions
- `mockery` — regenerates Sarama mocks into `./mocks`

## Repository Structure

- This repo does not follow the standard Go service layout (`api/`, `service/`, `store/`, `worker/`); it is a flat library package with examples
- `go-kafka.go` — package globals and Sarama default config
- `client.go` — shared Sarama client initialization
- `listener.go` — consumer listener, handler config, retry/forward flow
- `producer.go` — sync producer wrapper
- `options.go` — listener and producer options
- `instrumenting.go` — consumer Prometheus metrics
- `producer_instrumenting.go` — producer and deadletter metrics
- `tracing.go` — OpenTelemetry helpers for Kafka headers and spans
- `logger.go` — `slog` context enrichment helpers
- `errors.go` — retriable/unretriable/omitted error helpers and sentinels
- `murmur.go` — JVM-compatible Murmur2 partitioner
- `mocks/` — generated Sarama mocks
- `examples/` — runnable example consumers and local Docker Compose setup
- `MIGRATION.md` — v3 to v4 migration notes
- `README.md` — public usage guide
- `Makefile` — build, test, mock generation targets

## Build & Test

- `make build` — build the library
- `make test` — run `go test -race -v ./...`
- `make mocks` — regenerate mocks from vendored Sarama interfaces
