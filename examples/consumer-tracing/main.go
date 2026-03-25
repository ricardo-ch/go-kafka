package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ricardo-ch/go-kafka/v4"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

var (
	brokers = []string{"localhost:9092"}
	appName = "example-kafka"
)

func main() {
	rootCtx := context.Background()
	// Tracing: default exporter is stdout (pretty JSON) — no collector needed.
	// Set EXAMPLE_TRACE_EXPORTER=otlp to send spans via OTLP gRPC (e.g. OTEL_EXPORTER_OTLP_ENDPOINT).
	tp, err := InitTracing(rootCtx, appName)
	if err != nil {
		log.Fatalln("could not initialise tracing:", err)
	}
	if tp != nil {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if shutdownErr := tp.Shutdown(shutdownCtx); shutdownErr != nil {
				slog.Warn("tracer provider shutdown", "error", shutdownErr)
			}
		}()
	}

	handlers := kafka.Handlers{}
	handlers["test-users"] = makeUserHandler(NewService())
	kafka.Brokers = brokers
	slog.Info("starting listener")

	listener, err := kafka.NewListener(appName, handlers,
		kafka.WithTracing(kafka.DefaultTracing),
	)
	if err != nil {
		log.Fatalln("could not initialise listener:", err)
	}
	defer listener.Close()

	err = listener.Listen(context.Background())
	if err != nil {
		log.Fatalln("listener closed with error:", err)
	}
	log.Println("listener stopped")
}

// InitTracing initialises the global TracerProvider.
//
// By default uses the stdout trace exporter (JSON, pretty-printed) so you can verify spans locally
// without an OTLP collector. See:
// https://pkg.go.dev/go.opentelemetry.io/otel/exporters/stdout/stdouttrace
//
// Set EXAMPLE_TRACE_EXPORTER=otlp to use OTLP gRPC instead (configure OTEL_EXPORTER_OTLP_ENDPOINT, etc.).
func InitTracing(ctx context.Context, appName string) (*sdktrace.TracerProvider, error) {
	var spanExporter sdktrace.SpanExporter
	var err error

	switch strings.ToLower(strings.TrimSpace(os.Getenv("EXAMPLE_TRACE_EXPORTER"))) {
	case "otlp":
		spanExporter, err = otlptracegrpc.New(ctx, otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("creating OTLP trace exporter: %w", err)
		}
	default:
		spanExporter, err = stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("creating stdout trace exporter: %w", err)
		}
	}

	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceName(appName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("merging resources: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExporter),
		sdktrace.WithResource(r),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}
