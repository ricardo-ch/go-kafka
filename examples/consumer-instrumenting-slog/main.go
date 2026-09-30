package main

import (
	"context"
	"log"
	"log/slog"
	"time"

	"github.com/ricardo-ch/go-kafka/v4"
)

var (
	brokers = []string{"localhost:9092"}
	appName = "example-kafka"
)

func main() {

	handlers := kafka.Handlers{}
	handlers["test-users"] = makeUserHandler(NewService())
	kafka.Brokers = brokers
	slog.Info("starting listener")

	listener, err := kafka.NewListener(appName, handlers,
		kafka.WithLogContextStorer(ToContext),
		kafka.WithInstrumenting(),
	)
	if err != nil {
		log.Fatalln("could not initialise listener:", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := listener.Shutdown(shutdownCtx); err != nil {
			slog.Warn("listener shutdown", "error", err)
		}
	}()

	err = listener.Listen(context.Background())
	if err != nil {
		log.Println("listener closed with error:", err)
		return
	}
	log.Println("listener stopped")
}

// Logger context helpers
// These helpers are used to store and retrieve the logger from the context.
// Logger context key
type ctxLogKey struct{}

// ToContext stores the logger in the context.
// If ctx is nil, context.Background() is used.
// If logger is nil, slog.Default() is stored.
func ToContext(ctx context.Context, logger *slog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}

	return context.WithValue(ctx, ctxLogKey{}, logger)
}

// From retrieves a logger from context.
// If ctx is nil, missing a logger, or contains an unexpected type,
// slog.Default() is returned.
func From(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return slog.Default()
	}

	logger, ok := ctx.Value(ctxLogKey{}).(*slog.Logger)
	if !ok || logger == nil {
		return slog.Default()
	}

	return logger
}
