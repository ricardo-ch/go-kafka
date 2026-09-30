package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"time"

	"github.com/ricardo-ch/go-kafka/v4"
)

var (
	brokers = []string{"localhost:9092"}
	appName = "example-kafka-retry-cancel"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlers := kafka.Handlers{}
	handlers["test-users"] = makeUserHandler(NewService(cancel))
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

	err = listener.Listen(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Println("listener closed with error:", err)
		return
	}
	log.Println("listener stopped")
}

type ctxLogKey struct{}

func ToContext(ctx context.Context, logger *slog.Logger) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}

	return context.WithValue(ctx, ctxLogKey{}, logger)
}

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
