package main

import (
	"context"
	"fmt"

	"github.com/ricardo-ch/go-kafka/v3"
)

var (
	brokers = []string{"localhost:9092"}
	appName = "example-kafka"
)

func main() {

	handlers := kafka.Handlers{}
	handlers["test-users"] = makeUserHandler(NewService())
	kafka.Brokers = brokers
	fmt.Printf("starting listener")

	// Disable forwarding to retry and deadletter topics
	// You have to put before creating the listener
	kafka.PushConsumerErrorsToDeadletterTopic = false
	kafka.PushConsumerErrorsToRetryTopic = false

	listener, err := kafka.NewListener(appName, handlers)

	if err != nil {
		fmt.Printf("could not initialise listener: %v\n", err)
	}
	defer listener.Close()

	err = listener.Listen(context.Background())
	if err != nil {
		fmt.Printf("listener closed with error: %v\n", err)
	}
	fmt.Printf("listener stopped")
}
