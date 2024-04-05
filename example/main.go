package main

import (
	"context"
	"log"

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

	listener, err := kafka.NewListener(appName, handlers)
	if err != nil {
		log.Fatalln("could not initialise listener:", err)
	}

	err = listener.Listen(context.Background())
	if err != nil {
		log.Fatalln("listener closed with error:", err)
	}
	log.Println("listener stopped")
}
