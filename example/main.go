package main

import (
	"context"
	"github.com/ricardo-ch/go-kafka"
	"log"
)

var (
	brokers = []string{"localhost:9092"}
	appName = "example-kafka"
)

func main() {
	handlers := kafka.Handlers{}
	handlers["test-users"] = makeUserHandler(NewService())
	listener, err := kafka.NewListener(brokers, appName, handlers)
	if err != nil {
		log.Fatalln("could not initialise listener:", err)
	}

	err = listener.Listen(context.Background())
	if err != nil {
		log.Fatalln("listener closed with error:", err)
	}
	log.Println("listener stopped")
}
