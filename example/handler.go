package main

import (
	"context"
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/ricardo-ch/go-kafka/v2"
)

func makeUserHandler(s Service) kafka.Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		parsedMsg, err := decodeUserEvent(msg.Value)
		if err != nil {
			return err
		}

		return s.OnUserEvent(parsedMsg)
	}
}

func decodeUserEvent(data []byte) (UserEvent, error) {
	parsedMsg := UserEvent{}
	err := json.Unmarshal(data, &parsedMsg)
	return parsedMsg, err
}
