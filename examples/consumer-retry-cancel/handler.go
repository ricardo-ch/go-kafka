package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/ricardo-ch/go-kafka/v4"
)

func makeUserHandler(s Service) kafka.Handler {
	return kafka.Handler{
		Processor: func(ctx context.Context, msg *sarama.ConsumerMessage) error {
			parsedMsg, err := decodeUserEvent(msg.Value)
			if err != nil {
				return err
			}

			return s.OnUserEvent(ctx, parsedMsg)
		},
		Config: kafka.HandlerConfig{
			ConsumerMaxRetries:  new(2),
			DurationBeforeRetry: new(5 * time.Second),
		},
	}
}

func decodeUserEvent(data []byte) (UserEvent, error) {
	parsedMsg := UserEvent{}
	err := json.Unmarshal(data, &parsedMsg)
	return parsedMsg, err
}
