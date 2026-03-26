package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/ricardo-ch/go-kafka/v4"
)

type UserEvent struct {
	Content string
}

type Service interface {
	OnUserEvent(context.Context, UserEvent) error
}

func NewService() Service {
	return service{}
}

type service struct{}

func (s service) OnUserEvent(ctx context.Context, msg UserEvent) error {
	fmt.Println("received user event ", msg.Content)

	if msg.Content == "retry" {
		return errors.New("retry")
	}
	if msg.Content == "omit" {
		return kafka.NewOmittedError(errors.New("omit"))
	}
	if msg.Content == "deadletter" {
		return kafka.NewUnretriableError(errors.New("deadletter"))
	}
	return nil
}
