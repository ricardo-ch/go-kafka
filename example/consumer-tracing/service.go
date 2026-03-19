package main

import (
	"context"
	"fmt"
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
	fmt.Println("received user event")
	return nil
}
