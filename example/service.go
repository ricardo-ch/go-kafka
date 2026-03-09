package main

import (
	"context"

	"github.com/ricardo-ch/go-utils/v3/slogr"
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
	slogr.From(ctx).Info("received user event")
	return nil
}
