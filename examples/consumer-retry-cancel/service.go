package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

type UserEvent struct {
	Content string
}

type Service interface {
	OnUserEvent(context.Context, UserEvent) error
}

func NewService(cancel context.CancelFunc) Service {
	return &service{cancel: cancel}
}

type service struct {
	cancel          context.CancelFunc
	retryCancelOnce sync.Once
}

func (s *service) OnUserEvent(ctx context.Context, msg UserEvent) error {
	From(ctx).Info("received user event", "content", msg.Content)

	if msg.Content == "retry-cancel" {
		s.retryCancelOnce.Do(func() {
			go func() {
				time.Sleep(500 * time.Millisecond)
				From(ctx).Info("canceling listener context during library retry backoff")
				if s.cancel != nil {
					s.cancel()
				}
			}()
		})
		return errors.New("retry-cancel")
	}

	return nil
}
