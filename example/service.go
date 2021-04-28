package main

import "fmt"

type UserEvent struct {
	Content string
}

type Service interface {
	OnUserEvent(UserEvent) error
}

func NewService() Service {
	return service{}
}

type service struct{}

func (s service) OnUserEvent(msg UserEvent) error {
	fmt.Println("received this message", msg)
	return nil
}
