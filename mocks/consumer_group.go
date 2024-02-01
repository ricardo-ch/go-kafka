// Code generated by mockery v2.37.1. DO NOT EDIT.

package mocks

import (
	context "context"

	sarama "github.com/IBM/sarama"
	mock "github.com/stretchr/testify/mock"
)

// ConsumerGroup is an autogenerated mock type for the ConsumerGroup type
type ConsumerGroup struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *ConsumerGroup) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Consume provides a mock function with given fields: ctx, topics, handler
func (_m *ConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	ret := _m.Called(ctx, topics, handler)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, []string, sarama.ConsumerGroupHandler) error); ok {
		r0 = rf(ctx, topics, handler)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Errors provides a mock function with given fields:
func (_m *ConsumerGroup) Errors() <-chan error {
	ret := _m.Called()

	var r0 <-chan error
	if rf, ok := ret.Get(0).(func() <-chan error); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(<-chan error)
		}
	}

	return r0
}

// Pause provides a mock function with given fields: partitions
func (_m *ConsumerGroup) Pause(partitions map[string][]int32) {
	_m.Called(partitions)
}

// PauseAll provides a mock function with given fields:
func (_m *ConsumerGroup) PauseAll() {
	_m.Called()
}

// Resume provides a mock function with given fields: partitions
func (_m *ConsumerGroup) Resume(partitions map[string][]int32) {
	_m.Called(partitions)
}

// ResumeAll provides a mock function with given fields:
func (_m *ConsumerGroup) ResumeAll() {
	_m.Called()
}

// NewConsumerGroup creates a new instance of ConsumerGroup. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewConsumerGroup(t interface {
	mock.TestingT
	Cleanup(func())
}) *ConsumerGroup {
	mock := &ConsumerGroup{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
