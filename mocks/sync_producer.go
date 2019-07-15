// Code generated by mockery v1.0.0. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"
import sarama "github.com/Shopify/sarama"

// SyncProducer is an autogenerated mock type for the SyncProducer type
type SyncProducer struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *SyncProducer) Close() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendMessage provides a mock function with given fields: msg
func (_m *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	ret := _m.Called(msg)

	var r0 int32
	if rf, ok := ret.Get(0).(func(*sarama.ProducerMessage) int32); ok {
		r0 = rf(msg)
	} else {
		r0 = ret.Get(0).(int32)
	}

	var r1 int64
	if rf, ok := ret.Get(1).(func(*sarama.ProducerMessage) int64); ok {
		r1 = rf(msg)
	} else {
		r1 = ret.Get(1).(int64)
	}

	var r2 error
	if rf, ok := ret.Get(2).(func(*sarama.ProducerMessage) error); ok {
		r2 = rf(msg)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// SendMessages provides a mock function with given fields: msgs
func (_m *SyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	ret := _m.Called(msgs)

	var r0 error
	if rf, ok := ret.Get(0).(func([]*sarama.ProducerMessage) error); ok {
		r0 = rf(msgs)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
