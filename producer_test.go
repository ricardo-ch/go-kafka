package kafka

import (
	"errors"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/ricardo-ch/go-kafka/v2/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_Producer_SyncProducer_Error(t *testing.T) {
	mockProducer := &mocks.SyncProducer{}
	mockProducer.On("SendMessage", mock.Anything).Return(int32(0), int64(0), errors.New("error"))

	p := &producer{
		producer: mockProducer,
		handler:  produce,
	}

	err := p.Produce(&sarama.ProducerMessage{})
	assert.NotNil(t, err)
	mockProducer.AssertExpectations(t)
}

func Test_Producer_SyncProducer_OK(t *testing.T) {
	mockProducer := &mocks.SyncProducer{}
	mockProducer.On("SendMessage", mock.Anything).Return(int32(0), int64(0), nil)

	p := &producer{
		producer: mockProducer,
		handler:  produce,
	}

	err := p.Produce(&sarama.ProducerMessage{})
	assert.Nil(t, err)
	mockProducer.AssertExpectations(t)
}
