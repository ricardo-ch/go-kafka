package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/ricardo-ch/go-kafka/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func Test_NewListener_Should_Return_Error_When_No_Broker_Provided(t *testing.T) {
	// Arrange
	handlers := make(map[string]Handler)
	var f func(context.Context, *sarama.ConsumerMessage) error
	handlers["topic"] = f
	// Act
	l, err := NewListener([]string{}, "groupID", handlers)
	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Should_Return_Error_When_No_GroupID_Provided(t *testing.T) {
	// Arrange
	handlers := make(map[string]Handler)
	var f func(context.Context, *sarama.ConsumerMessage) error
	handlers["topic"] = f
	// Act
	l, err := NewListener([]string{"broker1", "broker2"}, "", handlers)
	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Should_Return_Error_When_No_Handlers_Provided(t *testing.T) {
	// Act
	l, err := NewListener([]string{"broker1", "broker2"}, "groupID", nil)
	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Happy_Path(t *testing.T) {
	leaderBroker := sarama.NewMockBroker(t, 1)

	metadataResponse := &sarama.MetadataResponse{
		Version: 5,
	}
	metadataResponse.AddBroker(leaderBroker.Addr(), leaderBroker.BrokerID())
	metadataResponse.AddTopicPartition("topic-test", 0, leaderBroker.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leaderBroker.Returns(metadataResponse)

	consumerMetadataResponse := sarama.ConsumerMetadataResponse{
		CoordinatorID:   leaderBroker.BrokerID(),
		CoordinatorHost: leaderBroker.Addr(),
		CoordinatorPort: leaderBroker.Port(),
		Err:             sarama.ErrNoError,
	}
	leaderBroker.Returns(&consumerMetadataResponse)

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		return nil
	}

	handlers := map[string]Handler{"topic-test": handler}
	listener, err := NewListener([]string{leaderBroker.Addr()}, "groupID", handlers)
	assert.NotNil(t, listener)
	assert.Nil(t, err)
}

func Test_Listen_Happy_Path(t *testing.T) {
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	handlerCalled := false
	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		return nil
	}

	tested := listener{
		handlers: map[string]Handler{"topic-test": handler},
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
}

func Test_Listen_Message_Error_WithErrorTopic(t *testing.T) {
	PushConsumerErrorsToTopic = true

	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	producer := &mocks.SyncProducer{}
	producer.On("SendMessage", mock.Anything).Return(int32(0), int64(0), nil)

	handlerCalled := false
	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		return fmt.Errorf("I want an error to be logged")
	}

	errorLogged := false
	mockLogger := &mocks.StdLogger{}
	mockLogger.On("Printf", mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged = true
	})
	ErrorLogger = mockLogger

	tested := listener{
		handlers: map[string]Handler{"topic-test": handler},
		producer: producer,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.True(t, errorLogged)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
	producer.AssertExpectations(t)
}

func Test_Listen_Message_Error_WithPanicTopic(t *testing.T) {
	PushConsumerErrorsToTopic = true

	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	producer := &mocks.SyncProducer{}
	producer.On("SendMessage", mock.Anything).Return(int32(0), int64(0), nil)

	handlerCalled := false
	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		panic("I want an error to be logged")
	}

	errorLogged := false
	mockLogger := &mocks.StdLogger{}
	mockLogger.On("Printf", mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged = true
	})
	ErrorLogger = mockLogger

	tested := listener{
		handlers: map[string]Handler{"topic-test": handler},
		producer: producer,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.True(t, errorLogged)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
	producer.AssertExpectations(t)
}
