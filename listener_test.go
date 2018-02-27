package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockConsumer struct {
	Consumer
	mock.Mock
}

func (c *mockConsumer) Messages() <-chan *sarama.ConsumerMessage {
	args := c.Called()
	return args.Get(0).(chan *sarama.ConsumerMessage)
}
func (c *mockConsumer) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	c.Called()
}
func (c *mockConsumer) Notifications() <-chan *cluster.Notification {
	args := c.Called()
	return args.Get(0).(chan *cluster.Notification)
}
func (c *mockConsumer) Errors() <-chan error {
	args := c.Called()
	return args.Get(0).(chan error)
}
func (c *mockConsumer) CommitOffsets() error {
	args := c.Called()
	return args.Error(0)
}
func (c *mockConsumer) Close() (err error) {
	args := c.Called()
	return args.Error(0)
}

type mockProducer struct {
	mock.Mock
}

func (m *mockProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockProducer) SendMessage(key []byte, msg []byte, topic string) (partition int32, offset int64, err error) {
	args := m.Called(key, msg, topic)
	return args.Get(0).(int32), args.Get(1).(int64), args.Error(2)
}

type mockLogger struct {
	mock.Mock
}

func (m *mockLogger) Print(v ...interface{}) {
	m.Called(v)
}

func (m *mockLogger) Printf(format string, v ...interface{}) {
	m.Called(format, v)
}

func (m *mockLogger) Println(v ...interface{}) {
	m.Called(v)
}

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

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leaderBroker.Addr(), leaderBroker.BrokerID())
	metadataResponse.AddTopicPartition("topic-test", 0, leaderBroker.BrokerID(), nil, nil, sarama.ErrNoError)
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
	timeout := make(chan interface{})
	go func() {
		time.Sleep(10 * time.Second)
		close(timeout)
	}()

	msgChanel := make(chan *sarama.ConsumerMessage)
	errChanel := make(chan error)
	notifChanel := make(chan *cluster.Notification)

	handlerCalled := make(chan interface{}, 1)
	offsetMarked := make(chan interface{}, 1)

	mockConsumer := &mockConsumer{}
	mockConsumer.On("Messages").Return(msgChanel)
	mockConsumer.On("Errors").Return(errChanel)
	mockConsumer.On("MarkOffset").Return().Run(func(mock.Arguments) {
		offsetMarked <- true
	})
	mockConsumer.On("CommitOffsets").Return(nil)
	mockConsumer.On("Notifications").Return(notifChanel)

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled <- true
		return nil
	}

	tested := listener{
		consumer: mockConsumer,
		handlers: map[string]Handler{"topic-test": handler},
	}

	go tested.Listen(context.Background())
	go func() {
		msgChanel <- &sarama.ConsumerMessage{
			Topic: "topic-test",
		}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-timeout:
			assert.Fail(t, "timeout waiting for consumer to process message")
		case <-handlerCalled:
		case <-offsetMarked:
		}
	}
}

func Test_Listen_Message_Error_NoTopic(t *testing.T) {
	timeout := make(chan interface{})
	go func() {
		time.Sleep(10 * time.Second)
		close(timeout)
	}()
	msgChanel := make(chan *sarama.ConsumerMessage)
	errChanel := make(chan error)
	notifChanel := make(chan *cluster.Notification)
	errorLogged := make(chan interface{}, 1)
	offsetMarked := make(chan interface{}, 1)

	mockLogger := &mockLogger{}
	mockLogger.On("Printf", mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged <- true
	})
	ErrorLogger = mockLogger
	PushConsumerErrorsToTopic = false
	DurationBeforeRetry = 2 * time.Millisecond

	mockConsumer := &mockConsumer{}
	mockConsumer.On("Messages").Return(msgChanel)
	mockConsumer.On("Errors").Return(errChanel)
	mockConsumer.On("MarkOffset").Return().Run(func(mock.Arguments) {
		offsetMarked <- true
	})
	mockConsumer.On("CommitOffsets").Return(nil)
	mockConsumer.On("Notifications").Return(notifChanel)

	mockProducer := &mockProducer{}

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		return fmt.Errorf("I want an error to be logged")
	}

	tested := listener{
		consumer: mockConsumer,
		producer: mockProducer,
		handlers: map[string]Handler{"topic-test": handler},
	}

	go tested.Listen(context.Background())
	go func() {
		msgChanel <- &sarama.ConsumerMessage{
			Topic: "topic-test",
		}
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-timeout:
			assert.Fail(t, "timeout waiting for consumer to process message")
		case <-errorLogged:
		case <-offsetMarked:
		}
	}

	mockProducer.AssertNotCalled(t, "SendMessage", mock.Anything, mock.Anything, mock.Anything)
}

func Test_Listen_Message_Error_WithErrorTopic(t *testing.T) {
	timeout := make(chan interface{})
	go func() {
		time.Sleep(10 * time.Second)
		close(timeout)
	}()
	msgChanel := make(chan *sarama.ConsumerMessage)
	errChanel := make(chan error)
	notifChanel := make(chan *cluster.Notification)
	errorLogged := make(chan interface{}, 1)
	messageSent := make(chan interface{}, 1)
	offsetMarked := make(chan interface{}, 1)

	mockLogger := &mockLogger{}
	mockLogger.On("Printf", mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged <- true
	})
	ErrorLogger = mockLogger
	PushConsumerErrorsToTopic = true
	DurationBeforeRetry = 2 * time.Millisecond

	mockConsumer := &mockConsumer{}
	mockConsumer.On("Messages").Return(msgChanel)
	mockConsumer.On("Errors").Return(errChanel)
	mockConsumer.On("MarkOffset").Return().Run(func(mock.Arguments) {
		offsetMarked <- true
	})
	mockConsumer.On("CommitOffsets").Return(nil)
	mockConsumer.On("Notifications").Return(notifChanel)

	mockProducer := &mockProducer{}
	var capturedErrorTopic string
	mockProducer.On("SendMessage", mock.Anything, mock.Anything, mock.Anything).Return(int32(0), int64(0), nil).Run(func(args mock.Arguments) {
		capturedErrorTopic = args.Get(2).(string)
		messageSent <- true
	})

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		return fmt.Errorf("I want an error to be logged")
	}

	tested := listener{
		groupID:  "group",
		consumer: mockConsumer,
		producer: mockProducer,
		handlers: map[string]Handler{"topic-test": handler},
	}

	go tested.Listen(context.Background())
	go func() {
		msgChanel <- &sarama.ConsumerMessage{
			Topic: "topic-test",
		}
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-timeout:
			assert.Fail(t, "timeout waiting for consumer to process message")
		case <-errorLogged:
		case <-messageSent:
		case <-offsetMarked:
		}
	}

	mockProducer.AssertNumberOfCalls(t, "SendMessage", 1)
	assert.Equal(t, "group-topic-test-error", capturedErrorTopic)
}

func Test_Listen_Message_Error_WithPanicTopic(t *testing.T) {
	timeout := make(chan interface{})
	go func() {
		time.Sleep(10 * time.Second)
		close(timeout)
	}()
	msgChanel := make(chan *sarama.ConsumerMessage)
	errChanel := make(chan error)
	notifChanel := make(chan *cluster.Notification)
	errorLogged := make(chan interface{}, 1)
	messageSent := make(chan interface{}, 1)
	offsetMarked := make(chan interface{}, 1)

	mockLogger := &mockLogger{}
	mockLogger.On("Printf", mock.Anything, mock.Anything).Return().Run(func(args mock.Arguments) {
		printedError := args.Get(1).([]interface{})[0].(error)
		assert.Contains(t, printedError.Error(), "Panic")
		errorLogged <- true
	})
	ErrorLogger = mockLogger
	PushConsumerErrorsToTopic = true
	DurationBeforeRetry = 2 * time.Millisecond

	mockConsumer := &mockConsumer{}
	mockConsumer.On("Messages").Return(msgChanel)
	mockConsumer.On("Errors").Return(errChanel)
	mockConsumer.On("MarkOffset").Return().Run(func(mock.Arguments) {
		offsetMarked <- true
	})
	mockConsumer.On("CommitOffsets").Return(nil)
	mockConsumer.On("Notifications").Return(notifChanel)

	mockProducer := &mockProducer{}
	var capturedErrorTopic string
	mockProducer.On("SendMessage", mock.Anything, mock.Anything, mock.Anything).Return(int32(0), int64(0), nil).Run(func(args mock.Arguments) {
		capturedErrorTopic = args.Get(2).(string)
		messageSent <- true
	})

	handler := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		panic("Panicking, I want to it to be handle as an error")
	}

	tested := listener{
		groupID:  "group",
		consumer: mockConsumer,
		producer: mockProducer,
		handlers: map[string]Handler{"topic-test": handler},
	}

	go tested.Listen(context.Background())
	go func() {
		msgChanel <- &sarama.ConsumerMessage{
			Topic: "topic-test",
		}
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-timeout:
			assert.Fail(t, "timeout waiting for consumer to process message")
		case <-errorLogged:
		case <-messageSent:
		case <-offsetMarked:
		}
	}

	mockProducer.AssertNumberOfCalls(t, "SendMessage", 1)
	assert.Equal(t, "group-topic-test-error", capturedErrorTopic)
}

func Test_Consumer_Context_Cancel_Works(t *testing.T) {
	timeout := time.After(10 * time.Second)

	contextCanceled := make(chan interface{}, 1)

	mockConsumer := &mockConsumer{}
	mockConsumer.On("Messages").Return(make(chan *sarama.ConsumerMessage))
	mockConsumer.On("Errors").Return(make(chan error))
	mockConsumer.On("Notifications").Return(make(chan *cluster.Notification))

	tested := listener{
		consumer: mockConsumer,
		closed:   make(chan interface{}),
	}

	ctx := context.Background()
	ctx, cancelFunc := context.WithCancel(ctx)

	go func() {
		tested.Listen(ctx)
		contextCanceled <- true
	}()

	cancelFunc()

	select {
	case <-timeout:
		assert.Fail(t, "timeout waiting for consummer to cancel itself")
	case <-contextCanceled:
	}
}

func Test_Consumer_Close_Works(t *testing.T) {
	timeout := time.After(10 * time.Second)

	consumerClosed := make(chan interface{}, 1)

	mockConsumer := &mockConsumer{}
	mockConsumer.On("Messages").Return(make(chan *sarama.ConsumerMessage))
	mockConsumer.On("Errors").Return(make(chan error))
	mockConsumer.On("Notifications").Return(make(chan *cluster.Notification))
	mockConsumer.On("Close").Return(nil)

	tested := listener{
		consumer: mockConsumer,
		closed:   make(chan interface{}),
	}

	go func() {
		tested.Listen(context.Background())
		consumerClosed <- true
	}()

	tested.Close()

	select {
	case <-timeout:
		assert.Fail(t, "timeout waiting for consumer to close")
	case <-consumerClosed:
	}
}
