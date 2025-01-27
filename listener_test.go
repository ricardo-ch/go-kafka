package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"errors"

	"github.com/IBM/sarama"
	"github.com/ricardo-ch/go-kafka/v3/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var (
	testHandler = Handler{
		Processor: func(ctx context.Context, msg *sarama.ConsumerMessage) error { return nil },
	}
	testHandlerConfig = HandlerConfig{
		ConsumerMaxRetries:  Ptr(10),
		DurationBeforeRetry: Ptr(1 * time.Millisecond),
		RetryTopic:          "retry-topic",
		DeadletterTopic:     "deadletter-topic",
	}
	testHandlerWithConfig = Handler{
		Processor: func(ctx context.Context, msg *sarama.ConsumerMessage) error { return nil },
		Config:    testHandlerConfig,
	}
)

func Test_NewListener_Should_Return_Error_When_No_Broker_Provided(t *testing.T) {
	// Arrange
	handlers := map[string]Handler{"topic": testHandler}
	groupID := "groupID"
	Brokers = []string{}

	// Act
	l, err := NewListener(groupID, handlers)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Should_Return_Error_When_No_GroupID_Provided(t *testing.T) {
	// Arrange
	handlers := map[string]Handler{"topic": testHandler}
	groupID := ""
	Brokers = []string{"localhost:9092"}

	// Act
	l, err := NewListener(groupID, handlers)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Should_Return_Error_When_No_Handlers_Provided(t *testing.T) {
	// Arrange
	handlers := map[string]Handler{}
	groupID := "groupID"
	Brokers = []string{"localhost:9092"}

	// Act
	l, err := NewListener(groupID, handlers)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Should_Return_Error_When_Initial_Topic_Equals_Retry_Topic(t *testing.T) {
	// Arrange
	leaderBroker := sarama.NewMockBroker(t, 1)

	metadataResponse := &sarama.MetadataResponse{
		Version: 5,
	}
	metadataResponse.AddBroker(leaderBroker.Addr(), leaderBroker.BrokerID())
	metadataResponse.AddTopicPartition("retry-topic", 0, leaderBroker.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leaderBroker.Returns(metadataResponse)

	consumerMetadataResponse := sarama.ConsumerMetadataResponse{
		CoordinatorID:   leaderBroker.BrokerID(),
		CoordinatorHost: leaderBroker.Addr(),
		CoordinatorPort: leaderBroker.Port(),
		Err:             sarama.ErrNoError,
	}
	leaderBroker.Returns(&consumerMetadataResponse)

	Brokers = []string{leaderBroker.Addr()}

	handlers := map[string]Handler{"retry-topic": testHandlerWithConfig}

	// Act
	l, err := NewListener("groupID", handlers)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Should_Return_Error_When_Initial_Topic_Equals_Deadletter_Topic(t *testing.T) {
	// Arrange
	leaderBroker := sarama.NewMockBroker(t, 1)

	metadataResponse := &sarama.MetadataResponse{
		Version: 5,
	}
	metadataResponse.AddBroker(leaderBroker.Addr(), leaderBroker.BrokerID())
	metadataResponse.AddTopicPartition("deadletter-topic", 0, leaderBroker.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leaderBroker.Returns(metadataResponse)

	consumerMetadataResponse := sarama.ConsumerMetadataResponse{
		CoordinatorID:   leaderBroker.BrokerID(),
		CoordinatorHost: leaderBroker.Addr(),
		CoordinatorPort: leaderBroker.Port(),
		Err:             sarama.ErrNoError,
	}
	leaderBroker.Returns(&consumerMetadataResponse)

	Brokers = []string{leaderBroker.Addr()}

	handlers := map[string]Handler{"deadletter-topic": testHandlerWithConfig}

	// Act
	l, err := NewListener("groupID", handlers)

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

	Brokers = []string{leaderBroker.Addr()}

	handlers := map[string]Handler{"topic-test": testHandler}
	listener, err := NewListener("groupID", handlers)
	assert.NotNil(t, listener)
	assert.Nil(t, err)
}

func Test_ConsumeClaim_Happy_Path(t *testing.T) {
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic:   "topic-test",
		Headers: []*sarama.RecordHeader{{Key: []byte("user-id"), Value: []byte("123456")}},
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	handlerCalled := false
	var headerVal interface{}
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		headerVal = ctx.Value(listenerContextKey("user-id"))
		handlerCalled = true
		return nil
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	tested := listener{
		handlers: map[string]Handler{"topic-test": handler},
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.Equal(t, string(headerVal.([]byte)), "123456")
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
}

func Test_ConsumeClaim_Message_Error_WithErrorTopic(t *testing.T) {
	// Reduce the retry interval to speed up the test
	DurationBeforeRetry = 1 * time.Millisecond

	PushConsumerErrorsToDeadletterTopic = true

	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	producer := &mocks.MockProducer{}
	producer.On("Produce", mock.Anything).Return(nil)

	handlerCalled := false
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		return fmt.Errorf("I want an error to be logged")
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	defaultLogger := ErrorLogger
	defer func() { ErrorLogger = defaultLogger }()

	errorLogged := false
	mockLogger := &mocks.StdLogger{}
	mockLogger.On("Println", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged = true
	})
	ErrorLogger = mockLogger

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.True(t, errorLogged)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
	producer.AssertExpectations(t)
}

func Test_ConsumeClaim_Message_Error_WithPanicTopic(t *testing.T) {
	PushConsumerErrorsToDeadletterTopic = true

	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	producer := &mocks.MockProducer{}
	producer.On("Produce", mock.Anything).Return(nil)

	handlerCalled := false
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		panic("I want an error to be logged")
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	defaultLogger := ErrorLogger
	defer func() { ErrorLogger = defaultLogger }()

	errorLogged := false
	mockLogger := &mocks.StdLogger{}
	mockLogger.On("Println", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged = true
	})
	ErrorLogger = mockLogger

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.True(t, errorLogged)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
	producer.AssertExpectations(t)
}

func Test_ConsumeClaim_Message_Error_WithHandlerSpecificRetryTopic(t *testing.T) {
	PushConsumerErrorsToRetryTopic = false // global value that is overwritten for the handler in this test

	// Arrange
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	producer := &mocks.MockProducer{}
	producer.On("Produce", mock.Anything).Return(nil)

	handlerCalled := false
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		panic("I want an error to be logged")
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config: HandlerConfig{
			ConsumerMaxRetries:  Ptr(3),
			DurationBeforeRetry: Ptr(1 * time.Millisecond),
			RetryTopic:          "retry-topic", // Here is the important part
		},
	}

	defaultLogger := ErrorLogger
	defer func() { ErrorLogger = defaultLogger }()

	errorLogged := false
	mockLogger := &mocks.StdLogger{}
	mockLogger.On("Println", mock.Anything, mock.Anything, mock.Anything).Return().Run(func(mock.Arguments) {
		errorLogged = true
	})
	ErrorLogger = mockLogger

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	// Act
	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	// Assert
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	assert.True(t, errorLogged)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
	producer.AssertExpectations(t)
}

func Test_ConsumeClaim_Message_Error_Context_Cancelled_Does_Not_Commit_Offset(t *testing.T) {
	PushConsumerErrorsToRetryTopic = false
	PushConsumerErrorsToDeadletterTopic = false

	// Arrange
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())

	producer := &mocks.MockProducer{}

	handlerCalled := false
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		return context.Canceled
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config: HandlerConfig{
			ConsumerMaxRetries:  Ptr(3),
			DurationBeforeRetry: Ptr(1 * time.Millisecond),
		},
	}

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	// Act
	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	// Assert
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
	producer.AssertExpectations(t)
}

func Test_handleErrorMessage_OmittedError(t *testing.T) {

	omittedError := errors.New("this error should be omitted")

	l := listener{}

	defaultLogger := ErrorLogger
	defer func() { ErrorLogger = defaultLogger }()

	errorLogged := false
	mockLogger := &mocks.StdLogger{}
	mockLogger.On("Println", mock.Anything, "error", "omitted message").Return().Run(func(mock.Arguments) {
		errorLogged = true
	}).Once()
	ErrorLogger = mockLogger

	l.handleErrorMessage(NewEventProcessingError(fmt.Errorf("failed in context blablah. %w", omittedError), false, true), Handler{}, nil)

	assert.True(t, errorLogged)
}

func Test_handleMessageWithRetry(t *testing.T) {
	// Reduce the retry interval to speed up the test
	DurationBeforeRetry = 1 * time.Millisecond

	err := errors.New("This error should be retried")
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++
		return NewEventProcessingError(fmt.Errorf("failed in context blablah. %w", err), false, false)
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, 3, 0, false)

	assert.Equal(t, 4, handlerCalled)
}

func Test_handleMessageWithRetryWithBackoff(t *testing.T) {
	// Reduce the retry interval to speed up the test
	DurationBeforeRetry = 1 * time.Millisecond

	err := errors.New("This error should be retried")
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++
		return err
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, 3, 0, true)

	assert.Equal(t, 4, handlerCalled)
}

func Test_handleMessageWithRetry_UnretriableError(t *testing.T) {
	err := errors.New("This error should not be retried")
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++
		return NewEventProcessingError(fmt.Errorf("failed in context blablah. %w", err), true, false)
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, 3, 0, false)

	assert.Equal(t, 1, handlerCalled)
}

func Test_handleMessageWithRetry_UnretriableErrorWithBackoff(t *testing.T) {
	err := errors.New("This error should not be retried")
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++
		return fmt.Errorf("%w: %w", err, ErrEventUnretriable)
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, 3, 0, true)

	assert.Equal(t, 1, handlerCalled)
}

func Test_handleMessageWithRetry_InfiniteRetries(t *testing.T) {
	// Reduce the retry interval to speed up the test
	DurationBeforeRetry = 1 * time.Millisecond

	err := errors.New("This error should be retried")
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++

		// We simulate an infinite retry by failing 5 times, and then succeeding,
		// which is above the 3 retries normally expected
		if handlerCalled < 5 {
			return err
		}
		return nil
	}

	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, InfiniteRetries, 0, false)

	assert.Equal(t, 5, handlerCalled)

}
func Test_handleMessageWithRetry_InfiniteRetriesWithBackoff(t *testing.T) {
	// Reduce the retry interval to speed up the test
	DurationBeforeRetry = 1 * time.Millisecond

	err := errors.New("This error should be retried")
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++

		// We simulate an infinite retry by failing 5 times, and then succeeding,
		// which is above the 3 retries normally expected
		if handlerCalled < 5 {
			return err
		}
		return nil
	}

	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, InfiniteRetries, 0, true)

	assert.Equal(t, 5, handlerCalled)

}

func Test_handleMessageWithRetry_InfiniteRetriesWithContextCancel(t *testing.T) {
	// Reduce the retry interval to speed up the test
	DurationBeforeRetry = 1 * time.Millisecond
	err := errors.New("This error should be retried")

	handlerCalled := 0
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++

		// We simulate an infinite retry by failing 5 times, and then a context is canceled,
		// which is above the 3 retries normally expected
		if handlerCalled > 4 {
			cancel()
		}
		return err
	}

	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(ctx, handler, nil, InfiniteRetries, 0, false)

	assert.Equal(t, 5, handlerCalled)

}

// Basically a copy paste of the happy path but with tracing
// This test only checks that the tracing is not preventing the consumption
func Test_ConsumerClaim_HappyPath_WithTracing(t *testing.T) {
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	handlerCalled := false
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		return nil
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	tested := listener{
		handlers: map[string]Handler{"topic-test": handler},
		tracer:   DefaultTracing, // this is the important part
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
}

// Test that as long as context is not canceled and not error is returned, `Consume` is called again
// (when rebalance is called, the consumer will be part of next session)
func Test_Listen_Happy_Path(t *testing.T) {
	calledCounter := 0
	consumeCalled := make(chan interface{})
	consumerGroup := &mocks.ConsumerGroup{}

	// Mimic the end of a consumerGroup session by just not blocking
	consumerGroup.On("Consume", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			calledCounter++
			consumeCalled <- true
			if calledCounter >= 2 {
				time.Sleep(1000 * time.Second) // just wait
			}
		}).
		Return(nil).Twice()

	tested := listener{consumerGroup: consumerGroup}

	// Listen() is blocking as long as there is no error or context is not canceled
	go func() {
		tested.Listen(context.Background())
		assert.Fail(t, `We should have blocked on "listen", even if a consumer group session has ended`)
	}()

	// Assert that consume is called twice (2 consumer group sessions are expected)
	<-consumeCalled
	<-consumeCalled

	consumerGroup.AssertExpectations(t)
}

// Test that when the context is canceled, as soon as the consumerGroup's session ends, `Listen` returns
func Test_Listen_ContextCanceled(t *testing.T) {
	consumerGroup := &mocks.ConsumerGroup{}

	consumerGroup.On("Consume", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			ctx := args.Get(0).(context.Context)
			<-ctx.Done()
		}).
		Return(nil)

	tested := listener{consumerGroup: consumerGroup}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tested.Listen(ctx)

	assert.Equal(t, context.Canceled, err)
	consumerGroup.AssertExpectations(t)
}

func Test_calculateExponentialBackoffDuration(t *testing.T) {
	tests := []struct {
		name          string
		retries       int
		baseDuration  *time.Duration
		expectedDelay time.Duration
	}{
		{
			name:          "nil base duration",
			retries:       3,
			baseDuration:  nil,
			expectedDelay: 0,
		},
		{
			name:          "zero retries",
			retries:       0,
			baseDuration:  Ptr(1 * time.Second),
			expectedDelay: 1 * time.Second,
		},
		{
			name:          "one retry",
			retries:       1,
			baseDuration:  Ptr(1 * time.Second),
			expectedDelay: 2 * time.Second,
		},
		{
			name:          "two retries",
			retries:       2,
			baseDuration:  Ptr(1 * time.Second),
			expectedDelay: 4 * time.Second,
		},
		{
			name:          "three retries",
			retries:       3,
			baseDuration:  Ptr(1 * time.Second),
			expectedDelay: 8 * time.Second,
		},
		{
			name:          "three retries with different base duration",
			retries:       3,
			baseDuration:  Ptr(500 * time.Millisecond),
			expectedDelay: 4 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := calculateExponentialBackoffDuration(tt.retries, tt.baseDuration)
			assert.Equal(t, tt.expectedDelay, delay)
		})
	}
}
