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

// setupConsumerGroupClaimMock configures the ConsumerGroupClaim mock with default values for logging
func setupConsumerGroupClaimMock(claim *mocks.ConsumerGroupClaim, topic string, partition int32, msgChan <-chan *sarama.ConsumerMessage) {
	claim.On("Messages").Return(msgChan)
	claim.On("Topic").Return(topic)
	claim.On("Partition").Return(partition)
	claim.On("InitialOffset").Return(int64(0))
}

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

	// Sarama v1.46 handshake & requests
	md := sarama.NewMockMetadataResponse(t).
		SetBroker(leaderBroker.Addr(), leaderBroker.BrokerID()).
		SetLeader("retry-topic", 0, leaderBroker.BrokerID())
	fc := sarama.NewMockFindCoordinatorResponse(t).
		SetCoordinator(sarama.CoordinatorGroup, "groupID", leaderBroker)
	leaderBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest":     sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest":        md,
		"FindCoordinatorRequest": fc,
	})

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

	// Sarama v1.46 handshake & requests
	md := sarama.NewMockMetadataResponse(t).
		SetBroker(leaderBroker.Addr(), leaderBroker.BrokerID()).
		SetLeader("deadletter-topic", 0, leaderBroker.BrokerID())
	fc := sarama.NewMockFindCoordinatorResponse(t).
		SetCoordinator(sarama.CoordinatorGroup, "groupID", leaderBroker)
	leaderBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest":     sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest":        md,
		"FindCoordinatorRequest": fc,
	})

	Brokers = []string{leaderBroker.Addr()}

	handlers := map[string]Handler{"deadletter-topic": testHandlerWithConfig}

	// Act
	l, err := NewListener("groupID", handlers)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, l)
}

func Test_NewListener_Happy_Path(t *testing.T) {
	resetClient()
	leaderBroker := sarama.NewMockBroker(t, 1)

	// Sarama v1.46 handshake & requests
	md := sarama.NewMockMetadataResponse(t).
		SetBroker(leaderBroker.Addr(), leaderBroker.BrokerID()).
		SetLeader("topic-test", 0, leaderBroker.BrokerID())
	fc := sarama.NewMockFindCoordinatorResponse(t).
		SetCoordinator(sarama.CoordinatorGroup, "groupID", leaderBroker)
	leaderBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest":     sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest":        md,
		"FindCoordinatorRequest": fc,
	})

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
	setupConsumerGroupClaimMock(consumerGroupClaim, "topic-test", 0, (<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	handlerCalled := false
	var headerVal []byte
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		for _, h := range msg.Headers {
			if string(h.Key) == "user-id" {
				headerVal = h.Value
			}
		}
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
	assert.Equal(t, string(headerVal), "123456")
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
	setupConsumerGroupClaimMock(consumerGroupClaim, "topic-test", 0, (<-chan *sarama.ConsumerMessage)(msgChanel))

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

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	// Note: Error logging is now handled by the structured logger (DefaultStructuredLogger)
	// and can be seen in the test output
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
	setupConsumerGroupClaimMock(consumerGroupClaim, "topic-test", 0, (<-chan *sarama.ConsumerMessage)(msgChanel))

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

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	// Note: Error logging is now handled by the structured logger (DefaultStructuredLogger)
	// and can be seen in the test output
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
	setupConsumerGroupClaimMock(consumerGroupClaim, "topic-test", 0, (<-chan *sarama.ConsumerMessage)(msgChanel))

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

	tested := listener{
		handlers:           map[string]Handler{"topic-test": handler},
		deadletterProducer: producer,
	}

	// Act
	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	// Assert
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	// Note: Error logging is now handled by the structured logger (DefaultStructuredLogger)
	// and can be seen in the test output
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
	setupConsumerGroupClaimMock(consumerGroupClaim, "topic-test", 0, (<-chan *sarama.ConsumerMessage)(msgChanel))

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
	omittedError := errors.New("This error should be omitted")

	l := listener{}

	// This test verifies that omitted errors are handled correctly.
	// The structured logger will output a WARN level message "message omitted by handler"
	// which can be seen in the test output.
	l.handleErrorMessage(fmt.Errorf("%w: %w", omittedError, ErrEventOmitted), Handler{}, nil)

	// If we reach here without panic, the error was handled correctly
}

func Test_handleMessageWithRetry(t *testing.T) {
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
		return fmt.Errorf("%w: %w", err, ErrEventUnretriable)
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

// customUnretriableError is a custom error type that implements UnretriableError interface
type customUnretriableError struct {
	message string
}

func (e customUnretriableError) Error() string       { return e.message }
func (e customUnretriableError) IsUnretriable() bool { return true }

// customOmittedError is a custom error type that implements OmittedError interface
type customOmittedError struct {
	message string
}

func (e customOmittedError) Error() string   { return e.message }
func (e customOmittedError) IsOmitted() bool { return true }

func Test_handleMessageWithRetry_CustomUnretriableError(t *testing.T) {
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++
		return customUnretriableError{message: "custom business error - do not retry"}
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, 3, 0, false)

	// Should only be called once because custom error implements UnretriableError
	assert.Equal(t, 1, handlerCalled)
}

func Test_handleErrorMessage_CustomOmittedError(t *testing.T) {
	l := listener{}

	// This test verifies that custom omitted errors are handled correctly.
	// The structured logger will output a WARN level message "message omitted by handler"
	l.handleErrorMessage(customOmittedError{message: "custom omitted error"}, Handler{}, nil)

	// If we reach here without panic, the custom error was handled correctly
}

func Test_NewUnretriableError(t *testing.T) {
	t.Run("wraps error correctly", func(t *testing.T) {
		originalErr := errors.New("original error")
		wrappedErr := NewUnretriableError(originalErr)

		assert.NotNil(t, wrappedErr)
		assert.Equal(t, "original error", wrappedErr.Error())
		assert.True(t, errors.Is(wrappedErr, originalErr))
	})

	t.Run("returns nil for nil error", func(t *testing.T) {
		wrappedErr := NewUnretriableError(nil)
		assert.Nil(t, wrappedErr)
	})

	t.Run("is detected as unretriable", func(t *testing.T) {
		err := NewUnretriableError(errors.New("some error"))
		var ue UnretriableError
		assert.True(t, errors.As(err, &ue))
		assert.True(t, ue.IsUnretriable())
	})
}

func Test_NewOmittedError(t *testing.T) {
	t.Run("wraps error correctly", func(t *testing.T) {
		originalErr := errors.New("original error")
		wrappedErr := NewOmittedError(originalErr)

		assert.NotNil(t, wrappedErr)
		assert.Equal(t, "original error", wrappedErr.Error())
		assert.True(t, errors.Is(wrappedErr, originalErr))
	})

	t.Run("returns nil for nil error", func(t *testing.T) {
		wrappedErr := NewOmittedError(nil)
		assert.Nil(t, wrappedErr)
	})

	t.Run("is detected as omitted", func(t *testing.T) {
		err := NewOmittedError(errors.New("some error"))
		var oe OmittedError
		assert.True(t, errors.As(err, &oe))
		assert.True(t, oe.IsOmitted())
	})
}

func Test_handleMessageWithRetry_NewUnretriableError(t *testing.T) {
	handlerCalled := 0
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled++
		return NewUnretriableError(errors.New("validation failed"))
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	l := listener{}
	l.handleMessageWithRetry(context.Background(), handler, nil, 3, 0, false)

	// Should only be called once because error is wrapped with NewUnretriableError
	assert.Equal(t, 1, handlerCalled)
}

func Test_handleErrorMessage_NewOmittedError(t *testing.T) {
	l := listener{}

	// This test verifies that NewOmittedError wrapped errors are handled correctly
	l.handleErrorMessage(NewOmittedError(errors.New("outdated event")), Handler{}, nil)

	// If we reach here without panic, the error was handled correctly
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

func Test_getBackoffDuration(t *testing.T) {
	// getBackoffDuration uses sarama.NewExponentialBackoff which implements KIP-580 with jitter
	// So we test that the backoff is within a reasonable range

	// Test with handler's custom BackoffFunc
	t.Run("custom backoff func", func(t *testing.T) {
		customBackoff := func(retries, maxRetries int) time.Duration {
			return time.Duration(retries+1) * time.Second
		}
		handler := Handler{
			Config: HandlerConfig{
				BackoffFunc: customBackoff,
			},
		}

		delay := getBackoffDuration(handler, 2, 5)
		assert.Equal(t, 3*time.Second, delay)
	})

	// Test with global ExponentialBackoffFunc (sarama.NewExponentialBackoff)
	t.Run("global exponential backoff", func(t *testing.T) {
		handler := Handler{
			Config: HandlerConfig{
				BackoffFunc: nil, // Uses global ExponentialBackoffFunc
			},
		}

		// sarama.NewExponentialBackoff includes jitter, so we just verify the backoff is positive
		// and doesn't exceed MaxBackoffDuration
		delay := getBackoffDuration(handler, 0, 5)
		assert.True(t, delay > 0, "backoff should be positive")
		assert.True(t, delay <= MaxBackoffDuration, "backoff should not exceed MaxBackoffDuration")

		// Verify that backoff generally increases with retries (may not always due to jitter)
		delay1 := getBackoffDuration(handler, 1, 5)
		assert.True(t, delay1 > 0, "backoff should be positive")
		assert.True(t, delay1 <= MaxBackoffDuration, "backoff should not exceed MaxBackoffDuration")
	})
}
