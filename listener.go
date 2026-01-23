package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/opentracing/opentracing-go"
)

type HandlerConfig struct {
	ConsumerMaxRetries  *int
	DurationBeforeRetry *time.Duration
	ExponentialBackoff  bool
	// BackoffFunc is the function used to calculate backoff duration when ExponentialBackoff is true.
	// If nil, the global ExponentialBackoffFunc (using sarama.NewExponentialBackoff) will be used.
	// The function signature is: func(retries, maxRetries int) time.Duration
	BackoffFunc    func(retries, maxRetries int) time.Duration
	RetryTopic     string
	DeadletterTopic string
}

// Handler Processor that handle received kafka messages
// Handler Config can be used to override global configuration for a specific handler
type Handler struct {
	Processor func(ctx context.Context, msg *sarama.ConsumerMessage) error
	Config    HandlerConfig
}

// Handlers defines a handler for a given topic
type Handlers map[string]Handler

// listener object represents kafka consumer
// Listener implement both `Listener` interface and `ConsumerGroupHandler` from sarama
type listener struct {
	consumerGroup      sarama.ConsumerGroup
	deadletterProducer Producer
	topics             []string
	handlers           Handlers
	groupID            string
	instrumenting      *ConsumerMetricsService
	tracer             TracingFunc
}

// listenerContextKey defines the key to provide in context
// needs to be define to avoid collision.
// Explanation https://golang.org/pkg/context/#WithValue
type listenerContextKey string

const (
	contextTopicKey     = listenerContextKey("topic")
	contextkeyKey       = listenerContextKey("key")
	contextOffsetKey    = listenerContextKey("offset")
	contextTimestampKey = listenerContextKey("timestamp")
)

// Listener is able to listen multiple topics with one handler by topic
type Listener interface {
	Listen(ctx context.Context) error
	Close()
	GroupID() string
}

// NewListener creates a new instance of Listener
func NewListener(groupID string, handlers Handlers, options ...ListenerOption) (Listener, error) {
	if groupID == "" {
		return nil, errors.New("cannot create new listener, group_id cannot be empty")
	}
	if len(handlers) == 0 {
		return nil, errors.New("cannot create new listener, handlers cannot be empty")
	}

	// Init consumer, consume errors & messages
	var topics []string
	for k := range handlers {
		topics = append(topics, k)
	}
	client, err := getClient()
	if err != nil {
		return nil, err
	}

	producer, err := NewProducer(WithDeadletterProducerInstrumenting())
	if err != nil {
		return nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, *client)
	if err != nil {
		return nil, err
	}

	go func() {
		errConsumer := <-consumerGroup.Errors()
		if errConsumer != nil {
			LogError("sarama consumer error", "error", errConsumer, "consumer_group", groupID)
		}
	}()

	// Fill handler config unset elements with global default values.
	fillHandlerConfigWithDefault(handlers)

	// Log configuration for each topic
	logHandlersConfig(groupID, handlers)

	// Sanity check for error topics, to avoid infinite loop
	err = checkErrorTopicToAvoidInfiniteLoop(handlers)
	if err != nil {
		return nil, err
	}

	l := &listener{
		groupID:            groupID,
		deadletterProducer: producer,
		handlers:           handlers,
		consumerGroup:      consumerGroup,
		topics:             topics,
	}

	// execute all method passed as option
	for _, o := range options {
		o(l)
	}

	return l, nil
}

// GroupID return the groupID of the listener
func (l *listener) GroupID() string {
	return l.groupID
}

func checkErrorTopicToAvoidInfiniteLoop(handlers Handlers) error {
	for topic, handler := range handlers {
		if handler.Config.RetryTopic == topic {
			return fmt.Errorf("retry topic cannot be the same as the original topic: %s", topic)
		}
		if handler.Config.DeadletterTopic == topic {
			return fmt.Errorf("deadletter topic cannot be the same as the original topic: %s", topic)
		}
	}
	return nil
}

func fillHandlerConfigWithDefault(handlers Handlers) {
	for k, h := range handlers {
		if h.Config.ConsumerMaxRetries == nil {
			h.Config.ConsumerMaxRetries = &ConsumerMaxRetries
		}
		if h.Config.DurationBeforeRetry == nil {
			h.Config.DurationBeforeRetry = &DurationBeforeRetry
		}
		handlers[k] = h
	}
}

// logHandlersConfig logs the retry configuration for each topic handler.
func logHandlersConfig(groupID string, handlers Handlers) {
	for topic, handler := range handlers {
		retryMode := "finite"
		maxRetries := *handler.Config.ConsumerMaxRetries
		if maxRetries == InfiniteRetries {
			retryMode = "infinite"
		}

		retryTopic := handler.Config.RetryTopic
		if retryTopic == "" {
			if PushConsumerErrorsToRetryTopic {
				retryTopic = strings.Replace(RetryTopicPattern, "$$CG$$", groupID, 1)
				retryTopic = strings.Replace(retryTopic, "$$T$$", topic, 1)
			} else {
				retryTopic = "disabled"
			}
		}

		deadletterTopic := handler.Config.DeadletterTopic
		if deadletterTopic == "" {
			if PushConsumerErrorsToDeadletterTopic {
				deadletterTopic = strings.Replace(DeadletterTopicPattern, "$$CG$$", groupID, 1)
				deadletterTopic = strings.Replace(deadletterTopic, "$$T$$", topic, 1)
			} else {
				deadletterTopic = "disabled"
			}
		}

		// Build backoff description
		backoffDesc := handler.Config.DurationBeforeRetry.String()
		if handler.Config.ExponentialBackoff {
			backoffDesc = fmt.Sprintf("%s -> %s (exponential)", handler.Config.DurationBeforeRetry, MaxBackoffDuration)
		}

		LogInfo("topic handler configuration",
			"consumer_group", groupID,
			"topic", topic,
			"retry_mode", retryMode,
			"max_retries", maxRetries,
			"backoff", backoffDesc,
			"retry_topic", retryTopic,
			"deadletter_topic", deadletterTopic,
		)
	}
}

func Ptr[T any](v T) *T {
	return &v
}

// ListenerOption add listener option
type ListenerOption func(l *listener)

// Listen process incoming kafka messages with handlers configured by the listener
func (l *listener) Listen(consumerContext context.Context) error {
	if l.consumerGroup == nil {
		return errors.New("consumerGroup is nil, cannot listen")
	}

	LogInfo("starting listener", "consumer_group", l.groupID, "topics", l.topics)

	// When a session is over, make consumer join a new session, as long as the context is not cancelled
	for {
		// Consume make this consumer join the next session
		// This block until the `session` is over. (basically until next rebalance)
		err := l.consumerGroup.Consume(consumerContext, l.topics, l)
		if err != nil {
			LogError("consumer group consume error", "error", err, "consumer_group", l.groupID)
			return err
		}
		if err := consumerContext.Err(); err != nil {
			// Check if context is cancelled
			LogInfo("listener stopping (context cancelled)", "consumer_group", l.groupID)
			return err
		}
		LogDebug("consumer group session ended, rejoining", "consumer_group", l.groupID)
	}
}

// Close the listener and dependencies
func (l *listener) Close() {
	if l.consumerGroup != nil {
		err := l.consumerGroup.Close()
		if err != nil {
			LogError("failed to close consumer group", "error", err, "consumer_group", l.groupID)
		} else {
			LogInfo("consumer group closed", "consumer_group", l.groupID)
		}
	}
}

// The `Setup`, `Cleanup` and `ConsumeClaim` are actually implementation of ConsumerGroupHandler from sarama
// Copied from From the sarama lib:
//
// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.

// Setup is run at the beginning of a new session, before ConsumeClaim
func (l *listener) Setup(session sarama.ConsumerGroupSession) error {
	LogInfo("consumer group session started",
		"consumer_group", l.groupID,
		"generation_id", session.GenerationID(),
		"member_id", session.MemberID(),
		"claims", session.Claims(),
	)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (l *listener) Cleanup(session sarama.ConsumerGroupSession) error {
	LogInfo("consumer group session ended",
		"consumer_group", l.groupID,
		"generation_id", session.GenerationID(),
		"member_id", session.MemberID(),
	)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (l *listener) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	LogDebug("starting to consume partition",
		"consumer_group", l.groupID,
		"topic", claim.Topic(),
		"partition", claim.Partition(),
		"initial_offset", claim.InitialOffset(),
	)

	for msg := range claim.Messages() {
		l.onNewMessage(msg, session)
	}

	LogDebug("stopped consuming partition",
		"consumer_group", l.groupID,
		"topic", claim.Topic(),
		"partition", claim.Partition(),
	)
	return nil
}

func (l *listener) onNewMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	mc := NewMessageContext(msg, l.groupID)
	LogMessageDebug("received message", mc)

	messageContext := context.WithValue(session.Context(), contextTopicKey, msg.Topic)
	messageContext = context.WithValue(messageContext, contextkeyKey, msg.Key)
	messageContext = context.WithValue(messageContext, contextOffsetKey, msg.Offset)
	messageContext = context.WithValue(messageContext, contextTimestampKey, msg.Timestamp)
	for _, h := range msg.Headers {
		messageContext = context.WithValue(messageContext, listenerContextKey(h.Key), h.Value)
	}

	var span opentracing.Span
	if l.tracer != nil {
		span, messageContext = l.tracer(messageContext, msg)
		if span != nil {
			defer span.Finish()
		}
	}

	handler := l.handlers[msg.Topic]
	if l.instrumenting != nil {
		handler = l.instrumenting.Instrumentation(handler)
	}

	err := l.handleMessageWithRetry(messageContext, handler, msg, *handler.Config.ConsumerMaxRetries, 0, handler.Config.ExponentialBackoff)
	if err != nil {
		err = fmt.Errorf("processing failed: %w", err)
		l.handleErrorMessage(err, handler, msg)
	} else {
		LogMessageDebug("message processed successfully", mc)
	}

	if !errors.Is(err, context.Canceled) {
		session.MarkMessage(msg, "")
		LogMessageDebug("message offset committed", mc)
	}
}

func (l *listener) handleErrorMessage(initialError error, handler Handler, msg *sarama.ConsumerMessage) {
	mc := NewMessageContext(msg, l.groupID)

	if isOmittedError(initialError) {
		l.handleOmittedMessage(initialError, msg)
		return
	}

	// Log the error
	LogMessageError("message processing failed, applying error handling policy", mc, "error", initialError)

	if isRetriableError(initialError) {
		// First, check if handler's config defines retry topic
		if handler.Config.RetryTopic != "" {
			LogMessageInfo("forwarding message to retry topic", mc, "retry_topic", handler.Config.RetryTopic)
			err := forwardToTopic(l, msg, handler.Config.RetryTopic)
			if err != nil {
				LogMessageError("failed to send message to retry topic", mc, "error", err, "retry_topic", handler.Config.RetryTopic)
			}
			return
		}

		// If not, check if global retry topic pattern is defined
		if PushConsumerErrorsToRetryTopic {
			topicName := l.deduceTopicNameFromPattern(msg.Topic, RetryTopicPattern)
			LogMessageInfo("forwarding message to retry topic (global pattern)", mc, "retry_topic", topicName)
			err := forwardToTopic(l, msg, topicName)
			if err != nil {
				LogMessageError("failed to send message to retry topic", mc, "error", err, "retry_topic", topicName)
			}
			return
		}
	}

	// If the error is not retriable, or if there is no retry topic defined at all, then try to send to dead letter topic
	// First, check if handler's config defines deadletter topic
	if handler.Config.DeadletterTopic != "" {
		LogMessageInfo("forwarding message to deadletter topic", mc, "deadletter_topic", handler.Config.DeadletterTopic)
		err := forwardToTopic(l, msg, handler.Config.DeadletterTopic)
		if err != nil {
			LogMessageError("failed to send message to deadletter topic", mc, "error", err, "deadletter_topic", handler.Config.DeadletterTopic)
		}
		return
	}

	// If not, check if global deadletter topic pattern is defined
	if PushConsumerErrorsToDeadletterTopic {
		topicName := l.deduceTopicNameFromPattern(msg.Topic, DeadletterTopicPattern)
		LogMessageInfo("forwarding message to deadletter topic (global pattern)", mc, "deadletter_topic", topicName)
		err := forwardToTopic(l, msg, topicName)
		if err != nil {
			LogMessageError("failed to send message to deadletter topic", mc, "error", err, "deadletter_topic", topicName)
		}
		return
	}

	// If we do nothing the message is implicitly omitted
	LogMessageWarn("message dropped: no retry or deadletter topic configured", mc)
	if l.instrumenting != nil && l.instrumenting.recordOmittedCounter != nil {
		l.instrumenting.recordOmittedCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}
}

func (l *listener) deduceTopicNameFromPattern(topic string, pattern string) string {
	topicName := pattern
	topicName = strings.Replace(topicName, "$$CG$$", l.groupID, 1)
	topicName = strings.Replace(topicName, "$$T$$", topic, 1)
	return topicName
}

func forwardToTopic(l *listener, msg *sarama.ConsumerMessage, topicName string) error {
	err := l.deadletterProducer.Produce(&sarama.ProducerMessage{
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
		Topic: topicName,
	})
	return err
}

func (l *listener) handleOmittedMessage(initialError error, msg *sarama.ConsumerMessage) {
	mc := NewMessageContext(msg, l.groupID)
	LogMessageWarn("message omitted by handler", mc, "error", initialError)

	// Inc dropped messages metrics
	if l.instrumenting != nil && l.instrumenting.recordOmittedCounter != nil {
		l.instrumenting.recordOmittedCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}
}

// handleMessageWithRetry call the handler function and retry if it fails
func (l *listener) handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries, retryNumber int, exponentialBackoff bool) (err error) {
	mc := NewMessageContext(msg, l.groupID)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic happened during handle of message: %v", r)
			LogMessageError("panic recovered during message processing", mc, "panic", r)
		}
	}()

	// Check if context is still valid
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Log when starting to process a message (debug level)
	if retryNumber == 0 {
		LogMessageDebug("processing message", mc)
	}

	err = handler.Processor(ctx, msg)
	if err != nil {
		// Inc dropped messages metrics
		if l.instrumenting != nil && l.instrumenting.recordErrorCounter != nil {
			l.instrumenting.recordErrorCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
		}
		if shouldRetry(retries, err) {
			var backoffDuration time.Duration
			if exponentialBackoff {
				backoffDuration = getBackoffDuration(handler, retryNumber, *handler.Config.ConsumerMaxRetries)
			} else {
				backoffDuration = *handler.Config.DurationBeforeRetry
			}

			remainingRetries := "infinite"
			if retries != InfiniteRetries {
				remainingRetries = fmt.Sprintf("%d", retries)
			}

			LogMessageWarn("message processing failed, will retry", mc,
				"error", err,
				"retry_number", retryNumber+1,
				"remaining_retries", remainingRetries,
				"backoff_duration", backoffDuration,
				"exponential_backoff", exponentialBackoff,
			)

			time.Sleep(backoffDuration)

			if retries != InfiniteRetries {
				retries--
			}
			retryNumber++
			return l.handleMessageWithRetry(ctx, handler, msg, retries, retryNumber, exponentialBackoff)
		}
	}
	return err
}

func shouldRetry(retries int, err error) bool {
	if retries == 0 {
		return false
	}

	if isUnretriableError(err) || isOmittedError(err) {
		return false
	}

	return true
}

// getBackoffDuration returns the backoff duration using the handler's BackoffFunc
// or the global ExponentialBackoffFunc (which uses sarama.NewExponentialBackoff with KIP-580 jitter).
func getBackoffDuration(handler Handler, retryNumber, maxRetries int) time.Duration {
	if handler.Config.BackoffFunc != nil {
		return handler.Config.BackoffFunc(retryNumber, maxRetries)
	}
	// Use the global ExponentialBackoffFunc which leverages sarama.NewExponentialBackoff
	return ExponentialBackoffFunc(retryNumber, maxRetries)
}
