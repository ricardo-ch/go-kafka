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

var (
	ErrEventUnretriable = errors.New("the event will not be retried")
	ErrEventOmitted     = errors.New("the event will be omitted")
)

type HandlerConfig struct {
	ConsumerMaxRetries  *int
	DurationBeforeRetry *time.Duration
	RetryTopic          string
	DeadletterTopic     string
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
}

// NewListener creates a new instance of Listener
func NewListener(groupID string, handlers Handlers, options ...ListenerOption) (Listener, error) {
	if groupID == "" {
		return nil, errors.New("cannot create new listener, groupID cannot be empty")
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
		err := <-consumerGroup.Errors()
		if err != nil {
			ErrorLogger.Println("sarama error: %s", err.Error())
		}
	}()

	// Fill handler config unset elements with global default values.
	fillHandlerConfigWithDefault(handlers)

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

func checkErrorTopicToAvoidInfiniteLoop(handlers Handlers) error {
	for topic, handler := range handlers {
		if handler.Config.RetryTopic == topic {
			return fmt.Errorf("Retry topic cannot be the same as the original topic: %s", topic)
		}
		if handler.Config.DeadletterTopic == topic {
			return fmt.Errorf("Deadletter topic cannot be the same as the original topic: %s", topic)
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

func Ptr[T any](v T) *T {
	return &v
}

// ListenerOption add listener option
type ListenerOption func(l *listener)

// Listen process incoming kafka messages with handlers configured by the listener
func (l *listener) Listen(consumerContext context.Context) error {
	if l.consumerGroup == nil {
		return errors.New("cannot subscribe. ConsumerGroup is nil")
	}

	// When a session is over, make consumer join a new session, as long as the context is not cancelled
	for {
		// Consume make this consumer join the next session
		// This block until the `session` is over. (basically until next rebalance)
		err := l.consumerGroup.Consume(consumerContext, l.topics, l)
		if err != nil {
			return err
		}
		if err := consumerContext.Err(); err != nil {
			// Check if context is cancelled
			return err
		}
	}
}

// Close the listener and dependencies
func (l *listener) Close() {
	if l.consumerGroup != nil {
		err := l.consumerGroup.Close()
		if err != nil {
			ErrorLogger.Printf("Error while closing sarama consumerGroup: %s", err.Error())
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
func (l *listener) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (l *listener) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (l *listener) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		l.onNewMessage(msg, session)
	}
	return nil
}

func (l *listener) onNewMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	messageContext := context.WithValue(context.Background(), contextTopicKey, msg.Topic)
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

	err := l.handleMessageWithRetry(messageContext, handler, msg, *handler.Config.ConsumerMaxRetries)
	if err != nil {
		err = fmt.Errorf("processing failed after all possible attempts attempts: %w", err)
		l.handleErrorMessage(err, handler, msg)
	}

	session.MarkMessage(msg, "")
}

func (l *listener) handleErrorMessage(initialError error, handler Handler, msg *sarama.ConsumerMessage) {
	if errors.Is(initialError, ErrEventOmitted) {
		l.handleOmittedMessage(initialError, msg)
		return
	}

	// Log
	ErrorLogger.Printf("Consume: %+v", initialError)

	// Inc dropped messages metrics
	if l.instrumenting != nil && l.instrumenting.recordErrorCounter != nil {
		l.instrumenting.recordErrorCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}

	if isRetriableError(initialError) {
		// First, check if handler's config defines retry topic
		if handler.Config.RetryTopic != "" {
			Logger.Printf("Sending message to retry topic: %s", handler.Config.RetryTopic)
			err := forwardToTopic(l, msg, handler.Config.RetryTopic)
			if err != nil {
				ErrorLogger.Printf("Cannot send message to handler's retry topic %s: %+v", handler.Config.RetryTopic, err)
			}
			return
		}

		// If not, check if global retry topic pattern is defined
		if PushConsumerErrorsToRetryTopic {
			topicName := l.deduceTopicNameFromPattern(msg.Topic, RetryTopicPattern)
			Logger.Printf("Sending message to retry topic: %s", topicName)
			err := forwardToTopic(l, msg, topicName)
			if err != nil {
				ErrorLogger.Printf("Cannot send message to handler's retry topic defined with global pattern %s: %+v", topicName, err)
			}
			return
		}
	}

	// If the error is not retriable, or if there is no retry topic defined at all, then try to send to dead letter topic
	// First, check if handler's config defines deadletter topic
	if handler.Config.DeadletterTopic != "" {
		Logger.Printf("Sending message to handler's deadletter topic: %s", handler.Config.DeadletterTopic)
		err := forwardToTopic(l, msg, handler.Config.DeadletterTopic)
		if err != nil {
			ErrorLogger.Printf("Cannot send message to handler's deadletter topic %s: %+v", handler.Config.RetryTopic, err)
		}
		return
	}

	// If not, check if global deadletter topic pattern is defined
	if PushConsumerErrorsToDeadletterTopic {
		topicName := l.deduceTopicNameFromPattern(msg.Topic, DeadletterTopicPattern)
		Logger.Printf("Sending message to deadletter topic: %s", topicName)
		err := forwardToTopic(l, msg, topicName)
		if err != nil {
			ErrorLogger.Printf("Cannot send message to handler's deadletter topic defined with global pattern %s: %+v", topicName, err)
		}
		return
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

func isRetriableError(initialError error) bool {
	return !errors.Is(initialError, ErrEventUnretriable) && !errors.Is(initialError, ErrEventOmitted)
}

func (l *listener) handleOmittedMessage(initialError error, msg *sarama.ConsumerMessage) {
	ErrorLogger.Printf("Omitted message: %+v", initialError)

	// Inc dropped messages metrics
	if l.instrumenting != nil && l.instrumenting.recordOmittedCounter != nil {
		l.instrumenting.recordOmittedCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}
}

// handleMessageWithRetry call the handler function and retry if it fails
func (l *listener) handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Panic happened during handle of message: %v", r)
		}
	}()

	// Check if context is still valid
	if ctx.Err() != nil {
		return ctx.Err()
	}

	err = handler.Processor(ctx, msg)
	if err != nil && shouldRetry(retries, err) {
		time.Sleep(*handler.Config.DurationBeforeRetry)
		if retries != InfiniteRetries {
			retries--
		} else {
			ErrorLogger.Printf("Error for message with infinite retry %+v: ", err)
		}
		return l.handleMessageWithRetry(ctx, handler, msg, retries)
	}

	return err
}

func shouldRetry(retries int, err error) bool {
	if retries == 0 {
		return false
	}

	if errors.Is(err, ErrEventUnretriable) || errors.Is(err, ErrEventOmitted) {
		return false
	}

	return true
}
