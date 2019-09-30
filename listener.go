package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"strings"
	"time"
)

// Handler that handle received kafka messages
type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error

// Handlers defines a handler for a given topic
type Handlers map[string]Handler

// listener object represents kafka consumer
// Listener implement both `Listener` interface and `ConsumerGroupHandler` from sarama
type listener struct {
	consumerGroup sarama.ConsumerGroup
	producer      sarama.SyncProducer
	topics        []string
	handlers      Handlers
	groupID       string
	instrumenting *ConsumerMetricsService
	tracer        ContextFunc
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
func NewListener(brokers []string, groupID string, handlers Handlers, options ...ListenerOption) (Listener, error) {
	if len(brokers) == 0 {
		return nil, errors.New("cannot create new listener, brokers cannot be empty")
	}
	if groupID == "" {
		return nil, errors.New("cannot create new listener, groupID cannot be empty")
	}
	if handlers == nil || len(handlers) == 0 {
		return nil, errors.New("cannot create new listener, handlers cannot be empty")
	}

	// Init consumer, consume errors & messages
	var topics []string
	for k := range handlers {
		topics = append(topics, k)
	}
	client, err := sarama.NewClient(brokers, Config)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, client)
	if err != nil {
		return nil, err
	}

	go func() {
		err := <-consumerGroup.Errors()
		if err != nil {
			ErrorLogger.Println("sarama error: %s", err.Error())
		}
	}()

	l := &listener{
		groupID:       groupID,
		producer:      producer,
		handlers:      handlers,
		consumerGroup: consumerGroup,
		topics:        topics,
	}

	// execute all method passed as option
	for _, o := range options {
		o(l)
	}

	// initialize vec counter metric to 0 for easier query
	for topic := range l.handlers {
		if l.instrumenting != nil && l.instrumenting.droppedRequest != nil {
			l.instrumenting.droppedRequest.With(map[string]string{"kafka_topic": topic, "group_id": l.groupID})
		}
	}

	return l, nil
}

//ListenerOption add listener option
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

	var span opentracing.Span
	if l.tracer != nil {
		span, messageContext = l.tracer(messageContext)
		defer span.Finish()
	}

	handler := l.handlers[msg.Topic]
	if l.instrumenting != nil {
		handler = l.instrumenting.Instrumentation(handler)
	}

	err := handleMessageWithRetry(messageContext, handler, msg, ConsumerMaxRetries)
	if err != nil {
		err = errors.Wrapf(err, "processing failed after %d attempts", ConsumerMaxRetries)
		l.handleErrorMessage(messageContext, err, msg)
	}

	session.MarkMessage(msg, "")
}

func (l *listener) handleErrorMessage(ctx context.Context, initialError error, msg *sarama.ConsumerMessage) {
	// Log
	ErrorLogger.Printf("Consume: %+v", initialError)

	// Inc dropped messages metrics
	if l.instrumenting != nil && l.instrumenting.droppedRequest != nil {
		l.instrumenting.droppedRequest.With(map[string]string{"kafka_topic": msg.Topic, "group_id": l.groupID}).Inc()
	}

	// publish to dead queue if requested in config
	if PushConsumerErrorsToTopic {
		if l.producer == nil {
			ErrorLogger.Printf("Cannot send message to error topic: producer is nil")
		}

		topicName := ErrorTopicPattern
		topicName = strings.Replace(topicName, "$$CG$$", l.groupID, 1)
		topicName = strings.Replace(topicName, "$$T$$", msg.Topic, 1)

		// Send fee message to kafka
		_, _, err := l.producer.SendMessage(&sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(msg.Key),
			Value: sarama.ByteEncoder(msg.Value),
			Topic: topicName,
		})
		if err != nil {
			ErrorLogger.Printf("Cannot send message to error topic: %+v", err)
		}
	}
}

// handleMessageWithRetry call the handler function and retry if it fails
func handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.Errorf("Panic happened during handle of message: %v", r)
		}
	}()

	err = handler(ctx, msg)
	if err != nil && retries > 0 {
		time.Sleep(DurationBeforeRetry)
		return handleMessageWithRetry(ctx, handler, msg, retries-1)
	}
	return err
}
