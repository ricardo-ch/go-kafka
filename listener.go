package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"
)

// Consumer interface to test
type Consumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	Errors() <-chan error
	CommitOffsets() error
	Close() (err error)
	Notifications() <-chan *cluster.Notification
	HighWaterMarks() map[string]map[int32]int64
	MarkPartitionOffset(topic string, partition int32, offset int64, metadata string)
	MarkOffsets(s *cluster.OffsetStash)
	Subscriptions() map[string][]int32
}

// Handler that handle received kafka messages
type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error

// Handlers defines a handler for a given topic
type Handlers map[string]Handler

// listener object represents kafka consumer
type listener struct {
	consumer Consumer
	handlers Handlers
	closed   chan interface{}
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
func NewListener(brokers []string, groupID string, handlers Handlers) (Listener, error) {
	if brokers == nil || len(brokers) == 0 {
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
	consumer, err := cluster.NewConsumer(brokers, groupID, topics, Config)
	if err != nil {
		return nil, err
	}

	return &listener{
		consumer: consumer,
		handlers: handlers,
		closed:   make(chan interface{}),
	}, nil
}

// Listen process incoming kafka messages with handlers configured by the listener
func (l *listener) Listen(consumerContext context.Context) error {
	if l.consumer == nil {
		return errors.New("cannot subscribe. Consumer is nil")
	}

	// Consume all channels, wait for signal to exit
	for {
		select {
		case msg, more := <-l.consumer.Messages():
			if more {
				// TODO need to get context from kafka message header when feature available
				messageContext := context.WithValue(context.Background(), contextTopicKey, msg.Topic)
				messageContext = context.WithValue(messageContext, contextkeyKey, msg.Key)
				messageContext = context.WithValue(messageContext, contextOffsetKey, msg.Offset)
				messageContext = context.WithValue(messageContext, contextTimestampKey, msg.Timestamp)

				err := handleMessageWithRetry(messageContext, l.handlers[msg.Topic], msg, ConsumerMaxRetries)
				if err != nil {
					err = errors.Wrapf(err, "processing failed after %d attempts", ConsumerMaxRetries)
					ErrorLogger.Printf("Consume: %+v", err)
					// TODO add process failure escalation
				}
				l.consumer.MarkOffset(msg, "")
			}
		case ntf, more := <-l.consumer.Notifications():
			if more {
				Logger.Printf("Rebalanced: %+v", ntf)
			}
		case err, more := <-l.consumer.Errors():
			if more {
				ErrorLogger.Printf("Error: %+v", err)
			}
		case <-consumerContext.Done():
			return errors.New("context canceled")
		case <-l.closed:
			return errors.New("Listener Closed")
		}
	}
}

// Close the listener and dependencies
func (l *listener) Close() {
	if l.consumer != nil {
		l.consumer.Close() //this line may take a few seconds to execute
		close(l.closed)
	}
}

// handleMessageWithRetry call the handler function and retry if it fails
func handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries int) error {
	err := handler(ctx, msg)
	if err != nil && retries > 0 {
		time.Sleep(DurationBeforeRetry)
		return handleMessageWithRetry(ctx, handler, msg, retries-1)
	}
	return err
}
