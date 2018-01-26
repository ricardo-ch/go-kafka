package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/pkg/errors"

	"github.com/ricardo-ch/bookkeeping-worker/utility/logger"
)

const (
	maxRetries        = 3
	sleepBetweenRetry = 2 * time.Second
)

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

//Handler that handle kafka messages received
type Handler func(ctx context.Context, msg *sarama.ConsumerMessage) error

//Handlers defines a handler for a given topic
type Handlers map[string]Handler

//listener object represents kafka customer
type listener struct {
	consumer Consumer
	handlers Handlers
	closed   chan interface{}
}

const (
	contextTopicKey     = "topic"
	contextkeyKey       = "key"
	contextOffsetKey    = "offset"
	contextTimestampKey = "timestamp"
)

//Listener ...
type Listener interface {
	Listen(ctx context.Context) error
	Close()
}

//NewListener ...
func NewListener(brokers []string, groupID string, handlers Handlers, offset int64) (Listener, error) {
	if brokers == nil || len(brokers) == 0 {
		return nil, errors.New("cannot create new listener, brokers cannot be empty")
	}
	if groupID == "" {
		return nil, errors.New("cannot create new listener, groupID cannot be empty")
	}
	if handlers == nil || len(handlers) == 0 {
		return nil, errors.New("cannot create new listener, handlers cannot be empty")
	}

	// Init config
	config := cluster.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = offset

	// Init consumer, consume errors & messages
	var topics []string
	for k := range handlers {
		topics = append(topics, k)
	}
	consumer, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return nil, err
	}

	return &listener{
		consumer: consumer,
		handlers: handlers,
		closed:   make(chan interface{}),
	}, nil
}

func (l *listener) Listen(consumerContext context.Context) error {
	if l.consumer == nil {
		return errors.New("cannot subscribe. Customer is nil")
	}

	// Consume all channels, wait for signal to exit
	for {
		select {
		case msg, more := <-l.consumer.Messages():
			if more {
				//TODO need to get context from kafka message header when feature available
				messageContext := context.WithValue(context.Background(), contextTopicKey, msg.Topic)
				messageContext = context.WithValue(messageContext, contextkeyKey, msg.Key)
				messageContext = context.WithValue(messageContext, contextOffsetKey, msg.Offset)
				messageContext = context.WithValue(messageContext, contextTimestampKey, msg.Timestamp)

				err := retry(messageContext, maxRetries, sleepBetweenRetry, l.handlers[msg.Topic], msg)
				if err != nil {
					logger.LogStdErr.Errorf("Consume: %+v", err)
					//TODO add process failure escalation
				}
				l.consumer.MarkOffset(msg, "")
			}
		case ntf, more := <-l.consumer.Notifications():
			if more {
				logger.LogStdOut.Infof("Rebalanced: %+v", ntf)
			}
		case err, more := <-l.consumer.Errors():
			if more {
				logger.LogStdErr.Errorf("Error: %s", err.Error())
			}
		case <-consumerContext.Done():
			return errors.New("context canceled")
		case <-l.closed:
			return errors.New("Listener Closed")
		}
	}
}

func (l *listener) Close() {
	if l.consumer != nil {
		l.consumer.Close() //this line may take a few seconds to execute
		close(l.closed)
	}
}

func retry(ctx context.Context, attempts int, sleep time.Duration, fn Handler, msg *sarama.ConsumerMessage) (err error) {
	for i := 0; ; i++ {
		err = fn(ctx, msg)
		if err == nil {
			return nil
		}
		if i >= (attempts - 1) {
			break
		}
		time.Sleep(sleep)
	}
	return fmt.Errorf("processing failed after %d attempts, last error: %+v", attempts, err)
}
