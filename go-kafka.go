// Package kafka provides opinionated Kafka consumer and producer abstractions
// built on top of IBM/sarama, with retry, dead-lettering, Prometheus metrics,
// and OpenTelemetry tracing.
//
// Global variables (Brokers, Config, ConsumerMaxRetries, etc.) must be configured
// before creating any Listener or Producer. They are not safe for concurrent modification.
package kafka

import (
	"time"

	"github.com/IBM/sarama"
)

// Brokers is the list of Kafka brokers to connect to.
var Brokers []string

// ConsumerMaxRetries is the maximum number of time we want to retry
// to process an event before throwing the error.
// By default 3 times.
var ConsumerMaxRetries = 3

// InfiniteRetries is a constant to define infinite retries.
// It is used to set the ConsumerMaxRetries to a blocking retry process.
const InfiniteRetries = -1

// DurationBeforeRetry is the duration we wait between process retries.
// By default 2 seconds.
var DurationBeforeRetry = 2 * time.Second

// MaxBackoffDuration is the maximum backoff duration for message processing retries.
// By default 10 minute.
var MaxBackoffDuration = 10 * time.Minute

// ForwardMaxBackoffDuration is the maximum backoff duration when retrying to forward
// a message to a retry or deadletter topic after a producer failure.
// By default 30 seconds.
var ForwardMaxBackoffDuration = 30 * time.Second

// ExponentialBackoffFunc is the function used to calculate exponential backoff duration.
// If nil (default), it is evaluated lazily using the current values of DurationBeforeRetry
// and MaxBackoffDuration at the time of the first retry, via sarama.NewExponentialBackoff
// (KIP-580 with jitter).
// Set this to a custom function to override the default backoff strategy.
var ExponentialBackoffFunc func(retries, maxRetries int) time.Duration

// PushConsumerErrorsToRetryTopic is a boolean to define if messages in error have to be pushed to a retry topic.
var PushConsumerErrorsToRetryTopic = true

// PushConsumerErrorsToDeadletterTopic is a boolean to define if messages in error have to be pushed to a deadletter topic.
var PushConsumerErrorsToDeadletterTopic = true

// RetryTopicPattern is the retry topic name pattern.
// By default "consumergroup-topicname-retry"
// Use $$CG$$ as consumer group placeholder
// Use $$T$$ as original topic name placeholder
var RetryTopicPattern = "$$CG$$-$$T$$-retry"

// DeadletterTopicPattern is the deadletter topic name pattern.
// By default "consumergroup-topicname-deadletter"
// Use $$CG$$ as consumer group placeholder
// Use $$T$$ as original topic name placeholder
var DeadletterTopicPattern = "$$CG$$-$$T$$-deadletter"

// Config is the sarama (cluster) config used for the consumer and producer.
var Config = sarama.NewConfig()

func init() {
	// Init config with default values
	Config.Consumer.Return.Errors = true
	Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	Config.Consumer.Offsets.Retention = 30 * 24 * time.Hour // 30 days, because we tend to increase the retention of a topic to a few weeks for practical purpose
	Config.Producer.Timeout = 5 * time.Second
	Config.Producer.Retry.Max = 3
	Config.Producer.Return.Successes = true
	Config.Producer.RequiredAcks = sarama.WaitForAll
	Config.Producer.Partitioner = NewJVMCompatiblePartitioner
	Config.Version = sarama.MaxVersion
}
