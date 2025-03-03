package kafka

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
)

// Brokers is the list of Kafka brokers to connect to.
var Brokers []string

// StdLogger is used to log messages.

// StdLogger is the interface used to log messages.
// Print and println provides this type of log.
// print(ctx, err, "key", "value")
// print(err, "key", "value")
// print(ctx, "key", "value")
// print(ctx, err)
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Logger is the instance of a StdLogger interface.
// By default it is set to discard all log messages via ioutil.Discard,
// but you can set it to redirect wherever you want.
var Logger StdLogger = log.New(io.Discard, "[Go-Kafka] ", log.LstdFlags)

// ErrorLogger is the instance of a StdLogger interface.
// By default it is set to output on stderr all log messages,
// but you can set it to redirect wherever you want.
var ErrorLogger StdLogger = log.New(os.Stderr, "[Go-Kafka] ", log.LstdFlags)

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
	Config.Version = sarama.V1_1_1_0
}
