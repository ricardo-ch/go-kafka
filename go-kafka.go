package kafka

import (
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

// StdLogger is used to log messages.
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// Logger is the instance of a StdLogger interface.
// By default it is set to discard all log messages via ioutil.Discard,
// but you can set it to redirect wherever you want.
var Logger StdLogger = log.New(ioutil.Discard, "[Go-Kafka] ", log.LstdFlags)

// ErrorLogger is the instance of a StdLogger interface.
// By default it is set to output on stderr all log messages,
// but you can set it to redirect wherever you want.
var ErrorLogger StdLogger = log.New(os.Stderr, "[Go-Kafka] ", log.LstdFlags)

// ConsumerMaxRetries is the maximum number of time we want to retry
// to process an event before throwing the error.
// By default 3 times.
var ConsumerMaxRetries = 3

// DurationBeforeRetry is the duration we wait between process retries.
// By default 2 seconds.
var DurationBeforeRetry = 2 * time.Second

// Config is the sarama (cluster) config used for the consumer and producer.
var Config = cluster.NewConfig()

func init() {
	// Init config with default values
	Config.Group.Return.Notifications = true
	Config.Consumer.Return.Errors = true
	Config.Consumer.Offsets.Initial = sarama.OffsetOldest
	Config.Producer.Timeout = 5 * time.Second
	Config.Producer.Retry.Max = 3
	Config.Producer.Return.Successes = true
	Config.Producer.RequiredAcks = sarama.WaitForAll
}
