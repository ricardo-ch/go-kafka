# GO-KAFKA

[![Build Status](https://travis-ci.org/ricardo-ch/go-kafka.svg?branch=master)](https://travis-ci.org/ricardo-ch/go-kafka)
[![Coverage Status](https://coveralls.io/repos/github/ricardo-ch/go-kafka/badge.svg?branch=master)](https://coveralls.io/github/ricardo-ch/go-kafka?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ricardo-ch/go-kafka)](https://goreportcard.com/report/github.com/ricardo-ch/go-kafka)

Go-kafka provides an easy way to use kafka listeners and producers with only a few lines of code.
The listener is able to consume from multiple topics, and will execute a separate handler for each topic.

## Quick start

Simple consumer
```golang
// topic-specific handlers
var handler1 kafka.Handler
var handler2 kafka.Handler

// map your topics to their handlers
handlers := map[string]kafka.Handler{
    "topic-1": handler1,
    "topic-2": handler2,
}

// define your listener
kafka.Brokers = []string{"localhost:9092"}
listener, _ := kafka.NewListener("my-consumer-group", handlers)
defer listener.Close()

// listen and enjoy
errc <- listener.Listen(ctx)
```

Simple producer
```golang
// define your producer
kafka.Brokers = []string{"localhost:9092"}
producer, _ := kafka.NewProducer()

// send your message
message := &sarama.ProducerMessage{
	Topic: "my-topic",
	Value: sarama.StringEncoder("my-message"),
}
_ = producer.Produce(message)
```

## Features

* Create a listener on multiple topics
* Retry policy on message handling
* Create a producer
* Prometheus instrumenting

## Consumer error handling

You can customize the error handling of the consumer.
And if there's still an error after all possible retries (3 by default), the error is logged and the faulty event can be pushed to a deadletter topic.

Here is the overall logic applied to handle errors:
```mermaid
stateDiagram-v2

init: Error processing an event
state is_omitable_err <<choice>>
skipWithoutCounting: Skip the event without impacting counters
state is_retriable_err <<choice>>
state is_deadletter_configured <<choice>>
skip: Skip the event
forwardDL: Forward to deadletter topic
state should_retry <<choice>>
blocking_retry : Blocking Retry of this event
state is_retry_topic_configured <<choice>>
state is_deadletter_configured2 <<choice>>
forwardRQ: Forward to Retry topic
skip2: Skip the event
defaultDL: Forward to Deadletter topic

init --> is_omitable_err
is_omitable_err --> skipWithoutCounting: Error is of type ErrEventOmitted
is_omitable_err --> is_retriable_err: Error is not an ErrEventOmitted
is_retriable_err --> is_deadletter_configured: Error is of type ErrEventUnretriable
is_retriable_err --> should_retry: Error is retriable
should_retry --> blocking_retry: There are some retries left
should_retry --> is_retry_topic_configured : No more blocking retry
is_deadletter_configured --> skip: No Deadletter topic configured
is_deadletter_configured --> forwardDL: Deadletter topic configured
is_retry_topic_configured --> forwardRQ: Retry Topic Configured
is_retry_topic_configured --> is_deadletter_configured2: No Retry Topic Configured
is_deadletter_configured2 --> skip2: No Deadletter topic configured
is_deadletter_configured2 --> defaultDL: Deadletter topic configured 

```

### Deadletter

By default, events that have exceeded the maximum number of retries will be pushed to a dead letter topic.
This behaviour can be disabled through the `PushConsumerErrorsToTopic` property.
```go
PushConsumerErrorsToTopic = false
```
The name of the deadletter topic is dynamically generated based on the original topic name and the consumer group.
For example, if the original topic is `my-topic` and the consumer group is `my-consumer-group`, the deadletter topic will be `my-consumer-group-my-topic-error`.
This pattern can be overridden through the `ErrorTopicPattern` property.
```go
ErrorTopicPattern = "custom-deadletter-topic"
```

### Retries

By default, failed events consumptions will be retried 3 times (each attempt is separated by 2 seconds).
This can be configured through the following properties:
* `ConsumerMaxRetries`
* `DurationBeforeRetry`

If you want to achieve a blocking retry pattern (ie. continuously retrying until the event is successfully consumed), you can set `ConsumerMaxRetries` to `InfiniteRetries` (-1).

If you want to **not** retry specific errors, you can wrap them in a `kafka.ErrNonRetriable` error before returning them, or return a `kafka.ErrNonRetriable` directly.
```go
// This error will not be retried
err := errors.New("my error")
return errors.Wrap(kafka.ErrNonRetriable, err.Error())

// This error will also not be retried
return kafka.ErrNonRetriable
```

### Omitting specific errors

In certain scenarios, you might want to omit some errors. For example, you might want to discard outdated events that are not relevant anymore.
Such events would increase a separate, dedicated metric instead of the error one, and would not be retried.
To do so, wrap the errors that should lead to omitted events in a ErrEventOmitted, or return a kafka.ErrEventOmitted directly.
```go
// This error will be omitted
err := errors.New("my error")
return errors.Wrap(kafka.ErrEventOmitted, err.Error())

// This error will also be omitted
return kafka.ErrEventOmitted
```

## Instrumenting

Metrics for the listener and the producer can be exported to Prometheus.
The following metrics are available:
| Metric name | Labels | Description |
|-------------|--------|-------------|
| kafka_consumer_record_consumed_total | kafka_topic, consumer_group | Number of messages consumed |
| kafka_consumer_record_latency_seconds | kafka_topic, consumer_group | Latency of consuming a message |
| kafka_consumer_record_omitted_total | kafka_topic, consumer_group | Number of messages omitted |
| kafka_consumer_record_error_total | kafka_topic, consumer_group | Number of errors when consuming a message |
| kafka_consumergroup_current_message_timestamp| kafka_topic, consumer_group, partition, type | Timestamp of the current message being processed. Type can be either of `LogAppendTime` or `CreateTime`. |
| kafka_producer_record_send_total | kafka_topic | Number of messages sent |
| kafka_producer_dead_letter_created_total | kafka_topic | Number of messages sent to a dead letter topic |
| kafka_producer_record_error_total | kafka_topic | Number of errors when sending a message |

To activate the tracing on go-Kafka:

```golang
// define your listener
listener, _ := kafka.NewListener(brokers, "my-consumer-group", handlers, kafka.WithInstrumenting())
defer listener.Close()

// Instances a new HTTP server for metrics using prometheus 
go func() {
	httpAddr := ":8080" 
	mux.Handle("/metrics", promhttp.Handler())
	errc <- http.ListenAndServe(httpAddr, mux)
}()

```

## Default configuration

Configuration of consumer/producer is opinionated. It aim to resolve simply problems that have taken us by surprise in the past.
For this reason:
- the default partioner is based on murmur2 instead of the one sarama use by default
- offset retention is set to 30 days
- initial offset is oldest

## License

go-kafka is licensed under the MIT license. (http://opensource.org/licenses/MIT)

## Contributing

Pull requests are the way to help us here. We will be really grateful.
