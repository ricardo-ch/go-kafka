# GO-KAFKA

![Build Status](https://github.com/ricardo-ch/go-kafka/actions/workflows/quality-gate.yml/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/ricardo-ch/go-kafka)](https://goreportcard.com/report/github.com/ricardo-ch/go-kafka)

Go-kafka provides an easy way to use kafka listeners and producers with only a few lines of code.
The listener is able to consume from multiple topics, and will execute a separate handler for each topic.

> 📘 Important note for v3 upgrade:
> - The library now relies on the IBM/sarama library instead of Shopify/sarama, which is no longer maintained.
> - The `kafka.Handler` type has been changed to a struct containing both the function to execute and the handler's optional configuration.
> - The global variable `PushConsumerErrorsToTopic` has been replaced by the `PushConsumerErrorsToRetryTopic` and `PushConsumerErrorsToDeadletterTopic` properties on the handler.
>
> These two changes should be the only breaking changes in the v3 release. The rest of the library should be compatible with the previous version. 

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

You can customize the error handling of the consumer, using various patterns:
* Blocking retries of the same event (Max number, and delay are configurable by handler)
* Forward to retry topic for automatic retry without blocking the consumer
* Forward to deadletter topic for manual investigation

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
### Error types
Two types of errors are introduced, so that application code can return them whenever relevant
* `kafka.ErrEventUnretriable` - Errors that should not be retried 
* `kafka.ErrEventOmitted` - Errors that should lead to the event being omitted without impacting metrics

All the other errors will be considered as "retryable" errors. 

Depending on the Retry topic/Deadletter topic/Max retries configuration, the event will be retried, forwarded to a retry topic, or forwarded to a deadletter topic.

### Blocking Retries

By default, failed events consumptions will be retried 3 times (each attempt is separated by 2 seconds) with no exponential backoff.
This can be globally configured through the following properties:
* `ConsumerMaxRetries` (int)
* `DurationBeforeRetry` (duration)

These properties can also be configured on a per-topic basis by setting the `ConsumerMaxRetries`, `DurationBeforeRetry` and `ExponentialBackoff` properties on the handler.

If you want to achieve a blocking retry pattern (ie. continuously retrying until the event is successfully consumed), you can set `ConsumerMaxRetries` to `InfiniteRetries` (-1).

If you want to **not** retry specific errors, you can wrap them in a `kafka.ErrEventUnretriable` error before returning them, or return a `kafka.ErrNonRetriable` directly.
```go
// This error will not be retried
err := errors.New("my error")
return errors.Wrap(kafka.ErrNonRetriable, err.Error())

// This error will also not be retried
return kafka.ErrNonRetriable
```
 
#### exponential backoff 
You can activate it by setting `ExponentialBackoff` config variable as true. You can set this properties as global, you have to use the configuration per-topic.  This configuration is useful in case of infinite retry configuration.
The exponential backoff algorithm is defined like this.

$`retryDuration  = durationBeforeRetry * 2^{retries}`$

### Deadletter And Retry topics

By default, events that have exceeded the maximum number of blocking retries will be pushed to a retry topic or dead letter topic.
This behaviour can be disabled through the `PushConsumerErrorsToRetryTopic` and `PushConsumerErrorsToDeadletterTopic` properties.
```go
PushConsumerErrorsToRetryTopic = false
PushConsumerErrorsToDeadletterTopic = false
```
If these switches are ON, the names of the deadletter and retry topics are dynamically generated based on the original topic name and the consumer group.
For example, if the original topic is `my-topic` and the consumer group is `my-consumer-group`, the deadletter topic will be `my-consumer-group-my-topic-deadletter`.
This pattern can be overridden through the `ErrorTopicPattern` property.
Also, the retry and deadletter topics name can be overridden through the `RetryTopic` and `DeadLetterTopic` properties on the handler.

Note that, if global `PushConsumerErrorsToRetryTopic` or `PushConsumerErrorsToDeadletterTopic` property are false, but you configure `RetryTopic` or `DeadLetterTopic` properties on a handler, then the events in error will be forwarder to the error topics only for this handler.

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
