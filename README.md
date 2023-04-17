# GO-KAFKA

[![Build Status](https://travis-ci.org/ricardo-ch/go-kafka.svg?branch=master)](https://travis-ci.org/ricardo-ch/go-kafka)
[![Coverage Status](https://coveralls.io/repos/github/ricardo-ch/go-kafka/badge.svg?branch=master)](https://coveralls.io/github/ricardo-ch/go-kafka?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ricardo-ch/go-kafka)](https://goreportcard.com/report/github.com/ricardo-ch/go-kafka)

Go-kafka provides an easy way to use kafka listeners, producers and go-kit like server with only few lines of code.
The listener is able to consume from multiple topics, and will execute a separate go-kit endpoint for each topic.

## Quick start

Simple consumer
```golang
// go-kit event endpoints
var endpointEvent1 endpoint.Endpoint
var endpointEvent2 endpoint.Endpoint

// set your handlers
myEvent1Handler := kafka.NewServer(endpointEvent1, myDecodeMethod1, nil)
myEvent2Handler := kafka.NewServer(endpointEvent2, myDecodeMethod2, nil)
handlers := map[string]kafka.Handler{
    "topic-event-1": myEvent1Handler,
    "topic-event-2": myEvent2Handler,
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
* Create a go-kit like server
* Prometheus instrumenting

## Consumer error handling

You can customize the error handling of the consumer.
And if there's still an error after the 3 retries, the error is logged and pushed to a topic named like `"group-id"-"original-topic-name"-error`.

### Deadletter

By default, events that have exceeded the maximum number of retries will be pushed to a dead letter topic.
This behaviour can be disabled through the `PushConsumerErrorsToTopic` property.
```go
PushConsumerErrorsToTopic = false
```
The name of the deadletter topic is dynamically generated based on the original topic name and the consumer group.
For example, if the original topic is `my-topic` and the consumer group is `my-consumer-group`, the deadletter topic will be `my-consumer-group-my-topic-deadletter`.
This pattern can be overridden through the `ErrorTopicPattern` property.
```go
ErrorTopicPattern = "custom-deadletter-topic"
```

### Retries

By default, failed events consumptions will be retried 3 times (each attempt is separated by 2 seconds).
This can be configured through the following properties:
* `ConsumerMaxRetries`
* `DurationBeforeRetry`

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
