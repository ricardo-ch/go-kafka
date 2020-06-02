# GO-KAFKA

[![Build Status](https://travis-ci.org/ricardo-ch/go-kafka.svg?branch=master)](https://travis-ci.org/ricardo-ch/go-kafka)
[![Coverage Status](https://coveralls.io/repos/github/ricardo-ch/go-kafka/badge.svg?branch=master)](https://coveralls.io/github/ricardo-ch/go-kafka?branch=master)
[![Go Report Card](https://goreportcard.com/badge/github.com/ricardo-ch/go-kafka)](https://goreportcard.com/report/github.com/ricardo-ch/go-kafka)

Go-kafka provides an easy way to use kafka listener, producer and go-kit like server with only few lines of code.
The listener is able to listen multiple topics, and will execute a defined go-kit endpoint by topic message.

## Quick start

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
listener, _ := kafka.NewListener(brokers, "my-consumer-group", handlers)
defer listener.Close()

// listen and enjoy
errc <- listener.Listen(ctx)
```

## Features

* Create a listener on multiple topics
* Retry policy on message handling
* Create a producer
* Create a go-kit like server
* Add instrumenting on Prometheus

## Instrumenting

 Currently the instrumenting is implemented only on consumer part.
 The metrics are exported on prometheus
 The metrics are :
* Number of requests processed (label: kafka_topic, success)
* Total duration in milliseconds (label: kafka_topic)

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

## Consumer error handling

The listener object is able to have a specific handle for consuming errors.
By default, if an error occurs, it's retried 3 times (each attempt is separated by 2 seconds).
And if there's still an error after the 3 retries, the error is logged and pushed to a topic named like `"group-id"-"original-topic-name"-error`.

All this strategy can be overridden through the following config variables:

* ConsumerMaxRetries
* DurationBeforeRetry
* PushConsumerErrorsToTopic
* ErrorTopicPattern

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
