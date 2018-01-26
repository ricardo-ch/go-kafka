# GO-KAFKA

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

## License

go-kafka is licensed under the MIT license. (http://opensource.org/licenses/MIT)

## Contributing

Pull requests are the way to help us here. We will be really grateful.
