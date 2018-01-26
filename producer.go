package kafka

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
)

type Encoder interface {
	Encode(payload interface{}) ([]byte, error)
}

type DefaultEncoder struct{}

func (f DefaultEncoder) Encode(payload interface{}) ([]byte, error) {
	buffer := bytes.Buffer{}
	err := json.NewEncoder(&buffer).Encode(payload)
	return buffer.Bytes(), err
}

//producer object represents kafka customer
type producer struct {
	syncProducer sarama.SyncProducer
}

//Producer ...
type Producer interface {
	SendMessage(key []byte, msg []byte, topic string) (partition int32, offset int64, err error)
	Close() error
}

//NewProducer ...
func NewProducer(brokers []string) (Producer, error) {
	if brokers == nil || len(brokers) == 0 {
		return nil, errors.New("cannot create new producer, brokers cannot be empty")
	}

	// Init config
	config := sarama.NewConfig()
	config.Producer.Timeout = 5 * time.Second
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	p, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return producer{p}, nil
}

//SendMessage ...
func (p producer) SendMessage(key []byte, msg []byte, topic string) (partition int32, offset int64, err error) {
	if p.syncProducer == nil {
		return 0, 0, errors.New("cannot send message. producer is nil")
	}

	message := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(msg),
	}

	return p.syncProducer.SendMessage(message)
}

//Close ...
func (p producer) Close() error {
	if p.syncProducer != nil {
		return p.syncProducer.Close()
	}
	return nil
}
