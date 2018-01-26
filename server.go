package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
)

// NewServer ...
func NewServer(e endpoint.Endpoint, decodeRequestFunc DecodeRequestFunc, encodeResponseFunc EncodeResponseFunc) Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		m, err := decodeRequestFunc(ctx, msg)
		if err != nil {
			return err
		}

		_, err = e(ctx, m)

		return err
	}
}

// DecodeRequestFunc ...
type DecodeRequestFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (request interface{}, err error)

// EncodeResponseFunc ...
type EncodeResponseFunc func(ctx context.Context, msg []byte) (request interface{}, err error)
