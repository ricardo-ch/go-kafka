package kafka

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/endpoint"
)

// NewServer creates a new Handler implementing go-kit interface
func NewServer(e endpoint.Endpoint, decodeRequestFunc DecodeRequestFunc, encodeResponseFunc EncodeResponseFunc) Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		var m interface{}
		if decodeRequestFunc != nil {
			m, err = decodeRequestFunc(ctx, msg)
			if err != nil {
				return err
			}
		}

		_, err = e(ctx, m)

		return err
	}
}

// DecodeRequestFunc struct using go-kit signature adapted to kafka
type DecodeRequestFunc func(ctx context.Context, msg *sarama.ConsumerMessage) (request interface{}, err error)

// EncodeResponseFunc struct using go-kit signature adapted to kafka
type EncodeResponseFunc func(ctx context.Context, msg []byte) (request interface{}, err error)
