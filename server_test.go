package kafka

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"github.com/Shopify/sarama"
	"time"
)

func Test_NewServer_Should_Return_Working_Endpoint(t *testing.T) {
	timeout := time.After(10 * time.Second) //timeout

	expectedRequest := "bla"

	chanDone := make(chan interface{}, 1)

	mockEndpoint := func(ctx context.Context, request interface{}) (response interface{}, err error) {
		chanDone <- request
		return expectedRequest, nil
	}

	decodeRequest := func(ctx context.Context, msg *sarama.ConsumerMessage) (request interface{}, err error) {
		return expectedRequest, nil
	}

	// Act
	handler := NewServer(mockEndpoint, decodeRequest, nil)
	// Assert
	assert.NotNil(t, handler)

	err := handler(context.Background(), &sarama.ConsumerMessage{})
	assert.Nil(t, err)

	select {
		case <- timeout:
			assert.Fail(t, "timeout waiting for endpoint to be called")
		case request := <- chanDone:
			assert.Equal(t, expectedRequest, request)
	}
}
