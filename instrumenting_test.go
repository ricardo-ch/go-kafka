package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
)

var testEncodedMessage = []byte{10, 3, 49, 50, 51}

func Test_NewConsumerMetricsService_Should_Return_Success_When_Success(t *testing.T) {
	// Arrange
	s := NewConsumerMetricsService("test_ok")
	h := func(context.Context, *sarama.ConsumerMessage) error { return nil }

	// Act
	handler := s.Instrumentation(h)

	err := handler(context.Background(), &sarama.ConsumerMessage{Value: testEncodedMessage, Topic: "test-topic"})

	fmt.Println("han ", err)

	// Assert
	assert.Nil(t, err)
}
