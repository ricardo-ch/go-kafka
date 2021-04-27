package kafka

import (
	"context"
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

	// Assert
	assert.Nil(t, err)
}

func Test_NewConsumerMetricsService_Should_Allow_Multiple_Instance(t *testing.T) {
	// Arrange
	group1 := "test_ok"
	group2 := "test_ok_other"
	s1 := NewConsumerMetricsService(group1)
	s2 := NewConsumerMetricsService(group2)

	assert.Equal(t, group1, s1.groupID)
	assert.Equal(t, group2, s2.groupID)
}
