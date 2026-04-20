package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/ricardo-ch/go-kafka/v3/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_ConsumerClaim_HappyPath_WithOTELTracing(t *testing.T) {
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	consumerGroupClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(msgChanel))

	consumerGroupSession := &mocks.ConsumerGroupSession{}
	consumerGroupSession.On("Context").Return(context.Background())
	consumerGroupSession.On("MarkMessage", mock.Anything, mock.Anything).Return()

	handlerCalled := false
	handlerProcessor := func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		handlerCalled = true
		return nil
	}
	handler := Handler{
		Processor: handlerProcessor,
		Config:    testHandlerConfig,
	}

	tested := listener{
		handlers:   map[string]Handler{"topic-test": handler},
		otelTracer: DefaultOTELTracing,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
}

func Test_GetOTELKafkaHeadersFromContext_EmptyContext(t *testing.T) {
	headers := GetOTELKafkaHeadersFromContext(context.Background())
	assert.NotNil(t, headers)
}

func Test_SerializeOTELKafkaHeadersFromContext_EmptyContext(t *testing.T) {
	jsonStr, err := SerializeOTELKafkaHeadersFromContext(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, jsonStr)
	assert.Contains(t, jsonStr, "{")
}

func Test_DeserializeOTELContextFromKafkaHeaders_InvalidJSON(t *testing.T) {
	baseCtx := context.Background()
	ctx, err := DeserializeOTELContextFromKafkaHeaders(baseCtx, "invalid json")
	assert.Error(t, err)
	assert.Equal(t, baseCtx, ctx)
}

func Test_DeserializeOTELContextFromKafkaHeaders_ValidEmptyJSON(t *testing.T) {
	ctx, err := DeserializeOTELContextFromKafkaHeaders(context.Background(), "{}")
	assert.NoError(t, err)
	assert.NotNil(t, ctx)
}

func Test_GetOTELContextFromKafkaMessage(t *testing.T) {
	span, ctx := GetOTELContextFromKafkaMessage(context.Background(), &sarama.ConsumerMessage{Topic: "topic-test"})
	assert.NotNil(t, span)
	assert.NotNil(t, ctx)
	span.End()
}
