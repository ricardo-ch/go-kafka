package kafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/ricardo-ch/go-kafka/v3/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_ConsumerClaim_HappyPath_WithTracing(t *testing.T) {
	msgChanel := make(chan *sarama.ConsumerMessage, 1)
	msgChanel <- &sarama.ConsumerMessage{
		Topic: "topic-test",
	}
	close(msgChanel)

	consumerGroupClaim := &mocks.ConsumerGroupClaim{}
	setupConsumerGroupClaimMock(t, consumerGroupClaim, msgChanel)

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
		handlers: map[string]Handler{"topic-test": handler},
		tracer:   DefaultTracing,
	}

	err := tested.ConsumeClaim(consumerGroupSession, consumerGroupClaim)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	consumerGroupClaim.AssertExpectations(t)
	consumerGroupSession.AssertExpectations(t)
}

func Test_GetKafkaHeadersFromContext_EmptyContext(t *testing.T) {
	headers := GetKafkaHeadersFromContext(context.Background())
	assert.NotNil(t, headers)
	// Without TracerProvider configured, propagator is no-op, headers are typically empty
}

func Test_SerializeKafkaHeadersFromContext_EmptyContext(t *testing.T) {
	jsonStr, err := SerializeKafkaHeadersFromContext(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, jsonStr)
	assert.Contains(t, jsonStr, "{")
}

func Test_DeserializeContextFromKafkaHeaders_InvalidJSON(t *testing.T) {
	baseCtx := context.Background()
	ctx, err := DeserializeContextFromKafkaHeaders(baseCtx, "invalid json")
	assert.Error(t, err)
	assert.Equal(t, baseCtx, ctx)
}

func Test_DeserializeContextFromKafkaHeaders_ValidEmptyJSON(t *testing.T) {
	ctx, err := DeserializeContextFromKafkaHeaders(context.Background(), "{}")
	assert.NoError(t, err)
	assert.NotNil(t, ctx)
}

func setupConsumerGroupClaimMock(t *testing.T, claim *mocks.ConsumerGroupClaim, msgChan <-chan *sarama.ConsumerMessage) {
	t.Helper()
	claim.On("Messages").Return(msgChan)
}
