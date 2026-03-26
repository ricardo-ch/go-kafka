package kafka

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_kafkaMessageInfo_LogValue_AllFields(t *testing.T) {
	info := kafkaMessageInfo{
		Topic:         "orders",
		Partition:     3,
		Offset:        42,
		Key:           "abc-123",
		ConsumerGroup: "my-group",
	}

	val := info.LogValue()
	assert.Equal(t, slog.KindGroup, val.Kind())

	attrs := val.Group()
	attrMap := make(map[string]slog.Value, len(attrs))
	for _, a := range attrs {
		attrMap[a.Key] = a.Value
	}

	assert.Equal(t, "orders", attrMap["topic"].String())
	assert.Equal(t, "my-group", attrMap["consumer_group"].String())
	assert.Equal(t, int64(3), attrMap["partition"].Int64())
	assert.Equal(t, int64(42), attrMap["offset"].Int64())
	assert.Equal(t, "abc-123", attrMap["key"].String())
}

func Test_kafkaMessageInfo_LogValue_EmptyFields(t *testing.T) {
	info := kafkaMessageInfo{}

	val := info.LogValue()
	attrs := val.Group()

	assert.Empty(t, attrs, "empty kafkaMessageInfo should produce no attributes")
}

func Test_kafkaMessageInfo_LogValue_ZeroPartitionWithTopic(t *testing.T) {
	info := kafkaMessageInfo{
		Topic:     "events",
		Partition: 0,
		Offset:    0,
	}

	val := info.LogValue()
	attrs := val.Group()
	attrMap := make(map[string]slog.Value, len(attrs))
	for _, a := range attrs {
		attrMap[a.Key] = a.Value
	}

	assert.Contains(t, attrMap, "topic")
	assert.Contains(t, attrMap, "partition", "partition=0 should be included when topic is set")
	assert.Contains(t, attrMap, "offset", "offset=0 should be included when topic is set")
}

func Test_kafkaMessageInfo_LogValue_PartialFields(t *testing.T) {
	info := kafkaMessageInfo{
		Topic:         "orders",
		ConsumerGroup: "my-group",
	}

	val := info.LogValue()
	attrs := val.Group()
	attrMap := make(map[string]slog.Value, len(attrs))
	for _, a := range attrs {
		attrMap[a.Key] = a.Value
	}

	assert.Contains(t, attrMap, "topic")
	assert.Contains(t, attrMap, "consumer_group")
	assert.NotContains(t, attrMap, "key", "empty key should be omitted")
}

func Test_WithLogContextStorer(t *testing.T) {
	called := false
	storer := func(ctx context.Context, logger *slog.Logger) context.Context {
		called = true
		return ctx
	}

	l := &listener{}
	opt := WithLogContextStorer(storer)
	opt(l)

	assert.NotNil(t, l.logContextStorer)

	l.logContextStorer(context.Background(), slog.Default())
	assert.True(t, called)
}
