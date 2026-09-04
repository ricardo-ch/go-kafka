package kafka

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	timestampTypeLogAppendTime = "LogAppendTime"
	timestampTypeCreateTime    = "CreateTime"
)

var (
	consumerRecordConsumedCounter     *prometheus.CounterVec
	consumerRecordConsumedLatency     *prometheus.HistogramVec
	consumerRecordErrorCounter        *prometheus.CounterVec
	consumerRecordOmittedCounter      *prometheus.CounterVec
	consumerRecordDroppedCounter      *prometheus.CounterVec
	consumerRecordRetryCounter        *prometheus.CounterVec
	consumerRecordForwardedCounter    *prometheus.CounterVec
	consumerRecordForwardRetryCounter *prometheus.CounterVec

	consumergroupCurrentMessageTimestamp *prometheus.GaugeVec

	consumerMetricLabels        = []string{"kafka_topic", "consumer_group"}
	consumerForwardMetricLabels = []string{"kafka_topic", "consumer_group", "type"}

	consumerConsumedOnce     sync.Once
	consumerLatencyOnce      sync.Once
	consumerErrorOnce        sync.Once
	consumerOmittedOnce      sync.Once
	consumerDroppedOnce      sync.Once
	consumerRetryOnce        sync.Once
	consumerForwardedOnce    sync.Once
	consumerForwardRetryOnce sync.Once
	consumerTimestampOnce    sync.Once
)

// ConsumerMetricsService object represents consumer metrics
type ConsumerMetricsService struct {
	groupID string

	recordConsumedCounter     *prometheus.CounterVec
	recordConsumedLatency     *prometheus.HistogramVec
	recordErrorCounter        *prometheus.CounterVec
	recordOmittedCounter      *prometheus.CounterVec
	recordDroppedCounter      *prometheus.CounterVec
	recordRetryCounter        *prometheus.CounterVec
	recordForwardedCounter    *prometheus.CounterVec
	recordForwardRetryCounter *prometheus.CounterVec

	currentMessageTimestamp *prometheus.GaugeVec
}

func getConsumerRecordConsumedCounter() *prometheus.CounterVec {
	consumerConsumedOnce.Do(func() {
		consumerRecordConsumedCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_consumed_total",
				Help:      "Number of records consumed",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordConsumedCounter)
	})

	return consumerRecordConsumedCounter
}

func getConsumerRecordConsumedLatency() *prometheus.HistogramVec {
	consumerLatencyOnce.Do(func() {
		consumerRecordConsumedLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_latency_seconds",
				Help:      "Record processing latency in seconds",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordConsumedLatency)
	})

	return consumerRecordConsumedLatency
}

func getConsumerRecordErrorCounter() *prometheus.CounterVec {
	consumerErrorOnce.Do(func() {
		consumerRecordErrorCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_error_total",
				Help:      "Number of non-omitted records whose processing ended with an error",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordErrorCounter)
	})

	return consumerRecordErrorCounter
}

func getConsumerRecordOmittedCounter() *prometheus.CounterVec {
	consumerOmittedOnce.Do(func() {
		consumerRecordOmittedCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_omitted_total",
				Help:      "Number of records omitted by handler",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordOmittedCounter)
	})

	return consumerRecordOmittedCounter
}

func getConsumerRecordDroppedCounter() *prometheus.CounterVec {
	consumerDroppedOnce.Do(func() {
		consumerRecordDroppedCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_dropped_total",
				Help:      "Number of records dropped without retry or deadletter forwarding",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordDroppedCounter)
	})

	return consumerRecordDroppedCounter
}

func getConsumerRecordRetryCounter() *prometheus.CounterVec {
	consumerRetryOnce.Do(func() {
		consumerRecordRetryCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_retry_total",
				Help:      "Number of handler retry attempts",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordRetryCounter)
	})

	return consumerRecordRetryCounter
}

func getConsumerRecordForwardedCounter() *prometheus.CounterVec {
	consumerForwardedOnce.Do(func() {
		consumerRecordForwardedCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_forwarded_total",
				Help:      "Number of records forwarded after processing failure",
			}, consumerForwardMetricLabels)
		prometheus.MustRegister(consumerRecordForwardedCounter)
	})

	return consumerRecordForwardedCounter
}

func getConsumerRecordForwardRetryCounter() *prometheus.CounterVec {
	consumerForwardRetryOnce.Do(func() {
		consumerRecordForwardRetryCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_forward_retry_total",
				Help:      "Number of retries after a failed forward",
			}, consumerForwardMetricLabels)
		prometheus.MustRegister(consumerRecordForwardRetryCounter)
	})

	return consumerRecordForwardRetryCounter
}

func getConsumerCurrentMessageTimestamp() *prometheus.GaugeVec {
	consumerTimestampOnce.Do(func() {
		consumergroupCurrentMessageTimestamp = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka",
				Subsystem: "consumergroup",
				Name:      "current_message_timestamp",
				Help:      "Current message timestamp",
			}, []string{"kafka_topic", "consumer_group", "partition", "type"})
		prometheus.MustRegister(consumergroupCurrentMessageTimestamp)
	})

	return consumergroupCurrentMessageTimestamp
}

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(groupID string) *ConsumerMetricsService {
	return &ConsumerMetricsService{
		groupID:                   groupID,
		recordConsumedCounter:     getConsumerRecordConsumedCounter(),
		recordConsumedLatency:     getConsumerRecordConsumedLatency(),
		recordErrorCounter:        getConsumerRecordErrorCounter(),
		recordOmittedCounter:      getConsumerRecordOmittedCounter(),
		recordDroppedCounter:      getConsumerRecordDroppedCounter(),
		recordRetryCounter:        getConsumerRecordRetryCounter(),
		recordForwardedCounter:    getConsumerRecordForwardedCounter(),
		recordForwardRetryCounter: getConsumerRecordForwardRetryCounter(),
		currentMessageTimestamp:   getConsumerCurrentMessageTimestamp(),
	}
}

// Instrumentation middleware used to add metrics
func (c *ConsumerMetricsService) Instrumentation(next Handler) Handler {
	return Handler{
		Processor: func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
			// If sarama sets the timestamp to the block timestamp, it means that the message was
			// produced with the LogAppendTime timestamp type. Otherwise, it was produced with the
			// CreateTime timestamp type.
			// Since sarama anyways sets msg.BlockTimestamp to the block timestamp,
			// we can compare it with msg.Timestamp to know if the message was produced with the
			// LogAppendTime timestamp type or not.
			timestampType := timestampTypeLogAppendTime
			if msg.Timestamp != msg.BlockTimestamp {
				timestampType = timestampTypeCreateTime
			}
			c.currentMessageTimestamp.WithLabelValues(msg.Topic, c.groupID, strconv.FormatInt(int64(msg.Partition), 10), timestampType).Set(float64(msg.Timestamp.Unix()))

			return next.Processor(ctx, msg)
		},
		Config: next.Config,
	}
}

func (c *ConsumerMetricsService) observeProcessingLatency(msg *sarama.ConsumerMessage, begin time.Time) {
	c.recordConsumedLatency.WithLabelValues(msg.Topic, c.groupID).Observe(time.Since(begin).Seconds())
}
