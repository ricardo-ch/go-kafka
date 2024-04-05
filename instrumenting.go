package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	TimestampTypeLogAppendTime = "LogAppendTime"
	TimestampTypeCreateTime    = "CreateTime"
)

var (
	consumerRecordConsumedCounter *prometheus.CounterVec
	consumerRecordConsumedLatency *prometheus.HistogramVec
	consumerRecordErrorCounter    *prometheus.CounterVec
	consumerRecordOmittedCounter  *prometheus.CounterVec

	consumergroupCurrentMessageTimestamp *prometheus.GaugeVec

	consumerMetricsMutex = &sync.Mutex{}
	consumerMetricLabels = []string{"kafka_topic", "consumer_group"}
)

// ConsumerMetricsService object represents consumer metrics
type ConsumerMetricsService struct {
	groupID string

	recordConsumedCounter *prometheus.CounterVec
	recordConsumedLatency *prometheus.HistogramVec
	recordErrorCounter    *prometheus.CounterVec
	recordOmittedCounter  *prometheus.CounterVec

	currentMessageTimestamp *prometheus.GaugeVec
}

func getPrometheusRecordConsumedInstrumentation() *prometheus.CounterVec {
	if consumerRecordConsumedCounter != nil {
		return consumerRecordConsumedCounter
	}

	consumerMetricsMutex.Lock()
	defer consumerMetricsMutex.Unlock()
	if consumerRecordConsumedCounter == nil {
		consumerRecordConsumedCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_consumed_total",
				Help:      "Number of records consumed",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordConsumedCounter)
	}

	return consumerRecordConsumedCounter
}

func getPrometheusRecordConsumedLatencyInstrumentation() *prometheus.HistogramVec {
	if consumerRecordConsumedLatency != nil {
		return consumerRecordConsumedLatency
	}

	consumerMetricsMutex.Lock()
	defer consumerMetricsMutex.Unlock()
	if consumerRecordConsumedLatency == nil {
		consumerRecordConsumedLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_latency_seconds",
				Help:      "Total duration in milliseconds",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordConsumedLatency)
	}

	return consumerRecordConsumedLatency
}

func getPrometheusRecordConsumedErrorInstrumentation() *prometheus.CounterVec {
	if consumerRecordErrorCounter != nil {
		return consumerRecordErrorCounter
	}

	consumerMetricsMutex.Lock()
	defer consumerMetricsMutex.Unlock()
	if consumerRecordErrorCounter == nil {
		consumerRecordErrorCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_error_total",
				Help:      "Number of requests dropped",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordErrorCounter)
	}

	return consumerRecordErrorCounter
}

func getPrometheusRecordOmittedInstrumentation() *prometheus.CounterVec {
	if consumerRecordOmittedCounter != nil {
		return consumerRecordOmittedCounter
	}

	consumerMetricsMutex.Lock()
	defer consumerMetricsMutex.Unlock()
	if consumerRecordOmittedCounter == nil {
		consumerRecordOmittedCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "record_omitted_total",
				Help:      "Number of requests dropped",
			}, consumerMetricLabels)
		prometheus.MustRegister(consumerRecordOmittedCounter)
	}

	return consumerRecordOmittedCounter
}

func getPrometheusCurrentMessageTimestampInstrumentation() *prometheus.GaugeVec {
	if consumergroupCurrentMessageTimestamp != nil {
		return consumergroupCurrentMessageTimestamp
	}

	consumerMetricsMutex.Lock()
	defer consumerMetricsMutex.Unlock()
	if consumergroupCurrentMessageTimestamp == nil {
		consumergroupCurrentMessageTimestamp = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "kafka",
				Subsystem: "consumergroup",
				Name:      "current_message_timestamp",
				Help:      "Current message timestamp",
			}, []string{"kafka_topic", "consumer_group", "partition", "type"})
		prometheus.MustRegister(consumergroupCurrentMessageTimestamp)
	}

	return consumergroupCurrentMessageTimestamp
}

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(groupID string) *ConsumerMetricsService {
	return &ConsumerMetricsService{
		groupID:                 groupID,
		recordConsumedCounter:   getPrometheusRecordConsumedInstrumentation(),
		recordConsumedLatency:   getPrometheusRecordConsumedLatencyInstrumentation(),
		recordErrorCounter:      getPrometheusRecordConsumedErrorInstrumentation(),
		recordOmittedCounter:    getPrometheusRecordOmittedInstrumentation(),
		currentMessageTimestamp: getPrometheusCurrentMessageTimestampInstrumentation(),
	}
}

// Instrumentation middleware used to add metrics
func (c *ConsumerMetricsService) Instrumentation(next Handler) Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		defer func(begin time.Time) {
			c.recordConsumedLatency.WithLabelValues(msg.Topic, c.groupID).Observe(time.Since(begin).Seconds())
		}(time.Now())

		err = next(ctx, msg)
		if err == nil {
			c.recordConsumedCounter.WithLabelValues(msg.Topic, c.groupID).Inc()

			// If sarama sets the timestamp to the block timestamp, it means that the message was
			// produced with the LogAppendTime timestamp type. Otherwise, it was produced with the
			// CreateTime timestamp type.
			// Since sarama anyways sets msg.BlockTimestamp to the block timestamp,
			// we can compare it with msg.Timestamp to know if the message was produced with the
			// LogAppendTime timestamp type or not.
			timestampType := TimestampTypeLogAppendTime
			if msg.Timestamp != msg.BlockTimestamp {
				timestampType = TimestampTypeCreateTime
			}
			c.currentMessageTimestamp.WithLabelValues(msg.Topic, c.groupID, string(msg.Partition), timestampType).Set(float64(msg.Timestamp.Unix()))
		}
		return
	}
}
