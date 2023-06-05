package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	consumerRecordConsumedCounter *prometheus.CounterVec
	consumerRecordConsumedLatency *prometheus.HistogramVec
	consumerRecordErrorCounter    *prometheus.CounterVec
	consumerRecordOmittedCounter  *prometheus.CounterVec

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
				Name:      "records_consumed_total",
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
				Name:      "record_consumed_latency_seconds",
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

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(groupID string) *ConsumerMetricsService {
	return &ConsumerMetricsService{
		groupID:               groupID,
		recordConsumedCounter: getPrometheusRecordConsumedInstrumentation(),
		recordConsumedLatency: getPrometheusRecordConsumedLatencyInstrumentation(),
		recordErrorCounter:    getPrometheusRecordConsumedErrorInstrumentation(),
		recordOmittedCounter:  getPrometheusRecordOmittedInstrumentation(),
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
		}
		return
	}
}
