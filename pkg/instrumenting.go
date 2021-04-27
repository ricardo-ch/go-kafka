package kafka

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	request        *prometheus.CounterVec
	latency        *prometheus.SummaryVec
	droppedRequest *prometheus.CounterVec
	metricsMutex   = &sync.Mutex{}
	metricLabels   = []string{"kafka_topic", "success", "group_id"}
)

// ConsumerMetricsService object represents consumer metrics
type ConsumerMetricsService struct {
	request        *prometheus.CounterVec
	latency        *prometheus.SummaryVec
	droppedRequest *prometheus.CounterVec
	groupID        string
}

func getPrometheusRequestInstrumentation() *prometheus.CounterVec {
	if request != nil {
		return request
	}

	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	if request == nil {
		request = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "requests_total",
				Help:      "Number of requests processed",
			}, metricLabels)
		prometheus.MustRegister(request)
	}

	return request
}

func getPrometheusLatencyInstrumentation() *prometheus.SummaryVec {
	if latency != nil {
		return latency
	}

	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	if latency == nil {
		latency = prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "requests_latency_milliseconds",
				Help:      "Total duration in milliseconds",
			}, metricLabels)
		prometheus.MustRegister(latency)
	}

	return latency
}

func getPrometheusDroppedRequestInstrumentation() *prometheus.CounterVec {
	if droppedRequest != nil {
		return droppedRequest
	}

	metricsMutex.Lock()
	defer metricsMutex.Unlock()
	if droppedRequest == nil {
		droppedRequest = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "consumer",
				Name:      "requests_dropped_total",
				Help:      "Number of requests dropped",
			}, []string{"kafka_topic", "group_id"})
		prometheus.MustRegister(droppedRequest)
	}

	return droppedRequest
}

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(groupID string) *ConsumerMetricsService {
	var c ConsumerMetricsService
	c.groupID = groupID

	c.request = getPrometheusRequestInstrumentation()
	c.latency = getPrometheusLatencyInstrumentation()
	c.droppedRequest = getPrometheusDroppedRequestInstrumentation()

	return &c
}

// Instrumentation middleware used to add metrics
func (c *ConsumerMetricsService) Instrumentation(next Handler) Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		// add metrics to this method
		defer func(begin time.Time) {
			success := strconv.FormatBool(err == nil)
			c.latency.WithLabelValues(msg.Topic, success, c.groupID).Observe(time.Since(begin).Seconds() * 1e3)
			c.request.WithLabelValues(msg.Topic, success, c.groupID).Inc()
		}(time.Now())

		err = next(ctx, msg)
		return
	}
}
