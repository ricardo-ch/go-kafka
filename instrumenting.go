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
	request      *prometheus.CounterVec
	latency      *prometheus.SummaryVec
	requestMutex = &sync.Mutex{}
	latencyMutex = &sync.Mutex{}
	metricLabels = []string{"kafka_topic", "success", "group_id"}
)

type prometheusSummaryVec interface {
	WithLabelValues(lvs ...string) prometheus.Observer
}

type prometheusCounterVec interface {
	WithLabelValues(lvs ...string) prometheus.Counter
}

// ConsumerMetricsService object represents consumer metrics
type ConsumerMetricsService struct {
	request prometheusCounterVec
	latency prometheusSummaryVec
	groupID string
}

func getPrometheusRequestInstrumentation() prometheusCounterVec {
	if request != nil {
		return request
	}

	requestMutex.Lock()
	defer requestMutex.Unlock()
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

func getPrometheusLatencyInstrumentation() prometheusSummaryVec {
	if latency != nil {
		return latency
	}

	latencyMutex.Lock()
	defer latencyMutex.Unlock()
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

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(groupID string) *ConsumerMetricsService {
	var c ConsumerMetricsService
	c.groupID = groupID

	c.request = getPrometheusRequestInstrumentation()
	c.latency = getPrometheusLatencyInstrumentation()

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
