package kafka

import (
	"context"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

type ConsumerMetricsService struct {
	request       *prometheus.CounterVec
	requestFailed *prometheus.CounterVec
	latency       *prometheus.SummaryVec
}

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(appName string) *ConsumerMetricsService {
	var c ConsumerMetricsService
	fieldKeys := []string{"topic", "success"}

	c.request = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "requests_total",
			Help:      "Number of requests processed",
		}, fieldKeys)
	prometheus.MustRegister(c.request)

	c.latency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "requests_latency_milliseconds",
			Help:      "Total duration in milliseconds",
		}, []string{"kafka_topic"})
	prometheus.MustRegister(c.latency)

	return &c
}

// Instrumentation middleware used to add metrics
func (c *ConsumerMetricsService) Instrumentation(next Handler) Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		// add metrics to this method
		defer func(begin time.Time) {
			var success bool
			if err == nil {
				success = true
			}
			c.latency.WithLabelValues(msg.Topic, strconv.FormatBool(success)).Observe(time.Since(begin).Seconds() * 1e3)
			c.request.WithLabelValues(msg.Topic).Inc()
		}(time.Now())

		err = next(ctx, msg)
		return
	}
}
