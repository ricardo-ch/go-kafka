package kafka

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

type ConsumerMetricsService struct {
	request       *prometheus.CounterVec
	requestFailed *prometheus.CounterVec
	latency       *prometheus.SummaryVec
	appName       string
}

// NewConsumerMetricsService creates a layer of service that add metrics capability
func NewConsumerMetricsService(appName string) *ConsumerMetricsService {
	var c ConsumerMetricsService
	c.appName = appName
	fieldKeys := []string{"app_name", "topic"}

	c.request = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "total_request",
			Help:      "Number of requests processed",
		}, fieldKeys)
	prometheus.MustRegister(c.request)

	c.requestFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "total_request_failed",
			Help:      "Number of requests failed",
		}, fieldKeys)
	prometheus.MustRegister(c.requestFailed)

	c.latency = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: "kafka",
			Subsystem: "consumer",
			Name:      "request_latency_milliseconds",
			Help:      "Total duration in milliseconds",
		}, fieldKeys)
	prometheus.MustRegister(c.latency)

	return &c
}

// Instrumentation middleware used to add metrics
func (c *ConsumerMetricsService) Instrumentation(next Handler) Handler {
	return func(ctx context.Context, msg *sarama.ConsumerMessage) (err error) {
		// add metrics to this method
		defer func(begin time.Time) {
			c.latency.WithLabelValues(c.appName, msg.Topic).Observe(time.Since(begin).Seconds() * 1e3)
			c.request.WithLabelValues(c.appName, msg.Topic).Inc()
			// If error is not empty, we add to metrics that it failed
			if err != nil {
				c.requestFailed.WithLabelValues(c.appName, msg.Topic).Inc()
			}
		}(time.Now())

		err = next(ctx, msg)
		return
	}
}
