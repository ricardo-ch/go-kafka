package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	producerRecordSendCounter     *prometheus.CounterVec
	producerDeadletterSendCounter *prometheus.CounterVec
	producerRecordSendLatency     *prometheus.HistogramVec
	producerRecordErrorCounter    *prometheus.CounterVec

	producerMetricsLabel = []string{"kafka_topic"}

	producerSendOnce       sync.Once
	producerDeadletterOnce sync.Once
	producerLatencyOnce    sync.Once
	producerErrorOnce      sync.Once
)

// ProducerMetricsService is a service that provides metrics for the producer.
type ProducerMetricsService struct {
	recordSendCounter           *prometheus.CounterVec
	deadletterRecordSendCounter *prometheus.CounterVec
	recordSendLatency           *prometheus.HistogramVec
	errorCounter                *prometheus.CounterVec
}

func getProducerRecordSendCounter() *prometheus.CounterVec {
	producerSendOnce.Do(func() {
		producerRecordSendCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "record_send_total",
				Help:      "Number of records sent",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerRecordSendCounter)
	})

	return producerRecordSendCounter
}

func getProducerDeadletterSendCounter() *prometheus.CounterVec {
	producerDeadletterOnce.Do(func() {
		producerDeadletterSendCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "dead_letter_created_total",
				Help:      "Number of dead letters created",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerDeadletterSendCounter)
	})

	return producerDeadletterSendCounter
}

func getProducerRecordSendLatency() *prometheus.HistogramVec {
	producerLatencyOnce.Do(func() {
		producerRecordSendLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "record_send_latency_seconds",
				Help:      "Latency of records sent in seconds",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerRecordSendLatency)
	})

	return producerRecordSendLatency
}

func getProducerRecordErrorCounter() *prometheus.CounterVec {
	producerErrorOnce.Do(func() {
		producerRecordErrorCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "record_error_total",
				Help:      "Number of record send errors",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerRecordErrorCounter)
	})

	return producerRecordErrorCounter
}

func NewProducerMetricsService() *ProducerMetricsService {
	return &ProducerMetricsService{
		recordSendCounter: getProducerRecordSendCounter(),
		recordSendLatency: getProducerRecordSendLatency(),
		errorCounter:      getProducerRecordErrorCounter(),
	}
}

func NewDeadletterProducerMetricsService() *ProducerMetricsService {
	return &ProducerMetricsService{
		deadletterRecordSendCounter: getProducerDeadletterSendCounter(),
		recordSendLatency:           getProducerRecordSendLatency(),
		errorCounter:                getProducerRecordErrorCounter(),
	}
}

// Instrumentation is a middleware that provides metrics for the producer.
func (p *ProducerMetricsService) Instrumentation(next producerHandler) producerHandler {
	return func(ctx context.Context, producer *producer, msg *sarama.ProducerMessage) (err error) {
		defer func(begin time.Time) {
			p.recordSendLatency.WithLabelValues(msg.Topic).Observe(time.Since(begin).Seconds())
		}(time.Now())

		err = next(ctx, producer, msg)

		if err != nil {
			p.errorCounter.WithLabelValues(msg.Topic).Inc()
		} else {
			p.recordSendCounter.WithLabelValues(msg.Topic).Inc()
		}

		return
	}
}

// DeadletterInstrumentation is a middleware that provides metrics for a deadletter producer.
func (p *ProducerMetricsService) DeadletterInstrumentation(next producerHandler) producerHandler {
	return func(ctx context.Context, producer *producer, msg *sarama.ProducerMessage) (err error) {
		defer func(begin time.Time) {
			p.recordSendLatency.WithLabelValues(msg.Topic).Observe(time.Since(begin).Seconds())
		}(time.Now())

		err = next(ctx, producer, msg)

		if err != nil {
			p.errorCounter.WithLabelValues(msg.Topic).Inc()
		} else {
			p.deadletterRecordSendCounter.WithLabelValues(msg.Topic).Inc()
		}

		return
	}
}
