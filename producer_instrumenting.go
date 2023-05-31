package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	producerRecordSendCounter     *prometheus.CounterVec
	producerDeadletterSendCounter *prometheus.CounterVec
	producerRecordSendLatency     *prometheus.HistogramVec
	producerRecordErrorCounter    *prometheus.CounterVec

	producerMetricsMutex = &sync.Mutex{}
	producerMetricsLabel = []string{"kafka_topic"}
)

// ProducerMetricsService is a service that provides metrics for the producer.
type ProducerMetricsService struct {
	recordSendCounter           *prometheus.CounterVec
	deadletterRecordSendCounter *prometheus.CounterVec
	recordSendLatency           *prometheus.HistogramVec
	errorCounter                *prometheus.CounterVec
}

func getPrometheusRecordSendInstrumentation() *prometheus.CounterVec {
	if producerRecordSendCounter != nil {
		return producerRecordSendCounter
	}

	producerMetricsMutex.Lock()
	defer producerMetricsMutex.Unlock()

	if producerRecordSendCounter == nil {
		producerRecordSendCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "record_send_total",
				Help:      "Number of records sent",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerRecordSendCounter)
	}

	return producerRecordSendCounter
}

func getPrometheusDeadletterRecordSendInstrumentation() *prometheus.CounterVec {
	if producerDeadletterSendCounter != nil {
		return producerDeadletterSendCounter
	}

	producerMetricsMutex.Lock()
	defer producerMetricsMutex.Unlock()

	if producerDeadletterSendCounter == nil {
		producerDeadletterSendCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "dead_letter_created_total",
				Help:      "Number of dead letter created",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerDeadletterSendCounter)
	}

	return producerDeadletterSendCounter
}

func getPrometheusRecordSendLatencyInstrumentation() *prometheus.HistogramVec {
	if producerRecordSendLatency != nil {
		return producerRecordSendLatency
	}

	producerMetricsMutex.Lock()
	defer producerMetricsMutex.Unlock()

	if producerRecordSendLatency == nil {
		producerRecordSendLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "record_send_latency_seconds",
				Help:      "Latency of records sent",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerRecordSendLatency)
	}

	return producerRecordSendLatency
}

func getPrometheusSendErrorInstrumentation() *prometheus.CounterVec {
	if producerRecordErrorCounter != nil {
		return producerRecordErrorCounter
	}

	producerMetricsMutex.Lock()
	defer producerMetricsMutex.Unlock()

	if producerRecordErrorCounter == nil {
		producerRecordErrorCounter = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "kafka",
				Subsystem: "producer",
				Name:      "record_error_total",
				Help:      "Number of records send error",
			}, producerMetricsLabel)
		prometheus.MustRegister(producerRecordErrorCounter)
	}

	return producerRecordErrorCounter
}

func NewProducerMetricsService() *ProducerMetricsService {
	return &ProducerMetricsService{
		recordSendCounter: getPrometheusRecordSendInstrumentation(),
		recordSendLatency: getPrometheusRecordSendLatencyInstrumentation(),
		errorCounter:      getPrometheusSendErrorInstrumentation(),
	}
}

func NewDeadletterProducerMetricsService() *ProducerMetricsService {
	return &ProducerMetricsService{
		deadletterRecordSendCounter: getPrometheusDeadletterRecordSendInstrumentation(),
		recordSendLatency:           getPrometheusRecordSendLatencyInstrumentation(),
		errorCounter:                getPrometheusSendErrorInstrumentation(),
	}
}

// Instrumentation is a middleware that provides metrics for the producer.
func (p *ProducerMetricsService) Instrumentation(next ProducerHandler) ProducerHandler {
	return func(producer *producer, msg *sarama.ProducerMessage) (err error) {
		defer func(begin time.Time) {
			p.recordSendLatency.WithLabelValues(msg.Topic).Observe(time.Since(begin).Seconds())
		}(time.Now())

		err = next(producer, msg)

		if err != nil {
			p.errorCounter.WithLabelValues(msg.Topic).Inc()
		} else {
			p.recordSendCounter.WithLabelValues(msg.Topic).Inc()
		}
		return
	}
}

// DeadletterInstrumentation is a middleware that provides metrics for a deadletter producer.
func (p *ProducerMetricsService) DeadletterInstrumentation(next ProducerHandler) ProducerHandler {
	return func(producer *producer, msg *sarama.ProducerMessage) (err error) {
		defer func(begin time.Time) {
			p.recordSendLatency.WithLabelValues(msg.Topic).Observe(time.Since(begin).Seconds())
		}(time.Now())

		err = next(producer, msg)

		if err != nil {
			p.errorCounter.WithLabelValues(msg.Topic).Inc()
		} else {
			p.deadletterRecordSendCounter.WithLabelValues(msg.Topic).Inc()
		}
		return
	}
}
