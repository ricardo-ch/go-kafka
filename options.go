package kafka

// WithInstrumenting adds the instrumenting layer on a listener.
// Handlers are wrapped once at creation time rather than per-message.
func WithInstrumenting() ListenerOption {
	return func(l *listener) {
		l.instrumenting = NewConsumerMetricsService(l.groupID)
		for topic, handler := range l.handlers {
			l.handlers[topic] = l.instrumenting.Instrumentation(handler)
		}
	}
}

// ProducerOption is a function that is passed to the producer constructor to configure it.
type ProducerOption func(p *producer)

// WithProducerInstrumenting adds the instrumenting layer on a producer.
func WithProducerInstrumenting() ProducerOption {
	return func(p *producer) {
		p.instrumenting = NewProducerMetricsService()
		p.handler = p.instrumenting.Instrumentation(p.handler)
	}
}

// WithDeadletterProducerInstrumenting adds the instrumenting layer on a deadletter producer.
func WithDeadletterProducerInstrumenting() ProducerOption {
	return func(p *producer) {
		p.instrumenting = NewDeadletterProducerMetricsService()
		p.handler = p.instrumenting.DeadletterInstrumentation(p.handler)
	}
}
