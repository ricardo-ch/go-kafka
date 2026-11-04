package kafka

import (
	"math"
	"math/rand/v2"
	"time"
)

// defaultBackoffDuration preserves Sarama's retry numbering and jitter, uses
// go-kafka defaults for invalid durations, and caps floating-point growth before
// converting to time.Duration.
// This also handles retry counts large enough for Exp2 to return +Inf.
func defaultBackoffDuration(base, limit time.Duration, retryNumber int) time.Duration {
	if base <= 0 {
		base = defaultDurationBeforeRetry
	}
	if limit <= 0 {
		limit = defaultMaxBackoffDuration
	}
	base = min(base, limit)
	if retryNumber <= 0 {
		return base
	}

	delay := float64(base) * math.Exp2(float64(retryNumber-1))
	delay *= 0.8 + 0.4*rand.Float64()
	if delay >= float64(limit) {
		return limit
	}

	// Sub-nanosecond jitter must not turn a positive base into a zero delay.
	return max(time.Nanosecond, time.Duration(delay))
}
