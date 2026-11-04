package kafka

import (
	"math"
	"math/rand/v2"
	"time"
)

// defaultBackoffDuration preserves Sarama's retry numbering, jitter and fallback
// durations, but caps floating-point growth before converting to time.Duration.
// This also handles retry counts large enough for Exp2 to return +Inf.
func defaultBackoffDuration(base, limit time.Duration, retryNumber int) time.Duration {
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if limit <= 0 {
		limit = time.Second
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
