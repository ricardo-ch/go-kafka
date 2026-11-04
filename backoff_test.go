package kafka

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_defaultBackoffDuration(t *testing.T) {
	t.Parallel()

	type Inputs struct {
		base        time.Duration
		limit       time.Duration
		retryNumber int
	}
	type Calls struct{}
	type Outputs struct {
		minimum time.Duration
		maximum time.Duration
	}
	type TestCase struct {
		name    string
		inputs  Inputs
		calls   Calls
		outputs Outputs
	}

	testCases := []TestCase{
		{name: "First attempt uses the base without jitter", inputs: Inputs{2 * time.Second, 10 * time.Minute, 0}, outputs: Outputs{2 * time.Second, 2 * time.Second}},
		{name: "Negative retry count uses the base", inputs: Inputs{2 * time.Second, 10 * time.Minute, -1}, outputs: Outputs{2 * time.Second, 2 * time.Second}},
		{name: "Minimum integer retry count uses the base", inputs: Inputs{2 * time.Second, 10 * time.Minute, math.MinInt}, outputs: Outputs{2 * time.Second, 2 * time.Second}},
		{name: "Second attempt preserves base delay with jitter", inputs: Inputs{2 * time.Second, 10 * time.Minute, 1}, outputs: Outputs{1600 * time.Millisecond, 2400 * time.Millisecond}},
		{name: "Third attempt doubles with jitter", inputs: Inputs{2 * time.Second, 10 * time.Minute, 2}, outputs: Outputs{3200 * time.Millisecond, 4800 * time.Millisecond}},
		{name: "Jitter can cross the cap", inputs: Inputs{2 * time.Second, 10 * time.Minute, 9}, outputs: Outputs{409600 * time.Millisecond, 10 * time.Minute}},
		{name: "Growth saturates at the cap", inputs: Inputs{2 * time.Second, 10 * time.Minute, 10}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Before duration multiplication overflow", inputs: Inputs{2 * time.Second, 10 * time.Minute, 33}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Duration multiplication overflow boundary", inputs: Inputs{2 * time.Second, 10 * time.Minute, 34}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Signed shift overflow boundary", inputs: Inputs{time.Nanosecond, 10 * time.Minute, 64}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Shift overflow boundary", inputs: Inputs{time.Nanosecond, 10 * time.Minute, 65}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Floating point exponent overflow", inputs: Inputs{time.Nanosecond, 10 * time.Minute, 1025}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Billing engine production retry count", inputs: Inputs{2 * time.Second, 10 * time.Minute, 1014210}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Maximum integer retry count", inputs: Inputs{2 * time.Second, 10 * time.Minute, math.MaxInt}, outputs: Outputs{10 * time.Minute, 10 * time.Minute}},
		{name: "Maximum duration cannot overflow during conversion", inputs: Inputs{time.Duration(math.MaxInt64), time.Duration(math.MaxInt64), 3}, outputs: Outputs{time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)}},
		{name: "Base larger than cap is bounded on first attempt", inputs: Inputs{2 * time.Second, time.Second, 0}, outputs: Outputs{time.Second, time.Second}},
		{name: "Base larger than cap is bounded with jitter", inputs: Inputs{2 * time.Second, time.Second, 1}, outputs: Outputs{800 * time.Millisecond, time.Second}},
		{name: "Nonpositive base preserves Sarama fallback", inputs: Inputs{0, time.Second, 0}, outputs: Outputs{100 * time.Millisecond, 100 * time.Millisecond}},
		{name: "Negative base preserves Sarama fallback", inputs: Inputs{-time.Second, time.Second, 1}, outputs: Outputs{80 * time.Millisecond, 120 * time.Millisecond}},
		{name: "Nonpositive cap preserves Sarama fallback", inputs: Inputs{2 * time.Second, 0, 0}, outputs: Outputs{time.Second, time.Second}},
		{name: "Negative cap preserves Sarama fallback", inputs: Inputs{2 * time.Second, -time.Second, math.MaxInt}, outputs: Outputs{time.Second, time.Second}},
		{name: "Nanosecond base stays positive with jitter", inputs: Inputs{time.Nanosecond, time.Second, 1}, outputs: Outputs{time.Nanosecond, time.Nanosecond}},
		{name: "Nanosecond cap stays positive with jitter", inputs: Inputs{time.Second, time.Nanosecond, 1}, outputs: Outputs{time.Nanosecond, time.Nanosecond}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			for range 100 {
				delay := defaultBackoffDuration(testCase.inputs.base, testCase.inputs.limit, testCase.inputs.retryNumber)
				assert.GreaterOrEqual(t, delay, testCase.outputs.minimum)
				assert.LessOrEqual(t, delay, testCase.outputs.maximum)
			}
		})
	}
}
