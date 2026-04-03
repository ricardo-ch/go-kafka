package kafka

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_NewUnretriableError_wraps_error(t *testing.T) {
	original := errors.New("validation failed")
	wrapped := NewUnretriableError(original)

	assert.Equal(t, "validation failed", wrapped.Error())
	assert.True(t, errors.Is(wrapped, original))
}

func Test_NewUnretriableError_nil_returns_nil(t *testing.T) {
	assert.Nil(t, NewUnretriableError(nil))
}

func Test_NewUnretriableError_unwrap(t *testing.T) {
	original := errors.New("root cause")
	wrapped := NewUnretriableError(original)

	var unwrapper interface{ Unwrap() error }
	assert.True(t, errors.As(wrapped, &unwrapper))
	assert.Equal(t, original, unwrapper.Unwrap())
}

func Test_NewOmittedError_wraps_error(t *testing.T) {
	original := errors.New("outdated event")
	wrapped := NewOmittedError(original)

	assert.Equal(t, "outdated event", wrapped.Error())
	assert.True(t, errors.Is(wrapped, original))
}

func Test_NewOmittedError_nil_returns_nil(t *testing.T) {
	assert.Nil(t, NewOmittedError(nil))
}

func Test_NewOmittedError_unwrap(t *testing.T) {
	original := errors.New("root cause")
	wrapped := NewOmittedError(original)

	var unwrapper interface{ Unwrap() error }
	assert.True(t, errors.As(wrapped, &unwrapper))
	assert.Equal(t, original, unwrapper.Unwrap())
}

func Test_ErrEventUnretriable_is_unretriable(t *testing.T) {
	assert.True(t, isUnretriableError(ErrEventUnretriable))
	assert.False(t, isOmittedError(ErrEventUnretriable))
	assert.False(t, isRetriableError(ErrEventUnretriable))
}

func Test_ErrEventOmitted_is_omitted(t *testing.T) {
	assert.True(t, isOmittedError(ErrEventOmitted))
	assert.False(t, isUnretriableError(ErrEventOmitted))
	assert.False(t, isRetriableError(ErrEventOmitted))
}

func Test_isUnretriableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("fail"), false},
		{"NewUnretriableError", NewUnretriableError(errors.New("bad")), true},
		{"sentinel ErrEventUnretriable", ErrEventUnretriable, true},
		{"fmt.Errorf wrapped unretriable", fmt.Errorf("context: %w", NewUnretriableError(errors.New("x"))), true},
		{"double wrapped", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", NewUnretriableError(errors.New("x")))), true},
		{"joined with sentinel", fmt.Errorf("%w: %w", errors.New("msg"), ErrEventUnretriable), true},
		{"omitted is not unretriable", NewOmittedError(errors.New("skip")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isUnretriableError(tc.err))
		})
	}
}

func Test_isOmittedError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil", nil, false},
		{"plain error", errors.New("fail"), false},
		{"NewOmittedError", NewOmittedError(errors.New("dup")), true},
		{"sentinel ErrEventOmitted", ErrEventOmitted, true},
		{"fmt.Errorf wrapped omitted", fmt.Errorf("context: %w", NewOmittedError(errors.New("x"))), true},
		{"double wrapped", fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", NewOmittedError(errors.New("x")))), true},
		{"joined with sentinel", fmt.Errorf("%w: %w", errors.New("msg"), ErrEventOmitted), true},
		{"unretriable is not omitted", NewUnretriableError(errors.New("bad")), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isOmittedError(tc.err))
		})
	}
}

func Test_isRetriableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"plain error is retriable", errors.New("transient"), true},
		{"wrapped plain error", fmt.Errorf("wrap: %w", errors.New("transient")), true},
		{"unretriable is not retriable", NewUnretriableError(errors.New("x")), false},
		{"omitted is not retriable", NewOmittedError(errors.New("x")), false},
		{"sentinel unretriable", ErrEventUnretriable, false},
		{"sentinel omitted", ErrEventOmitted, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isRetriableError(tc.err))
		})
	}
}

type myUnretriable struct{ msg string }

func (e myUnretriable) Error() string       { return e.msg }
func (e myUnretriable) IsUnretriable() bool { return true }

type myOmitted struct{ msg string }

func (e myOmitted) Error() string   { return e.msg }
func (e myOmitted) IsOmitted() bool { return true }

func Test_isUnretriableError_custom_type(t *testing.T) {
	err := myUnretriable{msg: "custom business error"}
	assert.True(t, isUnretriableError(err))
	assert.False(t, isOmittedError(err))
	assert.False(t, isRetriableError(err))
}

func Test_isOmittedError_custom_type(t *testing.T) {
	err := myOmitted{msg: "custom omitted error"}
	assert.True(t, isOmittedError(err))
	assert.False(t, isUnretriableError(err))
	assert.False(t, isRetriableError(err))
}

func Test_custom_unretriable_wrapped_in_fmt_Errorf(t *testing.T) {
	err := fmt.Errorf("service layer: %w", myUnretriable{msg: "bad input"})
	assert.True(t, isUnretriableError(err))
}

func Test_custom_omitted_wrapped_in_fmt_Errorf(t *testing.T) {
	err := fmt.Errorf("service layer: %w", myOmitted{msg: "skip this"})
	assert.True(t, isOmittedError(err))
}
