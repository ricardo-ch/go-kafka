package kafka

import "errors"

var (
	// ErrRetryTopicCollision is returned when a handler's retry topic is the same as the consumed topic.
	ErrRetryTopicCollision = errors.New("retry topic cannot be the same as the original topic")

	// ErrDeadletterTopicCollision is returned when a handler's deadletter topic is the same as the consumed topic.
	ErrDeadletterTopicCollision = errors.New("deadletter topic cannot be the same as the original topic")
)

// UnretriableError is an interface for errors that should not be retried.
// Implement IsUnretriable() bool on your custom errors to mark them as non-retriable.
type UnretriableError interface {
	error
	IsUnretriable() bool
}

// OmittedError is an interface for errors that should be omitted without impacting metrics.
// Implement IsOmitted() bool on your custom errors to mark them as omitted.
type OmittedError interface {
	error
	IsOmitted() bool
}

// wrappedUnretriableError wraps an error and marks it as unretriable.
type wrappedUnretriableError struct {
	err error
}

func (e wrappedUnretriableError) Error() string       { return e.err.Error() }
func (e wrappedUnretriableError) IsUnretriable() bool { return true }
func (e wrappedUnretriableError) Unwrap() error       { return e.err }

// ErrEventUnretriable is a sentinel error for events that should not be retried.
// Prefer using NewUnretriableError(err) or implementing the UnretriableError interface.
var ErrEventUnretriable error = wrappedUnretriableError{err: errors.New("the event will not be retried")}

// wrappedOmittedError wraps an error and marks it as omitted.
type wrappedOmittedError struct {
	err error
}

func (e wrappedOmittedError) Error() string   { return e.err.Error() }
func (e wrappedOmittedError) IsOmitted() bool { return true }
func (e wrappedOmittedError) Unwrap() error   { return e.err }

// ErrEventOmitted is a sentinel error for events that should be omitted.
// Prefer using NewOmittedError(err) or implementing the OmittedError interface.
var ErrEventOmitted error = wrappedOmittedError{err: errors.New("the event will be omitted")}

// NewUnretriableError wraps an error to mark it as unretriable.
// The wrapped error will not be retried by the consumer.
//
// Example:
//
//	return kafka.NewUnretriableError(errors.New("invalid payload"))
//	return kafka.NewUnretriableError(fmt.Errorf("validation failed: %w", err))
func NewUnretriableError(err error) error {
	if err == nil {
		return nil
	}
	return wrappedUnretriableError{err: err}
}

// NewOmittedError wraps an error to mark it as omitted.
// The wrapped error will be omitted without impacting metrics or being sent to retry/deadletter topics.
//
// Example:
//
//	return kafka.NewOmittedError(errors.New("outdated event"))
//	return kafka.NewOmittedError(fmt.Errorf("duplicate message: %w", err))
func NewOmittedError(err error) error {
	if err == nil {
		return nil
	}
	return wrappedOmittedError{err: err}
}

// isUnretriableError reports whether an err is retriable by checking if it implements the UnretriableError interface, and calling it.
// Errors which do not implement this interface are considered retriable.
func isUnretriableError(err error) bool {
	var ue UnretriableError
	return errors.As(err, &ue) && ue.IsUnretriable()
}

// isOmittedError reports whether an err is omitted by checking if it implements the OmittedError interface, and calling it.
// Errors which do not implement this interface are considered retriable.
func isOmittedError(err error) bool {
	var oe OmittedError
	return errors.As(err, &oe) && oe.IsOmitted()
}

// isRetriableError checks if the error is retriable (not unretriable and not omitted).
func isRetriableError(err error) bool {
	return !isUnretriableError(err) && !isOmittedError(err)
}

// errorType returns a human-readable classification of the error for logging.
func errorType(err error) string {
	switch {
	case isOmittedError(err):
		return "omitted"
	case isUnretriableError(err):
		return "unretriable"
	default:
		return "retriable"
	}
}
