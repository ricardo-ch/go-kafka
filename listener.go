package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/trace"
)

type HandlerConfig struct {
	ConsumerMaxRetries  *int
	DurationBeforeRetry *time.Duration
	ExponentialBackoff  bool
	// BackoffFunc is the function used to calculate backoff duration when ExponentialBackoff is true.
	// If nil, the global ExponentialBackoffFunc (using sarama.NewExponentialBackoff) will be used.
	// The function signature is: func(retries, maxRetries int) time.Duration
	BackoffFunc     func(retries, maxRetries int) time.Duration
	RetryTopic      string
	DeadletterTopic string
}

// Handler Processor that handle received kafka messages
// Handler Config can be used to override global configuration for a specific handler
type Handler struct {
	Processor func(ctx context.Context, msg *sarama.ConsumerMessage) error
	Config    HandlerConfig
}

// Handlers defines a handler for a given topic
type Handlers map[string]Handler

// listener object represents kafka consumer
// Listener implement both `Listener` interface and `ConsumerGroupHandler` from sarama
type listener struct {
	consumerGroup      sarama.ConsumerGroup
	deadletterProducer Producer
	topics             []string
	handlers           Handlers
	groupID            string
	instrumenting      *ConsumerMetricsService
	tracer             TracingFunc
	logContextStorer   LogContextStorer
}

// Listener is able to listen multiple topics with one handler by topic
type Listener interface {
	Listen(ctx context.Context) error
	Close()
	GroupID() string
}

type LogContextStorer func(ctx context.Context, logger *slog.Logger) context.Context

// NewListener creates a new instance of Listener
func NewListener(groupID string, handlers Handlers, options ...ListenerOption) (Listener, error) {
	if groupID == "" {
		return nil, errors.New("cannot create new listener, group_id cannot be empty")
	}
	if len(handlers) == 0 {
		return nil, errors.New("cannot create new listener, handlers cannot be empty")
	}

	// Init consumer, consume errors & messages
	var topics []string
	for k := range handlers {
		topics = append(topics, k)
	}
	c, err := getClient()
	if err != nil {
		return nil, err
	}

	producer, err := NewProducer(WithDeadletterProducerInstrumenting())
	if err != nil {
		return nil, err
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(groupID, c)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range consumerGroup.Errors() {
			if err != nil {
				slog.Error("sarama consumer error", "error", err, "consumer_group", groupID)
			}
		}
	}()

	// Fill handler config unset elements with global default values.
	fillHandlerConfigWithDefault(handlers)

	// Log configuration for each topic
	logHandlersConfig(groupID, handlers)

	// Sanity check for error topics, to avoid infinite loop
	err = checkErrorTopicToAvoidInfiniteLoop(handlers)
	if err != nil {
		return nil, err
	}

	l := &listener{
		groupID:            groupID,
		deadletterProducer: producer,
		handlers:           handlers,
		consumerGroup:      consumerGroup,
		topics:             topics,
	}

	// execute all method passed as option
	for _, o := range options {
		o(l)
	}

	return l, nil
}

// GroupID return the groupID of the listener
func (l *listener) GroupID() string {
	return l.groupID
}

func checkErrorTopicToAvoidInfiniteLoop(handlers Handlers) error {
	for topic, handler := range handlers {
		if handler.Config.RetryTopic == topic {
			return fmt.Errorf("retry topic cannot be the same as the original topic: %s", topic)
		}
		if handler.Config.DeadletterTopic == topic {
			return fmt.Errorf("deadletter topic cannot be the same as the original topic: %s", topic)
		}
	}
	return nil
}

func fillHandlerConfigWithDefault(handlers Handlers) {
	for k, h := range handlers {
		if h.Config.ConsumerMaxRetries == nil {
			maxRetries := ConsumerMaxRetries
			h.Config.ConsumerMaxRetries = &maxRetries
		}
		if h.Config.DurationBeforeRetry == nil {
			duration := DurationBeforeRetry
			h.Config.DurationBeforeRetry = &duration
		}
		handlers[k] = h
	}
}

// logHandlersConfig logs the retry configuration for each topic handler.
func logHandlersConfig(groupID string, handlers Handlers) {
	for topic, handler := range handlers {
		retryMode := "finite"
		maxRetries := *handler.Config.ConsumerMaxRetries
		if maxRetries == InfiniteRetries {
			retryMode = "infinite"
		}

		r := strings.NewReplacer("$$CG$$", groupID, "$$T$$", topic)

		retryTopic := handler.Config.RetryTopic
		if retryTopic == "" {
			if PushConsumerErrorsToRetryTopic {
				retryTopic = r.Replace(RetryTopicPattern)
			} else {
				retryTopic = "disabled"
			}
		}

		deadletterTopic := handler.Config.DeadletterTopic
		if deadletterTopic == "" {
			if PushConsumerErrorsToDeadletterTopic {
				deadletterTopic = r.Replace(DeadletterTopicPattern)
			} else {
				deadletterTopic = "disabled"
			}
		}

		// Build backoff description
		backoffDesc := handler.Config.DurationBeforeRetry.String()
		if handler.Config.ExponentialBackoff {
			backoffDesc = fmt.Sprintf("%s -> %s (exponential)", handler.Config.DurationBeforeRetry, MaxBackoffDuration)
		}

		slog.Info("topic handler configuration",
			"consumer_group", groupID,
			"topic", topic,
			"retry_mode", retryMode,
			"max_retries", maxRetries,
			"backoff", backoffDesc,
			"retry_topic", retryTopic,
			"deadletter_topic", deadletterTopic,
		)
	}
}

// ListenerOption add listener option
type ListenerOption func(l *listener)

// Listen process incoming kafka messages with handlers configured by the listener
func (l *listener) Listen(consumerContext context.Context) error {
	if l.consumerGroup == nil {
		return errors.New("consumerGroup is nil, cannot listen")
	}

	slog.Info("starting listener", "consumer_group", l.groupID, "topics", l.topics)

	// When a session is over, make consumer join a new session, as long as the context is not cancelled
	for {
		// Consume make this consumer join the next session
		// This block until the `session` is over. (basically until next rebalance)
		err := l.consumerGroup.Consume(consumerContext, l.topics, l)
		if err != nil {
			slog.Error("consumer group consume error", "error", err, "consumer_group", l.groupID)
			return err
		}
		if err := consumerContext.Err(); err != nil {
			// Check if context is cancelled
			slog.Info("listener stopping (context cancelled)", "consumer_group", l.groupID)
			return err
		}
		slog.Debug("consumer group session ended, rejoining", "consumer_group", l.groupID)
	}
}

// Close the listener and dependencies
func (l *listener) Close() {
	if l.consumerGroup != nil {
		err := l.consumerGroup.Close()
		if err != nil {
			slog.Error("failed to close consumer group", "error", err, "consumer_group", l.groupID)
		} else {
			slog.Info("consumer group closed", "consumer_group", l.groupID)
		}
	}
}

// The `Setup`, `Cleanup` and `ConsumeClaim` are actually implementation of ConsumerGroupHandler from sarama
// Copied from From the sarama lib:
//
// ConsumerGroupHandler instances are used to handle individual topic/partition claims.
// It also provides hooks for your consumer group session life-cycle and allow you to
// trigger logic before or after the consume loop(s).
//
// PLEASE NOTE that handlers are likely be called from several goroutines concurrently,
// ensure that all state is safely protected against race conditions.

// Setup is run at the beginning of a new session, before ConsumeClaim
func (l *listener) Setup(session sarama.ConsumerGroupSession) error {
	slog.Info("consumer group session started",
		"consumer_group", l.groupID,
		"generation_id", session.GenerationID(),
		"member_id", session.MemberID(),
		"claims", session.Claims(),
	)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (l *listener) Cleanup(session sarama.ConsumerGroupSession) error {
	slog.Info("consumer group session ended",
		"consumer_group", l.groupID,
		"generation_id", session.GenerationID(),
		"member_id", session.MemberID(),
	)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (l *listener) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	slog.Debug("starting to consume partition",
		"consumer_group", l.groupID,
		"topic", claim.Topic(),
		"partition", claim.Partition(),
		"initial_offset", claim.InitialOffset(),
	)

	for msg := range claim.Messages() {
		l.onNewMessage(msg, session)
	}

	slog.Debug("stopped consuming partition",
		"consumer_group", l.groupID,
		"topic", claim.Topic(),
		"partition", claim.Partition(),
	)
	return nil
}

func (l *listener) onNewMessage(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) {
	ctx := session.Context()
	if l.logContextStorer != nil {
		info := kafkaMessageInfo{ConsumerGroup: l.groupID}
		if msg != nil {
			info.Topic = msg.Topic
			info.Partition = msg.Partition
			info.Offset = msg.Offset
			info.Key = string(msg.Key)
		}
		kLogger := slog.With("kafka", info)
		ctx = l.logContextStorer(ctx, kLogger)
	}

	var span trace.Span
	if l.tracer != nil {
		span, ctx = l.tracer(ctx, msg)
		if span != nil {
			defer span.End()
		}
	}

	handler := l.handlers[msg.Topic]

	err := l.handleMessageWithRetry(ctx, handler, msg, *handler.Config.ConsumerMaxRetries, 0, handler.Config.ExponentialBackoff)
	if err != nil {
		err = fmt.Errorf("processing failed: %w", err)
		if l.instrumenting != nil && l.instrumenting.recordErrorCounter != nil && !isOmittedError(err) {
			l.instrumenting.recordErrorCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
		}
		l.handleErrorMessage(ctx, err, handler, msg)
	} else {
		slog.Debug("message processed successfully", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	}

	if !errors.Is(err, context.Canceled) {
		session.MarkMessage(msg, "")
		slog.Debug("message offset committed", "offset", msg.Offset, "partition", msg.Partition, "topic", msg.Topic)
	}
}

func (l *listener) handleErrorMessage(ctx context.Context, initialError error, handler Handler, msg *sarama.ConsumerMessage) {
	if isOmittedError(initialError) {
		l.handleOmittedMessage(ctx, initialError, msg)
		return
	}

	slog.ErrorContext(ctx, "message processing failed, applying error handling policy", "error", initialError)

	if isRetriableError(initialError) && l.tryForwardToRetry(ctx, handler, msg) {
		return
	}

	if l.tryForwardToDeadletter(ctx, handler, msg) {
		return
	}

	slog.WarnContext(ctx, "message dropped: no retry or deadletter topic configured")
	l.incOmittedCounter(msg)
}

func (l *listener) tryForwardToRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage) bool {
	topicName := handler.Config.RetryTopic
	if topicName == "" && PushConsumerErrorsToRetryTopic {
		topicName = l.deduceTopicNameFromPattern(msg.Topic, RetryTopicPattern)
	}
	if topicName == "" {
		return false
	}

	slog.InfoContext(ctx, "forwarding message to retry topic", "retry_topic", topicName)
	if err := l.forwardToTopic(ctx, msg, topicName); err != nil {
		slog.ErrorContext(ctx, "failed to send message to retry topic", "error", err, "retry_topic", topicName)
	}
	return true
}

func (l *listener) tryForwardToDeadletter(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage) bool {
	topicName := handler.Config.DeadletterTopic
	if topicName == "" && PushConsumerErrorsToDeadletterTopic {
		topicName = l.deduceTopicNameFromPattern(msg.Topic, DeadletterTopicPattern)
	}
	if topicName == "" {
		return false
	}

	slog.InfoContext(ctx, "forwarding message to deadletter topic", "deadletter_topic", topicName)
	if err := l.forwardToTopic(ctx, msg, topicName); err != nil {
		slog.ErrorContext(ctx, "failed to send message to deadletter topic", "error", err, "deadletter_topic", topicName)
	}
	return true
}

func (l *listener) incOmittedCounter(msg *sarama.ConsumerMessage) {
	if l.instrumenting != nil && l.instrumenting.recordOmittedCounter != nil {
		l.instrumenting.recordOmittedCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}
}

func (l *listener) deduceTopicNameFromPattern(topic, pattern string) string {
	r := strings.NewReplacer("$$CG$$", l.groupID, "$$T$$", topic)
	return r.Replace(pattern)
}

func (l *listener) forwardToTopic(ctx context.Context, msg *sarama.ConsumerMessage, topicName string) error {
	headers := make([]sarama.RecordHeader, 0, len(msg.Headers))
	for _, h := range msg.Headers {
		if h != nil {
			headers = append(headers, *h)
		}
	}

	return l.deadletterProducer.Produce(ctx, &sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Value),
		Topic:   topicName,
		Headers: headers,
	})
}

func (l *listener) handleOmittedMessage(ctx context.Context, initialError error, msg *sarama.ConsumerMessage) {
	slog.WarnContext(ctx, "message omitted by handler", "error", initialError)
	l.incOmittedCounter(msg)
}

// handleMessageWithRetry calls the handler function and retries on failure using a loop.
func (l *listener) handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries, retryNumber int, exponentialBackoff bool) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if retryNumber == 0 {
			slog.DebugContext(ctx, "processing message")
		}

		err := l.safeProcess(ctx, handler, msg)
		if err == nil {
			return nil
		}

		if !shouldRetry(retries, err) {
			return err
		}

		var backoffDuration time.Duration
		if exponentialBackoff {
			backoffDuration = getBackoffDuration(handler, retryNumber, *handler.Config.ConsumerMaxRetries)
		} else {
			backoffDuration = *handler.Config.DurationBeforeRetry
		}

		remainingRetries := "infinite"
		if retries != InfiniteRetries {
			remainingRetries = fmt.Sprintf("%d", retries)
		}

		slog.WarnContext(ctx, "message processing failed, will retry",
			"error", err,
			"retry_number", retryNumber+1,
			"remaining_retries", remainingRetries,
			"backoff_duration", backoffDuration.Round(10*time.Millisecond).String(),
			"exponential_backoff", exponentialBackoff,
		)

		select {
		case <-time.After(backoffDuration):
		case <-ctx.Done():
			return ctx.Err()
		}

		if retries != InfiniteRetries {
			retries--
		}
		retryNumber++
	}
}

// safeProcess wraps handler.Processor with panic recovery.
func (l *listener) safeProcess(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic happened during handle of message: %v", r)
			slog.ErrorContext(ctx, "panic recovered during message processing", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	return handler.Processor(ctx, msg)
}

func shouldRetry(retries int, err error) bool {
	if retries == 0 {
		return false
	}

	if isUnretriableError(err) || isOmittedError(err) {
		return false
	}

	return true
}

// getBackoffDuration returns the backoff duration using the handler's BackoffFunc
// or the global ExponentialBackoffFunc (which uses sarama.NewExponentialBackoff with KIP-580 jitter).
func getBackoffDuration(handler Handler, retryNumber, maxRetries int) time.Duration {
	if handler.Config.BackoffFunc != nil {
		return handler.Config.BackoffFunc(retryNumber, maxRetries)
	}
	// Use the global ExponentialBackoffFunc which leverages sarama.NewExponentialBackoff
	return ExponentialBackoffFunc(retryNumber, maxRetries)
}
