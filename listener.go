package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type HandlerConfig struct {
	ConsumerMaxRetries  *int
	DurationBeforeRetry *time.Duration
	ExponentialBackoff  bool
	BackoffFunc         BackoffFunc
	RetryTopic          string
	DeadletterTopic     string
}

// Handler Processor that handle received kafka messages
// Handler Config can be used to override global configuration for a specific handler
type Handler struct {
	Processor func(ctx context.Context, msg *sarama.ConsumerMessage) error
	Config    HandlerConfig
}

// BackoffFunc is the function used to calculate backoff duration when ExponentialBackoff is true.
// If nil, the global ExponentialBackoffFunc (using sarama.NewExponentialBackoff) will be used.
// The function signature is: func(retries, maxRetries int) time.Duration
type BackoffFunc func(retries, maxRetries int) time.Duration

// Handlers defines a handler for a given topic
type Handlers map[string]Handler

// listener object represents kafka consumer.
// Listener implements both the Listener interface and ConsumerGroupHandler from sarama.
type listener struct {
	consumerGroup      sarama.ConsumerGroup
	deadletterProducer Producer
	topics             []string
	handlers           Handlers
	groupID            string
	instrumenting      *ConsumerMetricsService
	tracer             TracingFunc
	logContextStorer   LogContextStorer
	done               chan struct{}
	closeOnce          sync.Once
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
		producer.Close()
		return nil, err
	}

	done := make(chan struct{})

	go func() {
		for {
			select {
			case err, ok := <-consumerGroup.Errors():
				if !ok {
					return
				}
				if err != nil {
					slog.Error("sarama consumer error", "error", err, "consumer_group", groupID)
				}
			case <-done:
				return
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
		close(done)
		_ = consumerGroup.Close()
		producer.Close()
		return nil, err
	}

	l := &listener{
		groupID:            groupID,
		deadletterProducer: producer,
		handlers:           handlers,
		consumerGroup:      consumerGroup,
		topics:             topics,
		done:               done,
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
			return fmt.Errorf("%w: %s", ErrRetryTopicCollision, topic)
		}
		if handler.Config.DeadletterTopic == topic {
			return fmt.Errorf("%w: %s", ErrDeadletterTopicCollision, topic)
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

func groupIDAndTopicReplacer(groupID, topic string) *strings.Replacer {
	return strings.NewReplacer("$$CG$$", groupID, "$$T$$", topic)
}

// logHandlersConfig logs the retry configuration for each topic handler.
func logHandlersConfig(groupID string, handlers Handlers) {
	for topic, handler := range handlers {
		retryMode := "finite"
		maxRetries := *handler.Config.ConsumerMaxRetries
		if maxRetries == InfiniteRetries {
			retryMode = "infinite"
		}

		r := groupIDAndTopicReplacer(groupID, topic)

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
		err = consumerContext.Err()
		if err != nil {
			// Check if context is cancelled
			slog.Info("listener stopping (context cancelled)", "consumer_group", l.groupID)
			return err
		}
		slog.Debug("consumer group session ended, rejoining", "consumer_group", l.groupID)
	}
}

// Close shuts down the listener, its consumer group, and the internal error-draining goroutine.
// Close must be called to avoid goroutine leaks.
func (l *listener) Close() {
	l.closeOnce.Do(func() {
		if l.done != nil {
			close(l.done)
		}
	})
	if l.deadletterProducer != nil {
		l.deadletterProducer.Close()
	}

	if l.consumerGroup != nil {
		err := l.consumerGroup.Close()
		if err != nil {
			slog.Error("failed to close consumer group", "error", err, "consumer_group", l.groupID)
		} else {
			slog.Debug("consumer group closed", "consumer_group", l.groupID)
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
	slog.Debug("consumer group session started",
		"consumer_group", l.groupID,
		"generation_id", session.GenerationID(),
		"member_id", session.MemberID(),
		"claims", session.Claims(),
	)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (l *listener) Cleanup(session sarama.ConsumerGroupSession) error {
	slog.Debug("consumer group session ended",
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
	ctx := l.enrichContext(session.Context(), msg)

	ctx, endSpan := l.startMessageSpan(ctx, msg)
	defer endSpan()

	err := l.processMessage(ctx, msg)

	if !errors.Is(err, context.Canceled) {
		if err != nil {
			loggerFromContext(ctx).Error("message processing failed", "error", err, "error_type", errorType(err))
		}
		session.MarkMessage(msg, "")
		loggerFromContext(ctx).Debug("message offset committed")
	}
}

// enrichContext builds a context carrying the enriched logger with Kafka message metadata.
// If a LogContextStorer is configured, it is also called so the user's handler can retrieve the logger.
func (l *listener) enrichContext(ctx context.Context, msg *sarama.ConsumerMessage) context.Context {
	info := kafkaMessageInfo{ConsumerGroup: l.groupID}
	if msg != nil {
		info.Topic = msg.Topic
		info.Partition = msg.Partition
		info.Offset = msg.Offset
		info.Key = string(msg.Key)
	}
	kLogger := slog.With("kafka", info)
	ctx = context.WithValue(ctx, loggerKey{}, kLogger)

	if l.logContextStorer != nil {
		ctx = l.logContextStorer(ctx, kLogger)
	}
	return ctx
}

// processMessage handles the full lifecycle of a single message: retry loop, error
// classification, metrics, and forwarding to retry/deadletter topics.
func (l *listener) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	handler := l.handlers[msg.Topic]

	err := l.handleMessageWithRetry(ctx, handler, msg, *handler.Config.ConsumerMaxRetries, handler.Config.ExponentialBackoff)
	if err != nil {
		err = l.handleErrorMessage(ctx, err, handler, msg)
		if err != nil {
			return fmt.Errorf("message processing failed: %w", err)
		}
	}

	loggerFromContext(ctx).Debug("message processed successfully")
	return nil
}

func (l *listener) handleErrorMessage(ctx context.Context, initialError error, handler Handler, msg *sarama.ConsumerMessage) error {
	l.recordErrorCounter(msg, initialError)

	if isOmittedError(initialError) {
		l.handleOmittedMessage(ctx, initialError, msg)
		return nil
	}

	if !isRetriableError(initialError) {
		loggerFromContext(ctx).Warn("message not retriable, forwarding to deadletter", "error", initialError, "error_type", "unretriable")
		if l.tryForwardToDeadletter(ctx, handler, msg, initialError) {
			return nil
		}
		l.incOmittedCounter(msg)
		return fmt.Errorf("message dropped: no deadletter topic configured: %w", initialError)
	}

	if l.tryForwardToRetry(ctx, handler, msg, initialError) {
		return nil
	}

	if l.tryForwardToDeadletter(ctx, handler, msg, initialError) {
		return nil
	}

	l.incOmittedCounter(msg)
	return fmt.Errorf("message dropped: no retry or deadletter topic configured: %w", initialError)
}

// tryForwardToRetry resolves the retry topic and forwards the message to it.
// Returns false if no retry topic is configured. On producer failure, it retries
// with exponential backoff until the message is published or the context is cancelled.
func (l *listener) tryForwardToRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, initialError error) bool {
	if !PushConsumerErrorsToRetryTopic {
		return false
	}

	topicName := handler.Config.RetryTopic
	if topicName == "" {
		topicName = l.deduceTopicNameFromPattern(msg.Topic, RetryTopicPattern)
	}
	if topicName == "" {
		return false
	}

	log := loggerFromContext(ctx)
	log.Warn("forwarding message to retry topic due to initial error", "initial_error", initialError, "retry_topic", topicName)

	l.forwardWithRetry(ctx, msg, topicName, "retry")
	return true
}

// tryForwardToDeadletter resolves the deadletter topic and forwards the message to it.
// Returns false if no deadletter topic is configured. On producer failure, it retries
// with exponential backoff until the message is published or the context is cancelled.
func (l *listener) tryForwardToDeadletter(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, initialError error) bool {
	topicName := handler.Config.DeadletterTopic
	if topicName == "" && PushConsumerErrorsToDeadletterTopic {
		topicName = l.deduceTopicNameFromPattern(msg.Topic, DeadletterTopicPattern)
	}
	if topicName == "" {
		return false
	}
	log := loggerFromContext(ctx)
	log.Warn("forwarding message to deadletter topic due to initial error", "initial_error", initialError, "deadletter_topic", topicName)

	l.forwardWithRetry(ctx, msg, topicName, "deadletter")
	return true
}

// forwardWithRetry forwards a message to the given topic, retrying with exponential
// backoff on failure. It blocks until the message is successfully produced or the
// context is cancelled. This guarantees the message is not lost on transient producer errors.
func (l *listener) forwardWithRetry(ctx context.Context, msg *sarama.ConsumerMessage, topicName, kind string) {
	log := loggerFromContext(ctx)

	backoff := DurationBeforeRetry
	attempt := 0

	for {
		err := l.forwardToTopic(ctx, msg, topicName)
		if err == nil {
			if attempt > 0 {
				log.Debug("message forwarded to "+kind+" topic after retry", kind+"_topic", topicName, "attempts", attempt+1)
			}
			return
		}

		attempt++
		log.Error("failed to forward message to "+kind+" topic, will retry",
			"error", err, kind+"_topic", topicName, "attempt", attempt, "backoff", backoff.String())

		timer := time.NewTimer(backoff)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			log.Warn("context cancelled while retrying forward to "+kind+" topic",
				kind+"_topic", topicName, "attempts", attempt)
			return
		}

		backoff = min(backoff*2, ForwardMaxBackoffDuration)
	}
}

func (l *listener) incOmittedCounter(msg *sarama.ConsumerMessage) {
	if l.instrumenting != nil && l.instrumenting.recordOmittedCounter != nil {
		l.instrumenting.recordOmittedCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}
}

func (l *listener) recordErrorCounter(msg *sarama.ConsumerMessage, err error) {
	if l.instrumenting != nil && l.instrumenting.recordErrorCounter != nil && !isOmittedError(err) {
		l.instrumenting.recordErrorCounter.With(map[string]string{"kafka_topic": msg.Topic, "consumer_group": l.groupID}).Inc()
	}
}

func (l *listener) deduceTopicNameFromPattern(topic, pattern string) string {
	r := groupIDAndTopicReplacer(l.groupID, topic)
	return r.Replace(pattern)
}

func (l *listener) forwardToTopic(ctx context.Context, msg *sarama.ConsumerMessage, topicName string) error {
	// Inject current trace context so the forwarded message
	// is linked to the processing span that failed.
	traceHeaders := GetKafkaHeadersFromContext(ctx)
	traceKeys := make(map[string]struct{}, len(traceHeaders))
	for _, h := range traceHeaders {
		traceKeys[string(h.Key)] = struct{}{}
	}

	headers := make([]sarama.RecordHeader, 0, len(msg.Headers)+len(traceHeaders))
	for _, h := range msg.Headers {
		if h != nil {
			if _, isTrace := traceKeys[string(h.Key)]; !isTrace {
				headers = append(headers, *h)
			}
		}
	}
	headers = append(headers, traceHeaders...)

	return l.deadletterProducer.Produce(ctx, &sarama.ProducerMessage{
		Key:     sarama.ByteEncoder(msg.Key),
		Value:   sarama.ByteEncoder(msg.Value),
		Topic:   topicName,
		Headers: headers,
	})
}

func (l *listener) handleOmittedMessage(ctx context.Context, initialError error, msg *sarama.ConsumerMessage) {
	loggerFromContext(ctx).Warn("message omitted by handler", "error", initialError, "error_type", "omitted")
	l.incOmittedCounter(msg)
}

// handleMessageWithRetry calls the handler function and retries on failure using a loop.
func (l *listener) handleMessageWithRetry(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage, retries int, exponentialBackoff bool) error {
	retryNumber := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		loggerFromContext(ctx).Debug("processing message")

		err := l.safeProcess(ctx, handler, msg)
		if err == nil {
			return nil
		}

		if !shouldRetry(retries, err) {
			return err
		}

		retryWaitDuration := retryDuration(handler, retryNumber, exponentialBackoff)

		remainingRetries := "infinite"
		if retries != InfiniteRetries {
			retries--
			remainingRetries = strconv.Itoa(retries)
		}

		retryNumber++

		loggerFromContext(ctx).Error("message processing failed, will retry",
			"error", err,
			"retry_number", retryNumber,
			"remaining_retries", remainingRetries,
			"retry_wait_duration", retryWaitDuration.Round(10*time.Millisecond).String(),
			"exponential_backoff", exponentialBackoff,
		)

		// Use time.NewTimer instead of time.After to avoid leaking the timer
		// when the context is cancelled during the backoff wait.
		retryWaitTimer := time.NewTimer(retryWaitDuration)
		select {
		case <-retryWaitTimer.C:
		case <-ctx.Done():
			retryWaitTimer.Stop()
			return ctx.Err()
		}
	}
}

// safeProcess wraps handler.Processor with panic recovery.
func (l *listener) safeProcess(ctx context.Context, handler Handler, msg *sarama.ConsumerMessage) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic happened during handle of message: %v", r)
			loggerFromContext(ctx).Error("panic recovered during message processing", "panic", r, "stack", string(debug.Stack()))
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

// retryDuration returns the wait duration before the next retry attempt, capped
// by MaxBackoffDuration. When exponentialBackoff is true, it delegates to
// getBackoffDuration (which already respects the cap internally); otherwise it
// uses the handler's fixed DurationBeforeRetry with the same cap applied.
func retryDuration(handler Handler, retryNumber int, exponentialBackoff bool) time.Duration {
	if exponentialBackoff {
		return getBackoffDuration(handler, retryNumber, *handler.Config.ConsumerMaxRetries)
	}
	d := *handler.Config.DurationBeforeRetry
	if d > MaxBackoffDuration {
		return MaxBackoffDuration
	}
	return d
}

var defaultBackoffOnce sync.Once
var defaultBackoffFunc BackoffFunc

// getBackoffDuration returns the exponential backoff duration using (in priority order):
// 1. The handler's custom BackoffFunc
// 2. The global ExponentialBackoffFunc (if set by the client)
// 3. A lazily-created sarama.NewExponentialBackoff using the current DurationBeforeRetry/MaxBackoffDuration
func getBackoffDuration(handler Handler, retryNumber, maxRetries int) time.Duration {
	if handler.Config.BackoffFunc != nil {
		return handler.Config.BackoffFunc(retryNumber, maxRetries)
	}
	if ExponentialBackoffFunc != nil {
		return ExponentialBackoffFunc(retryNumber, maxRetries)
	}
	defaultBackoffOnce.Do(func() {
		defaultBackoffFunc = sarama.NewExponentialBackoff(DurationBeforeRetry, MaxBackoffDuration)
	})
	return defaultBackoffFunc(retryNumber, maxRetries)
}
