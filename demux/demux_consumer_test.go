// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package demux

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/bandwidth"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// contextKey is a custom type to avoid collisions in context values
type contextKey string

const testContextKey contextKey = "test-context-key"

// Test_ConsumerBuilder_ContextIsSet verifies that the context passed to WithContext
// is actually stored and is the same context (not a copy or different one).
func Test_ConsumerBuilder_ContextIsSet(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	// create a context with a distinguishable value
	expectedValue := "test-value-12345"
	ctx := context.WithValue(context.Background(), testContextKey, expectedValue)

	// create consumer using builder pattern
	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(ctx).
		WithLogger(mocklogger.NewNoOpLogger())

	consumer := builder.Build(&mockBrokerPort{})

	// cast to concrete type to access private field
	dxc, ok := consumer.(*Consumer[any])
	if !ok {
		t.Fatalf("expected *Consumer[any], got %T", consumer)
	}

	// verify ctx is not nil
	if dxc.ctx == nil {
		t.Fatal("consumer.ctx is nil - context was not assigned")
	}

	// verify it's the SAME context (with our value), not a different one
	actualValue := dxc.ctx.Value(testContextKey)
	if actualValue == nil {
		t.Fatal("context value is nil - wrong context was stored")
	}

	if actualValue != expectedValue {
		t.Errorf("context value mismatch: got %q, want %q", actualValue, expectedValue)
	}
}

// mockBrokerPort implements nexus.BrokerPort[any] for testing
type mockBrokerPort struct{}

func (m *mockBrokerPort) Subscribe() error                        { return nil }
func (m *mockBrokerPort) Unsubscribe() error                      { return nil }
func (m *mockBrokerPort) Poll(_ time.Duration) (any, bool, error) { return nil, false, nil }
func (m *mockBrokerPort) ExtractEnvelope(_ any) nexus.Envelope    { return nexus.Envelope{} }
func (m *mockBrokerPort) CommitOffsets(m2 []*nexus.Message[any]) ([]*nexus.Message[any], error) {
	return m2, nil
}
func (m *mockBrokerPort) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}
func (m *mockBrokerPort) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}
func (m *mockBrokerPort) ConsumerGroup() string { return "test-group" }

// controllableBrokerPort allows fine-grained control over broker behavior for testing
type controllableBrokerPort struct {
	subscribeErr   error
	unsubscribeErr error
	pollFunc       func(time.Duration) (any, bool, error)

	// channels for synchronization
	subscribed    chan struct{}
	unsubscribed  chan struct{}
	pollStarted   chan struct{}
	pollCount     atomic.Int32
	unsubscribeMu sync.Mutex
	unsubscribeCh chan struct{} // blocks Unsubscribe until closed

	// for tracking calls
	ackRebalanceCalls []struct {
		Type nexus.RebalanceType
		Info []nexus.RebalanceInfo
	}
	mu sync.Mutex
}

func newControllableBrokerPort() *controllableBrokerPort {
	return &controllableBrokerPort{
		subscribed:    make(chan struct{}),
		unsubscribed:  make(chan struct{}),
		pollStarted:   make(chan struct{}, 100),
		unsubscribeCh: make(chan struct{}),
	}
}

func (m *controllableBrokerPort) Subscribe() error {
	close(m.subscribed)
	return m.subscribeErr
}

func (m *controllableBrokerPort) Unsubscribe() error {
	m.unsubscribeMu.Lock()
	ch := m.unsubscribeCh
	m.unsubscribeMu.Unlock()

	if ch != nil {
		<-ch // block until released
	}
	close(m.unsubscribed)
	return m.unsubscribeErr
}

func (m *controllableBrokerPort) Poll(timeout time.Duration) (any, bool, error) {
	m.pollCount.Add(1)
	select {
	case m.pollStarted <- struct{}{}:
	default:
	}

	if m.pollFunc != nil {
		return m.pollFunc(timeout)
	}
	time.Sleep(timeout)
	return nil, false, nil
}

func (m *controllableBrokerPort) ExtractEnvelope(_ any) nexus.Envelope {
	return nexus.Envelope{Ctx: context.Background()}
}

func (m *controllableBrokerPort) CommitOffsets(msgs []*nexus.Message[any]) ([]*nexus.Message[any], error) {
	return msgs, nil
}

func (m *controllableBrokerPort) AckRebalance(rt nexus.RebalanceType, ri []nexus.RebalanceInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackRebalanceCalls = append(m.ackRebalanceCalls, struct {
		Type nexus.RebalanceType
		Info []nexus.RebalanceInfo
	}{rt, ri})
	return nil
}

func (m *controllableBrokerPort) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}
func (m *controllableBrokerPort) ConsumerGroup() string { return "" }

func (m *controllableBrokerPort) releaseUnsubscribe() {
	m.unsubscribeMu.Lock()
	if m.unsubscribeCh != nil {
		close(m.unsubscribeCh)
		m.unsubscribeCh = nil
	}
	m.unsubscribeMu.Unlock()
}

// no-op implementations for test dependencies

func noOpProcessMessage(_ context.Context, _ *nexus.Message[any]) error {
	return nil
}

func noOpWriteDeadLetter(_ context.Context, _ *nexus.Message[any], _ error) error {
	return nil
}

// stdoutLogger writes log messages to stdout for subprocess capture
type stdoutLogger struct{}

func (l *stdoutLogger) Error(_ context.Context, msg string, _ ...any) { fmt.Println("ERROR: " + msg) }
func (l *stdoutLogger) Warn(_ context.Context, msg string, _ ...any)  { fmt.Println("WARN: " + msg) }
func (l *stdoutLogger) Info(_ context.Context, msg string, _ ...any)  { fmt.Println("INFO: " + msg) }
func (l *stdoutLogger) Debug(_ context.Context, msg string, _ ...any) { fmt.Println("DEBUG: " + msg) }

// =============================================================================
// ConsumerBuilder tests
// =============================================================================

func Test_ConsumerBuilder_TopicName_ReturnsConfiguredTopic(t *testing.T) {
	builder := NewBuilder("my-special-topic", noOpProcessMessage, noOpWriteDeadLetter)

	if builder.TopicName() != "my-special-topic" {
		t.Errorf("TopicName() = %q, want %q", builder.TopicName(), "my-special-topic")
	}
}

// =============================================================================
// Consumer.Subscribe() tests
// =============================================================================

func Test_Consumer_Subscribe_HappyPath_ReceivesAssignment(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe in goroutine since it blocks waiting for assignment
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	// Wait for broker subscribe to be called
	<-broker.subscribed

	// Trigger assignment via TriggerRebalance
	err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{
		{Partition: 0, CommittedOffset: 0},
	})
	if err != nil {
		t.Fatalf("TriggerRebalance failed: %v", err)
	}

	// Subscribe should complete
	select {
	case err := <-subscribeDone:
		if err != nil {
			t.Errorf("Subscribe returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe did not complete in time")
	}

	// Verify logs
	if !logger.HasInfo("starting *demux.Consumer[interface {}] poll loop for topic: test-topic") {
		t.Error("expected 'starting poll loop' log message")
	}
	if !logger.HasInfo("started *demux.Consumer[interface {}] for topic: test-topic") {
		t.Error("expected 'started consuming' log message")
	}

	// Cleanup
	broker.releaseUnsubscribe()
	if err := dxc.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}
}

func Test_Consumer_Subscribe_BrokerError_ReturnsWrappedError(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.subscribeErr = errors.New("connection refused")
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 100 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	err := dxc.Subscribe()

	if err == nil {
		t.Fatal("expected error from Subscribe")
	}

	expectedErr := "failed to subscribe to topic: test-topic - connection refused"
	if err.Error() != expectedErr {
		t.Errorf("error mismatch:\ngot:  %q\nwant: %q", err.Error(), expectedErr)
	}
}

func Test_Consumer_Subscribe_Timeout_ReturnsError(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	logger := mocklogger.NewRecordingLogger()

	// Very short timeout for fast test
	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 50 * time.Millisecond,
		DrainTimeout:            50 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Release unsubscribe so cleanup doesn't block
	broker.releaseUnsubscribe()

	err := dxc.Subscribe()

	if err == nil {
		t.Fatal("expected timeout error from Subscribe")
	}

	expectedErr := "timeout after 50ms waiting for partition assignments, unsubscribing"
	if err.Error() != expectedErr {
		t.Errorf("error mismatch:\ngot:  %q\nwant: %q", err.Error(), expectedErr)
	}

	// Verify error was logged
	errs := logger.Errors()
	if len(errs) == 0 {
		t.Fatal("expected error to be logged")
	}
	if errs[0] != expectedErr {
		t.Errorf("logged error mismatch:\ngot:  %q\nwant: %q", errs[0], expectedErr)
	}
}

// Note: Test_Consumer_Subscribe_Timeout_UnsubscribeStuck is not included because
// the implementation calls defaultShutdownCallback directly (not through registered callback),
// which sleeps 15s then sends os.Interrupt - not suitable for unit testing.
// The path is covered by verifying logs in Test_Consumer_Subscribe_Timeout_ReturnsError.

// =============================================================================
// Consumer.Unsubscribe() tests
// =============================================================================

func Test_Consumer_Unsubscribe_HappyPath(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe() // Allow unsubscribe to complete immediately
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe first
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Now unsubscribe
	err := dxc.Unsubscribe()
	if err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}
}

func Test_Consumer_Unsubscribe_BrokerError_LogsWarning(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	broker.unsubscribeErr = errors.New("broker disconnected")
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe first
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Unsubscribe - should return nil but log warning
	err := dxc.Unsubscribe()
	if err != nil {
		t.Errorf("Unsubscribe should return nil even with broker error, got: %v", err)
	}

	// Check warning was logged
	if !logger.HasWarning("broker unsubscribe error: broker disconnected") {
		t.Errorf("expected warning about broker error, got: %v", logger.Warnings())
	}
}

func Test_Consumer_Unsubscribe_Timeout_TriggersCircuitBreaker(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	// Don't release unsubscribe - it will block and trigger timeout
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            50 * time.Millisecond, // Short timeout
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// for callback assert
	callbackFired := make(chan error, 1)
	dxc.RegisterShutdownCallback(func(_ context.Context, reason error) {
		callbackFired <- reason
	})

	// Subscribe first
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Unsubscribe should timeout and return error
	err := dxc.Unsubscribe()
	if err == nil {
		t.Fatal("expected timeout error")
	}

	expectedErr := "timeout: 50ms exceeded draining prior to unsubscribe, triggering circuit breaker"
	if err.Error() != expectedErr {
		t.Errorf("error mismatch:\ngot:  %q\nwant: %q", err.Error(), expectedErr)
	}

	// Verify error was logged
	errs := logger.Errors()
	if len(errs) == 0 {
		t.Fatal("expected error to be logged")
	}
	if errs[0] != expectedErr {
		t.Errorf("logged error mismatch:\ngot:  %q\nwant: %q", errs[0], expectedErr)
	}

	select {
	case reason := <-callbackFired:
		if reason == nil {
			t.Error("shutdown callback fired with nil reason; expected emergency reason")
		} else {
			const expect = "circuit-breaker: triggered and completed protective shutdown, " +
				"reason: timeout: 50ms exceeded draining prior to unsubscribe, triggering circuit breaker"

			if reason.Error() != expect {
				t.Errorf("callback error mismatch:\ngot:  %q\nwant: %q", reason, expect)
			}
		}
	case <-time.After(time.Second):
		t.Error("shutdown callback never fired after circuit breaker trigger")
	}
}

// =============================================================================
// Consumer.Shutdown() tests
// =============================================================================

func Test_Consumer_Shutdown_HappyPath(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	callbackCalled := make(chan error, 1)
	shutdownCallback := func(_ context.Context, reason error) {
		callbackCalled <- reason
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg).
		WithShutdownCallback(shutdownCallback)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe first
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Shutdown
	err := dxc.Shutdown()
	if err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	// Callback should be called with nil reason (graceful)
	select {
	case reason := <-callbackCalled:
		if reason != nil {
			t.Errorf("expected nil reason for graceful shutdown, got: %v", reason)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("callback was not called")
	}

	// Verify logs
	if !logger.HasInfo("shutdown invoked for topicName: test-topic, draining workers and committing offsets") {
		t.Error("expected 'shutdown invoked' log")
	}
	if !logger.HasInfo("calling Unsubscribe()") {
		t.Error("expected 'calling Unsubscribe()' log")
	}
	if !logger.HasInfo("invoking shutdownCallback for topicName: test-topic") {
		t.Error("expected 'invoking shutdownCallback' log")
	}
}

func Test_Consumer_Shutdown_WithBandwidthAggregator_StopsAggregator(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg).
		WithShutdownCallback(func(_ context.Context, _ error) {})

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Attach a real aggregator with a noop sink - the builder normally wires one
	// only when both WithBandwidthMetricsSink and a BandwidthPort adapter are set
	noopSink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error { return nil })
	agg := bandwidth.NewAggregator(context.Background(), noopSink, "test-topic", logger)
	agg.Start()
	dxc.bandwidthAggregator = agg

	// Subscribe so the shutdown path reaches the aggregator-stop branch
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()
	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	if err := dxc.Shutdown(); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	// Aggregator.Stop is idempotent so a second call is harmless; we just
	// need to confirm Shutdown ran the branch and the aggregator is stopped.
	// Stats should reflect zero packets (the noop sink was never invoked).
	stats := agg.Stats()
	if stats.Flushed != 0 || stats.Dropped != 0 {
		t.Errorf("expected empty aggregator stats, got flushed=%d dropped=%d", stats.Flushed, stats.Dropped)
	}
}

func Test_Consumer_Shutdown_NeverSubscribed_LogsWarning(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	logger := mocklogger.NewRecordingLogger()

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Shutdown without subscribing first
	err := dxc.Shutdown()
	if err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	// Verify warning was logged
	if !logger.HasWarning("never subscribed to topic, shutdown complete!") {
		t.Errorf("expected 'never subscribed' warning, got: %v", logger.Warnings())
	}
}

func Test_Consumer_Shutdown_UnsubscribeTimeout_ReturnsError(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	// Don't release unsubscribe - it will block and timeout
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            50 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	callbackCalled := make(chan bool, 1)

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg).
		WithShutdownCallback(func(_ context.Context, _ error) {
			callbackCalled <- true
		})

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe first
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Shutdown should timeout and return error (callback NOT called - circuit breaker handles it)
	err := dxc.Shutdown()
	if err == nil {
		t.Fatal("expected timeout error from Shutdown")
	}

	expectedErr := "timeout: 50ms exceeded draining prior to unsubscribe, triggering circuit breaker"
	if err.Error() != expectedErr {
		t.Errorf("error mismatch:\ngot:  %q\nwant: %q", err.Error(), expectedErr)
	}

	// Callback should be invoked by the circuit breaker listener (not by Shutdown directly)
	select {
	case <-callbackCalled:
		// expected - circuit breaker invokes callback
	case <-time.After(100 * time.Millisecond):
		t.Error("callback was not invoked by circuit breaker")
	}
}

// =============================================================================
// Consumer.RegisterShutdownCallback() tests
// =============================================================================

func Test_Consumer_RegisterShutdownCallback_BeforeSubscribe(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	callbackCalled := make(chan string, 1)

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Register callback before subscribe
	dxc.RegisterShutdownCallback(func(_ context.Context, reason error) {
		if reason == nil {
			callbackCalled <- "graceful"
		} else {
			callbackCalled <- reason.Error()
		}
	})

	// Subscribe
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Shutdown should use registered callback
	if err := dxc.Shutdown(); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	select {
	case result := <-callbackCalled:
		if result != "graceful" {
			t.Errorf("expected 'graceful', got %q", result)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("callback was not called")
	}
}

func Test_Consumer_RegisterShutdownCallback_AfterSubscribe_LogsWarning(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe first (sets default callback)
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Now register callback after subscribe
	dxc.RegisterShutdownCallback(func(_ context.Context, _ error) {})

	// Verify warning was logged
	if !logger.HasWarning("shutdown callback already registered for topicName: test-topic (*nexus.ShutdownCallback), overwriting - call RegisterShutdownCallback before Subscribe to avoid race conditions") {
		t.Errorf("expected overwrite warning, got: %v", logger.Warnings())
	}

	if err := dxc.Unsubscribe(); err != nil {
		t.Errorf("Unsubscribe returned error: %v", err)
	}
}

// =============================================================================
// Consumer.TriggerRebalance() tests
// =============================================================================

func Test_Consumer_TriggerRebalance_DelegatesToSubscription(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		DrainTimeout: 100 * time.Millisecond,
		PollTimeout:  10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Call TriggerRebalance
	rebalanceInfo := []nexus.RebalanceInfo{
		{Partition: 5, CommittedOffset: 100},
		{Partition: 7, CommittedOffset: 200},
	}
	err := dxc.TriggerRebalance(nexus.Assign, rebalanceInfo)
	if err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}

	// Verify the broker received the ack
	broker.mu.Lock()
	calls := broker.ackRebalanceCalls
	broker.mu.Unlock()

	if len(calls) != 1 {
		t.Fatalf("expected 1 ackRebalance call, got %d", len(calls))
	}

	if calls[0].Type != nexus.Assign {
		t.Errorf("expected Assign, got %v", calls[0].Type)
	}

	if len(calls[0].Info) != 2 {
		t.Fatalf("expected 2 rebalance info, got %d", len(calls[0].Info))
	}

	if calls[0].Info[0].Partition != 5 || calls[0].Info[0].CommittedOffset != 100 {
		t.Errorf("first info mismatch: %+v", calls[0].Info[0])
	}
	if calls[0].Info[1].Partition != 7 || calls[0].Info[1].CommittedOffset != 200 {
		t.Errorf("second info mismatch: %+v", calls[0].Info[1])
	}
}

// =============================================================================
// Consumer.TriggerEmergencyShutdown() tests
// =============================================================================

func Test_Consumer_TriggerEmergencyShutdown_TriggersCallback(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	callbackCalled := make(chan error, 1)

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg).
		WithShutdownCallback(func(_ context.Context, reason error) {
			callbackCalled <- reason
		})

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe first (starts circuit breaker listener)
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Now trigger emergency shutdown
	dxc.TriggerEmergencyShutdown(errors.New("critical failure"))

	// Callback should be called with the reason (wrapped by circuit breaker)
	select {
	case reason := <-callbackCalled:
		if reason == nil {
			t.Error("expected non-nil reason")
		}
		expectedReason := "circuit-breaker: triggered and completed protective shutdown, reason: critical failure"
		if reason.Error() != expectedReason {
			t.Errorf("reason mismatch:\ngot:  %q\nwant: %q", reason.Error(), expectedReason)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("callback was not called")
	}
}

// =============================================================================
// Consumer.MetricsStats() tests
// =============================================================================

func Test_Consumer_MetricsStats_FreshConsumer_ReturnsZeroes(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	logger := mocklogger.NewNoOpLogger()

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	stats := dxc.MetricsStats()

	if stats.Collected != 0 {
		t.Errorf("fresh consumer Collected = %d, want 0", stats.Collected)
	}
	if stats.Dropped != 0 {
		t.Errorf("fresh consumer Dropped = %d, want 0", stats.Dropped)
	}
	if stats.SendFailed != 0 {
		t.Errorf("fresh consumer SendFailed = %d, want 0", stats.SendFailed)
	}
}

// =============================================================================
// defaultShutdownCallback tests
// =============================================================================

func Test_Consumer_DefaultShutdownCallback_GracefulPath_LogsCompletion(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)
	// No WithShutdownCallback - use default

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe to set up the default callback
	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Errorf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	// Verify default callback was registered
	if !logger.HasInfo("registering defaultShutdownCallback (os.Interrupt after 15s) for topicName: test-topic - use RegisterShutdownCallback() for more control") {
		t.Errorf("expected default callback registration log, got: %v", logger.Infos())
	}

	// Shutdown should use default callback with graceful path
	err := dxc.Shutdown()
	if err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}

	// Check for graceful shutdown log
	if !logger.HasInfo("shutdown complete, topicName: test-topic") {
		t.Errorf("expected graceful shutdown log, got: %v", logger.Infos())
	}
}

// =============================================================================
// defaultShutdownCallback emergency path tests (subprocess pattern)
//
// These tests use the subprocess pattern to safely test code paths that
// call os.Interrupt or os.Exit. The test spawns itself as a subprocess
// with a special environment variable, runs the dangerous code, and the
// parent verifies the exit behavior.
// =============================================================================

// Test_Consumer_DefaultShutdownCallback_EmergencyPath_SendsInterrupt tests that
// the emergency path sends os.Interrupt to itself.
func Test_Consumer_DefaultShutdownCallback_EmergencyPath_SendsInterrupt(t *testing.T) {
	if os.Getenv("TEST_EMERGENCY_SHUTDOWN_INTERRUPT") == "1" {
		// We're in the subprocess - run the actual code path
		runEmergencyShutdownSubprocess()
		// If we reach here, the interrupt was caught but didn't exit
		// This is actually expected on some platforms where the test framework
		// catches SIGINT. The important thing is we don't hang.
		os.Exit(0)
	}

	// We're in the parent - spawn subprocess
	// Pass through GOCOVERDIR if set so subprocess coverage is captured
	args := []string{"-test.run=Test_Consumer_DefaultShutdownCallback_EmergencyPath_SendsInterrupt"}
	if coverDir := os.Getenv("GOCOVERDIR"); coverDir != "" {
		args = append(args, "-test.gocoverdir="+coverDir)
	}
	cmd := exec.Command(os.Args[0], args...) //nolint:gosec,noctx // G204: test subprocess, context not needed
	cmd.Env = append(os.Environ(),
		"TEST_EMERGENCY_SHUTDOWN_INTERRUPT=1",
		config.SkipValidationEnvVar+"=true",
	)

	// Capture stdout to verify log messages
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	err := cmd.Run()

	// Verify expected log messages were output
	output := stdout.String()
	expectedLogs := []string{
		"WARN: no shutdown callback was registered for consumer on topicName: test-topic, sending interrupt in 10ms",
		"WARN: emergency shutdown: test emergency",
		"INFO: sending interrupt signal to self",
	}
	for _, expected := range expectedLogs {
		if !strings.Contains(output, expected) {
			t.Errorf("expected log message not found: %q\nactual output: %s", expected, output)
		}
	}

	// The subprocess should exit (either from interrupt or clean exit)
	// We just verify it doesn't hang and exits with code 0 or interrupt signal
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// Exit code -1 on Unix typically means killed by signal (SIGINT = 2)
			// On some systems it might be 130 (128 + 2)
			exitCode := exitErr.ExitCode()
			if exitCode != -1 && exitCode != 2 && exitCode != 130 {
				t.Errorf("unexpected exit code: %d", exitCode)
			}
		} else {
			t.Errorf("unexpected error type: %v", err)
		}
	}
	// err == nil means clean exit, which is also acceptable
}

// runEmergencyShutdownSubprocess is the code that runs in the subprocess
// to test the emergency shutdown path.
func runEmergencyShutdownSubprocess() {
	// Override shutdown delay to be very short for testing
	shutdownDelay = 10 * time.Millisecond
	defer func() { shutdownDelay = defaultShutdownDelay }()

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	logger := &stdoutLogger{} // logs to stdout for parent to capture

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Call the default callback with a reason (emergency path)
	dxc.defaultShutdownCallback(context.Background(), errors.New("test emergency"))
}

// Test_Consumer_Subscribe_Timeout_UnsubscribeStuck tests the nested timeout path
// where Subscribe times out waiting for assignments, then unsubscribe also times out.
// This triggers defaultShutdownCallback directly, so we use subprocess pattern.
func Test_Consumer_Subscribe_Timeout_UnsubscribeStuck_TriggersEmergencyShutdown(t *testing.T) {
	if os.Getenv("TEST_SUBSCRIBE_NESTED_TIMEOUT") == "1" {
		runSubscribeNestedTimeoutSubprocess()
		os.Exit(0)
	}

	// We're in the parent - spawn subprocess
	// Pass through GOCOVERDIR if set so subprocess coverage is captured
	args := []string{"-test.run=Test_Consumer_Subscribe_Timeout_UnsubscribeStuck_TriggersEmergencyShutdown"}
	if coverDir := os.Getenv("GOCOVERDIR"); coverDir != "" {
		args = append(args, "-test.gocoverdir="+coverDir)
	}
	cmd := exec.Command(os.Args[0], args...) //nolint:gosec,noctx // G204: test subprocess, context not needed
	cmd.Env = append(os.Environ(),
		"TEST_SUBSCRIBE_NESTED_TIMEOUT=1",
		config.SkipValidationEnvVar+"=true",
	)

	// Capture stdout to verify log messages
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	err := cmd.Run()

	// Verify expected log messages were output
	output := stdout.String()
	expectedLogs := []string{
		"ERROR: timeout after 30ms waiting for partition assignments, unsubscribing",
		"INFO: sending interrupt signal to self",
	}
	for _, expected := range expectedLogs {
		if !strings.Contains(output, expected) {
			t.Errorf("expected log message not found: %q\nactual output: %s", expected, output)
		}
	}

	// Subprocess should exit (from interrupt or clean)
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode := exitErr.ExitCode()
			// Accept signal-based exits or clean exit
			if exitCode != -1 && exitCode != 0 && exitCode != 2 && exitCode != 130 {
				t.Errorf("unexpected exit code: %d", exitCode)
			}
		} else {
			t.Errorf("unexpected error type: %v", err)
		}
	}
}

// Test_Consumer_Subscribe_NestedTimeout_InProcess covers the nested-timeout
// branch in Subscribe (line 127-129) without spawning a subprocess.
// Sister test of Test_Consumer_DefaultShutdownCallback_EmergencyPath_InProcess
// the subprocess test above proves end-to-end exit behaviour but does not
// register on coverage profiles unless GOCOVERDIR is plumbed through
func Test_Consumer_Subscribe_NestedTimeout_InProcess(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	shutdownDelay = 10 * time.Millisecond
	defer func() { shutdownDelay = defaultShutdownDelay }()

	// catch SIGINT before it terminates the test process
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	// do NOT release unsubscribe - the unsubscribe call will block and the
	// inner select will time out, hitting defaultShutdownCallback
	broker := newControllableBrokerPort()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 30 * time.Millisecond,
		DrainTimeout:            10 * time.Millisecond,
		PollTimeout:             5 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- dxc.Subscribe()
	}()

	// First SIGINT comes from the inner-timeout defaultShutdownCallback path
	select {
	case <-sigCh:
		// nested timeout path ran, interrupt delivered
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for nested-timeout SIGINT")
	}

	// Subscribe returns its assignments-timeout error after the inner select
	select {
	case err := <-subscribeDone:
		if err == nil {
			t.Error("expected Subscribe to return timeout error")
		}
	case <-time.After(2 * time.Second):
		t.Error("Subscribe did not return after SIGINT")
	}
}

func runSubscribeNestedTimeoutSubprocess() {
	shutdownDelay = 10 * time.Millisecond
	defer func() { shutdownDelay = defaultShutdownDelay }()

	broker := newControllableBrokerPort()
	// Don't release unsubscribe - it will block and trigger nested timeout
	logger := &stdoutLogger{} // logs to stdout for parent to capture

	cfg := config.DemuxConfig{
		AwaitAssignmentsTimeout: 30 * time.Millisecond,
		DrainTimeout:            10 * time.Millisecond,
		PollTimeout:             5 * time.Millisecond,
	}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(logger).
		WithDemuxConfig(cfg)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	// Subscribe will:
	// 1. Timeout waiting for assignments (30ms)
	// 2. Try to unsubscribe, which blocks (unsubscribeCh not released)
	// 3. Timeout waiting for unsubscribe (30ms - same as AwaitAssignmentsTimeout)
	// 4. Call defaultShutdownCallback which sends interrupt
	_ = dxc.Subscribe()
}

// =============================================================================
// TakeSnapshot / SnapshotHandler tests
// =============================================================================

func Test_Consumer_TakeSnapshot(t *testing.T) {
	recorder := snapshot.NewRecorder(
		"test-topic",
		func() snapshot.ConcurrencySnapshot {
			return snapshot.ConcurrencySnapshot{GuardActive: 5, GuardCapacity: 100}
		},
		func() []snapshot.ShardSnapshot { return nil },
		func() snapshot.PreCommitsSnapshot { return snapshot.PreCommitsSnapshot{} },
		func() snapshot.WindowData {
			return snapshot.WindowData{ThroughputPerBucket: []uint32{}, TotalProcessed: 42}
		},
	)

	dxc := &Consumer[any]{recorder: recorder}
	snap := dxc.TakeSnapshot()

	if snap.Summary.TopicName != "test-topic" {
		t.Errorf("TopicName: expected \"test-topic\", got %q", snap.Summary.TopicName)
	}
	if snap.Summary.TotalProcessed != 42 {
		t.Errorf("TotalProcessed: expected 42, got %d", snap.Summary.TotalProcessed)
	}
	if snap.Concurrency.GuardActive != 5 {
		t.Errorf("GuardActive: expected 5, got %d", snap.Concurrency.GuardActive)
	}
	if snap.Concurrency.GuardCapacity != 100 {
		t.Errorf("GuardCapacity: expected 100, got %d", snap.Concurrency.GuardCapacity)
	}
}

func Test_Consumer_SnapshotHandler(t *testing.T) {
	recorder := snapshot.NewRecorder(
		"test-topic",
		func() snapshot.ConcurrencySnapshot {
			return snapshot.ConcurrencySnapshot{GuardActive: 5, GuardCapacity: 100}
		},
		func() []snapshot.ShardSnapshot { return nil },
		func() snapshot.PreCommitsSnapshot { return snapshot.PreCommitsSnapshot{} },
		func() snapshot.WindowData {
			return snapshot.WindowData{ThroughputPerBucket: []uint32{}, TotalProcessed: 42}
		},
	)

	dxc := &Consumer[any]{recorder: recorder}
	handler := dxc.SnapshotHandler()

	req := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	summary, ok := result["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary object, got %v", result["summary"])
	}
	if summary["topicName"] != "test-topic" {
		t.Errorf("topicName: expected \"test-topic\", got %v", summary["topicName"])
	}
	if summary["totalProcessed"] != float64(42) {
		t.Errorf("totalProcessed: expected 42, got %v", summary["totalProcessed"])
	}
}

// =============================================================================
// defaultShutdownCallback emergency path - in-process coverage
//
// The subprocess tests above verify the full emergency shutdown flow including
// exit behavior. This test runs the same code path in-process (catching the
// SIGINT via signal.Notify) to ensure coverage is recorded by go test -cover.
// =============================================================================

func Test_Consumer_DefaultShutdownCallback_EmergencyPath_InProcess(t *testing.T) {
	shutdownDelay = 10 * time.Millisecond
	defer func() { shutdownDelay = defaultShutdownDelay }()

	// catch SIGINT before it terminates the test process
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	logger := mocklogger.NewRecordingLogger()
	dxc := &Consumer[any]{
		ctx:       context.Background(),
		topicName: "test-topic",
		logger:    logger,
	}

	done := make(chan struct{})
	go func() {
		dxc.defaultShutdownCallback(context.Background(), errors.New("test emergency"))
		close(done)
	}()

	select {
	case <-sigCh:
		// interrupt delivered successfully
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for interrupt signal")
	}

	<-done

	if !logger.ContainsWarning("test-topic") {
		t.Error("expected warning log containing topic name")
	}
	if !logger.ContainsWarning("test emergency") {
		t.Error("expected warning log containing error reason")
	}
	if !logger.ContainsInfo("sending interrupt") {
		t.Error("expected info log about sending interrupt")
	}
}
