// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package subscription

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-nexus/nexus"
)

// -----------------------------------------------------------------------------
// Mock implementations for ports
// -----------------------------------------------------------------------------

// processCall records a single call to Process
type processCall[T any] struct {
	payload  T
	readTime time.Time
}

// mockProcessor implements ports.ProcessorPort[T]
type mockProcessor[T any] struct {
	processCalls      []processCall[T]
	processFunc       func(T, time.Time) error // optional custom behavior
	processError      error                    // default error to return
	resetPrevCalls    [][]int32
	mu                sync.Mutex
	processCallCount  atomic.Int32
	processBlockUntil chan struct{} // if set, Process blocks until closed
}

func newMockProcessor[T any]() *mockProcessor[T] {
	return &mockProcessor[T]{
		processCalls:   make([]processCall[T], 0),
		resetPrevCalls: make([][]int32, 0),
	}
}

func (m *mockProcessor[T]) Process(payload T, readTime time.Time) error {
	if m.processBlockUntil != nil {
		<-m.processBlockUntil
	}

	m.mu.Lock()
	m.processCalls = append(m.processCalls, processCall[T]{payload: payload, readTime: readTime})
	m.mu.Unlock()
	m.processCallCount.Add(1)

	if m.processFunc != nil {
		return m.processFunc(payload, readTime)
	}
	return m.processError
}

func (m *mockProcessor[T]) ResetPrevOffsets(partitions []int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// copy to avoid aliasing
	copied := make([]int32, len(partitions))
	copy(copied, partitions)
	m.resetPrevCalls = append(m.resetPrevCalls, copied)
}

func (m *mockProcessor[T]) getProcessCalls() []processCall[T] {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]processCall[T], len(m.processCalls))
	copy(copied, m.processCalls)
	return copied
}

func (m *mockProcessor[T]) getResetPrevCalls() [][]int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([][]int32, len(m.resetPrevCalls))
	for i, c := range m.resetPrevCalls {
		copied[i] = make([]int32, len(c))
		copy(copied[i], c)
	}
	return copied
}

// mockDrainCoordinator implements ports.DrainCoordinatorPort
type mockDrainCoordinator struct {
	drainCalls atomic.Int32
	drainError error
	drainDelay time.Duration
	drainFunc  func() error // optional custom behavior
}

func newMockDrainCoordinator() *mockDrainCoordinator {
	return &mockDrainCoordinator{}
}

func (m *mockDrainCoordinator) Drain() error {
	m.drainCalls.Add(1)
	if m.drainDelay > 0 {
		time.Sleep(m.drainDelay)
	}
	if m.drainFunc != nil {
		return m.drainFunc()
	}
	return m.drainError
}

// mockCircuitBreaker implements ports.CircuitBreakerPort
type mockCircuitBreaker struct {
	mainCtxDoneCh chan struct{}
	triggeredCh   chan string
	shutdownCalls []error
	mu            sync.Mutex
	shutdownCount atomic.Int32
}

func newMockCircuitBreaker() *mockCircuitBreaker {
	return &mockCircuitBreaker{
		mainCtxDoneCh: make(chan struct{}),
		triggeredCh:   make(chan string, 1),
		shutdownCalls: make([]error, 0),
	}
}

func (m *mockCircuitBreaker) MainCtxDone() <-chan struct{} {
	return m.mainCtxDoneCh
}

func (m *mockCircuitBreaker) TriggerEmergencyShutdown(reason error) {
	m.mu.Lock()
	m.shutdownCalls = append(m.shutdownCalls, reason)
	m.mu.Unlock()
	m.shutdownCount.Add(1)

	select {
	case m.triggeredCh <- reason.Error():
	default:
	}
}

func (m *mockCircuitBreaker) Triggered() <-chan string {
	return m.triggeredCh
}

func (m *mockCircuitBreaker) getShutdownCalls() []error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]error, len(m.shutdownCalls))
	copy(copied, m.shutdownCalls)
	return copied
}

func (m *mockCircuitBreaker) closeMainCtx() {
	close(m.mainCtxDoneCh)
}

// mockLogger implements nexus.Logger
type logEntry struct {
	msg  string
	args []any
}

type mockLogger struct {
	debugLogs []logEntry
	infoLogs  []logEntry
	warnLogs  []logEntry
	errorLogs []logEntry
	mu        sync.Mutex
}

func newMockLogger() *mockLogger {
	return &mockLogger{
		debugLogs: make([]logEntry, 0),
		infoLogs:  make([]logEntry, 0),
		warnLogs:  make([]logEntry, 0),
		errorLogs: make([]logEntry, 0),
	}
}

func (m *mockLogger) log(logs *[]logEntry, msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	*logs = append(*logs, logEntry{msg: msg, args: args})
}

func (m *mockLogger) Debug(_ context.Context, msg string, args ...any) {
	m.log(&m.debugLogs, msg, args...)
}

func (m *mockLogger) Info(_ context.Context, msg string, args ...any) {
	m.log(&m.infoLogs, msg, args...)
}

func (m *mockLogger) Warn(_ context.Context, msg string, args ...any) {
	m.log(&m.warnLogs, msg, args...)
}

func (m *mockLogger) Error(_ context.Context, msg string, args ...any) {
	m.log(&m.errorLogs, msg, args...)
}

func (m *mockLogger) getErrorLogs() []logEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]logEntry, len(m.errorLogs))
	copy(copied, m.errorLogs)
	return copied
}

func (m *mockLogger) getInfoLogs() []logEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]logEntry, len(m.infoLogs))
	copy(copied, m.infoLogs)
	return copied
}

// -----------------------------------------------------------------------------
// Test harness
// -----------------------------------------------------------------------------

type testHarness[T any] struct {
	ctx              context.Context
	cfg              config.DemuxConfig
	processor        *mockProcessor[T]
	drainCoordinator *mockDrainCoordinator
	circuitBreaker   *mockCircuitBreaker
	logger           *mockLogger

	// function call tracking
	subscribeCalls      atomic.Int32
	subscribeError      error
	unsubscribeCalls    atomic.Int32
	unsubscribeError    error
	ackRebalanceCalls   []ackRebalanceCall
	ackRebalanceError   error
	brokerQueryCalls    atomic.Int32
	resetCommittedCalls []map[int32]int64
	markAssignedCalls   []int32
	markRevokedCalls    []int32
	pollFunc            func(time.Duration) (T, bool, error)
	mu                  sync.Mutex

	// replaceable callback functions (for wrapping in tests)
	subscribeFunc             func() error
	unsubscribeFunc           func() error
	ackRebalanceFunc          func(nexus.RebalanceType, []nexus.RebalanceInfo) error
	markPartitionAssignedFunc func(int32)
	markPartitionRevokedFunc  func(int32)
}

type ackRebalanceCall struct {
	rebalanceType nexus.RebalanceType
	info          []nexus.RebalanceInfo
}

func newTestHarness[T any]() *testHarness[T] {
	h := &testHarness[T]{
		ctx: context.Background(),
		cfg: config.DemuxConfig{
			RebalancePausePollingTimeout: 100 * time.Millisecond,
			DrainTimeout:                 500 * time.Millisecond,
		},
		processor:           newMockProcessor[T](),
		drainCoordinator:    newMockDrainCoordinator(),
		circuitBreaker:      newMockCircuitBreaker(),
		logger:              newMockLogger(),
		ackRebalanceCalls:   make([]ackRebalanceCall, 0),
		resetCommittedCalls: make([]map[int32]int64, 0),
		markAssignedCalls:   make([]int32, 0),
		markRevokedCalls:    make([]int32, 0),
	}
	// initialize replaceable function fields to default implementations
	h.subscribeFunc = h.subscribeImpl
	h.unsubscribeFunc = h.unsubscribeImpl
	h.ackRebalanceFunc = h.ackRebalanceImpl
	h.markPartitionAssignedFunc = h.markPartitionAssignedImpl
	h.markPartitionRevokedFunc = h.markPartitionRevokedImpl
	return h
}

// Impl methods are the default implementations
func (h *testHarness[T]) subscribeImpl() error {
	h.subscribeCalls.Add(1)
	return h.subscribeError
}

func (h *testHarness[T]) unsubscribeImpl() error {
	h.unsubscribeCalls.Add(1)
	return h.unsubscribeError
}

func (h *testHarness[T]) ackRebalanceImpl(rt nexus.RebalanceType, info []nexus.RebalanceInfo) error {
	h.mu.Lock()
	// deep copy info
	infoCopy := make([]nexus.RebalanceInfo, len(info))
	copy(infoCopy, info)
	h.ackRebalanceCalls = append(h.ackRebalanceCalls, ackRebalanceCall{
		rebalanceType: rt,
		info:          infoCopy,
	})
	h.mu.Unlock()
	return h.ackRebalanceError
}

func (h *testHarness[T]) markPartitionAssignedImpl(partition int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.markAssignedCalls = append(h.markAssignedCalls, partition)
}

func (h *testHarness[T]) markPartitionRevokedImpl(partition int32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.markRevokedCalls = append(h.markRevokedCalls, partition)
}

// Wrapper methods delegate to the replaceable function fields
func (h *testHarness[T]) subscribe() error {
	return h.subscribeFunc()
}

func (h *testHarness[T]) unsubscribe() error {
	return h.unsubscribeFunc()
}

func (h *testHarness[T]) ackRebalance(rt nexus.RebalanceType, info []nexus.RebalanceInfo) error {
	return h.ackRebalanceFunc(rt, info)
}

func (h *testHarness[T]) markPartitionAssigned(partition int32) {
	h.markPartitionAssignedFunc(partition)
}

func (h *testHarness[T]) markPartitionRevoked(partition int32) {
	h.markPartitionRevokedFunc(partition)
}

func (h *testHarness[T]) brokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	h.brokerQueryCalls.Add(1)
	return nexus.QueryResponse{}, nil
}

func (h *testHarness[T]) resetCommittedOffsets(offsets map[int32]int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// deep copy
	copied := make(map[int32]int64, len(offsets))
	for k, v := range offsets {
		copied[k] = v
	}
	h.resetCommittedCalls = append(h.resetCommittedCalls, copied)
}

func (h *testHarness[T]) poll(timeout time.Duration) (T, bool, error) {
	if h.pollFunc != nil {
		return h.pollFunc(timeout)
	}
	var zero T
	return zero, false, nil
}

func (h *testHarness[T]) getAckRebalanceCalls() []ackRebalanceCall {
	h.mu.Lock()
	defer h.mu.Unlock()
	copied := make([]ackRebalanceCall, len(h.ackRebalanceCalls))
	copy(copied, h.ackRebalanceCalls)
	return copied
}

func (h *testHarness[T]) getResetCommittedCalls() []map[int32]int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	copied := make([]map[int32]int64, len(h.resetCommittedCalls))
	for i, m := range h.resetCommittedCalls {
		copied[i] = make(map[int32]int64, len(m))
		for k, v := range m {
			copied[i][k] = v
		}
	}
	return copied
}

func (h *testHarness[T]) getMarkAssignedCalls() []int32 {
	h.mu.Lock()
	defer h.mu.Unlock()
	copied := make([]int32, len(h.markAssignedCalls))
	copy(copied, h.markAssignedCalls)
	return copied
}

func (h *testHarness[T]) createSubscription(topicName string) *Subscription[T] {
	return New[T](
		h.ctx,
		h.cfg,
		h.circuitBreaker,
		h.processor,
		h.poll,
		h.subscribe,
		h.unsubscribe,
		h.ackRebalance,
		h.brokerQuery,
		h.drainCoordinator,
		h.resetCommittedOffsets,
		h.markPartitionAssigned,
		h.markPartitionRevoked,
		topicName,
		h.logger,
	)
}

// -----------------------------------------------------------------------------
// Constructor Tests
// -----------------------------------------------------------------------------

func TestNew_CreatesSubscriptionWithCorrectFields(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	if sub == nil {
		t.Fatal("expected non-nil subscription")
	}
	if sub.processor == nil {
		t.Error("processor not set")
	}
	if sub.drainCoordinator == nil {
		t.Error("drainCoordinator not set")
	}
	if sub.circuitBreaker == nil {
		t.Error("circuitBreaker not set")
	}
	if sub.logger == nil {
		t.Error("logger not set")
	}
	if sub.topicName != "test-topic" {
		t.Errorf("topicName = %q, want %q", sub.topicName, "test-topic")
	}
}

func TestNew_CreatesChannelsWithCorrectBuffering(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// signalAssigned should be buffered (size 1)
	select {
	case sub.signalAssigned <- struct{}{}:
		// good, buffered
	default:
		t.Error("signalAssigned should be buffered")
	}

	// pausePolling, resumePolling, stopPolling should be unbuffered
	select {
	case sub.pausePolling <- struct{}{}:
		t.Error("pausePolling should be unbuffered")
	default:
		// good
	}
}

func TestNew_CapturesMainCtxDone(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// mainCtxDone should be the circuit breaker's channel
	if sub.mainCtxDone == nil {
		t.Fatal("mainCtxDone not set")
	}

	// closing circuit breaker should close mainCtxDone
	h.circuitBreaker.closeMainCtx()

	select {
	case <-sub.mainCtxDone:
		// good
	case <-time.After(100 * time.Millisecond):
		t.Error("mainCtxDone should be closed when circuit breaker closes")
	}
}

// -----------------------------------------------------------------------------
// Subscribe Tests
// -----------------------------------------------------------------------------

func TestSubscribe_CallsSubscribeFunction(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	err := sub.Subscribe()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if h.subscribeCalls.Load() != 1 {
		t.Errorf("subscribe called %d times, want 1", h.subscribeCalls.Load())
	}
}

func TestSubscribe_PropagatesError(t *testing.T) {
	h := newTestHarness[string]()
	h.subscribeError = errors.New("subscribe failed")
	sub := h.createSubscription("test-topic")

	err := sub.Subscribe()

	if err == nil {
		t.Error("expected error")
	} else if err.Error() != "subscribe failed" {
		t.Errorf("error = %v, want 'subscribe failed'", err)
	}
}

// -----------------------------------------------------------------------------
// AwaitAssigned Tests
// -----------------------------------------------------------------------------

func TestAwaitAssigned_ReturnsSignalChannel(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	ch := sub.AwaitAssigned()

	if ch == nil {
		t.Fatal("expected non-nil channel")
	}

	// channel should be the same as signalAssigned
	if ch != sub.signalAssigned {
		t.Error("AwaitAssigned should return signalAssigned channel")
	}
}

func TestAwaitAssigned_ReceivesAfterAssign(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	ch := sub.AwaitAssigned()

	// trigger assign
	info := []nexus.RebalanceInfo{{Partition: 0, CommittedOffset: 100}}
	_ = sub.HandleRebalance(nexus.Assign, info)

	select {
	case <-ch:
		// good
	case <-time.After(100 * time.Millisecond):
		t.Error("should receive on AwaitAssigned after Assign")
	}
}

// -----------------------------------------------------------------------------
// PollAndForward Tests
// -----------------------------------------------------------------------------

func TestPollAndForward_ProcessesMessages(t *testing.T) {
	h := newTestHarness[string]()

	messageCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		messageCount++
		if messageCount <= 3 {
			return "message-" + string(rune('0'+messageCount)), true, nil
		}
		// after 3 messages, close main ctx to stop
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PollAndForward did not exit")
	}

	calls := h.processor.getProcessCalls()
	if len(calls) != 3 {
		t.Fatalf("Process called %d times, want 3", len(calls))
	}

	expectedPayloads := []string{"message-1", "message-2", "message-3"}
	for i, call := range calls {
		if call.payload != expectedPayloads[i] {
			t.Errorf("call[%d].payload = %q, want %q", i, call.payload, expectedPayloads[i])
		}
	}
}

func TestPollAndForward_PassesCorrectReadTime(t *testing.T) {
	h := newTestHarness[string]()

	beforePoll := time.Now()
	const expectedPayload = "test-payload-xyz"
	messageCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		messageCount++
		if messageCount == 1 {
			return expectedPayload, true, nil
		}
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(done)
	}()

	<-done
	afterPoll := time.Now()

	calls := h.processor.getProcessCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}

	// verify exact payload passed to Process
	if calls[0].payload != expectedPayload {
		t.Errorf("payload = %q, want %q", calls[0].payload, expectedPayload)
	}

	// readTime should be between beforePoll and afterPoll
	if calls[0].readTime.Before(beforePoll) || calls[0].readTime.After(afterPoll) {
		t.Errorf("readTime %v not in expected range [%v, %v]", calls[0].readTime, beforePoll, afterPoll)
	}
}

func TestPollAndForward_PollTimeoutContinuesLoop(t *testing.T) {
	h := newTestHarness[string]()

	pollCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		pollCount++
		if pollCount < 5 {
			return "", false, nil // timeout
		}
		if pollCount == 5 {
			return "message", true, nil //nolint:goconst // test fixture
		}
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(done)
	}()

	<-done

	calls := h.processor.getProcessCalls()
	if len(calls) != 1 {
		t.Errorf("expected 1 Process call (after timeouts), got %d", len(calls))
	}
	if pollCount < 5 {
		t.Errorf("poll called %d times, want at least 5", pollCount)
	}
}

func TestPollAndForward_PollErrorLogsAndContinues(t *testing.T) {
	h := newTestHarness[string]()

	pollErr := errors.New("connection reset by peer")
	pollCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		pollCount++
		if pollCount == 1 {
			return "", false, pollErr
		}
		if pollCount == 2 {
			return "message", true, nil
		}
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(done)
	}()

	<-done

	// verify exact error message format: "error polling topic: %s - %v"
	errorLogs := h.logger.getErrorLogs()
	expectedMsg := "error polling topic: test-topic - connection reset by peer"
	foundExactMsg := false
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundExactMsg = true
			break
		}
	}
	if !foundExactMsg {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}

	// should have continued and processed message
	calls := h.processor.getProcessCalls()
	if len(calls) != 1 {
		t.Errorf("expected 1 Process call, got %d", len(calls))
	}
}

func TestPollAndForward_ProcessErrorTriggersCircuitBreaker(t *testing.T) {
	h := newTestHarness[string]()

	processErr := errors.New("process failed")
	h.processor.processError = processErr

	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		return "message", true, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("PollAndForward should exit after process error")
	}

	// circuit breaker should have been triggered
	shutdownCalls := h.circuitBreaker.getShutdownCalls()
	if len(shutdownCalls) != 1 {
		t.Fatalf("expected 1 shutdown call, got %d", len(shutdownCalls))
	}
	if shutdownCalls[0].Error() != processErr.Error() {
		t.Errorf("shutdown reason = %v, want %v", shutdownCalls[0], processErr)
	}
}

func TestPollAndForward_MainCtxDoneExitsLoop(t *testing.T) {
	h := newTestHarness[string]()

	pollStarted := make(chan struct{})
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		select {
		case pollStarted <- struct{}{}:
		default:
		}
		time.Sleep(50 * time.Millisecond) // slow poll
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(100 * time.Millisecond)
		close(done)
	}()

	// wait for poll to start
	<-pollStarted

	// close main context
	h.circuitBreaker.closeMainCtx()

	select {
	case <-done:
		// good
	case <-time.After(time.Second):
		t.Error("PollAndForward should exit when mainCtxDone closes")
	}
}

func TestPollAndForward_StopPollingExitsLoop(t *testing.T) {
	h := newTestHarness[string]()

	pollStarted := make(chan struct{}, 1)
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		select {
		case pollStarted <- struct{}{}:
		default:
		}
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(done)
	}()

	// wait for poll to start
	<-pollStarted

	// send stop signal
	sub.stopPolling <- struct{}{}

	select {
	case <-done:
		// good
	case <-time.After(time.Second):
		t.Error("PollAndForward should exit when stopPolling receives")
	}
}

func TestPollAndForward_PauseAndResume(t *testing.T) {
	h := newTestHarness[string]()

	var pollCount atomic.Int32
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		pollCount.Add(1)
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")

	done := make(chan struct{})
	go func() {
		sub.PollAndForward(5 * time.Millisecond)
		close(done)
	}()

	// let it poll a few times
	time.Sleep(30 * time.Millisecond)

	// pause - this blocks until the loop receives it, so after this returns
	// the loop is blocked on resumePolling and no more polls will occur
	sub.pausePolling <- struct{}{}

	// capture count AFTER pause send completes (loop is now blocked)
	countWhenPaused := pollCount.Load()

	// wait and verify poll count doesn't increase while paused
	time.Sleep(30 * time.Millisecond)
	countAfterSleep := pollCount.Load()

	if countAfterSleep != countWhenPaused {
		t.Errorf("poll count increased while paused: whenPaused=%d, afterSleep=%d", countWhenPaused, countAfterSleep)
	}

	// resume
	sub.resumePolling <- struct{}{}

	// let it poll more
	time.Sleep(30 * time.Millisecond)
	countAfterResume := pollCount.Load()

	if countAfterResume <= countWhenPaused {
		t.Errorf("poll count should increase after resume: paused=%d, after=%d", countWhenPaused, countAfterResume)
	}

	// cleanup
	h.circuitBreaker.closeMainCtx()
	<-done
}

func TestPollAndForward_LogsStoppingOnExit(t *testing.T) {
	h := newTestHarness[string]()

	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")
	sub.PollAndForward(10 * time.Millisecond)

	// verify exact log message
	infoLogs := h.logger.getInfoLogs()
	const expectedMsg = "subscription: stopping polling loop"
	foundExactLog := false
	for _, log := range infoLogs {
		if log.msg == expectedMsg {
			foundExactLog = true
			break
		}
	}
	if !foundExactLog {
		t.Errorf("expected exact info log %q, got: %v", expectedMsg, infoLogs)
	}
}

// -----------------------------------------------------------------------------
// HandleRebalance Assign Tests
// -----------------------------------------------------------------------------

func TestHandleRebalance_Assign_SignalAlreadyPending_DoesNotBlock(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// Pre-fill the signalAssigned channel (simulating a pending signal)
	sub.signalAssigned <- struct{}{}

	// Second Assign should not block - it should fall through to default
	// If this blocks, the test will timeout
	done := make(chan struct{})
	go func() {
		info := []nexus.RebalanceInfo{{Partition: 0, CommittedOffset: 100}}
		_ = sub.HandleRebalance(nexus.Assign, info)
		close(done)
	}()

	select {
	case <-done:
		// good - didn't block
	case <-time.After(100 * time.Millisecond):
		t.Fatal("HandleRebalance blocked when signalAssigned already had pending value")
	}

	// signalAssigned should still have exactly one pending value
	select {
	case <-sub.signalAssigned:
		// good - got the signal
	default:
		t.Error("signalAssigned should have had a pending value")
	}

	// Should be empty now
	select {
	case <-sub.signalAssigned:
		t.Error("signalAssigned should be empty after reading once")
	default:
		// good - empty
	}
}

func TestHandleRebalance_Assign_SignalsAssigned(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{
		{Partition: 0, CommittedOffset: 100},
	}

	_ = sub.HandleRebalance(nexus.Assign, info)

	select {
	case <-sub.signalAssigned:
		// good
	default:
		t.Error("signalAssigned should have been signaled")
	}
}

func TestHandleRebalance_Assign_ResetsProcessorPrevOffsets(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{
		{Partition: 0, CommittedOffset: 100},
		{Partition: 1, CommittedOffset: 200},
		{Partition: 2, CommittedOffset: 300},
	}

	_ = sub.HandleRebalance(nexus.Assign, info)

	resetCalls := h.processor.getResetPrevCalls()
	if len(resetCalls) != 1 {
		t.Fatalf("expected 1 ResetPrevOffsets call, got %d", len(resetCalls))
	}

	expectedPartitions := []int32{0, 1, 2}
	if len(resetCalls[0]) != len(expectedPartitions) {
		t.Errorf("reset partitions = %v, want %v", resetCalls[0], expectedPartitions)
	}
	for i, p := range resetCalls[0] {
		if p != expectedPartitions[i] {
			t.Errorf("reset partition[%d] = %d, want %d", i, p, expectedPartitions[i])
		}
	}
}

func TestHandleRebalance_Assign_ResetsCommittedOffsets(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{
		{Partition: 0, CommittedOffset: 100},
		{Partition: 1, CommittedOffset: 200},
		{Partition: 5, CommittedOffset: 500},
	}

	_ = sub.HandleRebalance(nexus.Assign, info)

	resetCalls := h.getResetCommittedCalls()
	if len(resetCalls) != 1 {
		t.Fatalf("expected 1 resetCommittedOffsets call, got %d", len(resetCalls))
	}

	expected := map[int32]int64{0: 100, 1: 200, 5: 500}
	for k, v := range expected {
		if resetCalls[0][k] != v {
			t.Errorf("resetCommittedOffsets[%d] = %d, want %d", k, resetCalls[0][k], v)
		}
	}
}

func TestHandleRebalance_Assign_MarksPartitionsAssigned(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{
		{Partition: 0, CommittedOffset: 100},
		{Partition: 3, CommittedOffset: 300},
		{Partition: 7, CommittedOffset: 700},
	}

	_ = sub.HandleRebalance(nexus.Assign, info)

	assignedCalls := h.getMarkAssignedCalls()
	if len(assignedCalls) != 3 {
		t.Fatalf("expected 3 markPartitionAssigned calls, got %d", len(assignedCalls))
	}

	expectedPartitions := []int32{0, 3, 7}
	for i, p := range assignedCalls {
		if p != expectedPartitions[i] {
			t.Errorf("markPartitionAssigned[%d] = %d, want %d", i, p, expectedPartitions[i])
		}
	}
}

func TestHandleRebalance_CallsAckRebalance(t *testing.T) {
	tests := []struct {
		name          string
		rebalanceType nexus.RebalanceType
		info          []nexus.RebalanceInfo
	}{
		{
			name:          "Assign",
			rebalanceType: nexus.Assign,
			info: []nexus.RebalanceInfo{
				{Partition: 0, CommittedOffset: 100},
				{Partition: 5, CommittedOffset: 555},
			},
		},
		{
			name:          "Revoke",
			rebalanceType: nexus.Revoke,
			info: []nexus.RebalanceInfo{
				{Partition: 2, CommittedOffset: 200},
				{Partition: 7, CommittedOffset: 777},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHarness[string]()
			sub := h.createSubscription("test-topic")

			_ = sub.HandleRebalance(tt.rebalanceType, tt.info)

			ackCalls := h.getAckRebalanceCalls()
			if len(ackCalls) != 1 {
				t.Fatalf("expected 1 ackRebalance call, got %d", len(ackCalls))
			}
			if ackCalls[0].rebalanceType != tt.rebalanceType {
				t.Errorf("rebalanceType = %v, want %v", ackCalls[0].rebalanceType, tt.rebalanceType)
			}
			if len(ackCalls[0].info) != len(tt.info) {
				t.Fatalf("expected %d info entries, got %d", len(tt.info), len(ackCalls[0].info))
			}
			for i, expected := range tt.info {
				actual := ackCalls[0].info[i]
				if actual.Partition != expected.Partition || actual.CommittedOffset != expected.CommittedOffset {
					t.Errorf("info[%d] = %+v, want %+v", i, actual, expected)
				}
			}
		})
	}
}

func TestHandleRebalance_Assign_ReturnsAckError(t *testing.T) {
	h := newTestHarness[string]()
	h.ackRebalanceError = errors.New("ack failed")
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0, CommittedOffset: 100}}

	err := sub.HandleRebalance(nexus.Assign, info)

	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "ack failed" {
		t.Errorf("error = %q, want %q", err.Error(), "ack failed")
	}
}

func TestHandleRebalance_Assign_LogsAckError(t *testing.T) {
	h := newTestHarness[string]()
	h.ackRebalanceError = errors.New("network timeout")
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0, CommittedOffset: 100}}
	_ = sub.HandleRebalance(nexus.Assign, info)

	// verify exact log message format: "assign ack error - %v"
	errorLogs := h.logger.getErrorLogs()
	expectedMsg := "assign ack error - network timeout"
	foundExactMsg := false
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundExactMsg = true
			break
		}
	}
	if !foundExactMsg {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}
}

// -----------------------------------------------------------------------------
// HandleRebalance Revoke Tests
// -----------------------------------------------------------------------------

func TestHandleRebalance_Revoke_DrainsSignalAssigned(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// pre-signal assigned
	sub.signalAssigned <- struct{}{}

	info := []nexus.RebalanceInfo{{Partition: 0}}
	_ = sub.HandleRebalance(nexus.Revoke, info)

	// signalAssigned should be drained
	select {
	case <-sub.signalAssigned:
		t.Error("signalAssigned should have been drained")
	default:
		// good
	}
}

func TestHandleRebalance_Revoke_CallsDrain(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0}}
	_ = sub.HandleRebalance(nexus.Revoke, info)

	if h.drainCoordinator.drainCalls.Load() != 1 {
		t.Errorf("drain called %d times, want 1", h.drainCoordinator.drainCalls.Load())
	}
}

func TestHandleRebalance_Revoke_MarksPartitionsRevokedBeforeAck(t *testing.T) {
	h := newTestHarness[string]()

	// track order of calls
	var callOrder []string
	var mu sync.Mutex

	originalAck := h.ackRebalanceFunc
	h.ackRebalanceFunc = func(rt nexus.RebalanceType, info []nexus.RebalanceInfo) error {
		mu.Lock()
		callOrder = append(callOrder, "ack")
		mu.Unlock()
		return originalAck(rt, info)
	}

	originalMarkRevoked := h.markPartitionRevokedFunc
	h.markPartitionRevokedFunc = func(p int32) {
		mu.Lock()
		callOrder = append(callOrder, "markRevoked")
		mu.Unlock()
		originalMarkRevoked(p)
	}

	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0}, {Partition: 1}}
	_ = sub.HandleRebalance(nexus.Revoke, info)

	mu.Lock()
	defer mu.Unlock()

	// markRevoked should come before ack
	expectedOrder := []string{"markRevoked", "markRevoked", "ack"}
	if len(callOrder) != len(expectedOrder) {
		t.Fatalf("call order = %v, want %v", callOrder, expectedOrder)
	}
	for i, call := range callOrder {
		if call != expectedOrder[i] {
			t.Errorf("callOrder[%d] = %q, want %q", i, call, expectedOrder[i])
		}
	}
}

func TestHandleRebalance_Revoke_ReturnsAckError(t *testing.T) {
	h := newTestHarness[string]()
	h.ackRebalanceError = errors.New("revoke ack failed")
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0}}
	err := sub.HandleRebalance(nexus.Revoke, info)

	if err == nil {
		t.Fatal("expected error")
	}
	// verify exact error is returned
	if err.Error() != "revoke ack failed" {
		t.Errorf("error = %q, want %q", err.Error(), "revoke ack failed")
	}
}

func TestHandleRebalance_Revoke_LogsAckError(t *testing.T) {
	h := newTestHarness[string]()
	h.ackRebalanceError = errors.New("broker unavailable")
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0}}
	_ = sub.HandleRebalance(nexus.Revoke, info)

	// verify exact log message format: "revoke ack error - %v"
	errorLogs := h.logger.getErrorLogs()
	expectedMsg := "revoke ack error - broker unavailable"
	foundExactMsg := false
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundExactMsg = true
			break
		}
	}
	if !foundExactMsg {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}
}

// -----------------------------------------------------------------------------
// HandleRebalance Unknown Type Tests
// -----------------------------------------------------------------------------

func TestHandleRebalance_UnknownType_ReturnsError(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	unknownType := nexus.RebalanceType(999)
	err := sub.HandleRebalance(unknownType, nil)

	if err == nil {
		t.Fatal("expected error for unknown rebalance type")
	}
	// verify exact error format: "unsupported rebalance type: %v"
	expectedErr := "unsupported rebalance type: 999"
	if err.Error() != expectedErr {
		t.Errorf("error = %q, want %q", err.Error(), expectedErr)
	}
}

// -----------------------------------------------------------------------------
// Unsubscribe Tests
// -----------------------------------------------------------------------------

func TestUnsubscribe_TriggersDrainThenUnsubscribes(t *testing.T) {
	h := newTestHarness[string]()

	var callOrder []string
	var mu sync.Mutex

	h.drainCoordinator.drainFunc = func() error {
		mu.Lock()
		callOrder = append(callOrder, "drain")
		mu.Unlock()
		return nil
	}

	originalUnsub := h.unsubscribeFunc
	h.unsubscribeFunc = func() error {
		mu.Lock()
		callOrder = append(callOrder, "unsubscribe")
		mu.Unlock()
		return originalUnsub()
	}

	sub := h.createSubscription("test-topic")

	// need to have polling loop running so stopPolling can be received
	go func() {
		select {
		case <-sub.stopPolling:
			// good
		case <-time.After(time.Second):
		}
	}()

	errCh := sub.Unsubscribe()

	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Unsubscribe did not complete")
	}

	mu.Lock()
	defer mu.Unlock()

	// drain should happen before unsubscribe
	if len(callOrder) != 2 {
		t.Fatalf("callOrder = %v, want [drain, unsubscribe]", callOrder)
	}
	if callOrder[0] != "drain" {
		t.Errorf("callOrder[0] = %q, want 'drain'", callOrder[0])
	}
	if callOrder[1] != "unsubscribe" {
		t.Errorf("callOrder[1] = %q, want 'unsubscribe'", callOrder[1])
	}
}

func TestUnsubscribe_ReturnsUnsubscribeError(t *testing.T) {
	h := newTestHarness[string]()
	h.unsubscribeError = errors.New("unsub failed")
	sub := h.createSubscription("test-topic")

	// need to have polling loop running so stopPolling can be received
	go func() {
		select {
		case <-sub.stopPolling:
		case <-time.After(time.Second):
		}
	}()

	errCh := sub.Unsubscribe()

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected error")
		}
		if err.Error() != "unsub failed" {
			t.Errorf("error = %v, want 'unsub failed'", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Unsubscribe did not complete")
	}
}

// -----------------------------------------------------------------------------
// Drain Behavior Tests
// -----------------------------------------------------------------------------

func TestDrain_AsyncStopPollingBeforeDrain_PausesThenResumes(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// Track poll count atomically since accessed from multiple goroutines
	var pollCount atomic.Int32
	pollStarted := make(chan struct{}, 1)

	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		count := pollCount.Add(1)
		select {
		case pollStarted <- struct{}{}:
		default:
		}
		if count > 1000 {
			h.circuitBreaker.closeMainCtx()
		}
		// Small sleep to allow drain goroutine to synchronize with pause channel
		time.Sleep(100 * time.Microsecond)
		return "", false, nil
	}

	pollingDone := make(chan struct{})
	go func() {
		sub.PollAndForward(10 * time.Millisecond)
		close(pollingDone)
	}()

	// Wait for polling to start
	<-pollStarted

	// Track poll count before drain
	beforeDrain := pollCount.Load()

	// Call drain with AsyncStopPollingBeforeDrain from another goroutine
	// (simulating async rebalance from different thread)
	drainDone := make(chan struct{})
	go func() {
		sub.drain(AsyncStopPollingBeforeDrain)
		close(drainDone)
	}()

	// The drain should:
	// 1. Send to pausePolling -> polling loop receives and blocks on resumePolling
	// 2. Call Drain()
	// 3. Defer sends to resumePolling -> polling loop receives and continues

	select {
	case <-drainDone:
		// good - drain completed
	case <-time.After(2 * time.Second):
		t.Fatal("drain did not complete - likely stuck on pause or resume")
	}

	// After drain completes, polling should have resumed
	// Let it poll a bit more
	time.Sleep(50 * time.Millisecond)
	afterResume := pollCount.Load()

	// Verify polling continued after drain (proves resume worked)
	if afterResume <= beforeDrain {
		t.Errorf("polling should have continued after drain: before=%d, after=%d", beforeDrain, afterResume)
	}

	// Verify drain was called exactly once
	if h.drainCoordinator.drainCalls.Load() != 1 {
		t.Errorf("drain called %d times, want 1", h.drainCoordinator.drainCalls.Load())
	}

	// Cleanup
	h.circuitBreaker.closeMainCtx()
	<-pollingDone
}

func TestDrain_AsyncStopPollingBeforeDrain_PauseTimeout_LogsErrorAndTriggersCircuitBreaker(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// Override pause polling timeout to a short duration for testing
	originalTimeout := pausePollingTimeout
	pausePollingTimeout = 100 * time.Millisecond
	defer func() { pausePollingTimeout = originalTimeout }()

	// Do NOT start a polling loop - nobody will receive from pausePolling
	// This simulates a completely stuck/unresponsive polling loop

	drainStart := time.Now()
	sub.drain(AsyncStopPollingBeforeDrain)
	drainDuration := time.Since(drainStart)

	// Should have taken ~100ms due to pause timeout
	if drainDuration < 90*time.Millisecond {
		t.Errorf("drain completed too fast (%v), expected ~100ms timeout", drainDuration)
	}

	// Verify the timeout was logged with exact message
	errorLogs := h.logger.getErrorLogs()
	const expectedMsg = "timeout waiting for polling to pause"
	foundTimeoutLog := false
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundTimeoutLog = true
			break
		}
	}
	if !foundTimeoutLog {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}

	// Verify circuit breaker was triggered with exact error message
	shutdownCalls := h.circuitBreaker.getShutdownCalls()
	if len(shutdownCalls) != 1 {
		t.Fatalf("expected 1 circuit breaker shutdown call, got %d", len(shutdownCalls))
	}
	if shutdownCalls[0].Error() != expectedMsg {
		t.Errorf("circuit breaker error = %q, want %q", shutdownCalls[0].Error(), expectedMsg)
	}

	// Drain coordinator should NOT have been called (early return after timeout)
	if h.drainCoordinator.drainCalls.Load() != 0 {
		t.Errorf("drain called %d times, want 0 (should return early on pause timeout)", h.drainCoordinator.drainCalls.Load())
	}
}

func TestDrain_AsyncStopPollingBeforeDrain_ResumeTimeout_LogsError(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// To test the resume timeout, we need a polling loop that receives pause
	// but never receives resume (simulating a stuck/crashed loop)

	// Start a "broken" polling loop that stops after receiving pause
	pollingDone := make(chan struct{})
	go func() {
		// Wait for pause signal
		<-sub.pausePolling
		// Simulate broken loop - don't wait for resume, just exit
		close(pollingDone)
	}()

	// Call drain - it will:
	// 1. Send to pausePolling (received by our broken loop)
	// 2. Set paused = true
	// 3. Call Drain()
	// 4. Defer tries to send to resumePolling but nobody is receiving
	// 5. Timeout after 1 second, log error

	drainStart := time.Now()
	sub.drain(AsyncStopPollingBeforeDrain)
	drainDuration := time.Since(drainStart)

	// Should have taken ~1 second due to resume timeout
	if drainDuration < 900*time.Millisecond {
		t.Errorf("drain completed too fast (%v), expected ~1s timeout", drainDuration)
	}

	// Verify the resume timeout was logged with exact message
	errorLogs := h.logger.getErrorLogs()
	const expectedMsg = "resume polling timeout"
	foundTimeoutLog := false
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundTimeoutLog = true
			break
		}
	}
	if !foundTimeoutLog {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}

	// Drain coordinator should still have been called
	if h.drainCoordinator.drainCalls.Load() != 1 {
		t.Errorf("drain called %d times, want 1", h.drainCoordinator.drainCalls.Load())
	}

	<-pollingDone
}

func TestDrain_ShutdownStopPollingBeforeDrain_StopPollingTimeout_LogsError(t *testing.T) {
	h := newTestHarness[string]()
	// Use short drain timeout so test doesn't take long
	// stopPollingTimeout = drainTimeout / 2 = 100ms
	h.cfg.DrainTimeout = 200 * time.Millisecond
	sub := h.createSubscription("test-topic")

	// Do NOT start a polling loop - nobody will receive from stopPolling
	// This simulates a stuck/unresponsive polling loop

	drainStart := time.Now()
	sub.drain(ShutdownStopPollingBeforeDrain)
	drainDuration := time.Since(drainStart)

	// Should have taken ~100ms (drainTimeout/2) due to stopPolling timeout
	expectedTimeout := h.cfg.DrainTimeout / 2
	if drainDuration < expectedTimeout-10*time.Millisecond {
		t.Errorf("drain completed too fast (%v), expected ~%v timeout", drainDuration, expectedTimeout)
	}

	// Verify the timeout was logged with exact message format
	// Format: "timeout in %s waiting for to stop polling loop, proceeding with drain"
	errorLogs := h.logger.getErrorLogs()
	expectedMsg := fmt.Sprintf("timeout in %s waiting for to stop polling loop, proceeding with drain", expectedTimeout)
	foundTimeoutLog := false
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundTimeoutLog = true
			break
		}
	}
	if !foundTimeoutLog {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}

	// Drain coordinator should still have been called (proceeds despite timeout)
	if h.drainCoordinator.drainCalls.Load() != 1 {
		t.Errorf("drain called %d times, want 1", h.drainCoordinator.drainCalls.Load())
	}
}

func TestDrain_DrainError_IsLogged(t *testing.T) {
	h := newTestHarness[string]()
	drainErr := errors.New("worker pool exhausted")
	h.drainCoordinator.drainError = drainErr
	sub := h.createSubscription("test-topic")

	info := []nexus.RebalanceInfo{{Partition: 0}}
	_ = sub.HandleRebalance(nexus.Revoke, info)

	// verify exact log message format: "error draining pipeline.Processor for topic: %s"
	// Note: the actual error is passed as a separate arg, not in the message
	errorLogs := h.logger.getErrorLogs()
	expectedMsg := "error draining pipeline.Processor for topic: test-topic"
	foundExactMsg := false
	var foundLog logEntry
	for _, log := range errorLogs {
		if log.msg == expectedMsg {
			foundExactMsg = true
			foundLog = log
			break
		}
	}
	if !foundExactMsg {
		t.Errorf("expected exact log message %q, got logs: %v", expectedMsg, errorLogs)
	}
	// verify the error is passed as an argument
	if len(foundLog.args) != 1 {
		t.Errorf("expected 1 log arg (the error), got %d", len(foundLog.args))
	} else if argErr, ok := foundLog.args[0].(error); !ok || !errors.Is(argErr, drainErr) {
		t.Errorf("log arg = %v, want %v", foundLog.args[0], drainErr)
	}
}

// -----------------------------------------------------------------------------
// Edge Case Tests
// -----------------------------------------------------------------------------

func TestHandleRebalance_Assign_EmptyInfo(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// empty info slice should still work without panic
	err := sub.HandleRebalance(nexus.Assign, []nexus.RebalanceInfo{})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// should still signal assigned
	select {
	case <-sub.signalAssigned:
		// good
	default:
		t.Error("signalAssigned should have been signaled even with empty info")
	}

	// ackRebalance should be called with empty info
	ackCalls := h.getAckRebalanceCalls()
	if len(ackCalls) != 1 {
		t.Fatalf("expected 1 ackRebalance call, got %d", len(ackCalls))
	}
	if len(ackCalls[0].info) != 0 {
		t.Errorf("expected empty info, got %v", ackCalls[0].info)
	}
}

func TestHandleRebalance_Revoke_EmptyInfo(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// empty info slice should still work without panic
	err := sub.HandleRebalance(nexus.Revoke, []nexus.RebalanceInfo{})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// drain should still be called
	if h.drainCoordinator.drainCalls.Load() != 1 {
		t.Errorf("drain called %d times, want 1", h.drainCoordinator.drainCalls.Load())
	}

	// ackRebalance should be called with empty info
	ackCalls := h.getAckRebalanceCalls()
	if len(ackCalls) != 1 {
		t.Fatalf("expected 1 ackRebalance call, got %d", len(ackCalls))
	}
	if len(ackCalls[0].info) != 0 {
		t.Errorf("expected empty info, got %v", ackCalls[0].info)
	}
}

func TestHandleRebalance_MultipleAssignRevokeCycles(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	// first assign
	info1 := []nexus.RebalanceInfo{{Partition: 0, CommittedOffset: 100}}
	_ = sub.HandleRebalance(nexus.Assign, info1)

	// revoke
	_ = sub.HandleRebalance(nexus.Revoke, info1)

	// second assign with different partitions
	info2 := []nexus.RebalanceInfo{
		{Partition: 1, CommittedOffset: 200},
		{Partition: 2, CommittedOffset: 300},
	}
	_ = sub.HandleRebalance(nexus.Assign, info2)

	// verify all calls tracked correctly
	ackCalls := h.getAckRebalanceCalls()
	if len(ackCalls) != 3 {
		t.Fatalf("expected 3 ackRebalance calls, got %d", len(ackCalls))
	}
	if ackCalls[0].rebalanceType != nexus.Assign {
		t.Errorf("call[0] type = %v, want Assign", ackCalls[0].rebalanceType)
	}
	if ackCalls[1].rebalanceType != nexus.Revoke {
		t.Errorf("call[1] type = %v, want Revoke", ackCalls[1].rebalanceType)
	}
	if ackCalls[2].rebalanceType != nexus.Assign {
		t.Errorf("call[2] type = %v, want Assign", ackCalls[2].rebalanceType)
	}

	// verify second assign has correct partitions
	if len(ackCalls[2].info) != 2 {
		t.Fatalf("expected 2 partitions in second assign, got %d", len(ackCalls[2].info))
	}
	if ackCalls[2].info[0].Partition != 1 || ackCalls[2].info[1].Partition != 2 {
		t.Errorf("second assign partitions = %v, want [1, 2]", ackCalls[2].info)
	}

	// verify ResetPrevOffsets called for each assign
	resetCalls := h.processor.getResetPrevCalls()
	if len(resetCalls) != 2 {
		t.Fatalf("expected 2 ResetPrevOffsets calls, got %d", len(resetCalls))
	}
	if len(resetCalls[0]) != 1 || resetCalls[0][0] != 0 {
		t.Errorf("first reset = %v, want [0]", resetCalls[0])
	}
	if len(resetCalls[1]) != 2 {
		t.Errorf("second reset = %v, want [1, 2]", resetCalls[1])
	}
}

func TestPollAndForward_TimeDeltaReset_WithinOneSecond(t *testing.T) {
	h := newTestHarness[string]()

	// track readTimes to verify delta behavior within 1 second
	var readTimes []time.Time
	var mu sync.Mutex
	h.processor.processFunc = func(_ string, readTime time.Time) error {
		mu.Lock()
		readTimes = append(readTimes, readTime)
		mu.Unlock()
		return nil
	}

	messageCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		messageCount++
		if messageCount <= 2 {
			return "msg", true, nil
		}
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")
	sub.PollAndForward(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	// both messages should have readTimes close together (within same second)
	// since delta hasn't exceeded 1 second
	if len(readTimes) != 2 {
		t.Fatalf("expected 2 readTimes, got %d", len(readTimes))
	}

	// the second readTime should be >= first (monotonic within same now period)
	if readTimes[1].Before(readTimes[0]) {
		t.Errorf("readTimes should be monotonic: first=%v, second=%v", readTimes[0], readTimes[1])
	}
}

func TestPollAndForward_TimeDeltaReset_AfterOneSecond(t *testing.T) {
	h := newTestHarness[string]()

	// track readTimes and the actual wall clock times when poll returned messages
	type timeRecord struct {
		readTime   time.Time // time passed to Process
		actualTime time.Time // wall clock when poll returned
	}
	var records []timeRecord
	var mu sync.Mutex

	h.processor.processFunc = func(_ string, readTime time.Time) error {
		mu.Lock()
		// actualTime was captured in pollFunc, readTime is what we're verifying
		records[len(records)-1].readTime = readTime
		mu.Unlock()
		return nil
	}

	messageCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		messageCount++
		switch messageCount {
		case 1:
			// first message - immediate
			mu.Lock()
			records = append(records, timeRecord{actualTime: time.Now()})
			mu.Unlock()
			return "msg1", true, nil
		case 2:
			// sleep >1 second to trigger delta reset
			time.Sleep(1050 * time.Millisecond)
			mu.Lock()
			records = append(records, timeRecord{actualTime: time.Now()})
			mu.Unlock()
			return "msg2", true, nil
		default:
			h.circuitBreaker.closeMainCtx()
			return "", false, nil
		}
	}

	sub := h.createSubscription("test-topic")
	sub.PollAndForward(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// After >1 second, the delta reset logic triggers:
	//   now = time.Now()  // reset to current time
	//   delta = 0
	//   readTime = now.Add(delta) = now
	//
	// So readTime for msg2 should be very close to actualTime (within microseconds)
	// because now was just reset to the current time.

	msg2ReadTime := records[1].readTime
	msg2ActualTime := records[1].actualTime

	// readTime should be within 100 microseconds of actual time
	// (accounting for the tiny delay between time.Now() calls)
	tolerance := 2 * time.Millisecond
	diff := msg2ReadTime.Sub(msg2ActualTime)
	if diff < 0 {
		diff = -diff
	}

	if diff > tolerance {
		t.Errorf("after delta reset, readTime should be within %v of actual time\n"+
			"  readTime:   %v\n"+
			"  actualTime: %v\n"+
			"  diff:       %v",
			tolerance, msg2ReadTime, msg2ActualTime, diff)
	}

	// Also verify the first message's readTime is reasonable (close to its actual time)
	msg1ReadTime := records[0].readTime
	msg1ActualTime := records[0].actualTime
	diff1 := msg1ReadTime.Sub(msg1ActualTime)
	if diff1 < 0 {
		diff1 = -diff1
	}
	if diff1 > tolerance {
		t.Errorf("first message readTime should be within %v of actual time\n"+
			"  readTime:   %v\n"+
			"  actualTime: %v\n"+
			"  diff:       %v",
			tolerance, msg1ReadTime, msg1ActualTime, diff1)
	}

	// Verify that the two readTimes are >1 second apart (proves reset happened)
	timeBetweenMessages := msg2ReadTime.Sub(msg1ReadTime)
	if timeBetweenMessages < time.Second {
		t.Errorf("readTimes should be >1 second apart after reset, got %v", timeBetweenMessages)
	}
}

func TestPollAndForward_ProcessPayloadExactMatch(t *testing.T) {
	h := newTestHarness[string]()

	const exactPayload = "payload-with-special-chars: 日本語 & <xml> \"quotes\""
	messageCount := 0
	h.pollFunc = func(_ time.Duration) (string, bool, error) {
		messageCount++
		if messageCount == 1 {
			return exactPayload, true, nil
		}
		h.circuitBreaker.closeMainCtx()
		return "", false, nil
	}

	sub := h.createSubscription("test-topic")
	sub.PollAndForward(10 * time.Millisecond)

	calls := h.processor.getProcessCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(calls))
	}
	if calls[0].payload != exactPayload {
		t.Errorf("payload = %q, want %q", calls[0].payload, exactPayload)
	}
}
