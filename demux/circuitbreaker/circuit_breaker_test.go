// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/tests/mocklogger"
)

func Test_New_CreatesCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()

	cb := New(ctx, logger)

	if cb == nil {
		t.Fatal("expected circuit breaker to be created")
	}
	if cb.mainCtx == nil {
		t.Error("expected mainCtx to be initialised")
	}
	if cb.mainCancelFunc == nil {
		t.Error("expected mainCancelFunc to be initialised")
	}
	if cb.emergencyShutdown == nil {
		t.Error("expected emergencyShutdown channel to be initialised")
	}
	if cb.logger == nil {
		t.Error("expected logger to be set")
	}
}

func Test_MainCtxDone_ReturnsChannel(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	done := cb.MainCtxDone()

	if done == nil {
		t.Fatal("expected MainCtxDone to return a channel")
	}

	// channel should not be closed initially
	select {
	case <-done:
		t.Error("expected channel to not be closed initially")
	default:
		// expected
	}
}

func Test_MainCtxDone_ClosedAfterTrigger(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	done := cb.MainCtxDone()

	// trigger emergency shutdown
	cb.TriggerEmergencyShutdown(errors.New("test error"))

	// channel should be closed after trigger
	select {
	case <-done:
		// expected - channel closed
	case <-time.After(100 * time.Millisecond):
		t.Error("expected MainCtxDone channel to be closed after trigger")
	}
}

func Test_Triggered_ReturnsChannel(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	triggered := cb.Triggered()

	if triggered == nil {
		t.Fatal("expected Triggered to return a channel")
	}
}

func Test_TriggerEmergencyShutdown_SendsMessageToChannel(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	testError := errors.New("database connection failed")
	cb.TriggerEmergencyShutdown(testError)

	select {
	case msg := <-cb.Triggered():
		if !strings.Contains(msg, "database connection failed") {
			t.Errorf("expected message to contain error reason, got: %s", msg)
		}
		if !strings.Contains(msg, "circuit-breaker:") {
			t.Errorf("expected message to contain circuit-breaker prefix, got: %s", msg)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected message on Triggered channel")
	}
}

func Test_TriggerEmergencyShutdown_ClosesChannel(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	cb.TriggerEmergencyShutdown(errors.New("test"))

	// drain the message
	<-cb.Triggered()

	// channel should be closed
	select {
	case _, ok := <-cb.Triggered():
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected channel to be closed and readable")
	}
}

func Test_TriggerEmergencyShutdown_OnlyTriggersOnce(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	// trigger multiple times concurrently
	var wg sync.WaitGroup
	triggerCount := atomic.Int32{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			cb.TriggerEmergencyShutdown(fmt.Errorf("error %d", n))
			triggerCount.Add(1)
		}(i)
	}

	wg.Wait()

	// only one message should be sent
	messageCount := 0
	for range cb.Triggered() {
		messageCount++
	}

	if messageCount != 1 {
		t.Errorf("expected exactly 1 message, got %d", messageCount)
	}

	// verify only one set of logs
	errors := logger.Errors()
	if len(errors) != 1 {
		t.Errorf("expected 1 error log, got %d: %v", len(errors), errors)
	}

	warnings := logger.Warnings()
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning log, got %d: %v", len(warnings), warnings)
	}

	infos := logger.Infos()
	if len(infos) != 1 {
		t.Errorf("expected 1 info log, got %d: %v", len(infos), infos)
	}
}

func Test_TriggerEmergencyShutdown_LogsError(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	testError := errors.New("worker acquisition timeout")
	cb.TriggerEmergencyShutdown(testError)

	errors := logger.Errors()
	if len(errors) != 1 {
		t.Fatalf("expected 1 error log, got %d", len(errors))
	}

	errorLog := errors[0]
	if !strings.Contains(errorLog, "circuit-breaker:") {
		t.Errorf("expected error log to contain 'circuit-breaker:', got: %s", errorLog)
	}
	if !strings.Contains(errorLog, "protective shutdown initiated") {
		t.Errorf("expected error log to contain 'protective shutdown initiated', got: %s", errorLog)
	}
	if !strings.Contains(errorLog, "worker acquisition timeout") {
		t.Errorf("expected error log to contain the reason, got: %s", errorLog)
	}
}

func Test_TriggerEmergencyShutdown_LogsWarning(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	cb.TriggerEmergencyShutdown(errors.New("test"))

	warnings := logger.Warnings()
	if len(warnings) != 1 {
		t.Fatalf("expected 1 warning log, got %d", len(warnings))
	}

	warnLog := warnings[0]
	if !strings.Contains(warnLog, "circuit-breaker:") {
		t.Errorf("expected warning log to contain 'circuit-breaker:', got: %s", warnLog)
	}
	if !strings.Contains(warnLog, "ProcessMessage contexts cancelled") {
		t.Errorf("expected warning log to contain 'ProcessMessage contexts cancelled', got: %s",
			warnLog)
	}
	if !strings.Contains(warnLog, "stopping polling") {
		t.Errorf("expected warning log to contain 'stopping polling', got: %s", warnLog)
	}
}

func Test_TriggerEmergencyShutdown_LogsInfo(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	cb.TriggerEmergencyShutdown(errors.New("test"))

	infos := logger.Infos()
	if len(infos) != 1 {
		t.Fatalf("expected 1 info log, got %d", len(infos))
	}

	infoLog := infos[0]
	if !strings.Contains(infoLog, "circuit-breaker:") {
		t.Errorf("expected info log to contain 'circuit-breaker:', got: %s", infoLog)
	}
	if !strings.Contains(infoLog, "shutdown complete") {
		t.Errorf("expected info log to contain 'shutdown complete', got: %s", infoLog)
	}
}

func Test_TriggerEmergencyShutdown_LogOrder(t *testing.T) {
	// verify logs are emitted in correct order: Error -> Warn -> Info
	ctx := context.Background()

	type logEntry struct {
		level   string
		message string
		order   int
	}

	var mu sync.Mutex
	var logs []logEntry
	orderCounter := 0

	logger := &orderTrackingLogger{
		onError: func(msg string) {
			mu.Lock()
			defer mu.Unlock()
			logs = append(logs, logEntry{level: "error", message: msg, order: orderCounter})
			orderCounter++
		},
		onWarn: func(msg string) {
			mu.Lock()
			defer mu.Unlock()
			logs = append(logs, logEntry{level: "warn", message: msg, order: orderCounter})
			orderCounter++
		},
		onInfo: func(msg string) {
			mu.Lock()
			defer mu.Unlock()
			logs = append(logs, logEntry{level: "info", message: msg, order: orderCounter})
			orderCounter++
		},
	}

	cb := New(ctx, logger)
	cb.TriggerEmergencyShutdown(errors.New("test"))

	if len(logs) != 3 {
		t.Fatalf("expected 3 log entries, got %d", len(logs))
	}

	if logs[0].level != "error" {
		t.Errorf("expected first log to be error, got %s", logs[0].level)
	}
	if logs[1].level != "warn" {
		t.Errorf("expected second log to be warn, got %s", logs[1].level)
	}
	if logs[2].level != "info" {
		t.Errorf("expected third log to be info, got %s", logs[2].level)
	}
}

func Test_TriggerEmergencyShutdown_CancelsMainContext(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	// create a derived context from mainCtx to verify cancellation
	derivedCtx, cancel := context.WithCancel(cb.mainCtx)
	defer cancel()

	// verify context is not cancelled initially
	select {
	case <-derivedCtx.Done():
		t.Fatal("expected derived context to not be cancelled initially")
	default:
		// expected
	}

	// trigger shutdown
	cb.TriggerEmergencyShutdown(errors.New("test"))

	// verify main context is cancelled
	select {
	case <-cb.mainCtx.Done():
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Error("expected mainCtx to be cancelled after trigger")
	}
}

func Test_TriggerEmergencyShutdown_MessageFormat(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	testError := errors.New("dead letter write failed: connection refused")
	cb.TriggerEmergencyShutdown(testError)

	msg := <-cb.Triggered()

	// verify the message format matches the signalMessage constant
	expected := "circuit-breaker: triggered and completed protective shutdown, reason: " +
		"dead letter write failed: connection refused"
	if msg != expected {
		t.Errorf("message format mismatch\nexpected: %s\ngot:      %s", expected, msg)
	}
}

func Test_New_UsesIndependentMainContext(t *testing.T) {
	// verify that mainCtx is independent from the globalCtx passed to New
	globalCtx, globalCancel := context.WithCancel(context.Background())
	logger := mocklogger.NewRecordingLogger()
	cb := New(globalCtx, logger)

	// cancel the global context
	globalCancel()

	// mainCtx should NOT be cancelled
	select {
	case <-cb.mainCtx.Done():
		t.Error("mainCtx should not be cancelled when globalCtx is cancelled")
	default:
		// expected - mainCtx is independent
	}

	// mainCtxDone should also not be closed
	select {
	case <-cb.MainCtxDone():
		t.Error("MainCtxDone should not be closed when globalCtx is cancelled")
	default:
		// expected
	}
}

func Test_EmergencyShutdownChannel_Buffered(t *testing.T) {
	// verify the channel is buffered (non-blocking send)
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	// trigger without any receiver - should not block
	done := make(chan struct{})
	go func() {
		cb.TriggerEmergencyShutdown(errors.New("test"))
		close(done)
	}()

	select {
	case <-done:
		// expected - send should not block
	case <-time.After(100 * time.Millisecond):
		t.Error("TriggerEmergencyShutdown should not block (channel should be buffered)")
	}
}

func Test_EmergencyShutdownChannelCapacity(t *testing.T) {
	// This test kills ARITHMETIC_BASE mutations on the channel buffer size.
	// The buffer must be exactly 1:
	// - Buffer 0 would cause TriggerEmergencyShutdown to block (deadlock)
	// - Buffer 2+ wastes memory and indicates unintended change
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()
	cb := New(ctx, logger)

	// Verify channel capacity is exactly 1
	capacity := cap(cb.emergencyShutdown)
	if capacity != 1 {
		t.Errorf("emergencyShutdown channel capacity should be 1, got %d", capacity)
	}
}

// orderTrackingLogger tracks the order of log calls
type orderTrackingLogger struct {
	onError func(string)
	onWarn  func(string)
	onInfo  func(string)
}

func (l *orderTrackingLogger) call(fn func(string), format string, args ...any) {
	if fn != nil {
		msg := format
		if len(args) > 0 {
			msg = fmt.Sprintf(format, args...)
		}
		fn(msg)
	}
}

func (l *orderTrackingLogger) Error(_ context.Context, format string, args ...any) {
	l.call(l.onError, format, args...)
}

func (l *orderTrackingLogger) Warn(_ context.Context, format string, args ...any) {
	l.call(l.onWarn, format, args...)
}

func (l *orderTrackingLogger) Info(_ context.Context, format string, args ...any) {
	l.call(l.onInfo, format, args...)
}

func (l *orderTrackingLogger) Debug(_ context.Context, _ string, _ ...any) {}
