// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package drain

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
)

// mockWorkerDrainer implements workerDrainer for tests.
type mockWorkerDrainer struct {
	drainDelay time.Duration
	drainCalls int
}

func (m *mockWorkerDrainer) DrainWorkers() {
	m.drainCalls++
	if m.drainDelay > 0 {
		time.Sleep(m.drainDelay)
	}
}

// mockOffsetDrainer implements offsetDrainer for tests.
type mockOffsetDrainer struct {
	drainErr    error
	commitErr   error
	drainCalls  int
	commitCalls int
	drainDelay  time.Duration
}

func (m *mockOffsetDrainer) DrainCommitter(_ *time.Timer) error {
	m.drainCalls++
	if m.drainDelay > 0 {
		time.Sleep(m.drainDelay)
	}
	return m.drainErr
}

func (m *mockOffsetDrainer) CommitOffsets() error {
	m.commitCalls++
	return m.commitErr
}

// mockEmergencyShutdown implements emergencyShutdown for tests.
type mockEmergencyShutdown struct {
	triggeredWith error
	triggerCalls  int
}

func (m *mockEmergencyShutdown) TriggerEmergencyShutdown(reason error) {
	m.triggerCalls++
	m.triggeredWith = reason
}

func newTestCoordinator(
	drainer *mockWorkerDrainer,
	committer *mockOffsetDrainer,
	shutdown *mockEmergencyShutdown,
	timeout time.Duration,
) *Coordinator[string] {
	return &Coordinator[string]{
		ctx:            context.Background(),
		demux:          drainer,
		committer:      committer,
		circuitBreaker: shutdown,
		drainTimeout:   timeout,
		logger:         mocklogger.NewNoOpLogger(),
	}
}

func TestNewDrainCoordinator(t *testing.T) {
	cfg := config.DemuxConfig{DrainTimeout: 5 * time.Second}
	logger := mocklogger.NewNoOpLogger()

	// The constructor accepts concrete pointer types but never dereferences them,
	// so nil is safe here - we just want to exercise the wiring
	coord := NewDrainCoordinator[string](
		context.Background(),
		nil,
		nil,
		nil,
		cfg,
		logger,
	)

	if coord == nil {
		t.Fatal("NewDrainCoordinator returned nil")
	}
	if coord.drainTimeout != 5*time.Second {
		t.Errorf("drainTimeout = %v, want 5s", coord.drainTimeout)
	}
	if coord.logger == nil {
		t.Error("logger should be set")
	}
}

func TestCoordinator_Drain_Success(t *testing.T) {
	drainer := &mockWorkerDrainer{}
	committer := &mockOffsetDrainer{}
	shutdown := &mockEmergencyShutdown{}

	coord := newTestCoordinator(drainer, committer, shutdown, time.Second)

	err := coord.Drain()

	if err != nil {
		t.Errorf("Drain() returned error: %v", err)
	}
	if drainer.drainCalls != 1 {
		t.Errorf("DrainWorkers called %d times, want 1", drainer.drainCalls)
	}
	if committer.drainCalls != 1 {
		t.Errorf("DrainCommitter called %d times, want 1", committer.drainCalls)
	}
	if shutdown.triggerCalls != 0 {
		t.Errorf("TriggerEmergencyShutdown called %d times, want 0", shutdown.triggerCalls)
	}
}

func TestCoordinator_Drain_WorkerTimeout(t *testing.T) {
	drainer := &mockWorkerDrainer{drainDelay: 100 * time.Millisecond}
	committer := &mockOffsetDrainer{}
	shutdown := &mockEmergencyShutdown{}

	// Very short timeout to trigger timeout path
	coord := newTestCoordinator(drainer, committer, shutdown, 10*time.Millisecond)

	err := coord.Drain()

	if err == nil {
		t.Error("Drain() should return error on timeout")
	}
	if shutdown.triggerCalls != 1 {
		t.Errorf("TriggerEmergencyShutdown called %d times, want 1", shutdown.triggerCalls)
	}
	if shutdown.triggeredWith == nil {
		t.Error("TriggerEmergencyShutdown should be called with error")
	}
	// Committer should NOT be called if workers timeout
	if committer.drainCalls != 0 {
		t.Errorf("DrainCommitter called %d times, want 0 (workers timed out)", committer.drainCalls)
	}
}

func TestCoordinator_Drain_CommitterError(t *testing.T) {
	drainer := &mockWorkerDrainer{}
	committer := &mockOffsetDrainer{drainErr: errors.New("commit failed")}
	shutdown := &mockEmergencyShutdown{}

	coord := newTestCoordinator(drainer, committer, shutdown, time.Second)

	err := coord.Drain()

	if err == nil {
		t.Error("Drain() should return error when committer fails")
	}
	if err.Error() != "commit failed" {
		t.Errorf("error = %v, want 'commit failed'", err)
	}
	if drainer.drainCalls != 1 {
		t.Errorf("DrainWorkers called %d times, want 1", drainer.drainCalls)
	}
	if committer.drainCalls != 1 {
		t.Errorf("DrainCommitter called %d times, want 1", committer.drainCalls)
	}
	if shutdown.triggerCalls != 1 {
		t.Errorf("TriggerEmergencyShutdown called %d times, want 1", shutdown.triggerCalls)
	}
	if shutdown.triggeredWith == nil || shutdown.triggeredWith.Error() != "commit failed" {
		t.Errorf("TriggerEmergencyShutdown called with %v, want 'commit failed'", shutdown.triggeredWith)
	}
}

// overlapDetectingDrainer fails the drain contract if entered concurrently.
type overlapDetectingDrainer struct {
	inFlight atomic.Int32
	overlaps atomic.Int32
	delay    time.Duration
}

func (d *overlapDetectingDrainer) enter() {
	if d.inFlight.Add(1) > 1 {
		d.overlaps.Add(1)
	}
	time.Sleep(d.delay)
	d.inFlight.Add(-1)
}

type overlapDetectingWorkerDrainer struct{ overlapDetectingDrainer }

func (d *overlapDetectingWorkerDrainer) DrainWorkers() { d.enter() }

type overlapDetectingOffsetDrainer struct{ overlapDetectingDrainer }

func (d *overlapDetectingOffsetDrainer) DrainCommitter(_ *time.Timer) error {
	d.enter()
	return nil
}
func (d *overlapDetectingOffsetDrainer) CommitOffsets() error { return nil }

// TestCoordinator_Drain_SerializesConcurrentCallers: a revoke's drain and a
// shutdown's drain arrive on different goroutines. The coordinator must run
// them back to back; overlapping entries into DrainWorkers or DrainCommitter
// break their single-caller contracts.
func TestCoordinator_Drain_SerializesConcurrentCallers(t *testing.T) {
	drainer := &overlapDetectingWorkerDrainer{overlapDetectingDrainer{delay: 10 * time.Millisecond}}
	committer := &overlapDetectingOffsetDrainer{overlapDetectingDrainer{delay: 10 * time.Millisecond}}
	shutdown := &mockEmergencyShutdown{}

	coord := &Coordinator[string]{
		ctx:            context.Background(),
		demux:          drainer,
		committer:      committer,
		circuitBreaker: shutdown,
		drainTimeout:   time.Second,
		logger:         mocklogger.NewNoOpLogger(),
	}

	start := make(chan struct{})
	var drains sync.WaitGroup
	for i := 0; i < 2; i++ {
		drains.Add(1)
		go func() {
			defer drains.Done()
			<-start
			if err := coord.Drain(); err != nil {
				t.Errorf("Drain() returned error: %v", err)
			}
		}()
	}
	close(start)
	drains.Wait()

	if got := drainer.overlaps.Load(); got > 0 {
		t.Errorf("DrainWorkers entered concurrently %d times, want 0", got)
	}
	if got := committer.overlaps.Load(); got > 0 {
		t.Errorf("DrainCommitter entered concurrently %d times, want 0", got)
	}
	if shutdown.triggerCalls != 0 {
		t.Errorf("TriggerEmergencyShutdown called %d times, want 0", shutdown.triggerCalls)
	}
}

func TestCoordinator_ImmediateCommit_Success(t *testing.T) {
	drainer := &mockWorkerDrainer{}
	committer := &mockOffsetDrainer{}
	shutdown := &mockEmergencyShutdown{}

	coord := newTestCoordinator(drainer, committer, shutdown, time.Second)

	err := coord.ImmediateCommit()

	if err != nil {
		t.Errorf("ImmediateCommit() returned error: %v", err)
	}
	if committer.commitCalls != 1 {
		t.Errorf("CommitOffsets called %d times, want 1", committer.commitCalls)
	}
}

func TestCoordinator_ImmediateCommit_Error(t *testing.T) {
	drainer := &mockWorkerDrainer{}
	committer := &mockOffsetDrainer{commitErr: errors.New("immediate commit failed")}
	shutdown := &mockEmergencyShutdown{}

	coord := newTestCoordinator(drainer, committer, shutdown, time.Second)

	err := coord.ImmediateCommit()

	if err == nil {
		t.Error("ImmediateCommit() should return error")
	}
	if err.Error() != "immediate commit failed" {
		t.Errorf("error = %v, want 'immediate commit failed'", err)
	}
}
