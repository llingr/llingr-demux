// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package demux

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// The consumer satisfies the nexus EmergencyShutdowner assertion interface
// (asserted structurally here: this module builds against the published nexus,
// which gains the named interface in its next release), so adapters and
// applications can reach the emergency stop through the handles they already
// hold. The distinct method name is the version gate: only engines with the
// exactly-once delivery semantics satisfy it.
var _ interface{ EmergencyShutdown(reason error) } = (*Consumer[any])(nil)

// emergencyHarness builds a consumer with a counting shutdown callback.
func emergencyHarness(t *testing.T, cfg config.DemuxConfig) (*Consumer[any], *controllableBrokerPort, *atomicCallbackRecorder) {
	t.Helper()

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()
	recorder := &atomicCallbackRecorder{fired: make(chan error, 8)}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(mocklogger.NewNoOpLogger()).
		WithDemuxConfig(cfg).
		WithShutdownCallback(recorder.record)

	consumer := builder.Build(broker)
	return consumer.(*Consumer[any]), broker, recorder //nolint:forcetypeassert // test: known type from builder
}

type atomicCallbackRecorder struct {
	mu    sync.Mutex
	calls []error
	fired chan error
}

func (r *atomicCallbackRecorder) record(_ context.Context, reason error) {
	r.mu.Lock()
	r.calls = append(r.calls, reason)
	r.mu.Unlock()
	select {
	case r.fired <- reason:
	default:
	}
}

func (r *atomicCallbackRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func defaultEmergencyCfg() config.DemuxConfig {
	return config.DemuxConfig{
		AwaitAssignmentsTimeout: 5 * time.Second,
		DrainTimeout:            100 * time.Millisecond,
		PollTimeout:             10 * time.Millisecond,
	}
}

// A trip BEFORE Subscribe reaches the registered callback (no listener race:
// the observer is parked from Build), and a later Subscribe fails at entry
// instead of starting a consumer that is already stopping.
func Test_Consumer_EmergencyShutdown_BeforeSubscribe_DeliversAndDisarms(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")
	dxc, broker, recorder := emergencyHarness(t, defaultEmergencyCfg())

	dxc.EmergencyShutdown(errors.New("pre-subscribe failure"))

	select {
	case reason := <-recorder.fired:
		if reason == nil || !strings.Contains(reason.Error(), "pre-subscribe failure") {
			t.Errorf("callback reason = %v, want the trip reason", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("callback was not delivered for a pre-subscribe trip")
	}

	err := dxc.Subscribe()
	if err == nil {
		t.Fatal("Subscribe after a trip must fail")
	}
	if !strings.Contains(err.Error(), "emergency shutdown") ||
		!strings.Contains(err.Error(), "pre-subscribe failure") {
		t.Errorf("Subscribe error = %q, want the emergency reason", err)
	}
	select {
	case <-broker.subscribed:
		t.Error("Subscribe joined the broker despite the prior trip")
	default:
	}
}

// A trip while Subscribe is parked awaiting assignment converts the in-flight
// subscription into a clean error; the callback is still delivered exactly
// once by the observer.
func Test_Consumer_EmergencyShutdown_DuringAssignmentWait_FailsSubscribe(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")
	dxc, broker, recorder := emergencyHarness(t, defaultEmergencyCfg())

	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- dxc.Subscribe() }()
	<-broker.subscribed // joined, no assignment delivered

	dxc.EmergencyShutdown(errors.New("mid-assignment failure"))

	select {
	case err := <-subscribeDone:
		if err == nil {
			t.Fatal("Subscribe must fail when a trip lands during assignment")
		}
		if !strings.Contains(err.Error(), "mid-assignment failure") {
			t.Errorf("Subscribe error = %q, want the emergency reason", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Subscribe did not return after the trip")
	}

	select {
	case <-recorder.fired:
	case <-time.After(time.Second):
		t.Fatal("callback was not delivered for a mid-assignment trip")
	}
	if got := recorder.count(); got != 1 {
		t.Errorf("callback fired %d times, want exactly 1", got)
	}
}

// The callback fires exactly once across an emergency exit and a subsequent
// graceful Shutdown: the emergency delivery claims the slot, Shutdown still
// completes cleanly but does not notify a second time.
func Test_Consumer_EmergencyThenShutdown_CallbackExactlyOnce(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")
	dxc, broker, recorder := emergencyHarness(t, defaultEmergencyCfg())

	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- dxc.Subscribe() }()
	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Fatalf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	dxc.EmergencyShutdown(errors.New("runtime failure"))
	select {
	case reason := <-recorder.fired:
		if reason == nil {
			t.Error("emergency delivery carried a nil reason")
		}
	case <-time.After(time.Second):
		t.Fatal("emergency callback was not delivered")
	}

	if err := dxc.Shutdown(); err != nil {
		t.Errorf("Shutdown after emergency returned error: %v", err)
	}
	// allow any (incorrect) second delivery to surface before counting
	time.Sleep(50 * time.Millisecond)
	if got := recorder.count(); got != 1 {
		t.Errorf("callback fired %d times across emergency+Shutdown, want exactly 1", got)
	}
}

// The reverse order: a graceful shutdown delivers the nil-reason callback and
// releases the observer; a late trip is a latched no-op with no second
// delivery.
func Test_Consumer_ShutdownThenEmergency_CallbackExactlyOnce(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")
	dxc, broker, recorder := emergencyHarness(t, defaultEmergencyCfg())

	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- dxc.Subscribe() }()
	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Fatalf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	if err := dxc.Shutdown(); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	select {
	case reason := <-recorder.fired:
		if reason != nil {
			t.Errorf("graceful delivery carried reason %v, want nil", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful callback was not delivered")
	}

	dxc.EmergencyShutdown(errors.New("late trip"))
	time.Sleep(50 * time.Millisecond)
	if got := recorder.count(); got != 1 {
		t.Errorf("callback fired %d times across Shutdown+late trip, want exactly 1", got)
	}
}

// A shutdown callback that itself calls EmergencyShutdown (a host reacting to
// the notification) must not deadlock or double-deliver: the re-entrant trip
// hits the election default and returns.
func Test_Consumer_EmergencyShutdown_ReentrantFromCallback_NoDeadlock(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()

	var dxc *Consumer[any]
	calls := make(chan error, 4)
	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(mocklogger.NewNoOpLogger()).
		WithDemuxConfig(defaultEmergencyCfg()).
		WithShutdownCallback(func(_ context.Context, reason error) {
			calls <- reason
			dxc.EmergencyShutdown(errors.New("re-entrant from callback"))
		})

	consumer := builder.Build(broker)
	dxc = consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	dxc.EmergencyShutdown(errors.New("first"))

	select {
	case reason := <-calls:
		if !strings.Contains(reason.Error(), "first") {
			t.Errorf("delivered reason = %v, want the first trip's", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("callback was not delivered (re-entrant trip deadlocked?)")
	}

	time.Sleep(50 * time.Millisecond)
	select {
	case extra := <-calls:
		t.Errorf("callback fired a second time with %v", extra)
	default:
	}
}

// Concurrent trips and a concurrent graceful Shutdown resolve to exactly one
// delivery. Run under -race to exercise the notify election from both sides.
func Test_Consumer_ConcurrentTripsAndShutdown_CallbackExactlyOnce(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")
	dxc, broker, recorder := emergencyHarness(t, defaultEmergencyCfg())

	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- dxc.Subscribe() }()
	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Fatalf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); dxc.EmergencyShutdown(errors.New("concurrent trip")) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = dxc.Shutdown() }()
	wg.Wait()

	select {
	case <-recorder.fired:
	case <-time.After(time.Second):
		t.Fatal("no callback delivered")
	}
	time.Sleep(50 * time.Millisecond)
	if got := recorder.count(); got != 1 {
		t.Errorf("callback fired %d times, want exactly 1", got)
	}
}

// rearmablePort makes the controllable mock safe for REPEATED Unsubscribe:
// the engine permits repeated Shutdown (adapters tolerate repeated release),
// but the shared mock closes a channel per call and would panic on the second.
type rearmablePort struct {
	*controllableBrokerPort
	unsubOnce sync.Once
}

func (p *rearmablePort) Unsubscribe() error {
	p.unsubOnce.Do(func() { _ = p.controllableBrokerPort.Unsubscribe() })
	return nil
}

// Repeated Shutdown delivers the callback once: the notify election makes
// "the host hears about the consumer's end exactly once" hold across repeats,
// where the callback previously fired on every completed Shutdown.
func Test_Consumer_RepeatedShutdown_CallbackExactlyOnce(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := &rearmablePort{controllableBrokerPort: newControllableBrokerPort()}
	broker.releaseUnsubscribe()
	recorder := &atomicCallbackRecorder{fired: make(chan error, 8)}

	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(mocklogger.NewNoOpLogger()).
		WithDemuxConfig(defaultEmergencyCfg()).
		WithShutdownCallback(recorder.record)

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- dxc.Subscribe() }()
	<-broker.subscribed
	if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
		t.Fatalf("TriggerRebalance returned error: %v", err)
	}
	<-subscribeDone

	for i := 0; i < 3; i++ {
		if err := dxc.Shutdown(); err != nil {
			t.Fatalf("Shutdown %d returned error: %v", i, err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := recorder.count(); got != 1 {
		t.Errorf("callback fired %d times across 3 Shutdowns, want exactly 1", got)
	}
	if reason := recorder.calls[0]; reason != nil {
		t.Errorf("graceful delivery carried reason %v, want nil", reason)
	}
}

// A trip that lands before ANY callback exists is not lost: the recorded
// reason is delivered by the eventual Shutdown, as an emergency, never as a
// graceful nil.
func Test_Consumer_TripBeforeCallbackRegistration_ShutdownReportsEmergency(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	broker := newControllableBrokerPort()
	broker.releaseUnsubscribe()

	// no builder callback: the observer has nobody to deliver to at trip time
	builder := NewBuilder("test-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(context.Background()).
		WithLogger(mocklogger.NewNoOpLogger()).
		WithDemuxConfig(defaultEmergencyCfg())

	consumer := builder.Build(broker)
	dxc := consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder

	dxc.EmergencyShutdown(errors.New("tripped before registration"))

	// the observer must have recorded the reason before a Shutdown can run;
	// poll briefly rather than assuming scheduling
	deadline := time.Now().Add(time.Second)
	for dxc.trippedReason() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	recorded := make(chan error, 1)
	dxc.RegisterShutdownCallback(func(_ context.Context, reason error) { recorded <- reason })

	if err := dxc.Shutdown(); err != nil {
		t.Fatalf("Shutdown returned error: %v", err)
	}
	select {
	case reason := <-recorded:
		if reason == nil {
			t.Fatal("Shutdown reported a graceful nil after an emergency trip")
		}
		if !strings.Contains(reason.Error(), "tripped before registration") {
			t.Errorf("reason = %q, want the trip reason", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("no callback delivered by Shutdown")
	}
}

// A trip that lands before the assignment must always fail Subscribe, even
// when the assignment arrives immediately afterwards: the select's Tripped
// case (or the post-assign recheck, when the scheduler lets both become
// ready together) turns the startup into a clean error. Looped so schedule
// jitter exercises the select from a mix of ready-states.
func Test_Consumer_TripThenAssignment_SubscribeAlwaysErrors(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	for i := 0; i < 20; i++ {
		dxc, broker, _ := emergencyHarness(t, defaultEmergencyCfg())

		subscribeDone := make(chan error, 1)
		go func() { subscribeDone <- dxc.Subscribe() }()
		<-broker.subscribed

		dxc.EmergencyShutdown(errors.New("racing trip"))
		if err := dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}}); err != nil {
			t.Fatalf("TriggerRebalance returned error: %v", err)
		}

		select {
		case err := <-subscribeDone:
			if err == nil {
				t.Fatalf("iteration %d: Subscribe returned nil after a trip that preceded the assignment", i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Subscribe did not return", i)
		}
	}
}
