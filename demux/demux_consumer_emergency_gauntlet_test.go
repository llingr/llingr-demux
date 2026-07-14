// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package demux

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-nexus/nexus"
)

// The emergency-shutdown gauntlet: a randomized-schedule concurrency stress
// that builds a FRESH consumer per iteration and hammers it from every angle
// at once - concurrent trips, concurrent Shutdowns, Subscribe racing
// assignment, revokes mid-storm, callbacks and loggers that re-enter the
// engine - then asserts the end state every time:
//
//  1. the iteration completes (a watchdog turns any deadlock into a failure
//     with a full goroutine dump),
//  2. the registered shutdown callback fired EXACTLY once,
//  3. a nil (graceful) reason is only ever reported when a Shutdown
//     actually completed cleanly,
//  4. post-terminal probes (one more trip, one more Shutdown) change
//     nothing and return promptly,
//  5. goroutines do not grow beyond the engine's known per-consumer
//     parked set (catches runaway leaks).
//
// Iterations default to 1000 (200 under -short) and are configurable:
//
//	LLINGR_STRESS_ITERS=500000 go test -run Gauntlet ./demux/
//
// Runs larger than LLINGR_STRESS_BATCH (default 5000) are sharded across
// subprocesses so parked per-consumer goroutines (an engine design property:
// one consumer per process) cannot exhaust a single process. Failures
// reproduce deterministically: the seed is logged, and each iteration derives
// its schedule from LLINGR_STRESS_SEED + iteration index.
//
// Not a coverage-guided fuzz: the space being searched is goroutine
// interleavings, which Go's fuzzer does not drive; randomized schedules with
// a fixed default seed are the effective tool for that space.
const (
	gauntletItersEnv = "LLINGR_STRESS_ITERS"
	gauntletBatchEnv = "LLINGR_STRESS_BATCH"
	gauntletSeedEnv  = "LLINGR_STRESS_SEED"
	gauntletChildEnv = "LLINGR_STRESS_CHILD"

	gauntletDefaultIters = 1000
	gauntletShortIters   = 200
	gauntletDefaultBatch = 5000

	// generous per-iteration watchdog: any single lifecycle taking this long
	// under mocks is a stall, reported with a goroutine dump
	gauntletIterTimeout = 10 * time.Second
)

func gauntletEnvInt(name string, fallback int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// reentrantLogger re-enters EmergencyShutdown from inside the breaker's own
// log emission (the winner's logging window) when armed, mimicking a host
// log handler that reacts to the "protective shutdown" line by escalating.
type reentrantLogger struct {
	target func(error)
	armed  atomic.Bool
}

func (l *reentrantLogger) reenter(msg string) {
	if l.armed.Load() && strings.Contains(msg, "circuit-breaker:") {
		if l.armed.CompareAndSwap(true, false) {
			l.target(errors.New("re-entered from log handler"))
		}
	}
}
func (l *reentrantLogger) Error(_ context.Context, format string, args ...any) {
	l.reenter(fmt.Sprintf(format, args...))
}
func (l *reentrantLogger) Warn(_ context.Context, format string, args ...any) {
	l.reenter(fmt.Sprintf(format, args...))
}
func (l *reentrantLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (l *reentrantLogger) Debug(_ context.Context, _ string, _ ...any) {}

// gauntletScenario is the base lifecycle shape; flavors multiply it.
type gauntletScenario int

const (
	scenarioStormNoSubscribe gauntletScenario = iota
	scenarioSubscribeAssignThenStorm
	scenarioTripDuringAssignmentWait
	scenarioTripRacingAssignment
	scenarioGracefulThenLateTrip
	scenarioTripBeforeSubscribe
	scenarioCount
)

func Test_Consumer_EmergencyShutdown_Gauntlet(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	iters := gauntletEnvInt(gauntletItersEnv, gauntletDefaultIters)
	if testing.Short() && os.Getenv(gauntletItersEnv) == "" {
		iters = gauntletShortIters
	}
	batch := gauntletEnvInt(gauntletBatchEnv, gauntletDefaultBatch)
	seed := int64(gauntletEnvInt(gauntletSeedEnv, 1))

	// large runs shard across subprocesses so parked per-consumer goroutines
	// (one-consumer-per-process engine design) cannot exhaust this process
	if iters > batch && os.Getenv(gauntletChildEnv) == "" {
		runGauntletBatches(t, iters, batch, seed)
		return
	}

	t.Logf("gauntlet: %d iterations, seed %d", iters, seed)
	baseline := runtime.NumGoroutine()

	for i := 0; i < iters; i++ {
		iterSeed := seed + int64(i)
		done := make(chan struct{})
		go func() {
			defer close(done)
			runGauntletIteration(t, iterSeed)
		}()
		select {
		case <-done:
		case <-time.After(gauntletIterTimeout):
			dumpGoroutines(t)
			t.Fatalf("iteration %d (seed %d) stalled beyond %s: deadlock", i, iterSeed, gauntletIterTimeout)
		}
		if t.Failed() {
			t.Fatalf("iteration %d (seed %d) failed; reproduce with %s=%d %s=1",
				i, iterSeed, gauntletSeedEnv, iterSeed, gauntletItersEnv)
		}
	}

	// leak ceiling: each consumer legitimately parks a small fixed goroutine
	// set (warm worker, pool pruner - the engine is one-consumer-per-process);
	// anything past a generous linear bound is a NEW leak
	final := runtime.NumGoroutine()
	ceiling := baseline + iters*8 + 256
	t.Logf("gauntlet: goroutines %d -> %d (ceiling %d)", baseline, final, ceiling)
	if final > ceiling {
		dumpGoroutines(t)
		t.Fatalf("goroutine growth %d -> %d exceeds ceiling %d: leak", baseline, final, ceiling)
	}
}

// runGauntletBatches shards a large run across child processes of this test
// binary, each running `batch` iterations with a distinct seed range.
func runGauntletBatches(t *testing.T, iters, batch int, seed int64) {
	t.Helper()
	batches := (iters + batch - 1) / batch
	t.Logf("gauntlet: %d iterations across %d subprocess batches of %d (seed %d)", iters, batches, batch, seed)

	for b := 0; b < batches; b++ {
		n := batch
		if remaining := iters - b*batch; remaining < n {
			n = remaining
		}
		//nolint:gosec,noctx // G204: test subprocess of our own binary, context not needed
		cmd := exec.Command(os.Args[0], "-test.run=Test_Consumer_EmergencyShutdown_Gauntlet$")
		cmd.Env = append(os.Environ(),
			gauntletChildEnv+"=1",
			fmt.Sprintf("%s=%d", gauntletItersEnv, n),
			fmt.Sprintf("%s=%d", gauntletSeedEnv, seed+int64(b*batch)),
			config.SkipValidationEnvVar+"=true",
		)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("gauntlet batch %d/%d (seeds %d..%d) failed: %v\n%s",
				b+1, batches, seed+int64(b*batch), seed+int64(b*batch+n-1), err, out)
		}
	}
}

// runGauntletIteration builds one fresh consumer and runs one randomized
// schedule against it, then checks every invariant.
func runGauntletIteration(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic schedule, not crypto

	scenario := gauntletScenario(rng.Intn(int(scenarioCount)))
	trippers := 1 + rng.Intn(4)
	shutdowners := rng.Intn(3)
	callbackReenters := rng.Intn(4) == 0
	loggerReenters := rng.Intn(4) == 0
	revokeStorm := scenario == scenarioSubscribeAssignThenStorm && rng.Intn(3) == 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // releases the committer goroutines each iteration

	broker := &rearmablePort{controllableBrokerPort: newControllableBrokerPort()}
	broker.releaseUnsubscribe()

	recorder := &atomicCallbackRecorder{fired: make(chan error, 16)}
	var dxc *Consumer[any]
	var gracefulCompleted atomic.Bool

	callback := func(cbCtx context.Context, reason error) {
		recorder.record(cbCtx, reason)
		if callbackReenters {
			dxc.EmergencyShutdown(errors.New("re-entered from shutdown callback"))
		}
	}

	logger := &reentrantLogger{}
	cfg := config.DemuxConfig{
		ConcurrentKeys:          2,
		WorkerShardsCount:       2, // smallest accepted (power of 2, above 1)
		PerKeyBufferLen:         1,
		AwaitAssignmentsTimeout: 5 * time.Second, // only reached on a bug; watchdog reports it
		DrainTimeout:            20 * time.Millisecond,
		PollTimeout:             time.Millisecond,
		AutoCommitInterval:      time.Hour, // keep the commit tick out of the schedule
	}

	consumer := NewBuilder("gauntlet-topic", noOpProcessMessage, noOpWriteDeadLetter).
		WithContext(ctx).
		WithLogger(logger).
		WithDemuxConfig(cfg).
		WithShutdownCallback(callback).
		Build(broker)
	dxc = consumer.(*Consumer[any]) //nolint:forcetypeassert // test: known type from builder
	logger.target = dxc.EmergencyShutdown
	logger.armed.Store(loggerReenters)

	start := make(chan struct{})
	var actors sync.WaitGroup
	// per-actor jitter spreads start points across the schedule space; drawn
	// here on the scheduling goroutine because rand.Rand is not goroutine-safe
	runActor := func(fn func()) {
		spin := rng.Intn(64)
		actors.Add(1)
		go func() {
			defer actors.Done()
			<-start
			for n := spin; n > 0; n-- {
				runtime.Gosched()
			}
			fn()
		}()
	}

	trip := func(n int) func() {
		return func() { dxc.EmergencyShutdown(fmt.Errorf("gauntlet trip %d", n)) }
	}
	shutdown := func() {
		if err := dxc.Shutdown(); err == nil {
			gracefulCompleted.Store(true)
		}
	}
	assign := func() {
		_ = dxc.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{{Partition: 0}})
	}
	revoke := func() {
		_ = dxc.TriggerRebalance(nexus.Revoke, []nexus.RebalanceInfo{
			{RebalanceType: nexus.Revoke, Partition: 0, CommittedOffset: -1},
		})
	}

	var subscribeErr error
	subscribeReturned := make(chan struct{})
	subscribe := func() {
		subscribeErr = dxc.Subscribe()
		close(subscribeReturned)
	}

	// -- schedule per scenario ------------------------------------------------
	subscribeRan := false
	switch scenario {
	case scenarioStormNoSubscribe:
		for n := 0; n < trippers; n++ {
			runActor(trip(n))
		}
		for n := 0; n < shutdowners; n++ {
			runActor(shutdown)
		}

	case scenarioSubscribeAssignThenStorm:
		subscribeRan = true
		go subscribe()
		<-broker.subscribed
		assign()
		<-subscribeReturned
		for n := 0; n < trippers; n++ {
			runActor(trip(n))
		}
		for n := 0; n < shutdowners; n++ {
			runActor(shutdown)
		}
		if revokeStorm {
			runActor(revoke)
		}

	case scenarioTripDuringAssignmentWait:
		subscribeRan = true
		go subscribe()
		<-broker.subscribed
		for n := 0; n < trippers; n++ {
			runActor(trip(n))
		}

	case scenarioTripRacingAssignment:
		subscribeRan = true
		go subscribe()
		<-broker.subscribed
		runActor(assign)
		for n := 0; n < trippers; n++ {
			runActor(trip(n))
		}

	case scenarioGracefulThenLateTrip:
		subscribeRan = true
		go subscribe()
		<-broker.subscribed
		assign()
		<-subscribeReturned
		runActor(shutdown)
		for n := 0; n < trippers; n++ {
			runActor(trip(n))
		}

	case scenarioTripBeforeSubscribe:
		dxc.EmergencyShutdown(errors.New("gauntlet trip before subscribe"))
		subscribeRan = true
		go subscribe()
		for n := 0; n < shutdowners; n++ {
			runActor(shutdown)
		}

	case scenarioCount: // unreachable; keeps exhaustive-switch linters content
	}

	close(start)
	actors.Wait()
	if subscribeRan {
		<-subscribeReturned
	}

	// -- invariants ------------------------------------------------------------

	// the terminal event must deliver: every scenario either trips or
	// completes a graceful shutdown; wait for the first delivery
	select {
	case <-recorder.fired:
	case <-time.After(5 * time.Second):
		t.Errorf("seed %d scenario %d: no callback delivered", seed, scenario)
		return
	}

	// post-terminal probes: nothing changes, nothing blocks
	dxc.EmergencyShutdown(errors.New("post-terminal probe"))
	_ = dxc.Shutdown()
	time.Sleep(time.Millisecond)

	if got := recorder.count(); got != 1 {
		t.Errorf("seed %d scenario %d: callback fired %d times, want exactly 1", seed, scenario, got)
	}

	recorder.mu.Lock()
	reason := recorder.calls[0]
	recorder.mu.Unlock()
	if reason == nil && !gracefulCompleted.Load() {
		t.Errorf("seed %d scenario %d: graceful nil delivered but no Shutdown completed cleanly", seed, scenario)
	}

	// scenario-specific Subscribe consistency
	switch scenario {
	case scenarioTripBeforeSubscribe, scenarioTripDuringAssignmentWait:
		if subscribeErr == nil {
			t.Errorf("seed %d scenario %d: Subscribe returned nil despite a guaranteed prior trip", seed, scenario)
		}
	case scenarioStormNoSubscribe, scenarioSubscribeAssignThenStorm,
		scenarioTripRacingAssignment, scenarioGracefulThenLateTrip, scenarioCount:
		// result depends on the race; bounded completion is the invariant
	}
}

func dumpGoroutines(t *testing.T) {
	t.Helper()
	_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
}
