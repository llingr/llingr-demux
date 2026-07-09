// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// testWorkerShard creates a minimal WorkerShard for testing
func testWorkerShard[T any]() *WorkerShard[T] {
	ws := &WorkerShard[T]{
		//mu:          &sync.Mutex{},
		workers: make(map[string]*Worker[T]),
		//done:        &atomic.Bool{},
		//activeCount: &atomic.Int32{},
	}
	return ws
}

// newTestWorkItem creates a WorkItem for testing
func newTestWorkItem[T any](partition int32, offset int64, key string, payload T) *ports.WorkItem[T] {
	return &ports.WorkItem[T]{
		Message: &nexus.Message[T]{
			Partition: partition,
			Offset:    offset,
			Key:       key,
			Payload:   &payload,
		},
		Metrics: &nexus.Metrics{
			Partition: partition,
			Offset:    offset,
			ReadTime:  time.Now(),
		},
		Ctx:            context.Background(),
		PreviousOffset: offset - 1,
	}
}

// --- NewWorker tests ---

func TestNewWorker_InitialisesCorrectly(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	overflowGuard := make(chan struct{}, 5)
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, overflowGuard, logger)

	if worker == nil {
		t.Fatal("expected non-nil worker")
	}
	if worker.workItems == nil {
		t.Error("expected workItems channel to be initialised")
	}
	if cap(worker.workItems) != 16 {
		t.Errorf("expected workItems capacity 16, got %d", cap(worker.workItems))
	}
	if worker.workerShard != workerShard {
		t.Error("expected workerShard to be set")
	}
	//if worker.mu != workerShard.mu {
	//	t.Error("expected mutex to be shared with workerShard")
	//}
	if !worker.IsActive {
		t.Error("expected IsActive to be true on creation")
	}
	if worker.logger != logger {
		t.Error("expected logger to be set")
	}
	if worker.drained == nil {
		t.Error("expected drained condition to be initialised")
	}
	if worker.shutdown == nil {
		t.Error("expected shutdown channel to be initialised")
	}
}

// --- process tests ---

func TestProcess_SuccessfulProcessing(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)

	processCalled := false
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		processCalled = true
		return nil
	}

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")

	err := worker.process(workItem)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !processCalled {
		t.Error("expected processMessage to be called")
	}
	if workItem.Metrics.ProcessStartTime.IsZero() {
		t.Error("expected ProcessStartTime to be set")
	}
	if workItem.Metrics.ProcessDuration == 0 {
		t.Error("expected ProcessDuration to be set")
	}
}

func TestProcess_ReturnsError(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)

	expectedErr := errors.New("processing failed")
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return expectedErr
	}

	workItem := newTestWorkItem[string](1, 200, "key-2", "payload")

	err := worker.process(workItem)

	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected %v, got %v", expectedErr, err)
	}
	// check ProcessError trait is set
	if workItem.Message.Traits&nexus.ProcessError == 0 {
		t.Error("expected ProcessError trait to be set")
	}
	if logger.ErrorCount() != 1 {
		t.Errorf("expected 1 error log, got %d", logger.ErrorCount())
	}
}

func TestProcess_RecoversPanic(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		panic("test panic")
	}

	workItem := newTestWorkItem[string](2, 300, "key-3", "payload")

	err := worker.process(workItem)

	if err == nil {
		t.Error("expected error from panic recovery, got nil")
	}
	// check ProcessPanic trait is set
	if workItem.Message.Traits&nexus.ProcessPanic == 0 {
		t.Error("expected ProcessPanic trait to be set")
	}
	// check ProcessError trait is also set (set in defer)
	if workItem.Message.Traits&nexus.ProcessError == 0 {
		t.Error("expected ProcessError trait to be set after panic")
	}
}

func TestProcess_SetsTimingMetrics(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)

	processDuration := 10 * time.Millisecond
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		time.Sleep(processDuration)
		return nil
	}

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	workItem.Metrics.ReadTime = time.Now()

	_ = worker.process(workItem)

	if workItem.Metrics.ProcessStartTime.IsZero() {
		t.Error("expected ProcessStartTime to be set")
	}
	if workItem.Metrics.ProcessDuration < processDuration {
		t.Errorf("expected ProcessDuration >= %v, got %v", processDuration, workItem.Metrics.ProcessDuration)
	}
}

// --- writeDeadLetter tests ---

func TestWriteDeadLetter_SuccessfulWrite(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()

	writeCalled := false
	writeDeadLetterFunc := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		writeCalled = true
		return nil
	}

	dl := deadletter.New[string](writeDeadLetterFunc, logger)

	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	worker.deadLetter = dl

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	processingErr := errors.New("original processing error")

	err := worker.writeDeadLetter(workItem, processingErr)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if !writeCalled {
		t.Error("expected writeDeadLetter to be called")
	}
	// check DeadLetter trait is set
	if workItem.Message.Traits&nexus.DeadLetter == 0 {
		t.Error("expected DeadLetter trait to be set")
	}
}

func TestWriteDeadLetter_ReturnsError(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()

	expectedErr := errors.New("dead letter write failed")
	writeDeadLetterFunc := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return expectedErr
	}

	dl := deadletter.New[string](writeDeadLetterFunc, logger)

	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	worker.deadLetter = dl

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	processingErr := errors.New("original processing error")

	err := worker.writeDeadLetter(workItem, processingErr)

	if err == nil {
		t.Error("expected error, got nil")
	}
}

func TestWriteDeadLetter_RecoversPanic(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()

	writeDeadLetterFunc := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		panic("dead letter panic")
	}

	dl := deadletter.New[string](writeDeadLetterFunc, logger)

	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	worker.deadLetter = dl

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	processingErr := errors.New("original processing error")

	err := worker.writeDeadLetter(workItem, processingErr)

	if err == nil {
		t.Error("expected error from panic recovery, got nil")
	}
}

func TestWriteDeadLetter_RecoversPanicFromNilDeadLetter(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	// deliberately leave worker.deadLetter = nil to trigger panic

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	// set timing so WriteDeadLetterDuration calculation works
	workItem.Metrics.ProcessStartTime = time.Now()
	workItem.Metrics.ProcessDuration = 0
	processingErr := errors.New("original processing error")

	err := worker.writeDeadLetter(workItem, processingErr)

	if err == nil {
		t.Error("expected error from panic recovery, got nil")
	}
	// error message should contain panic information
	if !strings.Contains(err.Error(), "panic") {
		t.Errorf("expected error to mention panic, got: %v", err)
	}
}

func TestWriteDeadLetter_SetsWriteDuration(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()

	writeDuration := 5 * time.Millisecond
	writeDeadLetterFunc := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		time.Sleep(writeDuration)
		return nil
	}

	dl := deadletter.New[string](writeDeadLetterFunc, logger)

	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	worker.deadLetter = dl

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	// set up timing so WriteDeadLetterDuration calculation works correctly
	// WriteDeadLetterDuration = time.Since(processStartTime) - processDuration
	workItem.Metrics.ProcessStartTime = time.Now()
	workItem.Metrics.ProcessDuration = 0 // no process duration
	processingErr := errors.New("original processing error")

	_ = worker.writeDeadLetter(workItem, processingErr)

	// allow some tolerance for timing
	if workItem.Metrics.WriteDeadLetterDuration < writeDuration-time.Millisecond {
		t.Errorf("expected WriteDeadLetterDuration >= %v, got %v",
			writeDuration, workItem.Metrics.WriteDeadLetterDuration)
	}
}

// --- triggerCircuitBreaker tests ---

func TestTriggerCircuitBreaker_TriggersShutdown(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewRecordingLogger()

	cb := circuitbreaker.New(ctx, logger)

	workerShard := testWorkerShard[string]()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	worker.circuitBreaker = cb

	workItem := newTestWorkItem[string](5, 500, "key-5", "payload")
	dlErr := errors.New("dead letter failed")

	// trigger circuit breaker
	worker.triggerCircuitBreaker(workItem, dlErr)

	// verify circuit breaker was triggered
	select {
	case reason := <-cb.Triggered():
		if reason == "" {
			t.Error("expected non-empty reason")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected circuit breaker to be triggered")
	}
}

// Test_DeadLetterFailure_TriggersBreakerAndNeverCommits: when processing fails
// AND the dead-letter write fails, the work item's offset must NOT be
// committed (committing would silently lose the message) and the circuit
// breaker must fire. Pins the full processWorkItem path, not the helpers.
func Test_DeadLetterFailure_TriggersBreakerAndNeverCommits(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 1)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 4, guard, nil, logger)

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return errors.New("processing failed")
	}
	worker.deadLetter = deadletter.New[string](
		func(_ context.Context, _ *nexus.Message[string], _ error) error {
			return errors.New("dead letter write failed")
		}, logger)
	worker.circuitBreaker = circuitbreaker.New(context.Background(), logger)

	var commits atomic.Int32
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) { commits.Add(1) }
	worker.returnWorker = func(_ *Worker[string]) {}
	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()
	worker.workItems <- newTestWorkItem[string](0, 100, "key-1", "payload")

	select {
	case reason := <-worker.circuitBreaker.Triggered():
		if reason == "" {
			t.Error("expected a non-empty circuit breaker reason")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dead-letter failure did not trigger the circuit breaker")
	}

	if got := commits.Load(); got != 0 {
		t.Errorf("collectAndCommit called %d time(s) for an uncommittable message, want 0", got)
	}

	close(worker.shutdown)
}

// --- Drain tests ---

func TestDrain_WhenWorkerIsInactive_ReturnsImmediately(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)

	// set worker as inactive
	worker.IsActive = false

	done := make(chan struct{})
	go func() {
		worker.Drain()
		close(done)
	}()

	select {
	case <-done:
		// success - Drain returned immediately
	case <-time.After(100 * time.Millisecond):
		t.Error("Drain should return immediately when worker is inactive")
	}
}

func TestDrain_WhenWorkerIsActive_WaitsForDrainComplete(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)
	worker.IsActive = true

	drainFinished := make(chan struct{})
	go func() {
		worker.Drain()
		close(drainFinished)
	}()

	// Drain must still be waiting while the worker is active
	select {
	case <-drainFinished:
		t.Fatal("Drain returned while the worker was still active")
	case <-time.After(20 * time.Millisecond):
	}

	// the worker's self-removal step: deactivate then broadcast, under mu
	worker.mu.Lock()
	worker.IsActive = false
	worker.drained.Broadcast()
	worker.mu.Unlock()

	select {
	case <-drainFinished:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("Drain should complete after the drained broadcast")
	}
}

// Test_Drain_MultipleConcurrentWaiters: a shutdown's drain and a revoke's drain
// can overlap on different goroutines. Every concurrent Drain() on the same
// worker must return once it empties; a lost wakeup here turns a graceful stop
// into a coordinator timeout.
func Test_Drain_MultipleConcurrentWaiters(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 1)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 4, guard, nil, logger)

	release := make(chan struct{})
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		<-release
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}
	worker.returnWorker = func(_ *Worker[string]) {}
	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()
	worker.workItems <- newTestWorkItem[string](0, 100, "key-1", "payload")

	var waiters sync.WaitGroup
	waiters.Add(2)
	for i := 0; i < 2; i++ {
		go func() {
			defer waiters.Done()
			worker.Drain()
		}()
	}

	time.Sleep(20 * time.Millisecond) // let both waiters register before the worker empties
	close(release)

	drainDone := make(chan struct{})
	go func() {
		waiters.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
	case <-time.After(2 * time.Second):
		t.Error("a concurrent Drain waiter never woke after the worker emptied")
	}

	close(worker.shutdown)
}

// --- startProcessingWorkItems tests ---

func TestStartProcessingWorkItems_ColdPath_ProcessesFirstMessage(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	guard <- struct{}{} // pre-fill guard
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	processedItems := make(chan *ports.WorkItem[string], 10)
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	worker.collectAndCommit = func(item *ports.WorkItem[string]) {
		processedItems <- item
	}
	worker.returnWorker = func(_ *Worker[string]) {}

	// register worker in shard
	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send a work item (cold path - first message)
	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	worker.workItems <- workItem

	// wait for processing
	select {
	case processed := <-processedItems:
		if processed.Message.Key != "key-1" {
			t.Errorf("expected key 'key-1', got %s", processed.Message.Key)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("expected work item to be processed")
	}

	// shutdown worker
	close(worker.shutdown)
}

func TestStartProcessingWorkItems_HotPath_DrainsBatch(t *testing.T) {
	workerShard := testWorkerShard[string]()
	// guard pattern: send to acquire, receive to release
	// pre-fill with 1 token to simulate 1 acquired slot for this worker
	guard := make(chan struct{}, 10)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	var processedCount atomic.Int32
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		processedCount.Add(1)
		return nil
	}

	commitCount := atomic.Int32{}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {
		commitCount.Add(1)
	}

	returnedToPool := make(chan struct{}, 1)
	worker.returnWorker = func(_ *Worker[string]) {
		returnedToPool <- struct{}{}
	}

	// register worker in shard
	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send multiple work items rapidly (hot path)
	for i := 0; i < 5; i++ {
		workItem := newTestWorkItem[string](0, int64(i), "key-1", "payload")
		worker.workItems <- workItem
	}

	// wait for worker to return to pool (signals all processed)
	select {
	case <-returnedToPool:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Error("expected worker to return to pool after draining")
	}

	if count := processedCount.Load(); count != 5 {
		t.Errorf("expected 5 processed items, got %d", count)
	}
	if count := commitCount.Load(); count != 5 {
		t.Errorf("expected 5 committed items, got %d", count)
	}

	// verify worker removed from shard map
	workerShard.mu.Lock()
	if _, exists := workerShard.workers["key-1"]; exists {
		t.Error("expected worker to be removed from shard map")
	}
	workerShard.mu.Unlock()

	// verify guard was released (worker received from guard channel)
	// started with 1 token, worker released it, should now be 0
	if len(guard) != 0 {
		t.Errorf("expected guard to be released (len=0), got %d", len(guard))
	}

	close(worker.shutdown)
}

func TestStartProcessingWorkItems_Shutdown_ExitsLoop(t *testing.T) {
	workerShard := testWorkerShard[string]()
	logger := mocklogger.NewRecordingLogger()
	worker := NewWorker[string](workerShard, 16, nil, nil, logger)

	exited := make(chan struct{})
	go func() {
		worker.startProcessingWorkItems()
		close(exited)
	}()

	// give goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// send shutdown signal
	close(worker.shutdown)

	select {
	case <-exited:
		// success - worker exited
	case <-time.After(100 * time.Millisecond):
		t.Error("expected worker to exit on shutdown")
	}
}

func TestStartProcessingWorkItems_WorkerShardDone_SkipsProcessing(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	processedCount := atomic.Int32{}
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		processedCount.Add(1)
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}
	worker.returnWorker = func(_ *Worker[string]) {}

	workerShard.workers["key-1"] = worker

	// mark shard as done BEFORE starting
	workerShard.done.Store(true)

	go worker.startProcessingWorkItems()

	// send work item
	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	worker.workItems <- workItem

	// give time for processing
	time.Sleep(50 * time.Millisecond)

	// processMessage should NOT have been called
	if count := processedCount.Load(); count != 0 {
		t.Errorf("expected 0 processed items when shard is done, got %d", count)
	}

	close(worker.shutdown)
}

func TestStartProcessingWorkItems_ProcessError_WritesDeadLetter(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	processingErr := errors.New("processing failed")
	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return processingErr
	}

	deadLetterCalled := make(chan struct{}, 1)
	writeDeadLetterFunc := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		deadLetterCalled <- struct{}{}
		return nil
	}
	worker.deadLetter = deadletter.New[string](writeDeadLetterFunc, logger)

	commitCalled := make(chan struct{}, 1)
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {
		commitCalled <- struct{}{}
	}
	worker.returnWorker = func(_ *Worker[string]) {}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	worker.workItems <- workItem

	// verify dead letter was called
	select {
	case <-deadLetterCalled:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("expected dead letter to be called on processing error")
	}

	// verify commit was still called (message still advances)
	select {
	case <-commitCalled:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("expected commit to be called after dead letter")
	}

	close(worker.shutdown)
}

func TestStartProcessingWorkItems_DeadLetterError_TriggersCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return errors.New("processing failed")
	}

	writeDeadLetterFunc := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return errors.New("dead letter also failed")
	}
	worker.deadLetter = deadletter.New[string](writeDeadLetterFunc, logger)

	cb := circuitbreaker.New(ctx, logger)
	worker.circuitBreaker = cb

	commitCalled := atomic.Bool{}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {
		commitCalled.Store(true)
	}
	worker.returnWorker = func(_ *Worker[string]) {}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	worker.workItems <- workItem

	// verify circuit breaker was triggered
	select {
	case <-cb.Triggered():
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("expected circuit breaker to be triggered when dead letter fails")
	}

	// commit should NOT have been called (message not committed to avoid loss)
	time.Sleep(20 * time.Millisecond)
	if commitCalled.Load() {
		t.Error("expected commit NOT to be called when dead letter fails")
	}

	close(worker.shutdown)
}

func TestStartProcessingWorkItems_UsedOverflow_ReleasesOverflowGuard(t *testing.T) {
	workerShard := testWorkerShard[string]()
	// guard pattern: send to acquire, receive to release
	// regular guard should remain untouched when using overflow
	guard := make(chan struct{}, 10)
	// pre-fill overflow guard with 1 token (simulating 1 acquired slot via overflow)
	overflowGuard := make(chan struct{}, 5)
	overflowGuard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, overflowGuard, logger)

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}

	returnedToPool := make(chan struct{}, 1)
	worker.returnWorker = func(_ *Worker[string]) {
		returnedToPool <- struct{}{}
	}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send work item with UsedOverflow trait
	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	nexus.SetUsedOverflow(&workItem.Metrics.Traits)
	worker.workItems <- workItem

	// wait for worker to return to pool
	select {
	case <-returnedToPool:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Error("expected worker to return to pool")
	}

	// verify overflow guard was released (worker received from overflowGuard)
	// started with 1 token, worker released it by receiving, should now be 0
	if len(overflowGuard) != 0 {
		t.Errorf("expected overflowGuard to be released (len=0), got %d", len(overflowGuard))
	}
	// regular guard should remain untouched (still empty)
	if len(guard) != 0 {
		t.Errorf("expected guard to remain untouched (len=0), got %d", len(guard))
	}

	close(worker.shutdown)
}

func TestStartProcessingWorkItems_Draining_SignalsDrainComplete(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}
	worker.returnWorker = func(_ *Worker[string]) {}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send work item
	workItem := newTestWorkItem[string](0, 100, "key-1", "payload")
	worker.workItems <- workItem

	// start drain (in separate goroutine since it blocks)
	drainDone := make(chan struct{})
	go func() {
		worker.Drain()
		close(drainDone)
	}()

	// drain should complete when worker finishes processing
	select {
	case <-drainDone:
		// success
	case <-time.After(500 * time.Millisecond):
		t.Error("expected Drain to complete after worker processes all messages")
	}

	close(worker.shutdown)
}

func TestStartProcessingWorkItems_MessageArrivesAfterFallthrough_StillProcessed(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	var processedOffsets []int64
	var mu sync.Mutex
	worker.processMessage = func(_ context.Context, msg *nexus.Message[string]) error {
		mu.Lock()
		processedOffsets = append(processedOffsets, msg.Offset)
		mu.Unlock()
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}

	returnCount := atomic.Int32{}
	worker.returnWorker = func(_ *Worker[string]) {
		returnCount.Add(1)
	}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send first message to wake worker
	guard <- struct{}{}
	workItem1 := newTestWorkItem[string](0, 100, "key-1", "payload")
	worker.workItems <- workItem1

	// wait a bit then send second message
	time.Sleep(50 * time.Millisecond)
	guard <- struct{}{}
	workItem2 := newTestWorkItem[string](0, 101, "key-1", "payload")
	worker.workItems <- workItem2

	// wait for processing
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(processedOffsets) != 2 {
		t.Errorf("expected 2 processed offsets, got %d", len(processedOffsets))
	}
	mu.Unlock()

	close(worker.shutdown)
}

// --- Race condition tests ---

// TestStartProcessingWorkItems_CleanupRaceAborted tests the cleanup race condition
// modelled in TLA+ as WorkerAbortCleanup.
//
// Scenario: Worker processes a message, channel appears empty, worker attempts cleanup.
// However, a sender injects a message before the worker's len() check inside the mutex.
// The double-check pattern must detect this and abort cleanup, continuing to process.
//
// Timeline:
//  1. Worker processes first message
//  2. Worker hits hot path default (channel appears empty)
//  3. Worker tries to acquire mutex - but TEST holds it
//  4. TEST sends second message while holding mutex
//  5. TEST releases mutex
//  6. Worker acquires mutex, checks len(workItems) != 0, aborts cleanup
//  7. Worker continues and processes second message
func TestStartProcessingWorkItems_CleanupRaceAborted(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 10)
	guard <- struct{}{}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 16, guard, nil, logger)

	var processedCount atomic.Int32
	firstMsgProcessing := make(chan struct{})
	continueFirstMsg := make(chan struct{})

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		count := processedCount.Add(1)
		if count == 1 {
			close(firstMsgProcessing) // signal we're in first message
			<-continueFirstMsg        // wait to continue
		}
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}

	returnCount := atomic.Int32{}
	worker.returnWorker = func(_ *Worker[string]) {
		returnCount.Add(1)
	}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send first message
	workItem1 := newTestWorkItem[string](0, 100, "key-1", "first")
	worker.workItems <- workItem1

	// wait for worker to enter processMessage
	<-firstMsgProcessing

	// grab mutex now - when first message completes, worker will block on it
	workerShard.mu.Lock()

	// sanity check: worker must still be in map (can't have cleaned up yet because
	// it's still blocked in processMessage - we grabbed mutex before releasing it)
	if _, exists := workerShard.workers["key-1"]; !exists {
		workerShard.mu.Unlock()
		t.Fatal("test timing broken: worker cleaned up before race window opened")
	}

	// let first message complete - worker will block on mutex
	close(continueFirstMsg)

	// give worker time to finish processing and hit mutex.Lock()
	time.Sleep(10 * time.Millisecond)

	// send second message while we hold mutex (simulates sender winning the race)
	workItem2 := newTestWorkItem[string](0, 101, "key-1", "second")
	worker.workItems <- workItem2

	// release mutex - worker will see non-empty channel, abort cleanup
	workerShard.mu.Unlock()

	// wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	if count := processedCount.Load(); count != 2 {
		t.Errorf("expected 2 messages processed, got %d", count)
	}

	// cleanup was aborted for first message (channel not empty when checked)
	// worker returns to pool only after second message
	if count := returnCount.Load(); count != 1 {
		t.Errorf("expected 1 return to pool (cleanup race aborted), got %d", count)
	}

	close(worker.shutdown)
}

// --- Concurrency tests ---

func TestWorker_ConcurrentDrain(t *testing.T) {
	workerShard := testWorkerShard[string]()
	guard := make(chan struct{}, 100)
	for i := 0; i < 100; i++ {
		guard <- struct{}{}
	}
	logger := mocklogger.NewRecordingLogger()

	worker := NewWorker[string](workerShard, 64, guard, nil, logger)

	worker.processMessage = func(_ context.Context, _ *nexus.Message[string]) error {
		time.Sleep(time.Millisecond) // simulate work
		return nil
	}
	worker.collectAndCommit = func(_ *ports.WorkItem[string]) {}
	worker.returnWorker = func(_ *Worker[string]) {}

	workerShard.workers["key-1"] = worker

	go worker.startProcessingWorkItems()

	// send many messages
	for i := 0; i < 100; i++ {
		workItem := newTestWorkItem[string](0, int64(i), "key-1", "payload")
		worker.workItems <- workItem
	}

	// drain concurrently
	drainDone := make(chan struct{})
	go func() {
		worker.Drain()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		// success
	case <-time.After(2 * time.Second):
		t.Error("Drain timed out with concurrent messages")
	}

	close(worker.shutdown)
}
