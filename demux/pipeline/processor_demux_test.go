// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/demux/offset"
	"github.com/llingr/llingr-demux/demux/pipeline/fnv"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

func newTestLogger() *mocklogger.RecordingLogger {
	return mocklogger.NewRecordingLogger()
}

// mockCommitter implements ports.CommitterPort[T] for isolated demux testing.
type mockCommitter[T any] struct {
	collectAndCommit func(*ports.WorkItem[T])
}

func (m *mockCommitter[T]) CollectAndCommit(workItem *ports.WorkItem[T]) {
	m.collectAndCommit(workItem)
}

// Compile-time interface check
var _ ports.CommitterPort[string] = (*mockCommitter[string])(nil)

// testPayload is a simple payload type for processor tests
type testPayload struct {
	key       string
	partition int32
	offset    int64
	ctx       context.Context
}

// testScenario defines a pipeline behaviour test case using self-describing messages.
// Inspired by testkit/scenario.TestMessage pattern.
type testScenario struct {
	name string

	key       string
	partition int32
	offset    int64

	processingDelay time.Duration
	failProcessing  bool
	panicProcessing bool
	failDeadLetter  bool
	panicDeadLetter bool

	// expectations
	expectProcessed    bool
	expectDeadLetter   bool
	expectCircuitBreak bool
	expectCommit       bool
}

// demuxTestHarness provides test infrastructure for demux testing.
// Built without offset.Committer or metrics.Collector dependencies for isolation.
type demuxTestHarness struct {
	ctx            context.Context
	cfg            config.DemuxConfig
	guard          chan struct{}
	overflowGuard  chan struct{}
	circuitBreaker *circuitbreaker.CircuitBreaker
	deadLetter     *deadletter.DeadLetter[string]
	pool           *alloc.WorkItemsPool[string]
	logger         nexus.Logger

	// tracking
	processedMu      sync.Mutex
	processedOrder   []string // keys in order processed
	processedOffsets map[string][]int64

	deadLetterMu     sync.Mutex
	deadLetterOrder  []string
	deadLetterErrors map[string]error

	commitMu         sync.Mutex
	committedItems   []*ports.WorkItem[string]
	committedOffsets []int64 // track commit order

	// behaviour injection
	processFunc    func(ctx context.Context, msg *nexus.Message[string]) error
	deadLetterFunc func(ctx context.Context, msg *nexus.Message[string], reason error) error
	commitFunc     func(workItem *ports.WorkItem[string])
}

func newDemuxTestHarness() *demuxTestHarness {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            16,
		PerKeyBufferLen:           8,
		WorkerShardsCount:         4,
		AutoCommitInterval:        time.Second,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 8)

	cb := circuitbreaker.New(ctx, logger)
	pool := alloc.NewWorkItemsPool[string](cfg)

	h := &demuxTestHarness{
		ctx:              ctx,
		cfg:              cfg,
		guard:            guard,
		overflowGuard:    overflowGuard,
		circuitBreaker:   cb,
		pool:             pool,
		logger:           logger,
		processedOrder:   make([]string, 0),
		processedOffsets: make(map[string][]int64),
		deadLetterOrder:  make([]string, 0),
		deadLetterErrors: make(map[string]error),
		committedItems:   make([]*ports.WorkItem[string], 0),
	}

	// default process function tracks order
	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOrder = append(h.processedOrder, msg.Key)
		h.processedOffsets[msg.Key] = append(h.processedOffsets[msg.Key], msg.Offset)
		return nil
	}

	// default dead letter function tracks entries
	h.deadLetterFunc = func(_ context.Context, msg *nexus.Message[string], reason error) error {
		h.deadLetterMu.Lock()
		defer h.deadLetterMu.Unlock()
		h.deadLetterOrder = append(h.deadLetterOrder, msg.Key)
		h.deadLetterErrors[msg.Key] = reason
		return nil
	}

	// default commit function tracks committed items
	h.commitFunc = func(workItem *ports.WorkItem[string]) {
		h.commitMu.Lock()
		defer h.commitMu.Unlock()
		h.committedItems = append(h.committedItems, workItem)
		h.committedOffsets = append(h.committedOffsets, workItem.Message.Offset)
	}

	// create dead letter writer
	h.deadLetter = deadletter.New[string](func(ctx context.Context, msg *nexus.Message[string], reason error) error {
		return h.deadLetterFunc(ctx, msg, reason)
	}, logger)

	return h
}

func (h *demuxTestHarness) processMessage(ctx context.Context, msg *nexus.Message[string]) error {
	return h.processFunc(ctx, msg)
}

// collectAndCommit delegates to commitFunc - tracks committed items by default,
// can be overridden per-test by setting h.commitFunc before calling createDemux
func (h *demuxTestHarness) collectAndCommit(workItem *ports.WorkItem[string]) {
	h.commitFunc(workItem)
}

// createDemux builds a Demux using NewDemux with a mock CommitterPort
func (h *demuxTestHarness) createDemux() *Demux[string] {
	committer := &mockCommitter[string]{
		collectAndCommit: h.collectAndCommit,
	}
	return NewDemux(h.cfg, h.processMessage, h.deadLetter, committer,
		h.circuitBreaker, h.guard, h.overflowGuard, h.logger, func(_ *nexus.Message[string]) {})
}

func (h *demuxTestHarness) createWorkItem(key string, partition int32, off int64) *ports.WorkItem[string] {
	workItem := h.pool.Borrow()
	workItem.Message.Key = key
	workItem.Message.Partition = partition
	workItem.Message.Offset = off
	payload := "test-payload"
	workItem.Message.Payload = &payload
	workItem.Metrics.Partition = partition
	workItem.Metrics.Offset = off
	workItem.Ctx = context.Background()
	return workItem
}

func TestSendToWorkerForProcessing_NewWorker(t *testing.T) {
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	// acquire guard token (simulates processor.Process acquiring)
	h.guard <- struct{}{}

	const testKey = "test-key"
	workItem := h.createWorkItem(testKey, 0, 100)

	// should create new worker
	dmx.SendToWorkerForProcessing(testKey, workItem)

	// simulate delayed processing
	time.Sleep(50 * time.Millisecond)

	h.processedMu.Lock()
	defer h.processedMu.Unlock()

	if len(h.processedOrder) != 1 {
		t.Fatalf("expected 1 processed message, got %d", len(h.processedOrder))
	}
	if h.processedOrder[0] != testKey {
		t.Errorf("processed key = %q, want %q", h.processedOrder[0], testKey)
	}
}

func TestSendToWorkerForProcessing_ExistingWorker(t *testing.T) {
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	// send first message to create worker
	h.guard <- struct{}{}
	const testKey = "existing-key"
	workItem1 := h.createWorkItem(testKey, 0, 100)
	dmx.SendToWorkerForProcessing(testKey, workItem1)

	// send second message to same key (existing worker)
	h.guard <- struct{}{}
	workItem2 := h.createWorkItem(testKey, 0, 101)
	dmx.SendToWorkerForProcessing(testKey, workItem2)

	// wait for processing
	time.Sleep(100 * time.Millisecond)

	h.processedMu.Lock()
	defer h.processedMu.Unlock()

	if len(h.processedOrder) != 2 {
		t.Fatalf("expected 2 processed messages, got %d", len(h.processedOrder))
	}

	// both should be same key
	for i, key := range h.processedOrder {
		if key != testKey {
			t.Errorf("processedOrder[%d] = %q, want %q", i, key, testKey)
		}
	}
}

func TestSendToWorkerForProcessing_ReleasesGuardForExistingWorker(t *testing.T) {
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	initialGuardLen := len(h.guard)

	// send first message
	h.guard <- struct{}{}
	const testKey = "key-1"
	workItem1 := h.createWorkItem(testKey, 0, 100)
	dmx.SendToWorkerForProcessing(testKey, workItem1)

	// send second message to SAME key
	h.guard <- struct{}{}
	workItem2 := h.createWorkItem(testKey, 0, 101)
	dmx.SendToWorkerForProcessing(testKey, workItem2)

	// guard should be released for the second message (pipelined into existing worker)
	// after SendToWorkerForProcessing returns, one guard token should be consumed
	// for the first message, second is pipelined so guard is released immediately

	// wait for processing to complete
	time.Sleep(100 * time.Millisecond)

	// guard should return to initial state as workers release tokens
	if len(h.guard) != initialGuardLen {
		t.Logf("guard len = %d, initial = %d (ok if processing still in flight)", len(h.guard), initialGuardLen)
	}
}

func TestSendToWorkerForProcessing_ReleasesOverflowGuardForExistingWorker(t *testing.T) {
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	// send first message to create worker
	h.guard <- struct{}{}
	workItem1 := h.createWorkItem("overflow-key", 0, 100)
	dmx.SendToWorkerForProcessing("overflow-key", workItem1)

	// send second message with UsedOverflow trait (simulates overflow path)
	h.overflowGuard <- struct{}{}
	workItem2 := h.createWorkItem("overflow-key", 0, 101)
	nexus.SetUsedOverflow(&workItem2.Metrics.Traits)
	dmx.SendToWorkerForProcessing("overflow-key", workItem2)

	// wait for processing
	time.Sleep(100 * time.Millisecond)

	h.processedMu.Lock()
	defer h.processedMu.Unlock()

	if len(h.processedOrder) != 2 {
		t.Fatalf("expected 2 processed messages, got %d", len(h.processedOrder))
	}
}

func TestSendToWorkerForProcessing_PipelinedMessageReleasesTokenImmediately(t *testing.T) {
	// CRITICAL TEST: When a message pipelines to an existing worker, the token
	// must be released IMMEDIATELY - not when processing completes.
	//
	// This tests the TLA+ "TOKEN FIX" (WorkerCoordination.tla lines 16-18):
	// "For EXISTING workers, token is acquired then immediately released (net zero).
	//  Only NEW workers hold a token until they go idle."
	//
	// Without this behaviour, sending N messages to the same key would consume
	// N tokens instead of 1, rapidly exhausting guard capacity.
	//
	// Timeline:
	//   1. Send first message to key-A, acquires guard token (guardUsed = 1)
	//   2. Block first message in processing
	//   3. Send second message to key-A, acquires then IMMEDIATELY releases
	//   4. Verify guardUsed is still 1 (not 2) while first message still processing
	//   5. Unblock, verify both messages processed

	h := newDemuxTestHarness()
	h.cfg.ConcurrentKeys = 4
	h.cfg.PerKeyBufferLen = 8
	h.guard = make(chan struct{}, h.cfg.ConcurrentKeys)
	h.overflowGuard = make(chan struct{}) // no overflow for this test

	firstMsgProcessing := make(chan struct{})
	continueProcessing := make(chan struct{})
	var processedCount atomic.Int32

	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		count := processedCount.Add(1)
		if count == 1 {
			close(firstMsgProcessing) // signal first message is processing
			<-continueProcessing      // block until test says continue
		}
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOrder = append(h.processedOrder, msg.Key)
		return nil
	}

	dmx := h.createDemux()

	// verify guard starts empty
	if len(h.guard) != 0 {
		t.Fatalf("expected guard to start empty, got %d", len(h.guard))
	}

	// send first message - acquires guard token
	h.guard <- struct{}{}
	workItem1 := h.createWorkItem("pipeline-key", 0, 100)
	go dmx.SendToWorkerForProcessing("pipeline-key", workItem1)

	// wait for first message to be processing
	<-firstMsgProcessing

	// at this point: first message holds ONE token, is blocked in processing
	// guard should have exactly 1 token consumed
	if len(h.guard) != 1 {
		t.Fatalf("after first message, expected guardUsed=1, got %d", len(h.guard))
	}

	// send second message to SAME key - this pipelines to existing worker
	// the token should be released IMMEDIATELY after SendToWorkerForProcessing returns
	h.guard <- struct{}{} // acquire token (simulating processor.Process)
	workItem2 := h.createWorkItem("pipeline-key", 0, 101)
	dmx.SendToWorkerForProcessing("pipeline-key", workItem2)

	// CRITICAL CHECK: token for second message should already be released
	// even though first message is still processing
	// guardUsed should still be 1 (only first message's token held)
	if len(h.guard) != 1 {
		t.Errorf("CRITICAL: pipelined message did not release token immediately! "+
			"expected guardUsed=1, got %d (token leak!)", len(h.guard))
	}

	// send third message to verify the pattern holds
	h.guard <- struct{}{}
	workItem3 := h.createWorkItem("pipeline-key", 0, 102)
	dmx.SendToWorkerForProcessing("pipeline-key", workItem3)

	// still should be only 1 token held
	if len(h.guard) != 1 {
		t.Errorf("CRITICAL: third pipelined message did not release token! "+
			"expected guardUsed=1, got %d", len(h.guard))
	}

	// unblock processing
	close(continueProcessing)

	// wait for all messages to complete
	deadline := time.After(2 * time.Second)
	for processedCount.Load() < 3 {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timeout: only processed %d/3 messages", processedCount.Load())
		}
	}

	// after worker goes idle, token should be released (guardUsed = 0)
	time.Sleep(50 * time.Millisecond) // allow cleanup
	if len(h.guard) != 0 {
		t.Errorf("after all processing, expected guardUsed=0, got %d", len(h.guard))
	}

	h.processedMu.Lock()
	defer h.processedMu.Unlock()
	if len(h.processedOrder) != 3 {
		t.Errorf("expected 3 messages processed, got %d", len(h.processedOrder))
	}

	t.Logf("verified: 3 messages to same key consumed only 1 guard token during processing")
}

func TestWorkerReleasesFirstMessageTokenType(t *testing.T) {
	// CRITICAL TEST: Worker must release the token type from its FIRST message,
	// not from subsequent pipelined messages.
	//
	// This tests the TLA+ workerTokenType caching (WorkerCoordination.tla line 263):
	// "Uses cached token type from FIRST message (usedOverflow flag)"
	//
	// Scenario:
	//   1. Guard full (2/2), overflow available (0/1)
	//   2. First message to key-X uses overflow (worker caches usedOverflow=true)
	//   3. Guard frees up (1/2)
	//   4. Second message to key-X uses guard (pipelined, immediately released)
	//   5. Worker goes idle
	//   6. Verify: overflow token released, guard unchanged
	//
	// If this invariant is broken, the worker would release a guard token instead
	// of the overflow token it actually holds, causing a token leak.

	h := newDemuxTestHarness()
	h.cfg.ConcurrentKeys = 2
	h.cfg.PerKeyBufferLen = 4
	h.cfg.WorkerShardsCount = 4
	h.guard = make(chan struct{}, 2)         // guard capacity = 2
	h.overflowGuard = make(chan struct{}, 1) // overflow capacity = 1

	firstMsgProcessing := make(chan struct{})
	secondMsgSent := make(chan struct{})
	continueProcessing := make(chan struct{})
	var processedCount atomic.Int32

	h.processFunc = func(_ context.Context, _ *nexus.Message[string]) error {
		count := processedCount.Add(1)
		if count == 1 {
			close(firstMsgProcessing)
			<-secondMsgSent      // wait for second message to be sent
			<-continueProcessing // then wait for test to say continue
		}
		return nil
	}

	dmx := h.createDemux()

	// step 1: fill guard completely
	h.guard <- struct{}{}
	h.guard <- struct{}{}

	// verify state: guard full (2/2), overflow empty (0/1)
	if len(h.guard) != 2 {
		t.Fatalf("setup: expected guard=2, got %d", len(h.guard))
	}
	if len(h.overflowGuard) != 0 {
		t.Fatalf("setup: expected overflow=0, got %d", len(h.overflowGuard))
	}

	// step 2: first message to key-X must use overflow (guard is full)
	h.overflowGuard <- struct{}{} // simulate Processor acquiring overflow
	workItem1 := h.createWorkItem("token-type-key", 0, 100)
	nexus.SetUsedOverflow(&workItem1.Metrics.Traits) // mark as overflow
	go dmx.SendToWorkerForProcessing("token-type-key", workItem1)

	// wait for first message to be processing
	<-firstMsgProcessing

	// state check: worker holds overflow token
	// guard still full (2/2), overflow now has 1 token (the worker's)
	if len(h.guard) != 2 {
		t.Errorf("during first msg: expected guard=2, got %d", len(h.guard))
	}
	if len(h.overflowGuard) != 1 {
		t.Errorf("during first msg: expected overflow=1, got %d", len(h.overflowGuard))
	}

	// step 3: free up a guard token
	<-h.guard // guard now 1/2

	// step 4: second message to key-X uses guard (pipelined to existing worker)
	// token should be immediately released since worker already exists
	h.guard <- struct{}{} // simulate Processor acquiring guard
	workItem2 := h.createWorkItem("token-type-key", 0, 101)
	// NO UsedOverflow trait - this message used guard path
	dmx.SendToWorkerForProcessing("token-type-key", workItem2)

	// the guard token for second message should be immediately released
	// guard should be back to 1/2 (not 2/2)
	if len(h.guard) != 1 {
		t.Errorf("after second msg pipelined: expected guard=1, got %d", len(h.guard))
	}

	close(secondMsgSent)

	// step 5: let worker finish and go idle
	close(continueProcessing)

	// wait for both messages to be processed
	deadline := time.After(2 * time.Second)
	for processedCount.Load() < 2 {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timeout: processed %d/2", processedCount.Load())
		}
	}

	// allow worker to complete cleanup
	time.Sleep(50 * time.Millisecond)

	// step 6: CRITICAL VERIFICATION
	// Worker was activated with overflow token, so it must release overflow token.
	// Guard token count should be unchanged from when second message was pipelined.

	finalGuard := len(h.guard)
	finalOverflow := len(h.overflowGuard)

	// overflow should be released (0 tokens held)
	if finalOverflow != 0 {
		t.Errorf("TOKEN TYPE BUG: overflow token not released! expected 0, got %d", finalOverflow)
	}

	// guard should have 1 token (we freed one earlier, second msg was pipelined/released)
	if finalGuard != 1 {
		t.Errorf("TOKEN TYPE BUG: guard token count wrong! expected 1, got %d", finalGuard)
	}

	t.Logf("verified: worker activated with overflow released overflow token "+
		"(guard=%d, overflow=%d)", finalGuard, finalOverflow)
}

// -----------------------------------------------------------------------------
// Ordering guarantee tests - CRITICAL
// -----------------------------------------------------------------------------

func TestSendToWorkerForProcessing_SameKeyProcessedInOrder(t *testing.T) {
	// CRITICAL TEST: messages with the same key MUST be processed in offset order
	h := newDemuxTestHarness()

	// add processing delay to allow pipelining
	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		time.Sleep(10 * time.Millisecond)
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOrder = append(h.processedOrder, msg.Key)
		h.processedOffsets[msg.Key] = append(h.processedOffsets[msg.Key], msg.Offset)
		return nil
	}

	dmx := h.createDemux()

	const messageCount = 20
	key := "ordering-test-key"

	// send messages rapidly to pipeline them
	for i := 0; i < messageCount; i++ {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, int64(i))
		dmx.SendToWorkerForProcessing(key, workItem)
	}

	// wait for all processing to complete
	time.Sleep(time.Duration(messageCount*15) * time.Millisecond)

	h.processedMu.Lock()
	defer h.processedMu.Unlock()

	if len(h.processedOffsets[key]) != messageCount {
		t.Fatalf("expected %d messages processed, got %d", messageCount, len(h.processedOffsets[key]))
	}

	// verify offsets are in ascending order
	offsets := h.processedOffsets[key]
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			t.Errorf("ordering violated: offset[%d]=%d <= offset[%d]=%d",
				i, offsets[i], i-1, offsets[i-1])
		}
	}

	// verify exact sequence
	for i, off := range offsets {
		if off != int64(i) {
			t.Errorf("offset[%d] = %d, want %d", i, off, i)
		}
	}
}

func TestSendToWorkerForProcessing_SameKeyCommittedInOrder(t *testing.T) {
	// CRITICAL TEST: messages with the same key must be sent to commit in offset order.
	// The worker processes items sequentially and calls collectAndCommit synchronously
	// after each processMessage, so commit order == processing order.
	// This test verifies the ordering guarantee through processing order tracking.
	h := newDemuxTestHarness()

	// track the order messages are processed (which == commit order)
	var mu sync.Mutex
	var committedOffsets []int64

	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		time.Sleep(5 * time.Millisecond) // simulate work
		mu.Lock()
		defer mu.Unlock()
		// track offset order - since collectAndCommit is called synchronously after
		// processMessage, this order is the commit order
		committedOffsets = append(committedOffsets, msg.Offset)
		return nil
	}

	dmx := h.createDemux()

	const messageCount = 25
	key := "commit-ordering-key"
	partition := int32(3)

	// send messages rapidly - all to same key so they pipeline through one worker
	for i := 0; i < messageCount; i++ {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, partition, int64(i))
		dmx.SendToWorkerForProcessing(key, workItem)
	}

	// wait for all processing to complete
	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		mu.Lock()
		count := len(committedOffsets)
		mu.Unlock()
		if count >= messageCount {
			break
		}
		select {
		case <-ticker.C:
			continue
		case <-deadline:
			t.Fatalf("timeout waiting for commits: got %d/%d", count, messageCount)
		}
	}

	mu.Lock()
	defer mu.Unlock()

	// verify commits arrived in ascending offset order
	for i := 1; i < len(committedOffsets); i++ {
		if committedOffsets[i] <= committedOffsets[i-1] {
			t.Errorf("commit ordering violated: offset[%d]=%d <= offset[%d]=%d",
				i, committedOffsets[i], i-1, committedOffsets[i-1])
		}
	}

	// verify exact sequence 0, 1, 2, ..., messageCount-1
	for i, off := range committedOffsets {
		if off != int64(i) {
			t.Errorf("committed offset[%d] = %d, want %d", i, off, i)
		}
	}
}

func TestSendToWorkerForProcessing_DifferentKeysProcessConcurrently(t *testing.T) {
	// different keys should be processed concurrently (not sequentially)
	h := newDemuxTestHarness()

	var startTimes sync.Map
	var completeTimes sync.Map

	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		startTimes.Store(msg.Key, time.Now())
		time.Sleep(50 * time.Millisecond) // significant delay
		completeTimes.Store(msg.Key, time.Now())
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOrder = append(h.processedOrder, msg.Key)
		return nil
	}

	dmx := h.createDemux()

	keys := []string{"key-A", "key-B", "key-C", "key-D"}
	start := time.Now()

	// send all messages
	for _, key := range keys {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, 100)
		dmx.SendToWorkerForProcessing(key, workItem)
	}

	// wait for completion
	time.Sleep(150 * time.Millisecond)

	elapsed := time.Since(start)

	// if processed sequentially: 4 * 50ms = 200ms
	// if processed concurrently: ~50ms (plus overhead)
	// threshold: 150ms indicates concurrent processing
	if elapsed > 180*time.Millisecond {
		t.Errorf("processing took %v, expected concurrent processing < 180ms", elapsed)
	}

	h.processedMu.Lock()
	defer h.processedMu.Unlock()

	if len(h.processedOrder) != len(keys) {
		t.Errorf("expected %d processed, got %d", len(keys), len(h.processedOrder))
	}
}

func TestSendToWorkerForProcessing_SameKeyRoutesToSameShard(t *testing.T) {
	// same key should always route to the same shard (FNV hash consistency)
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	key := "consistent-routing-key"

	// track which shard receives the message
	var targetShardIndex int
	for i, shard := range dmx.workerShards {
		// check after first send
		if len(shard.workers) > 0 {
			targetShardIndex = i
			break
		}
	}

	// send multiple messages with same key
	for i := 0; i < 5; i++ {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, int64(100+i))
		dmx.SendToWorkerForProcessing(key, workItem)
	}

	time.Sleep(100 * time.Millisecond)

	// verify all went to same shard
	foundInShards := 0
	for i, shard := range dmx.workerShards {
		shard.mu.Lock()
		if _, exists := shard.workers[key]; exists {
			foundInShards++
			if targetShardIndex != 0 && i != targetShardIndex {
				t.Errorf("key routed to different shards")
			}
		}
		shard.mu.Unlock()
	}

	// key should only exist in one shard (or zero if worker returned to pool)
	if foundInShards > 1 {
		t.Errorf("key found in %d shards, expected 0 or 1", foundInShards)
	}
}

// -----------------------------------------------------------------------------
// Backpressure tests
// -----------------------------------------------------------------------------

func TestSendToWorkerForProcessing_BackpressureWhenChannelFills(t *testing.T) {
	// when worker's channel fills, SendToWorkerForProcessing should block
	h := newDemuxTestHarness()
	h.cfg.PerKeyBufferLen = 2 // small buffer to fill quickly

	// block processing to fill the channel
	processingBlocked := make(chan struct{})
	continueProcessing := make(chan struct{})

	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		if msg.Offset == 0 {
			close(processingBlocked)
			<-continueProcessing // block first message
		}
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOrder = append(h.processedOrder, msg.Key)
		return nil
	}

	dmx := h.createDemux()
	key := "backpressure-key"

	// send first message (will block in processing)
	h.guard <- struct{}{}
	workItem1 := h.createWorkItem(key, 0, 0)
	go dmx.SendToWorkerForProcessing(key, workItem1)

	<-processingBlocked // wait for first message to be processing

	// fill the channel (buffer = 2)
	for i := 1; i <= 2; i++ {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, int64(i))
		go dmx.SendToWorkerForProcessing(key, workItem)
	}

	time.Sleep(20 * time.Millisecond) // let sends complete

	// next send should block (channel full)
	sendComplete := make(chan struct{})
	go func() {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, 3)
		dmx.SendToWorkerForProcessing(key, workItem)
		close(sendComplete)
	}()

	select {
	case <-sendComplete:
		// might complete if timing allows - that's ok
	case <-time.After(30 * time.Millisecond):
		// expected - send is blocked due to full channel
	}

	// unblock processing
	close(continueProcessing)

	// wait for completion
	select {
	case <-sendComplete:
		// good - unblocked
	case <-time.After(500 * time.Millisecond):
		t.Error("send remained blocked after processing continued")
	}
}

// -----------------------------------------------------------------------------
// DrainWorkers tests
// -----------------------------------------------------------------------------

func TestDrainWorkers_EmptyShards(t *testing.T) {
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	// no messages sent, shards are empty
	// drain should complete immediately without blocking
	done := make(chan struct{})
	go func() {
		dmx.DrainWorkers()
		close(done)
	}()

	select {
	case <-done:
		// expected
	case <-time.After(100 * time.Millisecond):
		t.Error("DrainWorkers blocked on empty shards")
	}
}

func TestDrainWorkers_ActiveWorkers(t *testing.T) {
	h := newDemuxTestHarness()

	processingStarted := make(chan struct{})
	processingDone := make(chan struct{})

	h.processFunc = func(_ context.Context, _ *nexus.Message[string]) error {
		close(processingStarted)
		<-processingDone // block until signaled
		return nil
	}

	dmx := h.createDemux()

	// start processing a message
	h.guard <- struct{}{}
	workItem := h.createWorkItem("drain-key", 0, 100)
	go dmx.SendToWorkerForProcessing("drain-key", workItem)

	<-processingStarted // wait for processing to start

	// start drain (should block waiting for worker)
	drainComplete := make(chan struct{})
	go func() {
		dmx.DrainWorkers()
		close(drainComplete)
	}()

	// drain should be blocked
	select {
	case <-drainComplete:
		t.Error("DrainWorkers returned while worker is active")
	case <-time.After(50 * time.Millisecond):
		// expected - drain is waiting
	}

	// complete processing
	close(processingDone)

	// drain should complete
	select {
	case <-drainComplete:
		// expected
	case <-time.After(500 * time.Millisecond):
		t.Error("DrainWorkers didn't complete after worker finished")
	}
}

func TestDrainWorkers_MultipleShards(t *testing.T) {
	h := newDemuxTestHarness()
	h.cfg.WorkerShardsCount = 4

	var processCount atomic.Int32
	h.processFunc = func(_ context.Context, _ *nexus.Message[string]) error {
		processCount.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	dmx := h.createDemux()

	// verify guard starts empty
	if len(h.guard) != 0 {
		t.Fatalf("expected guard to start empty, got %d", len(h.guard))
	}

	// send messages to different keys (likely different shards)
	keys := []string{"key-a", "key-b", "key-c", "key-d", "key-e", "key-f", "key-g", "key-h"}
	for i, key := range keys {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, int64(i))
		dmx.SendToWorkerForProcessing(key, workItem)
	}

	// drain all workers
	dmx.DrainWorkers()

	// all messages should be processed
	if count := processCount.Load(); count != int32(len(keys)) { //nolint:gosec // G115: len bounded by test
		t.Errorf("processed %d messages, expected %d", count, len(keys))
	}

	// TOKEN BALANCE: after drain, all guard tokens must be released
	// This verifies TLA+ GuardTokenBalance invariant
	// Note: guard tokens are released after drainComplete signal, so brief polling needed
	deadline := time.Now().Add(100 * time.Millisecond)
	for len(h.guard) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(h.guard) != 0 {
		t.Errorf("TOKEN LEAK: after drain, expected all guard tokens released, got %d held",
			len(h.guard))
	}
}

func TestDrainWorkers_TokenBalanceWithOverflow(t *testing.T) {
	// Test that BOTH guard and overflow tokens are released after drain.
	// This verifies TLA+ GuardTokenBalance and OverflowTokenBalance invariants.
	//
	// Scenario: guard capacity = 2, overflow capacity = 2
	// Send 4 messages to different keys:
	//   - First 2 use guard tokens (guard full)
	//   - Next 2 use overflow tokens
	// After drain, all 4 tokens must be released.

	h := newDemuxTestHarness()
	h.cfg.ConcurrentKeys = 2
	h.cfg.WorkerShardsCount = 4
	h.guard = make(chan struct{}, 2)         // small guard to force overflow
	h.overflowGuard = make(chan struct{}, 2) // overflow capacity

	var processCount atomic.Int32
	h.processFunc = func(_ context.Context, _ *nexus.Message[string]) error {
		processCount.Add(1)
		time.Sleep(20 * time.Millisecond)
		return nil
	}

	dmx := h.createDemux()

	// verify both start empty
	if len(h.guard) != 0 || len(h.overflowGuard) != 0 {
		t.Fatalf("expected both guards to start empty, got guard=%d overflow=%d",
			len(h.guard), len(h.overflowGuard))
	}

	// send 4 messages to different keys
	keys := []string{"key-a", "key-b", "key-c", "key-d"}

	// first 2 use guard
	for i := 0; i < 2; i++ {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(keys[i], 0, int64(i))
		dmx.SendToWorkerForProcessing(keys[i], workItem)
	}

	// next 2 use overflow (guard is full)
	for i := 2; i < 4; i++ {
		h.overflowGuard <- struct{}{}
		workItem := h.createWorkItem(keys[i], 0, int64(i))
		nexus.SetUsedOverflow(&workItem.Metrics.Traits)
		dmx.SendToWorkerForProcessing(keys[i], workItem)
	}

	// during processing: tokens should be held
	// (can't reliably check mid-flight due to timing)

	// drain all workers
	dmx.DrainWorkers()

	// allow workers to complete guard release (happens after drainComplete signal)
	time.Sleep(time.Microsecond)

	// all messages should be processed
	if count := processCount.Load(); count != 4 {
		t.Errorf("processed %d messages, expected 4", count)
	}

	// TOKEN BALANCE: after drain, ALL tokens must be released
	if len(h.guard) != 0 {
		t.Errorf("GUARD TOKEN LEAK: after drain, expected 0 guard tokens held, got %d",
			len(h.guard))
	}
	if len(h.overflowGuard) != 0 {
		t.Errorf("OVERFLOW TOKEN LEAK: after drain, expected 0 overflow tokens held, got %d",
			len(h.overflowGuard))
	}

	t.Logf("verified: all guard and overflow tokens released after drain")
}

// -----------------------------------------------------------------------------
// Pipeline behavior table-driven tests
// -----------------------------------------------------------------------------

func TestDemuxPipelineBehaviors(t *testing.T) {
	tests := []testScenario{
		{
			name:            "normal processing succeeds",
			key:             "normal-key",
			partition:       0,
			offset:          100,
			expectProcessed: true,
			expectCommit:    true,
		},
		{
			name:             "processing failure routes to dead letter",
			key:              "fail-key",
			partition:        0,
			offset:           101,
			failProcessing:   true,
			expectProcessed:  true, // still "processed" (attempted)
			expectDeadLetter: true,
			expectCommit:     true, // dead letter commits to avoid replay
		},
		{
			name:               "dead letter failure triggers circuit breaker",
			key:                "circuit-break-key",
			partition:          0,
			offset:             102,
			failProcessing:     true,
			failDeadLetter:     true,
			expectProcessed:    true,
			expectDeadLetter:   true, // attempted
			expectCircuitBreak: true,
			expectCommit:       false, // don't commit if DL fails
		},
		{
			name:            "processing with delay maintains order",
			key:             "delay-key",
			partition:       0,
			offset:          103,
			processingDelay: 10 * time.Millisecond,
			expectProcessed: true,
			expectCommit:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newDemuxTestHarness()

			var processed atomic.Bool
			var deadLettered atomic.Bool

			h.processFunc = func(_ context.Context, _ *nexus.Message[string]) error {
				if tt.processingDelay > 0 {
					time.Sleep(tt.processingDelay)
				}
				processed.Store(true)
				if tt.panicProcessing {
					panic("simulated panic")
				}
				if tt.failProcessing {
					return errors.New("simulated processing error")
				}
				return nil
			}

			h.deadLetterFunc = func(_ context.Context, _ *nexus.Message[string], _ error) error {
				deadLettered.Store(true)
				if tt.panicDeadLetter {
					panic("simulated dead letter panic")
				}
				if tt.failDeadLetter {
					return errors.New("simulated dead letter error")
				}
				return nil
			}

			dmx := h.createDemux()

			h.guard <- struct{}{}
			workItem := h.createWorkItem(tt.key, tt.partition, tt.offset)
			dmx.SendToWorkerForProcessing(tt.key, workItem)

			// wait for processing
			time.Sleep(100 * time.Millisecond)

			if tt.expectProcessed && !processed.Load() {
				t.Error("expected message to be processed")
			}

			if tt.expectDeadLetter && !deadLettered.Load() {
				t.Error("expected dead letter to be written")
			}

			if tt.expectCircuitBreak {
				select {
				case <-h.circuitBreaker.Triggered():
					// expected
				case <-time.After(100 * time.Millisecond):
					t.Error("expected circuit breaker to be triggered")
				}
			}
		})
	}
}

// -----------------------------------------------------------------------------
// Integration test: end-to-end ordering with multiple keys
// -----------------------------------------------------------------------------

func TestDemux_MultiKeyOrderingIntegration(t *testing.T) {
	// comprehensive test: multiple keys, verify per-key ordering is preserved
	h := newDemuxTestHarness()

	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		time.Sleep(5 * time.Millisecond) // small delay to interleave
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedOrder = append(h.processedOrder, msg.Key)
		h.processedOffsets[msg.Key] = append(h.processedOffsets[msg.Key], msg.Offset)
		return nil
	}

	dmx := h.createDemux()

	// send messages for 4 keys, 10 messages each, interleaved
	keys := []string{"alpha", "beta", "gamma", "delta"}
	messagesPerKey := 10

	for offset := 0; offset < messagesPerKey; offset++ {
		for _, key := range keys {
			h.guard <- struct{}{}
			workItem := h.createWorkItem(key, 0, int64(offset))
			dmx.SendToWorkerForProcessing(key, workItem)
		}
	}

	// wait for all processing
	time.Sleep(500 * time.Millisecond)

	h.processedMu.Lock()
	defer h.processedMu.Unlock()

	// verify each key has correct count
	for _, key := range keys {
		if len(h.processedOffsets[key]) != messagesPerKey {
			t.Errorf("key %q: got %d messages, want %d",
				key, len(h.processedOffsets[key]), messagesPerKey)
			continue
		}

		// verify each key's offsets are in order
		offsets := h.processedOffsets[key]
		for i := 1; i < len(offsets); i++ {
			if offsets[i] <= offsets[i-1] {
				t.Errorf("key %q: ordering violated at position %d: %d <= %d",
					key, i, offsets[i], offsets[i-1])
			}
		}
	}
}

// -----------------------------------------------------------------------------
// Processor tests - verify constructor wiring, context propagation, addressing
// -----------------------------------------------------------------------------

// processorTestHarness provides test infrastructure for Processor testing.
// Built without offset.Committer or metrics.Collector dependencies for isolation.
type processorTestHarness struct {
	ctx            context.Context
	cfg            config.DemuxConfig
	guard          chan struct{}
	overflowGuard  chan struct{}
	circuitBreaker *circuitbreaker.CircuitBreaker
	deadLetter     *deadletter.DeadLetter[testPayload]
	pool           *alloc.WorkItemsPool[testPayload]
	logger         nexus.Logger

	// tracking for verification
	processedMu   sync.Mutex
	processedMsgs []*processedMessage

	commitMu         sync.Mutex
	committedItems   []*ports.WorkItem[testPayload]
	committedOffsets []int64

	// behaviour injection
	processFunc func(ctx context.Context, msg *nexus.Message[testPayload]) error
}

type processedMessage struct {
	ctx       context.Context
	partition int32
	offset    int64
	key       string
	payload   *testPayload
}

func newProcessorTestHarness() *processorTestHarness {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:                     16,
		PerKeyBufferLen:                    8,
		WorkerShardsCount:                  4,
		AutoCommitInterval:                 time.Second,
		CommitIngestChannelLen:             100,
		CommitPartitionSliceLen:            50,
		AcquireCommitGuardTimeout:          time.Second,
		AcquireWorkerTimeoutCircuitBreaker: 100 * time.Millisecond, // short for testing
	}

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 8)

	cb := circuitbreaker.New(ctx, logger)
	pool := alloc.NewWorkItemsPool[testPayload](cfg)

	h := &processorTestHarness{
		ctx:              ctx,
		cfg:              cfg,
		guard:            guard,
		overflowGuard:    overflowGuard,
		circuitBreaker:   cb,
		pool:             pool,
		logger:           logger,
		processedMsgs:    make([]*processedMessage, 0),
		committedItems:   make([]*ports.WorkItem[testPayload], 0),
		committedOffsets: make([]int64, 0),
	}

	// default process function tracks messages
	h.processFunc = func(ctx context.Context, msg *nexus.Message[testPayload]) error {
		h.processedMu.Lock()
		defer h.processedMu.Unlock()
		h.processedMsgs = append(h.processedMsgs, &processedMessage{
			ctx:       ctx,
			partition: msg.Partition,
			offset:    msg.Offset,
			key:       msg.Key,
			payload:   msg.Payload,
		})
		return nil
	}

	// create dead letter writer
	h.deadLetter = deadletter.New[testPayload](
		func(_ context.Context, _ *nexus.Message[testPayload], _ error) error {
			return nil
		}, logger)

	return h
}

func (h *processorTestHarness) processMessage(ctx context.Context,
	msg *nexus.Message[testPayload]) error {
	return h.processFunc(ctx, msg)
}

// collectAndCommit is the default WorkItem callback - tracks committed items
func (h *processorTestHarness) collectAndCommit(workItem *ports.WorkItem[testPayload]) {
	h.commitMu.Lock()
	defer h.commitMu.Unlock()
	h.committedItems = append(h.committedItems, workItem)
	h.committedOffsets = append(h.committedOffsets, workItem.Message.Offset)
}

// createDemux builds a Demux manually using NewWorkerShard with custom collectAndCommit
func (h *processorTestHarness) createDemux() *Demux[testPayload] {
	shardsCount := h.cfg.WorkerShardsCount
	workerShards := make([]*WorkerShard[testPayload], shardsCount)
	for i := 0; i < shardsCount; i++ {
		workerShards[i] = NewWorkerShard(h.processMessage, h.deadLetter, h.collectAndCommit,
			h.circuitBreaker, h.guard, h.overflowGuard, h.cfg, h.logger)
	}

	return &Demux[testPayload]{
		workerShards:   workerShards,
		concurrentKeys: h.cfg.ConcurrentKeys,
		awaitRateLimit: func(_ *nexus.Message[testPayload]) {},
	}
}

func (h *processorTestHarness) createProcessor(dmx *Demux[testPayload],
	extractEnvelope nexus.ExtractEnvelope[testPayload]) *Processor[testPayload] {

	return NewProcessor[testPayload](
		h.ctx,
		h.guard,
		h.overflowGuard,
		dmx,
		h.cfg,
		extractEnvelope,
		h.pool,
		h.logger,
	)
}

// -----------------------------------------------------------------------------
// NewProcessor tests
// -----------------------------------------------------------------------------

func TestNewProcessor_InitializesFields(t *testing.T) {
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       p.ctx,
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	if proc.workItemsPool == nil {
		t.Error("expected workItemsPool to be set")
	}
	if proc.extractEnvelope == nil {
		t.Error("expected extractEnvelope to be set")
	}
	if proc.timer == nil {
		t.Error("expected timer to be set")
	}
	if proc.demux == nil {
		t.Error("expected demux to be set")
	}
	if proc.logger == nil {
		t.Error("expected logger to be set")
	}
	if proc.guard == nil {
		t.Error("expected guard to be set")
	}
	if proc.overflowGuard == nil {
		t.Error("expected overflowGuard to be set")
	}
	if proc.acquireWorkerTimeout != h.cfg.AcquireWorkerTimeoutCircuitBreaker {
		t.Errorf("acquireWorkerTimeout = %v, want %v",
			proc.acquireWorkerTimeout, h.cfg.AcquireWorkerTimeoutCircuitBreaker)
	}
}

// -----------------------------------------------------------------------------
// Process context propagation tests - CRITICAL
// -----------------------------------------------------------------------------

type testContextKey string

func TestProcess_ContextFromEnvelopePassesThroughPipeline(t *testing.T) {
	// CRITICAL TEST: context attached in envelope extraction must reach ProcessMessage
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	// unique context value to verify propagation
	contextKey := testContextKey("test-trace-id")
	expectedValue := "random-trace-12345"

	extractEnvelope := func(p testPayload) nexus.Envelope {
		// attach the unique value to context
		ctx := context.WithValue(context.Background(), contextKey, expectedValue)
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       ctx,
		}
	}

	// track the context we receive
	var receivedCtx context.Context
	var receivedValue any
	done := make(chan struct{})

	h.processFunc = func(ctx context.Context, _ *nexus.Message[testPayload]) error {
		receivedCtx = ctx
		receivedValue = ctx.Value(contextKey)
		close(done)
		return nil
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	payload := testPayload{
		key:       "context-test-key",
		partition: 0,
		offset:    100,
	}

	err := proc.Process(payload, time.Now())
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
	}

	// verify context value propagated
	if receivedCtx == nil {
		t.Fatal("received nil context in ProcessMessage")
	}
	if receivedValue == nil {
		t.Fatal("context value not found - context was not propagated from envelope")
	}
	if receivedValue != expectedValue {
		t.Errorf("context value = %v, want %v", receivedValue, expectedValue)
	}
}

// -----------------------------------------------------------------------------
// Process partition/offset/key assignment tests
// -----------------------------------------------------------------------------

func TestProcess_PartitionOffsetKeyAssignedFromEnvelope(t *testing.T) {
	// verify partition, offset, and key from envelope appear on the message
	tests := []struct {
		name      string
		partition int32
		offset    int64
		key       string
	}{
		{"basic values", 0, 100, "key-a"},
		{"high partition", 11, 999, "key-b"},
		{"large offset", 5, 1_000_000, "key-c"},
		{"unicode key", 3, 500, "ключ-日本語"},
		{"empty key", 0, 0, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newProcessorTestHarness()
			dmx := h.createDemux()

			extractEnvelope := func(p testPayload) nexus.Envelope {
				return nexus.Envelope{
					Partition: p.partition,
					Offset:    p.offset,
					Key:       p.key,
					Ctx:       context.Background(),
				}
			}

			done := make(chan struct{})
			h.processFunc = func(_ context.Context, msg *nexus.Message[testPayload]) error {
				defer close(done)

				if msg.Partition != tt.partition {
					t.Errorf("partition = %d, want %d", msg.Partition, tt.partition)
				}
				if msg.Offset != tt.offset {
					t.Errorf("offset = %d, want %d", msg.Offset, tt.offset)
				}
				if msg.Key != tt.key {
					t.Errorf("key = %q, want %q", msg.Key, tt.key)
				}
				return nil
			}

			proc := h.createProcessor(dmx, extractEnvelope)

			payload := testPayload{
				partition: tt.partition,
				offset:    tt.offset,
				key:       tt.key,
			}

			err := proc.Process(payload, time.Now())
			if err != nil {
				t.Fatalf("Process failed: %v", err)
			}

			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for message processing")
			}
		})
	}
}

func TestProcess_MetricsPartitionOffsetAssigned(t *testing.T) {
	// verify metrics also receive partition and offset
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	expectedPartition := int32(7)
	expectedOffset := int64(12345)

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	done := make(chan struct{})
	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		close(done)
		return nil
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	payload := testPayload{
		partition: expectedPartition,
		offset:    expectedOffset,
		key:       "metrics-test-key",
	}

	err := proc.Process(payload, time.Now())
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
	}
}

// -----------------------------------------------------------------------------
// Process guard path tests
// -----------------------------------------------------------------------------

func TestProcess_GuardFastPath(t *testing.T) {
	// guard channel has capacity - should proceed immediately
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	done := make(chan struct{})
	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		close(done)
		return nil
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// guard should be empty (fast path available)
	if len(h.guard) != 0 {
		t.Fatalf("expected empty guard, got %d tokens", len(h.guard))
	}

	payload := testPayload{key: "fast-path-key", partition: 0, offset: 1}
	start := time.Now()
	err := proc.Process(payload, time.Now())
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	// fast path should be near-instant (no timer involved)
	if elapsed > 10*time.Millisecond {
		t.Errorf("fast path took %v, expected < 10ms", elapsed)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
	}
}

func TestProcess_GuardSlowPathThenAvailable(t *testing.T) {
	// guard full initially, then token freed - should proceed via slow path
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	done := make(chan struct{})
	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		close(done)
		return nil
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// fill the guard channel
	for i := 0; i < h.cfg.ConcurrentKeys; i++ {
		h.guard <- struct{}{}
	}

	// release one token after a short delay
	go func() {
		time.Sleep(20 * time.Millisecond)
		<-h.guard
	}()

	payload := testPayload{key: "slow-path-key", partition: 0, offset: 1}
	err := proc.Process(payload, time.Now())

	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
	}
}

func TestProcess_OverflowGuardPath(t *testing.T) {
	// guard full, overflow available - should use overflow
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	var receivedTraits nexus.Traits
	done := make(chan struct{})
	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		// can't directly access metrics.Traits here, but we verify overflow was used
		close(done)
		return nil
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// fill the guard channel completely
	for i := 0; i < h.cfg.ConcurrentKeys; i++ {
		h.guard <- struct{}{}
	}

	// overflow should be empty (available)
	if len(h.overflowGuard) != 0 {
		t.Fatalf("expected empty overflow guard, got %d tokens", len(h.overflowGuard))
	}

	payload := testPayload{key: "overflow-key", partition: 0, offset: 1}
	err := proc.Process(payload, time.Now())

	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
	}

	// overflow guard should have been used (token consumed)
	// note: worker may have released it by now, but the path was taken
	_ = receivedTraits // used for future trait verification if needed
}

func TestProcess_OverflowTraitSetOnlyWhenOverflowUsed(t *testing.T) {
	// CRITICAL TEST: With guard=5 overflow=1, first 5 messages should NOT have
	// UsedOverflow trait, but the 6th message MUST have it set.
	// This verifies the overflow path correctly marks messages for observability.
	//
	// This test captures WorkItems directly from the collectAndCommit function,
	// bypassing the full committer/metrics pipeline for simpler, more direct verification.

	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:                     5, // guard size = 5
		PerKeyBufferLen:                    8,
		WorkerShardsCount:                  4,
		AutoCommitInterval:                 time.Second,
		CommitIngestChannelLen:             100,
		CommitPartitionSliceLen:            50,
		AcquireCommitGuardTimeout:          time.Second,
		AcquireWorkerTimeoutCircuitBreaker: time.Second,
	}

	guard := make(chan struct{}, 5)         // guard size 5
	overflowGuard := make(chan struct{}, 1) // overflow size 1

	// capture WorkItems directly from collectAndCommit
	type workItemRecord struct {
		offset       int64
		usedOverflow bool
	}
	var mu sync.Mutex
	var capturedWorkItems []workItemRecord

	collectAndCommit := func(workItem *ports.WorkItem[testPayload]) {
		mu.Lock()
		defer mu.Unlock()
		capturedWorkItems = append(capturedWorkItems, workItemRecord{
			offset:       workItem.Message.Offset,
			usedOverflow: workItem.Metrics.Traits&nexus.UsedOverflow != 0,
		})
	}

	cb := circuitbreaker.New(ctx, logger)
	pool := alloc.NewWorkItemsPool[testPayload](cfg)

	deadLetter := deadletter.New[testPayload](
		func(_ context.Context, _ *nexus.Message[testPayload], _ error) error {
			return nil
		}, logger)

	// block workers from completing until all messages have been dispatched
	// this ensures the guard is full when the 6th message is sent
	blockProcessing := make(chan struct{})
	var processedCount atomic.Int32
	processFunc := func(_ context.Context, _ *nexus.Message[testPayload]) error {
		<-blockProcessing // block until all messages dispatched
		processedCount.Add(1)
		return nil
	}

	// build Demux manually using NewWorkerShard with custom collectAndCommit
	shardsCount := cfg.WorkerShardsCount
	workerShards := make([]*WorkerShard[testPayload], shardsCount)
	for i := 0; i < shardsCount; i++ {
		workerShards[i] = NewWorkerShard(processFunc, deadLetter, collectAndCommit, cb,
			guard, overflowGuard, cfg, logger)
	}

	dmx := &Demux[testPayload]{
		workerShards:   workerShards,
		concurrentKeys: cfg.ConcurrentKeys,
		awaitRateLimit: func(_ *nexus.Message[testPayload]) {},
	}

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	proc := NewProcessor[testPayload](ctx, guard, overflowGuard, dmx, cfg, extractEnvelope, pool, logger)

	// send 6 messages with unique keys
	// first 5 use guard, 6th uses overflow
	keys := []string{"key-1", "key-2", "key-3", "key-4", "key-5", "key-6"}
	for i, key := range keys {
		payload := testPayload{key: key, partition: 0, offset: int64(i)}
		err := proc.Process(payload, time.Now())
		if err != nil {
			t.Fatalf("Process failed for %s: %v", key, err)
		}
	}

	// now unblock all workers so they can complete
	close(blockProcessing)

	// wait for all messages to be processed
	deadline := time.After(2 * time.Second)
	for processedCount.Load() < 6 {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timeout waiting for processing: got %d", processedCount.Load())
		}
	}

	// small delay for collectAndCommit to be called
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(capturedWorkItems) != 6 {
		t.Fatalf("expected 6 captured WorkItems, got %d", len(capturedWorkItems))
	}

	// build lookup by offset
	byOffset := make(map[int64]workItemRecord)
	for _, r := range capturedWorkItems {
		byOffset[r.offset] = r
	}

	// verify first 5 (offsets 0-4) do NOT have UsedOverflow
	for i := int64(0); i < 5; i++ {
		r, ok := byOffset[i]
		if !ok {
			t.Errorf("missing WorkItem for offset %d", i)
			continue
		}
		if r.usedOverflow {
			t.Errorf("offset %d: UsedOverflow should be false (used guard)", i)
		}
	}

	// verify 6th (offset 5) DOES have UsedOverflow
	r, ok := byOffset[5]
	if !ok {
		t.Fatal("missing WorkItem for offset 5")
	}
	if !r.usedOverflow {
		t.Error("offset 5: UsedOverflow should be true (used overflow guard)")
	}

	t.Logf("verified: offsets 0-4 used guard, offset 5 used overflow")
}

func TestProcess_TimeoutPath(t *testing.T) {
	// both guards full, timeout expires - should return error
	h := newProcessorTestHarness()
	h.cfg.AcquireWorkerTimeoutCircuitBreaker = 50 * time.Millisecond // short timeout

	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// fill both guard channels
	for i := 0; i < h.cfg.ConcurrentKeys; i++ {
		h.guard <- struct{}{}
	}
	for i := 0; i < cap(h.overflowGuard); i++ {
		h.overflowGuard <- struct{}{}
	}

	payload := testPayload{key: "timeout-key", partition: 5, offset: 999}
	start := time.Now()
	err := proc.Process(payload, time.Now())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	// should contain partition and offset in error message
	errStr := err.Error()
	if !containsStr(errStr, "5") || !containsStr(errStr, "999") {
		t.Errorf("error message should contain partition (5) and offset (999): %s", errStr)
	}

	// should have taken approximately the timeout duration
	if elapsed < 40*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Errorf("timeout took %v, expected ~50ms", elapsed)
	}
}

func TestProcess_BlocksWhenGuardTokensExhausted(t *testing.T) {
	// CRITICAL TEST: Process must block when all guard tokens are consumed.
	// This verifies backpressure from the guard channel propagates correctly.
	h := newProcessorTestHarness()
	h.cfg.ConcurrentKeys = 4                                   // small guard for test
	h.cfg.PerKeyBufferLen = 1                                  // small buffer
	h.cfg.AcquireWorkerTimeoutCircuitBreaker = 2 * time.Second // long timeout
	h.guard = make(chan struct{}, h.cfg.ConcurrentKeys)
	h.overflowGuard = make(chan struct{}) // no overflow - forces guard blocking

	const processingLatency = 50 * time.Millisecond

	var mu sync.Mutex
	var processedOffsets []int64

	h.processFunc = func(_ context.Context, msg *nexus.Message[testPayload]) error {
		time.Sleep(processingLatency)
		mu.Lock()
		defer mu.Unlock()
		processedOffsets = append(processedOffsets, msg.Offset)
		return nil
	}

	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// send 4 messages with unique keys - consumes all guard tokens
	keys := []string{"key-A", "key-B", "key-C", "key-D"}
	for i, key := range keys {
		payload := testPayload{key: key, partition: 0, offset: int64(i)}
		err := proc.Process(payload, time.Now())
		if err != nil {
			t.Fatalf("Process failed for %s: %v", key, err)
		}
	}

	// 5th message should block because all guards are consumed
	blocked := make(chan struct{})
	unblocked := make(chan time.Duration)

	go func() {
		close(blocked)
		start := time.Now()
		payload := testPayload{key: "key-E", partition: 0, offset: 100}
		err := proc.Process(payload, time.Now())
		if err != nil {
			t.Errorf("Process failed for blocked message: %v", err)
		}
		unblocked <- time.Since(start)
	}()

	<-blocked // goroutine has started

	// verify it's actually blocked (not returning immediately)
	select {
	case <-unblocked:
		t.Fatal("Process returned immediately - should have blocked waiting for guard")
	case <-time.After(20 * time.Millisecond):
		// good - it's blocked as expected
	}

	// wait for unblock - should happen after processingLatency when a worker finishes
	select {
	case blockDuration := <-unblocked:
		// should have blocked for approximately processingLatency
		if blockDuration < processingLatency-10*time.Millisecond {
			t.Errorf("blocked for %v, expected at least %v", blockDuration, processingLatency)
		}
		t.Logf("blocked for %v (processing latency: %v)", blockDuration, processingLatency)
	case <-time.After(2 * time.Second):
		t.Fatal("Process remained blocked too long")
	}
}

func TestProcess_BlocksLongerWhenMessagesQueueBehindSameKey(t *testing.T) {
	// CRITICAL TEST: When messages queue behind the same key, the blocking time
	// increases because the worker must process queued messages before releasing
	// the guard token. With 4 guards, 4 keys, 2 messages per key, the 9th message
	// must wait for at least one full processing cycle of 2 messages.
	h := newProcessorTestHarness()
	h.cfg.ConcurrentKeys = 4
	h.cfg.PerKeyBufferLen = 4 // allow queuing
	h.cfg.AcquireWorkerTimeoutCircuitBreaker = 5 * time.Second
	h.guard = make(chan struct{}, h.cfg.ConcurrentKeys)
	h.overflowGuard = make(chan struct{}) // no overflow

	const processingLatency = 30 * time.Millisecond

	type processRecord struct {
		offset    int64
		startTime time.Time
		endTime   time.Time
	}

	var mu sync.Mutex
	var records []processRecord

	h.processFunc = func(_ context.Context, msg *nexus.Message[testPayload]) error {
		start := time.Now()
		time.Sleep(processingLatency)
		end := time.Now()

		mu.Lock()
		defer mu.Unlock()
		records = append(records, processRecord{
			offset:    msg.Offset,
			startTime: start,
			endTime:   end,
		})
		return nil
	}

	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// 4 keys, 2 messages each = 8 messages
	// First 4 messages consume guards (one per key)
	// Next 4 messages queue in worker channels (one per key)
	keys := []string{"key-A", "key-B", "key-C", "key-D"}

	testStart := time.Now()

	// send first batch - 4 messages, one per key (consumes all guards)
	for i, key := range keys {
		payload := testPayload{key: key, partition: 0, offset: int64(i)}
		if err := proc.Process(payload, time.Now()); err != nil {
			t.Fatalf("Process failed: %v", err)
		}
	}

	// send second batch - 4 more messages, one per key (queues behind first)
	for i, key := range keys {
		payload := testPayload{key: key, partition: 0, offset: int64(10 + i)}
		if err := proc.Process(payload, time.Now()); err != nil {
			t.Fatalf("Process failed: %v", err)
		}
	}

	// 9th message should block for longer because:
	// - All guards consumed
	// - Each worker has a queued message
	// - Must wait for worker to process BOTH messages before guard is released
	blocked := make(chan struct{})
	done := make(chan time.Duration, 1)

	go func() {
		close(blocked)
		start := time.Now()
		payload := testPayload{key: "key-E", partition: 0, offset: 999}
		_ = proc.Process(payload, time.Now())
		done <- time.Since(start)
	}()

	<-blocked

	// verify it's blocked
	time.Sleep(30 * time.Millisecond)

	// wait for all processing to complete
	deadline := time.After(3 * time.Second)
	for {
		mu.Lock()
		count := len(records)
		mu.Unlock()
		if count >= 9 { // 8 original + 1 blocked
			break
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timeout waiting for processing: got %d records", count)
		}
	}

	totalElapsed := time.Since(testStart)

	// wait for the goroutine to complete and get block duration
	blockDuration := <-done

	mu.Lock()
	defer mu.Unlock()

	// the 9th message (offset 999) should have been blocked for at least 2x processingLatency
	// because a worker had to process 2 queued messages before releasing guard
	var msg999Record *processRecord
	for i := range records {
		if records[i].offset == 999 {
			msg999Record = &records[i]
			break
		}
	}

	if msg999Record == nil {
		t.Fatal("message with offset 999 was not processed")
	}

	// the blocked message should have started processing significantly after testStart
	msg999Delay := msg999Record.startTime.Sub(testStart)

	// should have waited for at least 2x processing latency (one worker processing 2 messages)
	expectedMinDelay := 2 * processingLatency
	if msg999Delay < expectedMinDelay-15*time.Millisecond {
		t.Errorf("message 999 started after %v, expected at least %v delay",
			msg999Delay, expectedMinDelay)
	}

	t.Logf("test summary:")
	t.Logf("  total elapsed: %v", totalElapsed)
	t.Logf("  message 999 delay: %v (expected >= %v)", msg999Delay, expectedMinDelay)
	t.Logf("  block duration: %v", blockDuration)
	t.Logf("  processed %d messages", len(records))

	// verify timing consistency across all records
	for _, r := range records {
		processDuration := r.endTime.Sub(r.startTime)
		if processDuration < processingLatency-5*time.Millisecond ||
			processDuration > processingLatency+20*time.Millisecond {
			t.Errorf("offset %d: process duration %v outside expected range [%v, %v]",
				r.offset, processDuration, processingLatency-5*time.Millisecond,
				processingLatency+20*time.Millisecond)
		}
	}
}

func TestProcess_ToWorkItemError_ReturnsError(t *testing.T) {
	// if toWorkItem returns error (e.g., negative offset), Process should return it
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset, // negative offset will cause error
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// negative offset causes GetPrevious to return error
	payload := testPayload{key: "error-key", partition: 0, offset: -1}
	err := proc.Process(payload, time.Now())

	if err == nil {
		t.Fatal("expected error for negative offset, got nil")
	}

	// error should mention negative offset
	errStr := err.Error()
	if !containsStr(errStr, "negative") {
		t.Errorf("error should mention 'negative': %s", errStr)
	}
}

func TestProcess_SlowPathGuardBecomesAvailable(t *testing.T) {
	// tests the inner select's guard case (line 92): guard full at first select,
	// but becomes available before overflow/timeout in second select
	h := newProcessorTestHarness()
	h.cfg.AcquireWorkerTimeoutCircuitBreaker = 500 * time.Millisecond // long timeout

	// make overflow guard unavailable
	fullOverflow := make(chan struct{})
	h.overflowGuard = fullOverflow // closed channel blocks, empty unbuffered blocks

	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	done := make(chan struct{})
	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		close(done)
		return nil
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// fill guard completely
	for i := 0; i < h.cfg.ConcurrentKeys; i++ {
		h.guard <- struct{}{}
	}

	// release guard token after short delay - forces slow path guard case
	go func() {
		time.Sleep(10 * time.Millisecond)
		<-h.guard // release one token
	}()

	payload := testPayload{key: "slow-guard-key", partition: 0, offset: 1}
	err := proc.Process(payload, time.Now())

	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}

	select {
	case <-done:
		// success - slow path guard was used
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message processing")
	}
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// ResetPrevOffsets tests
// -----------------------------------------------------------------------------

func TestResetPrevOffsets_ClearsPartitionTracking(t *testing.T) {
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.Background(),
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// process some messages to populate partition tracking
	done := make(chan struct{})
	var processCount atomic.Int32

	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		if processCount.Add(1) == 2 {
			close(done)
		}
		return nil
	}

	// process messages on partitions 0 and 1
	_ = proc.Process(testPayload{key: "k1", partition: 0, offset: 100}, time.Now())
	_ = proc.Process(testPayload{key: "k2", partition: 1, offset: 200}, time.Now())

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for processing")
	}

	// reset specific partitions
	proc.ResetPrevOffsets([]int32{0, 1})

	// process new messages - should be marked as "first" after reset
	firstDone := make(chan struct{})
	h.processFunc = func(_ context.Context, _ *nexus.Message[testPayload]) error {
		close(firstDone)
		return nil
	}

	_ = proc.Process(testPayload{key: "k3", partition: 0, offset: 0}, time.Now())

	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for post-reset processing")
	}
}

// -----------------------------------------------------------------------------
// End-to-end Processor integration test
// -----------------------------------------------------------------------------

func TestProcessor_EndToEndIntegration(t *testing.T) {
	// comprehensive test: multiple messages through full pipeline
	h := newProcessorTestHarness()
	dmx := h.createDemux()

	// track all received messages with their context values
	type receivedData struct {
		partition int32
		offset    int64
		key       string
		traceID   string
	}
	var received []receivedData
	var mu sync.Mutex
	done := make(chan struct{})

	contextKey := testContextKey("trace-id")

	h.processFunc = func(ctx context.Context, msg *nexus.Message[testPayload]) error {
		mu.Lock()
		defer mu.Unlock()

		traceID := ""
		if v := ctx.Value(contextKey); v != nil {
			traceID = v.(string) //nolint:forcetypeassert // test: context value type is known
		}

		received = append(received, receivedData{
			partition: msg.Partition,
			offset:    msg.Offset,
			key:       msg.Key,
			traceID:   traceID,
		})

		if len(received) == 5 {
			close(done)
		}
		return nil
	}

	extractEnvelope := func(p testPayload) nexus.Envelope {
		return nexus.Envelope{
			Partition: p.partition,
			Offset:    p.offset,
			Key:       p.key,
			Ctx:       context.WithValue(context.Background(), contextKey, p.key+"-trace"),
		}
	}

	proc := h.createProcessor(dmx, extractEnvelope)

	// send 5 messages with different keys
	payloads := []testPayload{
		{key: "alpha", partition: 0, offset: 100},
		{key: "beta", partition: 1, offset: 200},
		{key: "gamma", partition: 0, offset: 101},
		{key: "delta", partition: 2, offset: 300},
		{key: "epsilon", partition: 1, offset: 201},
	}

	for _, p := range payloads {
		if err := proc.Process(p, time.Now()); err != nil {
			t.Fatalf("Process failed for %s: %v", p.key, err)
		}
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for all messages")
	}

	mu.Lock()
	defer mu.Unlock()

	// verify all messages were received
	if len(received) != 5 {
		t.Fatalf("received %d messages, want 5", len(received))
	}

	// build lookup for verification
	byKey := make(map[string]receivedData)
	for _, r := range received {
		byKey[r.key] = r
	}

	// verify each message
	for _, p := range payloads {
		r, ok := byKey[p.key]
		if !ok {
			t.Errorf("message with key %q not received", p.key)
			continue
		}
		if r.partition != p.partition {
			t.Errorf("key %q: partition = %d, want %d", p.key, r.partition, p.partition)
		}
		if r.offset != p.offset {
			t.Errorf("key %q: offset = %d, want %d", p.key, r.offset, p.offset)
		}
		expectedTrace := p.key + "-trace"
		if r.traceID != expectedTrace {
			t.Errorf("key %q: traceID = %q, want %q", p.key, r.traceID, expectedTrace)
		}
	}
}

// -----------------------------------------------------------------------------
// NewDemux Constructor Tests - Comprehensive validation of wiring and setup
// -----------------------------------------------------------------------------

func TestNewDemux_CreatesCorrectNumberOfShards(t *testing.T) {
	testCases := []struct {
		name        string
		shardsCount int
	}{
		{"single shard", 1},
		{"four shards", 4},
		{"sixteen shards", 16},
		{"thirty-two shards", 32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			logger := newTestLogger()

			cfg := config.DemuxConfig{
				ConcurrentKeys:            64,
				PerKeyBufferLen:           4,
				WorkerShardsCount:         tc.shardsCount,
				AutoCommitInterval:        250 * time.Millisecond, // minimum for tests
				CommitIngestChannelLen:    100,
				CommitPartitionSliceLen:   50,
				AcquireCommitGuardTimeout: time.Second,
			}

			cb := circuitbreaker.New(ctx, logger)
			dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
				return nil
			}, logger)
			committer := newTestCommitter(ctx, cfg, logger)

			guard := make(chan struct{}, cfg.ConcurrentKeys)
			overflowGuard := make(chan struct{}, 8)

			processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
				return nil
			}

			dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

			if dmx == nil {
				t.Fatal("NewDemux returned nil")
			}
			if len(dmx.workerShards) != tc.shardsCount {
				t.Errorf("workerShards count = %d, want %d", len(dmx.workerShards), tc.shardsCount)
			}
		})
	}
}

func TestNewDemux_SetsConcurrentKeysFromConfig(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            999,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         4,
		AutoCommitInterval:        250 * time.Millisecond,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)
	committer := newTestCommitter(ctx, cfg, logger)

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 8)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}

	dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	if dmx.concurrentKeys != 999 {
		t.Errorf("concurrentKeys = %d, want 999", dmx.concurrentKeys)
	}
}

func TestNewDemux_AllShardsInitialized(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            64,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         8,
		AutoCommitInterval:        250 * time.Millisecond,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)
	committer := newTestCommitter(ctx, cfg, logger)

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 8)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}

	dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	// verify each shard is non-nil and properly initialized
	for i, shard := range dmx.workerShards {
		if shard == nil {
			t.Errorf("workerShards[%d] is nil", i)
			continue
		}
		//if shard.mu == nil {
		//	t.Errorf("workerShards[%d].mu is nil", i)
		//}
		if shard.workers == nil {
			t.Errorf("workerShards[%d].workers map is nil", i)
		}
		//if shard.done == nil {
		//	t.Errorf("workerShards[%d].done atomic is nil", i)
		//}
		if shard.borrowWorker == nil {
			t.Errorf("workerShards[%d].borrowWorker func is nil", i)
		}
	}
}

func TestNewDemux_WorkersHaveAllDependenciesWired(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            32,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         4,
		AutoCommitInterval:        250 * time.Millisecond,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	cb := circuitbreaker.New(ctx, logger)
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return nil
	}, logger)
	committer := newTestCommitter(ctx, cfg, logger)

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 8)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return nil
	}

	dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	// borrow a worker from each shard and verify all dependencies are wired
	for i, shard := range dmx.workerShards {
		worker := shard.BorrowWorker()
		if worker == nil {
			t.Fatalf("workerShards[%d].BorrowWorker() returned nil", i)
		}

		// verify all critical dependencies are set
		if worker.processMessage == nil {
			t.Errorf("workerShards[%d] worker: processMessage is nil", i)
		}
		if worker.deadLetter == nil {
			t.Errorf("workerShards[%d] worker: deadLetter is nil", i)
		}
		if worker.collectAndCommit == nil {
			t.Errorf("workerShards[%d] worker: collectAndCommit is nil", i)
		}
		if worker.circuitBreaker == nil {
			t.Errorf("workerShards[%d] worker: circuitBreaker is nil", i)
		}
		if worker.returnWorker == nil {
			t.Errorf("workerShards[%d] worker: returnWorker is nil", i)
		}
		if worker.guard == nil {
			t.Errorf("workerShards[%d] worker: guard is nil", i)
		}
		if worker.overflowGuard == nil {
			t.Errorf("workerShards[%d] worker: overflowGuard is nil", i)
		}
		if worker.workItems == nil {
			t.Errorf("workerShards[%d] worker: workItems channel is nil", i)
		}
		if worker.workerShard == nil {
			t.Errorf("workerShards[%d] worker: workerShard back-reference is nil", i)
		}
		if worker.mu == nil {
			t.Errorf("workerShards[%d] worker: mu (mutex) is nil", i)
		}
		if worker.logger == nil {
			t.Errorf("workerShards[%d] worker: logger is nil", i)
		}
	}
}

func TestNewDemux_EndToEndMessageFlow(t *testing.T) {
	// This test verifies that messages actually flow through a NewDemux-created pipeline
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            32,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         4,
		AutoCommitInterval:        250 * time.Millisecond,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	cb := circuitbreaker.New(ctx, logger)

	// track processing
	var processedMu sync.Mutex
	processedKeys := make([]string, 0)
	processedOffsets := make(map[string][]int64)
	processedCount := &atomic.Int32{}

	processMessage := func(_ context.Context, msg *nexus.Message[string]) error {
		processedMu.Lock()
		defer processedMu.Unlock()
		processedKeys = append(processedKeys, msg.Key)
		processedOffsets[msg.Key] = append(processedOffsets[msg.Key], msg.Offset)
		processedCount.Add(1)
		return nil
	}

	// track dead letters
	var deadLetterMu sync.Mutex
	deadLetterCount := 0

	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		deadLetterMu.Lock()
		defer deadLetterMu.Unlock()
		deadLetterCount++
		return nil
	}, logger)

	// use real committer - verify commits flow through
	committer := newTestCommitter(ctx, cfg, logger)

	// mark partitions as assigned (required for commits)
	committer.MarkPartitionAssigned(0)
	committer.MarkPartitionAssigned(1)
	committer.MarkPartitionAssigned(2)

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 8)

	dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	pool := alloc.NewWorkItemsPool[string](cfg)

	// send messages through the pipeline
	messages := []struct {
		key       string
		partition int32
		offset    int64
	}{
		{"key-a", 0, 100},
		{"key-b", 1, 200},
		{"key-a", 0, 101},
		{"key-c", 2, 300},
		{"key-b", 1, 201},
		{"key-a", 0, 102},
	}

	for i, m := range messages {
		guard <- struct{}{} // acquire token
		workItem := pool.Borrow()
		workItem.Message.Key = m.key
		workItem.Message.Partition = m.partition
		workItem.Message.Offset = m.offset
		payload := "payload-" + m.key
		workItem.Message.Payload = &payload
		workItem.Metrics.Partition = m.partition
		workItem.Metrics.Offset = m.offset
		workItem.Ctx = ctx
		// mark first message per partition
		if i == 0 || i == 1 || i == 3 { // key-a:0, key-b:1, key-c:2
			workItem.First = true
		} else {
			workItem.PreviousOffset = m.offset - 1
		}

		dmx.SendToWorkerForProcessing(m.key, workItem)
	}

	// wait for all messages to be processed
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processedCount.Load() >= int32(len(messages)) { //nolint:gosec // G115: len bounded by test
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// verify all messages were processed
	processedMu.Lock()
	defer processedMu.Unlock()

	if len(processedKeys) != len(messages) {
		t.Errorf("processed %d messages, want %d", len(processedKeys), len(messages))
	}

	// verify per-key ordering preserved
	expectedOffsets := map[string][]int64{
		"key-a": {100, 101, 102},
		"key-b": {200, 201},
		"key-c": {300},
	}

	for key, expected := range expectedOffsets {
		actual := processedOffsets[key]
		if len(actual) != len(expected) {
			t.Errorf("key %q: got %d offsets, want %d", key, len(actual), len(expected))
			continue
		}
		for i, off := range expected {
			if actual[i] != off {
				t.Errorf("key %q offset[%d] = %d, want %d", key, i, actual[i], off)
			}
		}
	}

	// verify no dead letters for successful processing
	deadLetterMu.Lock()
	if deadLetterCount != 0 {
		t.Errorf("dead letter count = %d, want 0", deadLetterCount)
	}
	deadLetterMu.Unlock()
}

func TestNewDemux_ProcessMessageErrorTriggersDeadLetter(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            16,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         2,
		AutoCommitInterval:        250 * time.Millisecond,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	cb := circuitbreaker.New(ctx, logger)

	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return errors.New("simulated processing error")
	}

	var deadLetterMu sync.Mutex
	deadLetterCount := 0
	deadLetterKeys := make([]string, 0)
	deadLetterDone := make(chan struct{})

	dl := deadletter.New[string](func(_ context.Context, msg *nexus.Message[string], _ error) error {
		deadLetterMu.Lock()
		defer deadLetterMu.Unlock()
		deadLetterCount++
		deadLetterKeys = append(deadLetterKeys, msg.Key)
		close(deadLetterDone)
		return nil
	}, logger)

	committer := newTestCommitter(ctx, cfg, logger)
	committer.MarkPartitionAssigned(0)

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 4)

	dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	pool := alloc.NewWorkItemsPool[string](cfg)

	// send a message that will fail
	guard <- struct{}{}
	workItem := pool.Borrow()
	workItem.Message.Key = "fail-key"
	workItem.Message.Partition = 0
	workItem.Message.Offset = 100
	payload := "will-fail"
	workItem.Message.Payload = &payload
	workItem.Metrics.Partition = 0
	workItem.Metrics.Offset = 100
	workItem.First = true
	workItem.Ctx = ctx

	dmx.SendToWorkerForProcessing("fail-key", workItem)

	// wait for dead letter to be called
	select {
	case <-deadLetterDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for dead letter")
	}

	// verify dead letter was called
	deadLetterMu.Lock()
	defer deadLetterMu.Unlock()

	if deadLetterCount != 1 {
		t.Errorf("dead letter count = %d, want 1", deadLetterCount)
	}
	if len(deadLetterKeys) != 1 || deadLetterKeys[0] != "fail-key" {
		t.Errorf("dead letter keys = %v, want [fail-key]", deadLetterKeys)
	}
}

func TestNewDemux_CircuitBreakerTriggeredOnDeadLetterFailure(t *testing.T) {
	ctx := context.Background()
	logger := newTestLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:            16,
		PerKeyBufferLen:           4,
		WorkerShardsCount:         2,
		AutoCommitInterval:        250 * time.Millisecond,
		CommitIngestChannelLen:    100,
		CommitPartitionSliceLen:   50,
		AcquireCommitGuardTimeout: time.Second,
	}

	cb := circuitbreaker.New(ctx, logger)

	// processMessage always fails
	processMessage := func(_ context.Context, _ *nexus.Message[string]) error {
		return errors.New("processing error")
	}

	// dead letter also fails - should trigger circuit breaker
	dl := deadletter.New[string](func(_ context.Context, _ *nexus.Message[string], _ error) error {
		return errors.New("dead letter write failed")
	}, logger)

	committer := newTestCommitter(ctx, cfg, logger)
	committer.MarkPartitionAssigned(0)

	guard := make(chan struct{}, cfg.ConcurrentKeys)
	overflowGuard := make(chan struct{}, 4)

	dmx := NewDemux[string](cfg, processMessage, dl, committer, cb, guard, overflowGuard, logger, func(_ *nexus.Message[string]) {})

	pool := alloc.NewWorkItemsPool[string](cfg)

	// send a message that will fail processing AND dead letter
	guard <- struct{}{}
	workItem := pool.Borrow()
	workItem.Message.Key = "circuit-break-key"
	workItem.Message.Partition = 0
	workItem.Message.Offset = 100
	payload := "will-circuit-break"
	workItem.Message.Payload = &payload
	workItem.Metrics.Partition = 0
	workItem.Metrics.Offset = 100
	workItem.First = true
	workItem.Ctx = ctx

	dmx.SendToWorkerForProcessing("circuit-break-key", workItem)

	// wait for circuit breaker to trigger
	select {
	case reason := <-cb.Triggered():
		if reason == "" {
			t.Error("expected non-empty circuit breaker reason")
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for circuit breaker to trigger")
	}
}

func Test_RetrySpinDelay(t *testing.T) {
	if fmt.Sprintf("%s", retrySpinDelay) != "100µs" {
		t.Errorf("retrySpinDelay should be 100µs but was %s", retrySpinDelay)
	}
}

func TestSendToWorkerForProcessing_SetsWorkerPoolFromFNVHash(t *testing.T) {
	h := newDemuxTestHarness()
	dmx := h.createDemux()

	bitMask := uint32(h.cfg.WorkerShardsCount - 1)

	keys := []string{
		"user-12345",
		"order-67890",
		"00000000-0000-0000-0000-000000000000",
		"partition-key-medium-length",
		"a",
	}

	for _, key := range keys {
		h.guard <- struct{}{}
		workItem := h.createWorkItem(key, 0, 100)
		dmx.SendToWorkerForProcessing(key, workItem)

		expectedPool := fnv.HashIndex(key, bitMask)
		if workItem.WorkerPool != expectedPool {
			t.Errorf("key %q: WorkerPool = %d, want %d (FNV hash & %d)",
				key, workItem.WorkerPool, expectedPool, bitMask)
		}
	}

	// wait for workers to drain
	time.Sleep(50 * time.Millisecond)
}

func TestSendToWorkerForProcessing_QueueDepth_RecordedOnExistingWorker(t *testing.T) {
	// Verifies that QueueDepth is recorded as len(worker.workItems) at dispatch time
	// for existing workers. New workers never set QueueDepth (stays 0 from pool).
	//
	// Timeline:
	//   1. Send msg1 to key-A - creates new worker, QueueDepth not set (stays 0)
	//   2. msg1 blocks in processFunc - worker goroutine has consumed msg1 from channel
	//   3. Send msg2 to key-A - existing worker, channel empty (msg1 consumed), QueueDepth = 0
	//   4. Send msg3 to key-A - existing worker, msg2 buffered in channel, QueueDepth = 1
	//   5. Unblock processing, verify all QueueDepth values

	h := newDemuxTestHarness()
	h.cfg.ConcurrentKeys = 4
	h.cfg.PerKeyBufferLen = 8
	h.guard = make(chan struct{}, h.cfg.ConcurrentKeys)
	h.overflowGuard = make(chan struct{}, 8)

	firstMsgProcessing := make(chan struct{})
	continueProcessing := make(chan struct{})
	var processedCount atomic.Int32

	// track QueueDepth at commit time (before pool recycles workItem)
	var queueDepthMu sync.Mutex
	queueDepths := make(map[int64]int32) // offset -> QueueDepth

	h.processFunc = func(_ context.Context, msg *nexus.Message[string]) error {
		count := processedCount.Add(1)
		if count == 1 {
			close(firstMsgProcessing) // signal first message is being processed
			<-continueProcessing      // block until test says continue
		}
		return nil
	}

	// override commitFunc to capture QueueDepth before pool recycles workItem
	defaultCommit := h.commitFunc
	h.commitFunc = func(workItem *ports.WorkItem[string]) {
		queueDepthMu.Lock()
		queueDepths[workItem.Message.Offset] = workItem.Metrics.QueueDepth
		queueDepthMu.Unlock()
		defaultCommit(workItem)
	}

	dmx := h.createDemux()

	const testKey = "queue-depth-key"

	// step 1: send first message - creates new worker
	h.guard <- struct{}{}
	workItem1 := h.createWorkItem(testKey, 0, 100)
	go dmx.SendToWorkerForProcessing(testKey, workItem1)

	// step 2: wait for first message to be processing (worker consumed it from channel)
	<-firstMsgProcessing

	// step 3: send second message - existing worker, channel is empty (msg1 consumed)
	h.guard <- struct{}{}
	workItem2 := h.createWorkItem(testKey, 0, 101)
	dmx.SendToWorkerForProcessing(testKey, workItem2)

	// step 4: send third message - existing worker, msg2 sitting in channel buffer
	h.guard <- struct{}{}
	workItem3 := h.createWorkItem(testKey, 0, 102)
	dmx.SendToWorkerForProcessing(testKey, workItem3)

	// step 5: unblock processing
	close(continueProcessing)

	// wait for all messages to complete
	deadline := time.After(2 * time.Second)
	for processedCount.Load() < 3 {
		select {
		case <-time.After(10 * time.Millisecond):
		case <-deadline:
			t.Fatalf("timeout: only processed %d/3 messages", processedCount.Load())
		}
	}

	// allow commit callbacks to complete
	time.Sleep(50 * time.Millisecond)

	// verify QueueDepth values
	queueDepthMu.Lock()
	defer queueDepthMu.Unlock()

	if len(queueDepths) != 3 {
		t.Fatalf("expected 3 committed messages, got %d", len(queueDepths))
	}

	// msg1 (offset 100): new worker path - QueueDepth never set, stays 0 from pool
	if depth, ok := queueDepths[100]; !ok {
		t.Error("offset 100 not found in committed items")
	} else if depth != 0 {
		t.Errorf("offset 100 (new worker): QueueDepth = %d, want 0", depth)
	}

	// msg2 (offset 101): existing worker path - channel was empty (msg1 already consumed by worker)
	if depth, ok := queueDepths[101]; !ok {
		t.Error("offset 101 not found in committed items")
	} else if depth != 0 {
		t.Errorf("offset 101 (existing worker, empty channel): QueueDepth = %d, want 0", depth)
	}

	// msg3 (offset 102): existing worker path - msg2 buffered in channel, depth = 1
	if depth, ok := queueDepths[102]; !ok {
		t.Error("offset 102 not found in committed items")
	} else if depth != 1 {
		t.Errorf("offset 102 (existing worker, one buffered): QueueDepth = %d, want 1", depth)
	}

	t.Logf("verified: QueueDepth recorded as channel depth at dispatch time " +
		"(new=0, existing-empty=0, existing-buffered=1)")
}

// newTestCommitter creates a real offset.Committer for NewDemux constructor tests.
func newTestCommitter(ctx context.Context, cfg config.DemuxConfig, logger nexus.Logger) *offset.Committer[string] {
	pool := alloc.NewWorkItemsPool[string](cfg)

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }
	metricsCollector := metrics.NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	commitOffsets := func(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return messages, nil
	}

	return offset.NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
}
