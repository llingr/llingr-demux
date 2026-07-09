// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_DrainCommitter_EmptyChannel verifies that DrainCommitter returns immediately
// when the commitsIn channel is already empty and commits any pending Ready items.
func Test_DrainCommitter_EmptyChannel(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	var commitCalled bool
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCalled = true
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// add a Ready item to verify commit is called
	workItem := pool.Borrow()
	workItem.Message.Partition = 0
	workItem.Message.Offset = 100
	tracker := &OffsetsTracker[string]{
		CommittedPlusOne: 100,
		Ready:            workItem,
	}
	committer.offsetsByPartition.PartitionMap[0] = tracker
	committer.MarkPartitionAssigned(0)

	// channel is empty - DrainCommitter should return immediately
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	start := time.Now()
	err := committer.DrainCommitter(timer)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("DrainCommitter returned error: %v", err)
	}
	if !commitCalled {
		t.Error("expected CommitOffsets to be called")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("DrainCommitter took too long for empty channel: %v", elapsed)
	}
}

// Test_DrainCommitter_WaitsForDrained verifies that DrainCommitter waits for the
// drained signal when commitsIn has items, then commits.
func Test_DrainCommitter_WaitsForDrained(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	var metricsCount atomic.Int32
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error {
			metricsCount.Add(1)
			return nil
		}, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	var commitCalled atomic.Bool
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCalled.Store(true)
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0)

	// send some messages that will be processed
	for i := int64(0); i < 10; i++ {
		workItem := pool.Borrow()
		workItem.Message.Partition = 0
		workItem.Message.Offset = i
		workItem.Metrics.Partition = 0
		workItem.Metrics.Offset = i
		if i == 0 {
			workItem.First = true
		} else {
			workItem.PreviousOffset = i - 1
		}
		committer.CollectAndCommit(workItem)
	}

	// wait for processing to complete
	deadline := time.Now().Add(2 * time.Second)
	for metricsCount.Load() < 10 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for messages to process: got %d/10", metricsCount.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	// now drain - should succeed quickly since channel is empty
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	err := committer.DrainCommitter(timer)

	if err != nil {
		t.Errorf("DrainCommitter returned error: %v", err)
	}
	if !commitCalled.Load() {
		t.Error("expected CommitOffsets to be called")
	}
}

// Test_DrainCommitter_Timeout verifies that DrainCommitter returns a timeout error
// when the channel doesn't drain within the timeout period.
func Test_DrainCommitter_Timeout(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
		CommitIngestChannelLen:    100_000, // large buffer to sustain fill rate
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// continuously fill channel
	stop := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			wi := pool.Borrow()
			wi.Message.Partition = 0
			wi.Message.Offset = int64(i)
			select {
			case committer.commitsIn <- wi:
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	// wait until channel has 50K messages before starting drain
	for len(committer.commitsIn) < 50_000 {
		runtime.Gosched()
	}

	// 10ms timeout - channel stays full, drain cannot complete
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	err := committer.DrainCommitter(timer)

	if err == nil {
		t.Error("expected timeout error, got nil")
	}
	if err != nil && err.Error() != "timeout" {
		t.Errorf("expected 'timeout' error, got: %v", err)
	}
}

// Test_DrainCommitter_TimeoutWithCommitError verifies that DrainCommitter logs the
// commit error when CommitOffsets fails during timeout.
// CommitOffsets only returns an error when it can't acquire the autoCommitGuard,
// so we block that guard to trigger the error path.
func Test_DrainCommitter_TimeoutWithCommitError(t *testing.T) {
	ctx := context.Background()

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond, // minimum allowed
		CommitIngestChannelLen:    100_000,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// block the autoCommitGuard so CommitOffsets will timeout and return an error
	committer.autoCommitGuard <- struct{}{}

	// continuously fill channel so drain can never complete
	stop := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			wi := pool.Borrow()
			wi.Message.Partition = 0
			wi.Message.Offset = int64(i)
			select {
			case committer.commitsIn <- wi:
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	// wait until channel has 50K messages
	for len(committer.commitsIn) < 50_000 {
		runtime.Gosched()
	}

	// 10ms timeout - channel stays full, drain cannot complete
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()

	err := committer.DrainCommitter(timer)

	if err == nil {
		t.Error("expected timeout error, got nil")
	}
	if err != nil && err.Error() != "timeout" {
		t.Errorf("expected 'timeout' error, got: %v", err)
	}

	// verify error was logged (CommitOffsets returns error when guard can't be acquired)
	if !logger.HasErrors() {
		t.Error("expected commit error to be logged")
	}
}

// Test_DrainCommitter_ClearsStaleDrainedSignal verifies that DrainCommitter clears
// any stale signal from the drained channel before waiting.
func Test_DrainCommitter_ClearsStaleDrainedSignal(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// pre-fill drained channel with stale signal
	committer.drained <- struct{}{}

	// channel is empty, should clear stale signal and return quickly
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	start := time.Now()
	err := committer.DrainCommitter(timer)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("DrainCommitter returned error: %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("DrainCommitter took too long (stale signal not cleared?): %v", elapsed)
	}
}

// Test_DrainCommitter_HighVolume verifies DrainCommitter works correctly under
// high message volume with out-of-order processing. This tests that the drain
// mechanism correctly waits for all messages to be ingested before returning.
//
// DrainCommitter guarantees:
// 1. commitsIn channel is empty (all messages ingested into offsetsByPartition)
// 2. CommitOffsets is called
//
// After drain, messages are either:
// - Collected for metrics (advanced through Ready)
// - In Ready state (waiting for next commit)
// - In gap buffer (waiting for earlier messages - but with all messages sent, gaps will fill)
//
// This test injects a synchronous collectMetrics function to verify every partition/offset
// pair flows through the complete pipeline without dropping.
func Test_DrainCommitter_HighVolume(t *testing.T) {
	const partitionCount = 12
	const messagesPerPartition = 4166 // 49992 total messages
	const messageCount = partitionCount * messagesPerPartition

	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 5 * time.Second,
		CommitIngestChannelLen:    messageCount / 2, // ensure backpressure
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// use a no-op metrics collector for NewCommitter (we'll swap it out)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	var commitCount atomic.Int32
	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		commitCount.Add(1)
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)

	// inject synchronous metrics collector that tracks every partition/offset pair
	var metricsMu sync.Mutex
	collected := make(map[int32]map[int64]bool) // partition -> offset -> collected
	for p := int32(0); p < partitionCount; p++ {
		collected[p] = make(map[int64]bool, messagesPerPartition)
	}

	committer.collectMetrics = func(wi *ports.WorkItem[string]) {
		metricsMu.Lock()
		collected[wi.Message.Partition][wi.Message.Offset] = true
		metricsMu.Unlock()
	}

	// mark all partitions as assigned
	for p := int32(0); p < partitionCount; p++ {
		committer.MarkPartitionAssigned(p)
	}

	// create all work items in order per partition
	workItems := make([]*ports.WorkItem[string], 0, messageCount)

	for p := int32(0); p < partitionCount; p++ {
		for i := int64(0); i < int64(messagesPerPartition); i++ {
			workItem := pool.Borrow()
			populateWorkItem(workItem, p, i)
			workItems = append(workItems, workItem)
		}
	}

	// scramble to simulate out-of-order completion
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // G404: deterministic seed for test reproducibility
	rng.Shuffle(len(workItems), func(i, j int) {
		workItems[i], workItems[j] = workItems[j], workItems[i]
	})

	// send all messages concurrently
	var wg sync.WaitGroup
	for _, wi := range workItems {
		wg.Add(1)
		go func(w *ports.WorkItem[string]) {
			defer wg.Done()
			committer.CollectAndCommit(w)
		}(wi)
	}
	wg.Wait()

	// now drain - waits for commitsIn to be empty
	timer := time.NewTimer(time.Minute)
	defer timer.Stop()

	drainStart := time.Now()
	err := committer.DrainCommitter(timer)
	drainDuration := time.Since(drainStart)

	if err != nil {
		t.Errorf("DrainCommitter returned error: %v", err)
	}

	// verify commitsIn is empty after drain
	if len(committer.commitsIn) != 0 {
		t.Errorf("commitsIn should be empty after drain, got %d", len(committer.commitsIn))
	}

	// verify commit was called
	if commitCount.Load() == 0 {
		t.Error("CommitOffsets should have been called")
	}

	// check gap buffer and Ready state (should be empty/minimal after drain)
	committer.mu.Lock()
	var pendingInGapBuffer, pendingInReady int
	for _, tracker := range committer.offsetsByPartition.PartitionMap {
		pendingInGapBuffer += len(tracker.GapBuffer)
		if tracker.Ready != nil {
			pendingInReady++
		}
	}
	committer.mu.Unlock()

	// verify every partition/offset pair was collected (except those still in Ready)
	metricsMu.Lock()
	var totalCollected int
	var missing []string
	for p := int32(0); p < partitionCount; p++ {
		partitionCollected := collected[p]
		totalCollected += len(partitionCollected)

		// check for missing offsets (excluding the last one which may be in Ready)
		for i := int64(0); i < int64(messagesPerPartition)-1; i++ {
			if !partitionCollected[i] {
				if len(missing) < 10 { // limit output
					missing = append(missing, fmt.Sprintf("p%d:o%d", p, i))
				}
			}
		}
	}
	metricsMu.Unlock()

	// expected: all messages collected except up to 12 in Ready (one per partition)
	expectedCollected := messageCount - partitionCount
	t.Logf("DrainCommitter completed in %v, commits=%d, collected=%d/%d, "+
		"gap_buffer=%d, ready=%d",
		drainDuration, commitCount.Load(), totalCollected, messageCount,
		pendingInGapBuffer, pendingInReady)

	if pendingInGapBuffer != 0 {
		t.Errorf("gap buffer should be empty after all messages sent, got %d", pendingInGapBuffer)
	}

	if totalCollected < expectedCollected {
		t.Errorf("expected at least %d collected (all except Ready), got %d; missing: %v",
			expectedCollected, totalCollected, missing)
	}
}

// Test_DrainCommitter_BusyThenEmpty verifies the transition from busy to empty state.
// This tests that DrainCommitter correctly detects when the channel empties after
// initially being busy.
func Test_DrainCommitter_BusyThenEmpty(t *testing.T) {
	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 100 * time.Millisecond,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)

	// block processing to control when items are drained
	blockProcessing := make(chan struct{})
	var metricsCount atomic.Int32
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error {
			metricsCount.Add(1)
			return nil
		}, nexus.SinkContext{}, pool, logger)
	metricsCollector.StartCollectingMetrics()

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0)

	// inject a slow collectMetrics to control processing rate
	originalCollect := committer.collectMetrics
	committer.collectMetrics = func(w *ports.WorkItem[string]) {
		select {
		case <-blockProcessing:
			// unblocked - process normally
		default:
			// block until unblocked
			<-blockProcessing
		}
		originalCollect(w)
	}

	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	// fill channel with items while processing is blocked
	const itemCount = 100
	for i := int64(0); i < itemCount; i++ {
		workItem := pool.Borrow()
		populateWorkItem(workItem, 0, i)
		committer.commitsIn <- workItem
	}

	channelLen := len(committer.commitsIn)
	t.Logf("channel length after filling: %d", channelLen)

	// start drain - it will wait because channel has items
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- committer.DrainCommitter(timer)
	}()

	// verify drain hasn't completed (items in channel, processing blocked)
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-drainDone:
		t.Fatalf("drain completed too early with error: %v", err)
	default:
		// good - drain is waiting
	}

	// unblock processing - close lets all blocked goroutines proceed
	close(blockProcessing)

	// drain should complete now that processing can proceed
	select {
	case err := <-drainDone:
		if err != nil {
			t.Errorf("drain returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain didn't complete after processing unblocked")
	}

	t.Logf("processed %d messages", metricsCount.Load())
}

// Test_DrainCommitter_ChannelDrains verifies DrainCommitter completes once
// a high volume of messages drains through the ingest loop.
func Test_DrainCommitter_ChannelDrains(t *testing.T) {
	const messageCount = 100_000

	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 5 * time.Second,
		CommitIngestChannelLen:    100_000,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	metricsCollector := metrics.NewCollector[string](ctx, cfg,
		func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }, nexus.SinkContext{}, pool, logger)

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, metricsCollector, logger)
	committer.MarkPartitionAssigned(0)

	// fill channel from a goroutine
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for i := int64(0); i < messageCount; i++ {
			workItem := pool.Borrow()
			populateWorkItem(workItem, 0, i)
			committer.commitsIn <- workItem
		}
	}()

	// wait for a sizeable backlog before starting drain; the ingest loop
	// consumes concurrently, so under some schedules the backlog never forms -
	// producer completion bounds the wait
	backlogReady := func() bool {
		select {
		case <-producerDone:
			return true
		default:
			return len(committer.commitsIn) >= 50_000
		}
	}
	for !backlogReady() {
		runtime.Gosched()
	}

	timer := time.NewTimer(time.Minute)
	defer timer.Stop()

	start := time.Now()
	err := committer.DrainCommitter(timer)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("DrainCommitter returned error: %v", err)
	}

	t.Logf("DrainCommitter drained %d messages in %v", messageCount, elapsed)
}

// countingMetricsCollector stubs MetricsPort, counting every message that
// makes it through offset resolution to metrics collection.
type countingMetricsCollector[T any] struct {
	collected atomic.Int64
}

func (c *countingMetricsCollector[T]) Collect(_ *ports.WorkItem[T]) {
	c.collected.Add(1)
}

// Test_DrainCommitter_ScrambledOffsets verifies that drain resolves all messages
// through the gap buffer sort-and-walk when offsets arrive out of order.
// Every message that reaches metrics collection has been fully resolved.
// 10k is enough to prove resolution; the fully-scrambled regime costs
// quadratic quick-scan work, and 100k blew CI's budget under -race.
func Test_DrainCommitter_ScrambledOffsets(t *testing.T) {
	const messageCount = 10_000

	ctx := context.Background()
	logger := mocklogger.NewNoOpLogger()

	cfg := config.DemuxConfig{
		AutoCommitInterval:        250 * time.Millisecond,
		AcquireCommitGuardTimeout: 5 * time.Second,
		CommitIngestChannelLen:    messageCount * 2,
	}
	cfg.SetDemuxConfigDefaults()

	pool := alloc.NewWorkItemsPool[string](cfg)
	countingMetrics := &countingMetricsCollector[string]{}

	commitOffsets := func(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
		return msgs, nil
	}

	committer := NewCommitter[string](ctx, cfg, commitOffsets, countingMetrics, logger)
	committer.MarkPartitionAssigned(0)

	// build scrambled work items: contiguous offsets 0..N-1, shuffled
	items := make([]*ports.WorkItem[string], messageCount)
	for i := range items {
		items[i] = pool.Borrow()
		items[i].Message.Partition = 0
		items[i].Message.Offset = int64(i)
		if i == 0 {
			items[i].First = true
		} else {
			items[i].PreviousOffset = int64(i - 1)
		}
	}
	rand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})

	// push all into commitsIn
	for _, item := range items {
		committer.commitsIn <- item
	}

	t.Logf("pushed %d scrambled messages, channel len: %d", messageCount, len(committer.commitsIn))

	timer := time.NewTimer(time.Minute)
	defer timer.Stop()

	start := time.Now()
	err := committer.DrainCommitter(timer)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("DrainCommitter returned error: %v", err)
	}

	collected := countingMetrics.collected.Load()
	t.Logf("DrainCommitter resolved %d/%d scrambled messages in %v",
		collected, messageCount, elapsed)

	if collected != messageCount {
		t.Errorf("expected %d messages collected, got %d", messageCount, collected)
	}
}
