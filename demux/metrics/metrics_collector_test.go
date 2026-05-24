// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package metrics

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_Collector_Stats_ReturnsAtomicCounters verifies Stats returns a
// snapshot of the three atomic counters
func Test_Collector_Stats_ReturnsAtomicCounters(t *testing.T) {
	cfg := config.DemuxConfig{}
	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	noopSink := func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }
	collector := NewCollector[string](ctx, cfg, noopSink, nexus.SinkContext{}, pool, logger)

	// fresh collector: all zero
	if got := collector.Stats(); got != (Stats{}) {
		t.Errorf("fresh Stats = %+v, want zero value", got)
	}

	// stamp the underlying counters and verify Stats reflects them
	collector.CollectedCount.Store(42)
	collector.DroppedCount.Store(7)
	collector.SendFailedCount.Store(3)

	got := collector.Stats()
	want := Stats{Collected: 42, Dropped: 7, SendFailed: 3}
	if got != want {
		t.Errorf("Stats = %+v, want %+v", got, want)
	}
}

// Test_CollectorHappyPath validates that metrics are
// collected (in order)
func Test_CollectorHappyPath(t *testing.T) {
	cfg := config.DemuxConfig{}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	// Capture metrics in order
	var capturedMetrics []nexus.Metrics
	var mu sync.Mutex

	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		mu.Lock()
		capturedMetrics = append(capturedMetrics, m)
		mu.Unlock()
		return nil
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)

	collector.StartCollectingMetrics()

	// send 500 messages with incrementing offsets, so ordering can be confirmed
	const messageCount = 20000
	partition := int32(5)

	for i := int64(0); i < messageCount; i++ {
		workItem := pool.Borrow()
		workItem.Message.Partition = partition
		workItem.Message.Offset = i
		collector.Collect(workItem)
	}

	// Wait for all messages to be collected
	for i := 0; i < 5000; i++ {
		runtime.Gosched()
		if collector.CollectedCount.Load() == messageCount {
			break
		}
		time.Sleep(50 * time.Microsecond)
	}

	// Verify CollectedCount
	if collected := collector.CollectedCount.Load(); collected != messageCount {
		t.Errorf("expected CollectedCount=%d, got %d", messageCount, collected)
	}

	// Verify no failures
	if failed := collector.SendFailedCount.Load(); failed != 0 {
		t.Errorf("expected SendFailedCount=0, got %d", failed)
	}

	// Verify no drops
	if dropped := collector.DroppedCount.Load(); dropped != 0 {
		t.Errorf("expected DroppedCount=0, got %d", dropped)
	}

	// Verify all metrics captured
	mu.Lock()
	capturedCount := len(capturedMetrics)
	mu.Unlock()

	if capturedCount != messageCount {
		t.Fatalf("expected %d captured metrics, got %d", messageCount, capturedCount)
	}

	// Verify metrics are in order with correct offsets
	mu.Lock()
	for i, m := range capturedMetrics {
		expectedOffset := int64(i)
		if m.Offset != expectedOffset {
			t.Errorf("metric[%d]: expected offset %d, got %d", i, expectedOffset, m.Offset)
		}
		if m.Partition != partition {
			t.Errorf("metric[%d]: expected partition %d, got %d", i, partition, m.Partition)
		}
	}
	mu.Unlock()
}

// Test_CalculateCollectBufferSize confirms
// buffer size is always between 20k and 100k
func Test_CalculateCollectBufferSize(t *testing.T) {
	tests := []struct {
		name            string
		concurrentKeys  int
		perKeyBufferLen int
		expectedSize    int
	}{
		{"zeros", -100, 0, 20000},
		{"zeros", 0, 0, 20000},
		{"minimum", 1, 1, 20000},
		{"at minimum", 400, 10, 20000},
		{"above minimum", 300, 16, 24000},
		{"between min and max", 400, 32, 64000},
		{"below maximum", 1200, 16, 96000},
		{"at maximum", 1250, 16, 100000},
		{"above maximum", 5000, 10, 100000},
		{"above maximum", 2000000000, 1, 100000},
		{"above maximum", 1, 2000000000, 100000},
		{"above maximum", 2000000000, 2000000000, 100000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size := CalculateCollectBufferSize(tt.concurrentKeys, tt.perKeyBufferLen)
			if size != tt.expectedSize {
				t.Errorf("expected buffer size %d, got %d", tt.expectedSize, size)
			}
		})
	}
}

// FuzzCalculateCollectBufferSize verifies that for any inputs, the buffer
// size is always within the valid range of 20000 to 100000
func FuzzCalculateCollectBufferSize(f *testing.F) {
	// Seed with interesting boundary values
	f.Add(0, 0)
	f.Add(1, 1)
	f.Add(-1, -1)
	f.Add(400, 10)
	f.Add(1250, 16)
	f.Add(5000, 10)
	f.Add(-6000, 10)
	f.Add(999, -20000)
	f.Add(2000000000, 2000000000)
	f.Add(1, 2000000000)
	f.Add(2000000000, 1)

	f.Fuzz(func(t *testing.T, concurrentKeys, perKeyBufferLen int) {
		size := CalculateCollectBufferSize(concurrentKeys, perKeyBufferLen)

		// Verify invariant: size must always be between 20k and 100k
		if size < 20000 {
			t.Errorf("buffer size %d below minimum 20000 (keys=%d, bufLen=%d)",
				size, concurrentKeys, perKeyBufferLen)
		}
		if size > 100000 {
			t.Errorf("buffer size %d above maximum 100000 (keys=%d, bufLen=%d)",
				size, concurrentKeys, perKeyBufferLen)
		}
	})
}

// TestCollectorBackpressureDropsMetrics validates that when the metrics channel
// is full, additional metrics are dropped and counted correctly
func TestCollectorBackpressureDropsMetrics(t *testing.T) {
	// Create minimal config to get smallest buffer (20k)
	cfg := config.DemuxConfig{
		ConcurrentKeys:  1,
		PerKeyBufferLen: 1,
	}

	bufferSize := CalculateCollectBufferSize(cfg.ConcurrentKeys, cfg.PerKeyBufferLen)

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	capturedMetrics := &atomic.Int64{}
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		capturedMetrics.Add(1)
		return nil // no-op
	}

	// don't start collector goroutine, so nothing drains
	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)

	// fill buffer completely
	for i := 0; i < bufferSize; i++ {
		workItem := pool.Borrow()
		collector.Collect(workItem)
	}

	// verify buffer is full, DroppedCount still zero
	if dropped := collector.DroppedCount.Load(); dropped != 0 {
		t.Errorf("expected DroppedCount=0 after filling buffer, got %d", dropped)
	}

	// send one more - should drop
	collector.Collect(pool.Borrow())

	if dropped := collector.DroppedCount.Load(); dropped != 1 {
		t.Errorf("expected DroppedCount=1 after overflow, got %d", dropped)
	}

	// Send 20 more, should cause 20 total drops
	for i := 0; i < 20; i++ {
		workItem := pool.Borrow()
		collector.Collect(workItem)
	}

	if dropped := collector.DroppedCount.Load(); dropped != 21 {
		t.Errorf("expected DroppedCount=21 after 21 overflows, got %d", dropped)
	}

	collector.StartCollectingMetrics()

	// Wait for collector to drain the initial buffer
	for capturedMetrics.Load() < int64(bufferSize) {
		runtime.Gosched()
	}

	// Send 25k more messages, pacing to ensure collector keeps up.
	// Wait if pending work exceeds half the buffer to avoid drops.
	const messagesToSend = 25000
	halfBuffer := int64(bufferSize / 2)
	for j := 0; j < messagesToSend; j++ {
		collector.Collect(pool.Borrow())
		for int64(bufferSize+j+1)-capturedMetrics.Load() > halfBuffer {
			runtime.Gosched()
		}
	}

	// Wait for all messages to be processed
	expectedTotal := int64(bufferSize + messagesToSend)
	for capturedMetrics.Load() < expectedTotal {
		runtime.Gosched()
	}

	if dropped := collector.DroppedCount.Load(); dropped != 21 {
		t.Errorf("no more messages should have been dropped since collector started but got %d", dropped)
	}
}

func Test_CollectorSendFailure_IncrementsFailedCounter(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  1,
		PerKeyBufferLen: 1,
	}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	// sink that always returns an error
	errorSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		return errors.New("metrics sink error")
	}

	collector := NewCollector[string](ctx, cfg, errorSink, nexus.SinkContext{}, pool, logger)

	collector.StartCollectingMetrics()

	// Send one message
	collector.Collect(pool.Borrow())

	// Stop drains the async pipeline synchronously, guaranteeing the sink
	// has been called (and SendFailedCount incremented) before we assert
	collector.Stop()

	// confirm SendFailedCount incremented
	if failed := collector.SendFailedCount.Load(); failed != 1 {
		t.Errorf("expected SendFailedCount=1 after sink error, got %d", failed)
	}

	// confirm CollectedCount did NOT increment (error path)
	if collected := collector.CollectedCount.Load(); collected != 0 {
		t.Errorf("expected CollectedCount=0 after sink error, got %d", collected)
	}

	// there should be no dropped metrics
	if dropped := collector.DroppedCount.Load(); dropped != 0 {
		t.Errorf("expected DroppedCount=0 (not a drop, send failed), got %d", dropped)
	}
}

func Test_CollectorSendPanic_IncrementsFailedCounter(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  1,
		PerKeyBufferLen: 1,
	}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	// sink that always panics
	panicSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		panic("metrics sink panic")
	}

	collector := NewCollector[string](ctx, cfg, panicSink, nexus.SinkContext{}, pool, logger)

	// start collecting metrics
	collector.StartCollectingMetrics()

	// send a message
	collector.Collect(pool.Borrow())

	// Stop drains the async pipeline synchronously, guaranteeing the sink
	// has been called (and SendFailedCount incremented) before we assert
	collector.Stop()

	// confirm SendFailedCount incremented (panic recovered and counted)
	if failed := collector.SendFailedCount.Load(); failed != 1 {
		t.Errorf("expected SendFailedCount=1 after sink panic, got %d", failed)
	}

	// confirm CollectedCount did NOT increment (panic path)
	if collected := collector.CollectedCount.Load(); collected != 0 {
		t.Errorf("expected CollectedCount=0 after sink panic, got %d", collected)
	}

	// there should be no dropped metrics
	if dropped := collector.DroppedCount.Load(); dropped != 0 {
		t.Errorf("expected DroppedCount=0 (not a drop, send panicked), got %d", dropped)
	}
}

// Test_CollectorStop_DrainsAllMessages validates that Stop() blocks
// until all in-flight messages are processed, ensuring graceful shutdown
func Test_CollectorStop_DrainsAllMessages(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  10,
		PerKeyBufferLen: 16,
	}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	var capturedCount atomic.Int64

	// sink sleeps 1ms to simulate processing latency
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		capturedCount.Add(1)
		time.Sleep(10 * time.Microsecond)
		return nil
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)

	collector.StartCollectingMetrics()

	now := time.Now()
	// Send 500 messages quickly (fills channel, sleep should mean it takes 500ms)
	const messageCount = 500
	for i := int64(0); i < messageCount; i++ {
		workItem := pool.Borrow()
		workItem.Message.Offset = i
		workItem.Metrics.Offset = i
		collector.Collect(workItem)
	}

	// wait until ~200 metrics should have processed
	time.Sleep(2000 * time.Microsecond)

	// Stop() should block until all 500 are drained
	stopStart := now.Add(time.Since(now))
	collector.Stop()
	stopDuration := time.Since(stopStart)

	// verify Stop() blocked for at least 2500μs (remaining should be about ~3000μs)
	if stopDuration < 2500*time.Microsecond {
		t.Errorf("expected Stop() to block for ~300ms, blocked for %v", stopDuration)
	}

	// verify all 500 messages collected
	if collected := collector.CollectedCount.Load(); collected != messageCount {
		t.Errorf("expected CollectedCount=%d after Stop(), got %d", messageCount, collected)
	}

	// verify capturedCount matches (sink was called for each)
	if captured := capturedCount.Load(); captured != messageCount {
		t.Errorf("expected %d messages captured by sink, got %d", messageCount, captured)
	}

	// and no drops or failures
	if dropped := collector.DroppedCount.Load(); dropped != 0 {
		t.Errorf("expected DroppedCount=0, got %d", dropped)
	}
	if failed := collector.SendFailedCount.Load(); failed != 0 {
		t.Errorf("expected SendFailedCount=0, got %d", failed)
	}
}

// Test_CollectorStop_Idempotent validates that calling Stop() multiple times
// doesn't panic or block - hits the default case when quit channel is full
func Test_CollectorStop_Idempotent(_ *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  10,
		PerKeyBufferLen: 16,
	}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	collector := NewCollector[string](ctx, cfg, func(_ nexus.SinkContext, _ nexus.Metrics) error {
		return nil
	}, nexus.SinkContext{}, pool, logger)

	collector.StartCollectingMetrics()

	// Send a message to ensure collector is running
	collector.Collect(pool.Borrow())

	// Wait briefly for message to process
	time.Sleep(10 * time.Millisecond)

	// Call Stop() 100 times - first one succeeds, rest hit default case
	for i := 0; i < 100; i++ {
		collector.Stop()
	}

	// If we get here without hanging/panicking, idempotency works
}

// TestCollector_RetryPathExercised verifies that when the sink is slow,
// the retry path (Gosched + second send attempt) is exercised and items
// are eventually dropped when even the retry fails.
func TestCollector_RetryPathExercised(t *testing.T) {
	// Minimal config for smallest buffer (20k)
	cfg := config.DemuxConfig{
		ConcurrentKeys:  1,
		PerKeyBufferLen: 1,
	}

	bufferSize := CalculateCollectBufferSize(cfg.ConcurrentKeys, cfg.PerKeyBufferLen)

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	var collectedCount atomic.Int64

	// Slow sink: 100µs per metric. Fast enough to drain in reasonable time,
	// slow enough to cause backpressure when sending faster.
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		collectedCount.Add(1)
		time.Sleep(100 * time.Microsecond)
		return nil
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	collector.StartCollectingMetrics()

	// Send 2x buffer size as fast as possible.
	// Sink processes ~10k/sec, we send 40k instantly.
	// Buffer holds 20k, so ~20k should be dropped after retry fails.
	messagesToSend := bufferSize * 2
	for i := 0; i < messagesToSend; i++ {
		collector.Collect(pool.Borrow())
	}

	// Wait briefly for some drops to occur (don't wait for full drain)
	time.Sleep(50 * time.Millisecond)

	dropped := collector.DroppedCount.Load()
	collected := collectedCount.Load()

	// Verify drops occurred (retry path was exercised and failed)
	if dropped == 0 {
		t.Errorf("expected some dropped metrics (retry path exercised), got 0")
	}

	// Verify some metrics were collected (sink is working)
	if collected == 0 {
		t.Errorf("expected some collected metrics, got 0")
	}

	t.Logf("buffer=%d, sent=%d, collected=%d, dropped=%d, in-flight=%d",
		bufferSize, messagesToSend, collected, dropped, int64(messagesToSend)-collected-dropped)

	// Don't call Stop() - it would wait to drain 20k items at 100µs each (~2s)
	// We've already proven the retry path works by observing drops
}

// Test_WorkerPoolTransposedToMetrics verifies that WorkerPool is copied from
// WorkItem to Metrics before sending to MetricsSink.
func Test_WorkerPoolTransposedToMetrics(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  10,
		PerKeyBufferLen: 16,
	}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	var capturedMetrics []nexus.Metrics
	var mu sync.Mutex
	done := make(chan struct{})

	const messageCount = 5
	metricsSink := func(_ nexus.SinkContext, m nexus.Metrics) error {
		mu.Lock()
		capturedMetrics = append(capturedMetrics, m)
		if len(capturedMetrics) == messageCount {
			close(done)
		}
		mu.Unlock()
		return nil
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, nexus.SinkContext{}, pool, logger)
	collector.StartCollectingMetrics()

	// Send messages with distinct WorkerPool values
	for i := 0; i < messageCount; i++ {
		workItem := pool.Borrow()
		workItem.Message.Partition = 3
		workItem.Message.Offset = int64(i)
		workItem.WorkerPool = uint32(i + 7) //nolint:gosec // G115: bounded
		collector.Collect(workItem)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for metrics sink calls")
	}

	mu.Lock()
	defer mu.Unlock()

	for i, m := range capturedMetrics {
		expectedPool := uint32(i + 7)
		if m.WorkerPool != expectedPool {
			t.Errorf("metric[%d]: WorkerPool = %d, want %d", i, m.WorkerPool, expectedPool)
		}
	}

	collector.Stop()
}

// Test_SinkContext verifies that SinkContext is passed to MetricsSink on every call
func Test_SinkContext(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  10,
		PerKeyBufferLen: 16,
	}

	ctx := context.Background()
	logger := nexus.NewDefaultLogger(slog.LevelInfo)
	pool := alloc.NewWorkItemsPool[string](cfg)

	var capturedCtx nexus.SinkContext
	var mu sync.Mutex
	done := make(chan struct{})

	metricsSink := func(sinkCtx nexus.SinkContext, _ nexus.Metrics) error {
		mu.Lock()
		capturedCtx = sinkCtx
		mu.Unlock()
		close(done)
		return nil
	}

	expectedCtx := nexus.SinkContext{
		Service:       &nexus.Service{Name: "test-app", Team: "test-team"},
		TopicName:     "test-topic-name",
		ConsumerGroup: "test-consumer-group",
	}

	collector := NewCollector[string](ctx, cfg, metricsSink, expectedCtx, pool, logger)

	collector.StartCollectingMetrics()

	// send a message
	collector.Collect(pool.Borrow())

	// wait for processing
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for metrics sink call")
	}

	mu.Lock()
	defer mu.Unlock()

	if capturedCtx.TopicName != expectedCtx.TopicName {
		t.Errorf("expected topic name %q, got %q", expectedCtx.TopicName, capturedCtx.TopicName)
	}
	if capturedCtx.ConsumerGroup != expectedCtx.ConsumerGroup {
		t.Errorf("expected consumer group %q, got %q", expectedCtx.ConsumerGroup, capturedCtx.ConsumerGroup)
	}
	if capturedCtx.Service == nil || capturedCtx.Service.Name != expectedCtx.Service.Name {
		t.Errorf("expected Service.Name %q, got %+v", expectedCtx.Service.Name, capturedCtx.Service)
	}
	if capturedCtx.Service == nil || capturedCtx.Service.Team != expectedCtx.Service.Team {
		t.Errorf("expected Service.Team %q, got %+v", expectedCtx.Service.Team, capturedCtx.Service)
	}

	collector.Stop()
}
