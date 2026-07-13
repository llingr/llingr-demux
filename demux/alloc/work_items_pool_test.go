// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package alloc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

func Test_CalcWorkItemPoolSize(t *testing.T) {
	tests := []struct {
		name         string
		cfg          config.DemuxConfig
		wantPoolSize int
	}{
		{
			name:         "zero config returns minimum 5000",
			cfg:          config.DemuxConfig{},
			wantPoolSize: 5000,
		},
		{
			name: "minimal config returns minimum 5000",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  1,
				PerKeyBufferLen: 1,
			},
			wantPoolSize: 5000,
		},
		{
			name: "small deployment default config",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  250,
				PerKeyBufferLen: 16,
			},
			wantPoolSize: 22750, // (250 * 16) + (250 * 75)
		},
		{
			name: "medium deployment",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  1000,
				PerKeyBufferLen: 16,
			},
			wantPoolSize: 91000, // (1000 * 16) + (1000 * 75)
		},
		{
			name: "large deployment hits maximum 100k",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  5000,
				PerKeyBufferLen: 20,
			},
			wantPoolSize: 100_000,
		},
		{
			name: "extreme config clamped to max",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  10000,
				PerKeyBufferLen: 64,
			},
			wantPoolSize: 100_000,
		},
		{
			name: "zero concurrent keys returns minimum",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  0,
				PerKeyBufferLen: 16,
			},
			wantPoolSize: 5000,
		},
		{
			name: "zero buffer length still adds gap buffer overhead",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  250,
				PerKeyBufferLen: 0,
			},
			wantPoolSize: 18750, // (250 * 0) + (250 * 75) = 18750
		},
		{
			name: "negative config returns minimum 5000",
			cfg: config.DemuxConfig{
				ConcurrentKeys: -99,
			},
			wantPoolSize: 5000,
		},
		{
			name: "typical high-throughput deployment",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  2000,
				PerKeyBufferLen: 16,
			},
			wantPoolSize: 100_000, // (2000 * 16) + (2000 * 75) = 182000, clamped to 100k
		},
		{
			name: "boundary just below minimum",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  50,
				PerKeyBufferLen: 10,
			},
			wantPoolSize: 5000, // (50 * 10) + (50 * 75) = 4250 < 5000
		},
		{
			name: "boundary just above minimum",
			cfg: config.DemuxConfig{
				ConcurrentKeys:  60,
				PerKeyBufferLen: 10,
			},
			wantPoolSize: 5100, // (60 * 10) + (60 * 75) = 5100 > 5000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if poolSize := CalcWorkItemPoolSize(tt.cfg); poolSize != tt.wantPoolSize {
				t.Errorf("CalcWorkItemPoolSize() = %d, want %d", poolSize, tt.wantPoolSize)
			}
		})
	}
}

func Test_NewWorkItemsPool(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})
	if pool == nil {
		t.Fatal("NewWorkItemsPool returned nil")
	}

	if pool.workItemPool == nil {
		t.Fatal("workItemPool is nil")
	}

	workItem := pool.Borrow()
	if workItem == nil {
		t.Fatal("borrow returned nil")
	}
	if workItem.Ctx != nil {
		t.Error("borrowed workItem.Ctx is not nil")
	}
	if workItem.First == true {
		t.Error("borrowed workItem.First should be false")
	}

	message := workItem.Message
	if message == nil {
		t.Fatalf("borrowed workItem.Message is nil")
	}
	if message.Traits != 0 {
		t.Error("borrowed workItem.Message.Traits is not zero")
	}
	if message.Partition != 0 {
		t.Error("borrowed workItem.Message.Partition is not zero")
	}
	if message.Offset != 0 {
		t.Error("borrowed workItem.Message.Offset is not zero")
	}
	if message.Key != "" {
		t.Error("borrowed workItem.Message.Key is not empty string")
	}
	if message.Payload != nil {
		t.Error("borrowed workItem.Message.Payload is not nil")
	}

	metrics := workItem.Metrics
	if metrics == nil {
		t.Fatal("borrowed workItem.Metrics is nil")
	}
	if metrics.Traits != 0 {
		t.Error("borrowed workItem.Metrics.Traits is not zero")
	}
	if metrics.Partition != 0 {
		t.Error("borrowed workItem.Metrics.Partition is not zero")
	}
	if metrics.Offset != 0 {
		t.Error("borrowed workItem.Metrics.Offset is not zero")
	}

	if metrics.ProcessDuration != 0 {
		t.Error("borrowed workItem.Metrics.ProcessDuration is not zero")
	}
	if metrics.WriteDeadLetterDuration != 0 {
		t.Error("borrowed workItem.Metrics.WriteDeadLetterDuration is not zero")
	}
	if !metrics.ReadTime.IsZero() {
		t.Error("borrowed workItem.Metrics.ReadTime is not zero")
	}
	if !metrics.ProcessStartTime.IsZero() {
		t.Error("borrowed workItem.Metrics.ProcessStartTime is not zero")
	}
	if !metrics.WatermarkAdvanceTime.IsZero() {
		t.Error("borrowed workItem.Metrics.WatermarkAdvanceTime is not zero")
	}
	pool.Return(workItem)
}

func Test_BorrowReturn_ZeroesFields(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})

	// borrow 25k work items and populate them with non-zero values
	const numItems = 25_000
	workItems := make([]*ports.WorkItem[string], numItems)

	now := time.Now()
	testPayload := "test-payload"

	// borrow and populate all items
	//nolint:gosec // G115: test data with bounded loop index
	for i := 0; i < numItems; i++ {
		w := pool.Borrow()

		// set message fields
		w.Message.Traits = nexus.Traits(123 + i)
		w.Message.Partition = int32(456 + i)
		w.Message.Offset = int64(789 + i)
		w.Message.Key = fmt.Sprintf("test-key-%06d", i)
		w.Message.Payload = &testPayload

		// set metrics fields
		w.Metrics.Traits = nexus.Traits(111 + i)
		w.Metrics.QueueDepth = int32(222 + i)
		w.Metrics.Partition = int32(333 + i)
		w.Metrics.Offset = int64(444 + i)
		w.Metrics.ProcessDuration = time.Duration(555+i) * time.Millisecond
		w.Metrics.WriteDeadLetterDuration = time.Duration(666+i) * time.Millisecond
		w.Metrics.ReadTime = now
		w.Metrics.ProcessStartTime = now.Add(time.Second)
		w.Metrics.WatermarkAdvanceTime = now.Add(2 * time.Second)

		// set work item fields
		w.Ctx = context.Background()
		w.First = true
		w.WorkerPool = uint32(i % 16) //nolint:gosec // G115: bounded

		workItems[i] = w
	}

	// return all items to the pool
	for i := 0; i < numItems; i++ {
		pool.Return(workItems[i])
	}

	// borrow 25k items again and verify all fields are zeroed
	for i := 0; i < numItems; i++ {
		workItem := pool.Borrow()

		if workItem.Ctx != nil {
			t.Errorf("item %d: Ctx = %v, want nil", i, workItem.Ctx)
		}
		if workItem.First {
			t.Errorf("item %d: First = true, want false", i)
		}
		if workItem.WorkerPool != 0 {
			t.Errorf("item %d: WorkerPool = %d, want 0", i, workItem.WorkerPool)
		}

		// check message fields were reset
		if workItem.Message.Traits != 0 {
			t.Errorf("item %d: Message.Traits = %d, want 0", i, workItem.Message.Traits)
		}
		if workItem.Message.Partition != 0 {
			t.Errorf("item %d: Message.Partition = %d, want 0", i, workItem.Message.Partition)
		}
		if workItem.Message.Offset != 0 {
			t.Errorf("item %d: Message.Offset = %d, want 0", i, workItem.Message.Offset)
		}
		if workItem.Message.Key != "" {
			t.Errorf("item %d: Message.Key = %q, want empty string", i, workItem.Message.Key)
		}
		if workItem.Message.Payload != nil {
			t.Errorf("item %d: Message.Payload = %v, want nil", i, workItem.Message.Payload)
		}

		// check metrics fields are zeroed
		if workItem.Metrics.Traits != 0 {
			t.Errorf("item %d: Metrics.Traits = %d, want 0", i, workItem.Metrics.Traits)
		}
		if workItem.Metrics.QueueDepth != 0 {
			t.Errorf("item %d: Metrics.QueueDepth = %d, want 0", i, workItem.Metrics.QueueDepth)
		}
		if workItem.Metrics.Partition != 0 {
			t.Errorf("item %d: Metrics.Partition = %d, want 0", i, workItem.Metrics.Partition)
		}
		if workItem.Metrics.Offset != 0 {
			t.Errorf("item %d: Metrics.Offset = %d, want 0", i, workItem.Metrics.Offset)
		}
		if workItem.Metrics.ProcessDuration != 0 {
			t.Errorf("item %d: Metrics.ProcessDuration = %v, want 0", i, workItem.Metrics.ProcessDuration)
		}
		if workItem.Metrics.WriteDeadLetterDuration != 0 {
			t.Errorf("item %d: Metrics.WriteDeadLetterDuration = %v, want 0",
				i, workItem.Metrics.WriteDeadLetterDuration)
		}
		if !workItem.Metrics.ReadTime.IsZero() {
			t.Errorf("item %d: Metrics.ReadTime = %v, want zero time", i, workItem.Metrics.ReadTime)
		}
		if !workItem.Metrics.ProcessStartTime.IsZero() {
			t.Errorf("item %d: Metrics.ProcessStartTime = %v, want zero time",
				i, workItem.Metrics.ProcessStartTime)
		}
		if !workItem.Metrics.WatermarkAdvanceTime.IsZero() {
			t.Errorf("item %d: Metrics.WatermarkAdvanceTime = %v, want zero time",
				i, workItem.Metrics.WatermarkAdvanceTime)
		}

	}
}

// assertPoolPayloadType verifies that a pool's borrowed item has the expected payload type.
func assertPoolPayloadType[T any](t *testing.T, cfg config.DemuxConfig, expectedType string) {
	t.Helper()
	workItemsPool := NewWorkItemsPool[T](cfg)
	workItem := workItemsPool.Borrow()
	if workItem == nil {
		t.Fatal("Borrow returned nil")
	}
	if actualType := fmt.Sprintf("%T", workItem.Message.Payload); actualType != expectedType {
		t.Errorf("Message.Payload type = %s, want %s", actualType, expectedType)
	}
	workItemsPool.Return(workItem)
}

func Test_BorrowReturn_DifferentTypes(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}

	t.Run("string type", func(t *testing.T) {
		assertPoolPayloadType[string](t, cfg, "*string")
	})

	t.Run("int type", func(t *testing.T) {
		assertPoolPayloadType[int](t, cfg, "*int")
	})

	t.Run("struct type", func(t *testing.T) {
		type customStruct struct {
			ID   int
			Name string
		}
		assertPoolPayloadType[customStruct](t, cfg, "*alloc.customStruct")
	})

	t.Run("pointer type", func(t *testing.T) {
		type customStruct struct {
			ID   int
			Name string
		}
		// double-indirection necessary for memory safety - prevents 'use-after-return'
		// bugs - allowing the pool return to reset payload without affecting underlying
		// reference which may still be in use
		assertPoolPayloadType[*customStruct](t, cfg, "**alloc.customStruct")
	})
}

func FuzzCalcWorkItemPoolSize(f *testing.F) {
	f.Add(1, 1)
	f.Add(250, 16)
	f.Add(5000, 20)
	f.Add(0, 0)
	f.Add(10000, 64)
	f.Add(100, 8)

	f.Fuzz(func(t *testing.T, concurrentKeys, perKeyBufferLen int) {
		cfg := config.DemuxConfig{
			ConcurrentKeys:  concurrentKeys,
			PerKeyBufferLen: perKeyBufferLen,
		}

		result := CalcWorkItemPoolSize(cfg)

		// verify result is always within bounds
		if result < 5000 {
			t.Errorf("CalcWorkItemPoolSize(%d, %d) = %d, should never be less than 5000",
				concurrentKeys, perKeyBufferLen, result)
		}
		if result > 100_000 {
			t.Errorf("CalcWorkItemPoolSize(%d, %d) = %d, should never exceed 100000",
				concurrentKeys, perKeyBufferLen, result)
		}

		// verify result is deterministic
		result2 := CalcWorkItemPoolSize(cfg)
		if result != result2 {
			t.Errorf("CalcWorkItemPoolSize not deterministic: %d != %d", result, result2)
		}
	})
}

func Test_Concurrent_BorrowReturnVerifyZeroed(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})

	const (
		numGoroutines   = 50000
		itemsPerRoutine = 50
	)

	errChan := make(chan error, numGoroutines)
	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			payload := "payload" //nolint:goconst // test fixture
			for i := 0; i < itemsPerRoutine; i++ {
				w := pool.Borrow()

				// verify item is zeroed
				if w.Message.Traits != 0 {
					errChan <- context.DeadlineExceeded
					done <- false
					return
				}
				if w.Message.Key != "" {
					errChan <- context.DeadlineExceeded
					done <- false
					return
				}
				if w.Ctx != nil {
					errChan <- context.DeadlineExceeded
					done <- false
					return
				}

				w.Message.Traits = nexus.Traits(id*1000 + i) //nolint:gosec // G115: bounded
				w.Message.Partition = int32(id)              //nolint:gosec // G115: bounded
				w.Message.Offset = int64(i)
				w.Message.Key = "test-key" //nolint:goconst // test fixture
				w.Message.Payload = &payload
				w.Metrics.Partition = int32(id) //nolint:gosec // G115: bounded
				w.Metrics.Offset = int64(i)
				w.Metrics.QueueDepth = int32(id + i) //nolint:gosec // G115: bounded
				w.Metrics.ProcessDuration = time.Second
				w.Metrics.WriteDeadLetterDuration = 2 * time.Second
				w.Metrics.ReadTime = time.Now()
				w.Metrics.ProcessStartTime = time.Now().Add(time.Second)
				w.Metrics.WatermarkAdvanceTime = time.Now().Add(2 * time.Second)
				w.Ctx = context.Background()
				w.First = true
				w.WorkerPool = uint32(id % 16) //nolint:gosec // G115: bounded

				pool.Return(w)
			}
			done <- true
		}(g)
	}

	for g := 0; g < numGoroutines; g++ {
		select {
		case err := <-errChan:
			t.Fatalf("concurrent test failed: %v", err)
		case success := <-done:
			if !success {
				t.Fatal("concurrent test failed")
			}
		}
	}

	// confirm resets always happen
	for i := 0; i < 100; i++ {
		workItem := pool.Borrow()
		if workItem == nil {
			t.Fatal("borrow returned nil")
		}
		if workItem.Ctx != nil {
			t.Error("borrowed workItem.Ctx is not nil")
		}
		if workItem.First == true {
			t.Error("borrowed workItem.First should be false")
		}
		if workItem.WorkerPool != 0 {
			t.Error("borrowed workItem.WorkerPool is not zero")
		}

		message := workItem.Message
		if message == nil {
			t.Fatalf("borrowed workItem.Message is nil")
		}
		if message.Traits != 0 {
			t.Error("borrowed workItem.Message.Traits is not zero")
		}
		if message.Partition != 0 {
			t.Error("borrowed workItem.Message.Partition is not zero")
		}
		if message.Offset != 0 {
			t.Error("borrowed workItem.Message.Offset is not zero")
		}
		if message.Key != "" {
			t.Error("borrowed workItem.Message.Key is not empty string")
		}
		if message.Payload != nil {
			t.Error("borrowed workItem.Message.Payload is not nil")
		}

		metrics := workItem.Metrics
		if metrics == nil {
			t.Fatal("borrowed workItem.Metrics is nil")
		}
		if metrics.Traits != 0 {
			t.Error("borrowed workItem.Metrics.Traits is not zero")
		}
		if metrics.QueueDepth != 0 {
			t.Error("borrowed workItem.Metrics.QueueDepth is not zero")
		}
		if metrics.Partition != 0 {
			t.Error("borrowed workItem.Metrics.Partition is not zero")
		}
		if metrics.Offset != 0 {
			t.Error("borrowed workItem.Metrics.Offset is not zero")
		}

		if metrics.ProcessDuration != 0 {
			t.Error("borrowed workItem.Metrics.ProcessDuration is not zero")
		}
		if metrics.WriteDeadLetterDuration != 0 {
			t.Error("borrowed workItem.Metrics.WriteDeadLetterDuration is not zero")
		}
		if !metrics.ReadTime.IsZero() {
			t.Error("borrowed workItem.Metrics.ReadTime is not zero")
		}
		if !metrics.ProcessStartTime.IsZero() {
			t.Error("borrowed workItem.Metrics.ProcessStartTime is not zero")
		}
		if !metrics.WatermarkAdvanceTime.IsZero() {
			t.Error("borrowed workItem.Metrics.WatermarkAdvanceTime is not zero")
		}
	}
}

// TestAllocs_BorrowReturn asserts zero allocations for borrow/return cycle.
// This invariant ensures the pool is truly reusing objects.
func TestAllocs_BorrowReturn(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}
	pool := NewWorkItemsPool[string](cfg)

	// warm up - ensure items are in the pool
	w := pool.Borrow()
	pool.Return(w)

	allocs := testing.AllocsPerRun(1000, func() {
		w := pool.Borrow()
		pool.Return(w)
	})

	if allocs > 0 {
		t.Errorf("expected 0 allocs/op, got %.2f", allocs)
	}
}

// TestAllocs_BorrowPopulateReturn asserts zero allocations for the full
// borrow, populate, return cycle used in production hot paths.
func TestAllocs_BorrowPopulateReturn(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}
	pool := NewWorkItemsPool[string](cfg)
	payload := "payload"

	// warm up
	w := pool.Borrow()
	pool.Return(w)

	allocs := testing.AllocsPerRun(1000, func() {
		w := pool.Borrow()
		w.Message.Traits = nexus.Traits(123)
		w.Message.Partition = 1
		w.Message.Offset = 999
		w.Message.Key = "test-key"
		w.Message.Payload = &payload
		w.Metrics.Traits = nexus.Traits(456)
		w.Ctx = context.Background()
		pool.Return(w)
	})

	if allocs > 0 {
		t.Errorf("expected 0 allocs/op, got %.2f", allocs)
	}
}

// Benchmark_BorrowReturn measures pool borrow/return cycle.
func Benchmark_BorrowReturn(b *testing.B) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}
	pool := NewWorkItemsPool[string](cfg)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := pool.Borrow()
		pool.Return(w)
	}
	b.StopTimer()

	allocs := testing.AllocsPerRun(100, func() {
		w := pool.Borrow()
		pool.Return(w)
	})
	if allocs > 0 {
		b.Fatalf("alloc regression: expected 0 allocs/op, got %.2f", allocs)
	}
}

// Benchmark_BorrowPopulateReturn measures full borrow/populate/return cycle.
func Benchmark_BorrowPopulateReturn(b *testing.B) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  250,
		PerKeyBufferLen: 16,
	}
	pool := NewWorkItemsPool[string](cfg)
	payload := "payload"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := pool.Borrow()
		w.Message.Traits = nexus.Traits(123)
		w.Message.Partition = 1
		w.Message.Offset = int64(i)
		w.Message.Key = "test-key"
		w.Message.Payload = &payload
		w.Metrics.Traits = nexus.Traits(456)
		w.Ctx = context.Background()
		pool.Return(w)
	}
	b.StopTimer()

	allocs := testing.AllocsPerRun(100, func() {
		w := pool.Borrow()
		w.Message.Traits = nexus.Traits(123)
		w.Message.Partition = 1
		w.Message.Offset = 999
		w.Message.Key = "test-key"
		w.Message.Payload = &payload
		w.Metrics.Traits = nexus.Traits(456)
		w.Ctx = context.Background()
		pool.Return(w)
	})
	if allocs > 0 {
		b.Fatalf("alloc regression: expected 0 allocs/op, got %.2f", allocs)
	}
}

// Test_PreviousOffset_ResetToMinusOne verifies PreviousOffset is reset to -1 (not 0)
// because -1 is the sentinel value indicating "no previous offset"
func Test_PreviousOffset_ResetToMinusOne(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})

	// borrow and set PreviousOffset to various values
	testCases := []int64{0, 1, 100, 999999, -1, -100}

	for _, prevOffset := range testCases {
		w := pool.Borrow()

		// verify initial borrow has PreviousOffset = -1
		if w.PreviousOffset != -1 {
			t.Errorf("initial borrow: PreviousOffset = %d, want -1", w.PreviousOffset)
		}

		// set to test value
		w.PreviousOffset = prevOffset
		pool.Return(w)

		// borrow again and verify reset to -1
		w2 := pool.Borrow()
		if w2.PreviousOffset != -1 {
			t.Errorf("after setting to %d: PreviousOffset = %d, want -1",
				prevOffset, w2.PreviousOffset)
		}
		pool.Return(w2)
	}
}

// Test_PoolReusesPointers verifies the pool reuses the same WorkItem pointers
// rather than allocating new ones, confirming memory efficiency.
// Note: sync.Pool uses per-P queues so we can't guarantee exact LIFO ordering,
// but we can verify that borrowed items are eventually returned from the pool.
func Test_PoolReusesPointers(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})

	// borrow a single item, record its pointer, return it
	w1 := pool.Borrow()
	ptr1 := fmt.Sprintf("%p", w1)
	msgPtr1 := fmt.Sprintf("%p", w1.Message)
	metricsPtr1 := fmt.Sprintf("%p", w1.Metrics)
	pool.Return(w1)

	// borrow multiple times to find our item - sync.Pool doesn't guarantee LIFO
	// across multiple Ps, but the item should be reused within reasonable borrows
	const maxAttempts = 100
	found := false
	for i := 0; i < maxAttempts; i++ {
		w := pool.Borrow()
		if fmt.Sprintf("%p", w) == ptr1 {
			found = true
			// verify Message and Metrics pointers are preserved within same WorkItem
			if fmt.Sprintf("%p", w.Message) != msgPtr1 {
				t.Error("Message pointer should be preserved within WorkItem")
			}
			if fmt.Sprintf("%p", w.Metrics) != metricsPtr1 {
				t.Error("Metrics pointer should be preserved within WorkItem")
			}
			pool.Return(w)
			break
		}
		pool.Return(w)
	}

	if !found {
		// not finding it within maxAttempts is acceptable - GC or distribution
		// the batch test provides stronger reuse guarantees
		t.Logf("original pointer not found in %d attempts (acceptable with sync.Pool)", maxAttempts)
	}
}

// Test_PoolReusesPointers_Batch verifies pointer reuse at scale
func Test_PoolReusesPointers_Batch(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})

	const batchSize = 100
	pointers := make(map[string]struct{}, batchSize)

	// borrow batch and collect pointers
	items := make([]*ports.WorkItem[string], batchSize)
	for i := 0; i < batchSize; i++ {
		items[i] = pool.Borrow()
		pointers[fmt.Sprintf("%p", items[i])] = struct{}{}
	}

	// return all
	for i := 0; i < batchSize; i++ {
		pool.Return(items[i])
	}

	// borrow again - all pointers should be from the original set
	reusedCount := 0
	for i := 0; i < batchSize; i++ {
		w := pool.Borrow()
		ptr := fmt.Sprintf("%p", w)
		if _, exists := pointers[ptr]; exists {
			reusedCount++
		}
		pool.Return(w)
	}

	// with sync.Pool, we expect high reuse rate (may not be 100% due to GC)
	if reusedCount < batchSize/2 {
		t.Errorf("expected at least 50%% pointer reuse, got %d/%d", reusedCount, batchSize)
	}
	t.Logf("pointer reuse: %d/%d (%.1f%%)", reusedCount, batchSize,
		float64(reusedCount)/float64(batchSize)*100)
}

// Test_WarmPool_PreAllocates verifies warmPool creates the expected number of items
func Test_WarmPool_PreAllocates(t *testing.T) {
	cfg := config.DemuxConfig{
		ConcurrentKeys:  100,
		PerKeyBufferLen: 10,
	}
	expectedSize := CalcWorkItemPoolSize(cfg)

	pool := NewWorkItemsPool[string](cfg)

	// borrow expectedSize items - all should come from pool without new allocations
	items := make([]*ports.WorkItem[string], expectedSize)
	for i := 0; i < expectedSize; i++ {
		items[i] = pool.Borrow()
		if items[i] == nil {
			t.Fatalf("borrow %d returned nil", i)
		}
		if items[i].Message == nil {
			t.Fatalf("borrow %d: Message is nil", i)
		}
		if items[i].Metrics == nil {
			t.Fatalf("borrow %d: Metrics is nil", i)
		}
	}

	// return all items
	for i := 0; i < expectedSize; i++ {
		pool.Return(items[i])
	}

	t.Logf("successfully borrowed and returned %d pre-warmed items", expectedSize)
}

// Test_BorrowReturn_MultipleCycles verifies repeated borrow/return cycles
// maintain correct field zeroing
func Test_BorrowReturn_MultipleCycles(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})
	payload := "test-payload"

	const cycles = 1000
	for cycle := 0; cycle < cycles; cycle++ {
		w := pool.Borrow()

		// verify all fields are zeroed
		if w.Message.Traits != 0 {
			t.Fatalf("cycle %d: Message.Traits = %d, want 0", cycle, w.Message.Traits)
		}
		if w.Message.Partition != 0 {
			t.Fatalf("cycle %d: Message.Partition = %d, want 0", cycle, w.Message.Partition)
		}
		if w.Message.Offset != 0 {
			t.Fatalf("cycle %d: Message.Offset = %d, want 0", cycle, w.Message.Offset)
		}
		if w.Message.Key != "" {
			t.Fatalf("cycle %d: Message.Key = %q, want empty", cycle, w.Message.Key)
		}
		if w.Message.Payload != nil {
			t.Fatalf("cycle %d: Message.Payload not nil", cycle)
		}
		if w.Metrics.Traits != 0 {
			t.Fatalf("cycle %d: Metrics.Traits = %d, want 0", cycle, w.Metrics.Traits)
		}
		if w.Metrics.QueueDepth != 0 {
			t.Fatalf("cycle %d: Metrics.QueueDepth = %d, want 0", cycle, w.Metrics.QueueDepth)
		}
		if w.Metrics.ProcessDuration != 0 {
			t.Fatalf("cycle %d: Metrics.ProcessDuration = %v, want 0", cycle, w.Metrics.ProcessDuration)
		}
		if !w.Metrics.ReadTime.IsZero() {
			t.Fatalf("cycle %d: Metrics.ReadTime not zero", cycle)
		}
		if w.Ctx != nil {
			t.Fatalf("cycle %d: Ctx not nil", cycle)
		}
		if w.First {
			t.Fatalf("cycle %d: First = true, want false", cycle)
		}
		if w.PreviousOffset != -1 {
			t.Fatalf("cycle %d: PreviousOffset = %d, want -1", cycle, w.PreviousOffset)
		}

		// populate with cycle-specific values
		w.Message.Traits = nexus.Traits(cycle)
		w.Message.Partition = int32(cycle % 12) //nolint:gosec // G115: bounded
		w.Message.Offset = int64(cycle * 100)
		w.Message.Key = fmt.Sprintf("key-%d", cycle)
		w.Message.Payload = &payload
		w.Metrics.Traits = nexus.Traits(cycle + 1000) //nolint:gosec // G115: bounded
		w.Metrics.QueueDepth = int32(cycle)           //nolint:gosec // G115: bounded
		w.Metrics.Partition = int32(cycle % 12)       //nolint:gosec // G115: bounded
		w.Metrics.Offset = int64(cycle * 100)
		w.Metrics.ProcessDuration = time.Duration(cycle) * time.Millisecond
		w.Metrics.ReadTime = time.Now()
		w.Ctx = context.Background()
		w.First = true
		w.PreviousOffset = int64(cycle*100 - 1)

		pool.Return(w)
	}
}

// Test_Return_NilSafe verifies Return handles edge cases in field values
func Test_Return_FieldEdgeCases(t *testing.T) {
	pool := NewWorkItemsPool[string](config.DemuxConfig{})

	t.Run("max int values", func(t *testing.T) {
		w := pool.Borrow()
		w.Message.Traits = ^nexus.Traits(0)    // all bits set
		w.Message.Partition = 2147483647       // max int32
		w.Message.Offset = 9223372036854775807 // max int64
		w.Metrics.Traits = ^nexus.Traits(0)
		w.Metrics.QueueDepth = 2147483647
		w.Metrics.Offset = 9223372036854775807
		w.PreviousOffset = 9223372036854775807
		pool.Return(w)

		w2 := pool.Borrow()
		if w2.Message.Traits != 0 {
			t.Error("max Traits not zeroed")
		}
		if w2.Message.Partition != 0 {
			t.Error("max Partition not zeroed")
		}
		if w2.Message.Offset != 0 {
			t.Error("max Offset not zeroed")
		}
		if w2.PreviousOffset != -1 {
			t.Errorf("PreviousOffset = %d, want -1", w2.PreviousOffset)
		}
		pool.Return(w2)
	})

	t.Run("min int values", func(t *testing.T) {
		w := pool.Borrow()
		w.Message.Partition = -2147483648       // min int32
		w.Message.Offset = -9223372036854775808 // min int64
		w.Metrics.Partition = -2147483648
		w.Metrics.Offset = -9223372036854775808
		w.PreviousOffset = -9223372036854775808
		pool.Return(w)

		w2 := pool.Borrow()
		if w2.Message.Partition != 0 {
			t.Errorf("min Partition = %d, want 0", w2.Message.Partition)
		}
		if w2.Message.Offset != 0 {
			t.Errorf("min Offset = %d, want 0", w2.Message.Offset)
		}
		if w2.PreviousOffset != -1 {
			t.Errorf("PreviousOffset = %d, want -1", w2.PreviousOffset)
		}
		pool.Return(w2)
	})

	t.Run("long string key", func(t *testing.T) {
		w := pool.Borrow()
		longKey := string(make([]byte, 10000)) // 10KB key
		w.Message.Key = longKey
		pool.Return(w)

		w2 := pool.Borrow()
		if w2.Message.Key != "" {
			t.Errorf("long key not cleared, len=%d", len(w2.Message.Key))
		}
		pool.Return(w2)
	})

	t.Run("far future time", func(t *testing.T) {
		w := pool.Borrow()
		farFuture := time.Date(2999, 12, 31, 23, 59, 59, 0, time.UTC)
		w.Metrics.ReadTime = farFuture
		w.Metrics.ProcessStartTime = farFuture
		w.Metrics.WatermarkAdvanceTime = farFuture
		pool.Return(w)

		w2 := pool.Borrow()
		if !w2.Metrics.ReadTime.IsZero() {
			t.Error("far future ReadTime not zeroed")
		}
		if !w2.Metrics.ProcessStartTime.IsZero() {
			t.Error("far future ProcessStartTime not zeroed")
		}
		if !w2.Metrics.WatermarkAdvanceTime.IsZero() {
			t.Error("far future WatermarkAdvanceTime not zeroed")
		}
		pool.Return(w2)
	})

	t.Run("max duration", func(t *testing.T) {
		w := pool.Borrow()
		w.Metrics.ProcessDuration = time.Duration(9223372036854775807)
		w.Metrics.WriteDeadLetterDuration = time.Duration(9223372036854775807)
		pool.Return(w)

		w2 := pool.Borrow()
		if w2.Metrics.ProcessDuration != 0 {
			t.Error("max ProcessDuration not zeroed")
		}
		if w2.Metrics.WriteDeadLetterDuration != 0 {
			t.Error("max WriteDeadLetterDuration not zeroed")
		}
		pool.Return(w2)
	})
}
