// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package prev

import (
	"errors"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestNewPartitionOffsets(t *testing.T) {
	p := NewPartitionOffsets()

	if p == nil {
		t.Fatal("expected non-nil PartitionOffsets")
	}
	if p.prevOffsets == nil {
		t.Fatal("expected prevOffsets map to be initialised")
	}
	if len(p.prevOffsets) != 0 {
		t.Errorf("expected empty map, got %d entries", len(p.prevOffsets))
	}
}

// TestInit_PopulatesEmptyMap exercises the in-place initialiser used when
// PartitionOffsets is embedded in a parent struct (rather than constructed
// via NewPartitionOffsets). Production callers do this from pipeline.NewProcessor
// to avoid a heap allocation - that call site is in a sibling package so it does
// not register on this package's coverage profile
func TestInit_PopulatesEmptyMap(t *testing.T) {
	var p PartitionOffsets // zero value - prevOffsets is nil
	p.Init()

	if p.prevOffsets == nil {
		t.Fatal("Init should allocate prevOffsets")
	}
	if len(p.prevOffsets) != 0 {
		t.Errorf("expected empty map after Init, got %d entries", len(p.prevOffsets))
	}

	// after Init the partition tracker is usable
	prev, isFirst, err := p.GetPrevious(0, 100)
	if err != nil {
		t.Errorf("GetPrevious after Init returned error: %v", err)
	}
	if !isFirst {
		t.Error("first call after Init should report isFirst=true")
	}
	if prev != 99 {
		t.Errorf("first call should return offset-1 (99), got %d", prev)
	}
}

func TestGetPrevious_FirstMessage(t *testing.T) {
	p := NewPartitionOffsets()

	prevOffset, isFirst, err := p.GetPrevious(0, 100)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isFirst {
		t.Error("expected isFirst=true for first message on partition")
	}
	if prevOffset != 99 {
		t.Errorf("expected prevOffset=99 (offset-1), got %d", prevOffset)
	}
	// verify internal state updated
	if p.prevOffsets[0] != 100 {
		t.Errorf("expected internal state to be 100, got %d", p.prevOffsets[0])
	}
}

func TestGetPrevious_NormalAscending(t *testing.T) {
	p := NewPartitionOffsets()

	// first message to establish state
	_, _, _ = p.GetPrevious(0, 100)

	// second message - normal ascending case
	prevOffset, isFirst, err := p.GetPrevious(0, 101)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isFirst {
		t.Error("expected isFirst=false for subsequent message")
	}
	if prevOffset != 100 {
		t.Errorf("expected prevOffset=100, got %d", prevOffset)
	}
	// verify internal state updated
	if p.prevOffsets[0] != 101 {
		t.Errorf("expected internal state to be 101, got %d", p.prevOffsets[0])
	}
}

func TestGetPrevious_NonContiguousAscending(t *testing.T) {
	// tests log compaction scenario - gaps are allowed if ascending
	p := NewPartitionOffsets()

	_, _, _ = p.GetPrevious(0, 100)

	// skip to 150 (log compaction removed 101-149)
	prevOffset, isFirst, err := p.GetPrevious(0, 150)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isFirst {
		t.Error("expected isFirst=false")
	}
	if prevOffset != 100 {
		t.Errorf("expected prevOffset=100, got %d", prevOffset)
	}
}

func TestGetPrevious_OffsetRegression_ReturnsError(t *testing.T) {
	p := NewPartitionOffsets()

	// establish offset 100
	_, _, _ = p.GetPrevious(0, 100)

	// send offset 50 - lower than previous (should error)
	prevOffset, isFirst, err := p.GetPrevious(0, 50)

	if err == nil {
		t.Fatal("expected error for offset regression")
	}
	if !errors.Is(err, ErrOffsetRegression) {
		t.Errorf("expected ErrOffsetRegression, got: %v", err)
	}
	const expected = "offset regression: prevOffset: 100, offset: 50 on partition: 0"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if prevOffset != -1 {
		t.Errorf("expected prevOffset=-1 on error, got %d", prevOffset)
	}
	if isFirst {
		t.Error("expected isFirst=false on error")
	}
}

func TestGetPrevious_DuplicateOffset_ReturnsError(t *testing.T) {
	p := NewPartitionOffsets()

	_, _, _ = p.GetPrevious(0, 100)

	// send same offset again (duplicate)
	_, _, err := p.GetPrevious(0, 100)

	if err == nil {
		t.Fatal("expected error for duplicate offset")
	}
	if !errors.Is(err, ErrDuplicateOffset) {
		t.Errorf("expected ErrDuplicateOffset, got: %v", err)
	}
	const expected = "duplicate offset: 100 on partition: 0"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
}

func TestGetPrevious_MultiplePartitions(t *testing.T) {
	p := NewPartitionOffsets()

	// partition 0
	prev0, first0, _ := p.GetPrevious(0, 100)
	if !first0 || prev0 != 99 {
		t.Errorf("partition 0 first: expected first=true, prev=99, got first=%v, prev=%d", first0, prev0)
	}

	// partition 1
	prev1, first1, _ := p.GetPrevious(1, 200)
	if !first1 || prev1 != 199 {
		t.Errorf("partition 1 first: expected first=true, prev=199, got first=%v, prev=%d", first1, prev1)
	}

	// partition 0 again
	prev0b, first0b, _ := p.GetPrevious(0, 101)
	if first0b || prev0b != 100 {
		t.Errorf("partition 0 second: expected first=false, prev=100, got first=%v, prev=%d", first0b, prev0b)
	}

	// partition 1 again
	prev1b, first1b, _ := p.GetPrevious(1, 201)
	if first1b || prev1b != 200 {
		t.Errorf("partition 1 second: expected first=false, prev=200, got first=%v, prev=%d", first1b, prev1b)
	}
}

func TestReset_AllPartitions(t *testing.T) {
	p := NewPartitionOffsets()

	// populate some partitions
	for i := int32(0); i < 10; i++ {
		p.prevOffsets[i] = int64(i * 100)
	}

	// reset all (no arguments)
	p.Reset()

	if len(p.prevOffsets) != 0 {
		t.Errorf("expected empty map after Reset(), got %d entries", len(p.prevOffsets))
	}

	// verify new messages are treated as first
	_, isFirst, _ := p.GetPrevious(0, 500)
	if !isFirst {
		t.Error("expected isFirst=true after reset")
	}
}

func TestReset_SpecificPartitions(t *testing.T) {
	p := NewPartitionOffsets()

	for i := int32(0); i < 10; i++ {
		p.prevOffsets[i] = int64(i * 100)
	}

	p.Reset(0, 1, 2, 3, 4)

	// partitions 0-4 should be removed
	for i := int32(0); i < 5; i++ {
		if _, ok := p.prevOffsets[i]; ok {
			t.Errorf("expected partition %d to be removed", i)
		}
	}

	// partitions 5-9 should remain
	for i := int32(5); i < 10; i++ {
		if _, ok := p.prevOffsets[i]; !ok {
			t.Errorf("expected partition %d to remain", i)
		}
	}
}

func TestReset_NonExistentPartitions(t *testing.T) {
	p := NewPartitionOffsets()

	p.prevOffsets[0] = 100

	// reset partitions that don't exist - should not panic
	p.Reset(99, 100, 101)

	// partition 0 should still exist
	if _, ok := p.prevOffsets[0]; !ok {
		t.Error("expected partition 0 to remain")
	}
}

// --- Concurrency Tests ---

func TestGetPrevious_ConcurrentAccess(_ *testing.T) {
	p := NewPartitionOffsets()
	const goroutines = 100
	const iterations = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			// each goroutine works on its own partition to test map concurrent access
			partition := int32(goroutineID % 10) //nolint:gosec // G115: bounded by goroutineID
			baseOffset := int64(goroutineID * iterations)

			for i := int64(0); i < iterations; i++ {
				_, _, _ = p.GetPrevious(partition, baseOffset+i)
			}
		}(g)
	}

	wg.Wait()
}

func TestGetPrevious_ConcurrentSamePartition(t *testing.T) {
	// multiple goroutines hitting the same partition - higher contention
	p := NewPartitionOffsets()
	const goroutines = 50
	const iterations = 500

	var wg sync.WaitGroup
	var errorCount int64
	var mu sync.Mutex

	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			// all goroutines use partition 0, but different offset ranges
			baseOffset := int64(goroutineID * iterations)

			for i := int64(0); i < iterations; i++ {
				_, _, err := p.GetPrevious(0, baseOffset+i)
				if err != nil {
					// expected: non-ascending errors due to interleaving
					mu.Lock()
					errorCount++
					mu.Unlock()
				}
			}
		}(g)
	}

	wg.Wait()
	// errors are expected due to interleaved offsets - just verify no panic/race
	t.Logf("concurrent same partition: %d non-ascending errors (expected)", errorCount)
}

func TestReset_WhileGetPreviousInFlight(t *testing.T) {
	p := NewPartitionOffsets()

	// establish initial state
	_, _, _ = p.GetPrevious(0, 100)

	var wg sync.WaitGroup
	wg.Add(2)

	// producer goroutine - continuously calls GetPrevious
	go func() {
		defer wg.Done()
		for i := int64(101); i < 5000; i++ {
			prev, isFirst, err := p.GetPrevious(0, i)
			// after a reset, isFirst should be true and prev should be i-1
			if isFirst && err == nil && prev != i-1 {
				t.Errorf("after reset, expected prev=%d, got %d", i-1, prev)
			}
		}
	}()

	// reset goroutine - periodically resets partition 0
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			time.Sleep(10 * time.Microsecond)
			p.Reset(0)
		}
	}()

	wg.Wait()
}

func TestReset_AllWhileGetPreviousInFlight(_ *testing.T) {
	p := NewPartitionOffsets()

	// establish state on multiple partitions
	for i := int32(0); i < 5; i++ {
		_, _, _ = p.GetPrevious(i, 100)
	}

	var wg sync.WaitGroup
	wg.Add(6) // 5 producers + 1 resetter

	// producer goroutines - one per partition
	for part := int32(0); part < 5; part++ {
		go func(partition int32) {
			defer wg.Done()
			for i := int64(101); i < 2000; i++ {
				_, _, _ = p.GetPrevious(partition, i)
			}
		}(part)
	}

	// reset goroutine - periodically resets ALL partitions
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			time.Sleep(50 * time.Microsecond)
			p.Reset() // reset all
		}
	}()

	wg.Wait()
}

// --- Edge Case Tests ---

func TestGetPrevious_FirstMessageAtOffsetZero(t *testing.T) {
	// offset 0 is valid - first message on a fresh partition
	p := NewPartitionOffsets()

	prevOffset, isFirst, err := p.GetPrevious(0, 0)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !isFirst {
		t.Error("expected isFirst=true for first message")
	}
	// prevOffset should be -1 (offset-1 = 0-1 = -1)
	if prevOffset != -1 {
		t.Errorf("expected prevOffset=-1 for first message at offset 0, got %d", prevOffset)
	}
	// internal state should be 0
	if p.prevOffsets[0] != 0 {
		t.Errorf("expected internal state to be 0, got %d", p.prevOffsets[0])
	}
}

func TestGetPrevious_DuplicateOffset_ReturnsAllValues(t *testing.T) {
	// verify all return values on duplicate error, not just the error
	p := NewPartitionOffsets()

	_, _, _ = p.GetPrevious(0, 100)

	prevOffset, isFirst, err := p.GetPrevious(0, 100)

	if err == nil {
		t.Fatal("expected error for duplicate offset")
	}
	if !errors.Is(err, ErrDuplicateOffset) {
		t.Errorf("expected ErrDuplicateOffset, got: %v", err)
	}
	if prevOffset != -1 {
		t.Errorf("expected prevOffset=-1 on error, got %d", prevOffset)
	}
	if isFirst {
		t.Error("expected isFirst=false on error")
	}
	// internal state should NOT be updated
	if p.prevOffsets[0] != 100 {
		t.Errorf("expected internal state to remain 100, got %d", p.prevOffsets[0])
	}
}

func TestGetPrevious_NegativePartition(t *testing.T) {
	// negative partition numbers are valid int32 values
	p := NewPartitionOffsets()

	prevOffset, isFirst, err := p.GetPrevious(-1, 100)

	if err != nil {
		t.Fatalf("unexpected error for negative partition: %v", err)
	}
	if !isFirst {
		t.Error("expected isFirst=true for first message on negative partition")
	}
	if prevOffset != 99 {
		t.Errorf("expected prevOffset=99, got %d", prevOffset)
	}

	// second message on negative partition
	prevOffset2, isFirst2, err2 := p.GetPrevious(-1, 101)
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	if isFirst2 {
		t.Error("expected isFirst=false for subsequent message")
	}
	if prevOffset2 != 100 {
		t.Errorf("expected prevOffset=100, got %d", prevOffset2)
	}
}

func TestGetPrevious_VeryLargeNegativeOffset(t *testing.T) {
	// test with a large negative value (not just -1)
	p := NewPartitionOffsets()

	prevOffset, isFirst, err := p.GetPrevious(5, -999999)

	if err == nil {
		t.Fatal("expected error for large negative offset")
	}
	if !errors.Is(err, ErrNegativeOffset) {
		t.Errorf("expected ErrNegativeOffset, got: %v", err)
	}
	if prevOffset != -1 {
		t.Errorf("expected prevOffset=-1 on error, got %d", prevOffset)
	}
	if isFirst {
		t.Error("expected isFirst=false on error")
	}
	// verify partition 5 was not added to map
	if _, ok := p.prevOffsets[5]; ok {
		t.Error("expected partition 5 to NOT be in map after error")
	}
}

func TestReset_SpecificPartitions_ThenGetPrevious(t *testing.T) {
	// test that reset partitions properly become "first" again
	p := NewPartitionOffsets()

	// establish messages on multiple partitions
	_, _, _ = p.GetPrevious(0, 100)
	_, _, _ = p.GetPrevious(1, 200)
	_, _, _ = p.GetPrevious(2, 300)

	// reset only partition 1
	p.Reset(1)

	// partition 0 should NOT be first
	prev0, isFirst0, _ := p.GetPrevious(0, 101)
	if isFirst0 {
		t.Error("partition 0 should not be first after reset of partition 1")
	}
	if prev0 != 100 {
		t.Errorf("partition 0 prev expected 100, got %d", prev0)
	}

	// partition 1 SHOULD be first (was reset)
	prev1, isFirst1, _ := p.GetPrevious(1, 500)
	if !isFirst1 {
		t.Error("partition 1 should be first after reset")
	}
	if prev1 != 499 {
		t.Errorf("partition 1 prev expected 499, got %d", prev1)
	}

	// partition 2 should NOT be first
	prev2, isFirst2, _ := p.GetPrevious(2, 301)
	if isFirst2 {
		t.Error("partition 2 should not be first after reset of partition 1")
	}
	if prev2 != 300 {
		t.Errorf("partition 2 prev expected 300, got %d", prev2)
	}
}

func TestGetPrevious_OffsetRegression_DoesNotUpdateState(t *testing.T) {
	// verify internal state is NOT modified on regression error
	p := NewPartitionOffsets()

	_, _, _ = p.GetPrevious(0, 100)

	// attempt regression
	_, _, err := p.GetPrevious(0, 50)
	if err == nil {
		t.Fatal("expected regression error")
	}

	// internal state should still be 100, not 50
	if p.prevOffsets[0] != 100 {
		t.Errorf("expected internal state to remain 100 after regression error, got %d", p.prevOffsets[0])
	}

	// next valid message should work correctly
	prev, isFirst, err := p.GetPrevious(0, 101)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if isFirst {
		t.Error("expected isFirst=false")
	}
	if prev != 100 {
		t.Errorf("expected prev=100, got %d", prev)
	}
}

// --- Overflow Protection Tests ---

func TestGetPrevious_NegativeOffset_ReturnsError(t *testing.T) {
	p := NewPartitionOffsets()

	prevOffset, isFirst, err := p.GetPrevious(0, -1)
	if err == nil {
		t.Fatal("expected error for negative offset")
	}
	if !errors.Is(err, ErrNegativeOffset) {
		t.Errorf("expected ErrNegativeOffset, got: %v", err)
	}
	const expected = "negative offset: -1 on partition: 0"
	if err.Error() != expected {
		t.Errorf("expected %q, got %q", expected, err.Error())
	}
	if prevOffset != -1 {
		t.Errorf("expected prevOffset=-1 on error, got %d", prevOffset)
	}
	if isFirst {
		t.Error("expected isFirst=false on error")
	}
}

func TestGetPrevious_MinInt64_ReturnsError(t *testing.T) {
	p := NewPartitionOffsets()

	// math.MinInt64 - 1 would overflow to MaxInt64
	_, _, err := p.GetPrevious(0, math.MinInt64)
	if err == nil {
		t.Fatal("expected error for MinInt64 offset (would overflow)")
	}
}

func TestGetPrevious_MaxInt64_Succeeds(t *testing.T) {
	p := NewPartitionOffsets()

	// MaxInt64 is valid - prev would be MaxInt64-1
	prev, isFirst, err := p.GetPrevious(0, math.MaxInt64)
	if err != nil {
		t.Fatalf("unexpected error for MaxInt64: %v", err)
	}
	if !isFirst {
		t.Error("expected isFirst=true")
	}
	if prev != math.MaxInt64-1 {
		t.Errorf("expected prev=MaxInt64-1, got %d", prev)
	}
}

// --- Benchmark for overflow check ---

func BenchmarkGetPrevious_FirstMessagePath(b *testing.B) {
	p := NewPartitionOffsets()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		delete(p.prevOffsets, 0) // force "first message" path
		_, _, _ = p.GetPrevious(0, int64(i))
	}
}

func BenchmarkGetPrevious_NormalPath(b *testing.B) {
	p := NewPartitionOffsets()
	_, _, _ = p.GetPrevious(0, 0) // establish partition

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = p.GetPrevious(0, int64(i+1))
	}
}

// BenchmarkOverflowCheckOnly isolates just the comparison cost
func BenchmarkOverflowCheckOnly(b *testing.B) {
	offset := int64(12345)
	var result bool

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = offset < 0
	}
	runtime.KeepAlive(result)
}
