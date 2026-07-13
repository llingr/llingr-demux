// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package offset

import (
	"testing"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// makeTracker creates a test OffsetsTracker.
// If offset >= 0, sets Ready with the given partition/offset.
// gapSize sets the GapBuffer length.
func makeTracker(partition int32, offset int64, gapSize int) *OffsetsTracker[string] {
	t := &OffsetsTracker[string]{
		GapBuffer: make([]*ports.WorkItem[string], gapSize),
	}
	if offset >= 0 {
		t.Ready = &ports.WorkItem[string]{
			Message: &nexus.Message[string]{
				Partition: partition,
				Offset:    offset,
			},
			Metrics: &nexus.Metrics{},
		}
	}
	return t
}

// Test_OffsetsByPartition_HasPendingCommits validates all code paths
// for detecting uncommitted messages across partitions
func Test_OffsetsByPartition_HasPendingCommits(t *testing.T) {
	tests := []struct {
		name               string
		setupPartitions    func() *OffsetsByPartition[string]
		expectedHasPending bool
	}{
		{
			name: "empty map returns false",
			setupPartitions: func() *OffsetsByPartition[string] {
				return &OffsetsByPartition[string]{
					PartitionMap: make(map[int32]*OffsetsTracker[string]),
				}
			},
			expectedHasPending: false,
		},
		{
			name: "single partition with pending (Ready not nil)",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](1)
				op.PartitionMap[0] = makeTracker(0, 42, 0)
				return op
			},
			expectedHasPending: true,
		},
		{
			name: "single partition with pending (GapBuffer not empty)",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](1)
				op.PartitionMap[0] = makeTracker(0, -1, 5)
				return op
			},
			expectedHasPending: true,
		},
		{
			name: "single partition with no pending",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](1)
				op.PartitionMap[0] = makeTracker(0, -1, 0)
				return op
			},
			expectedHasPending: false,
		},
		{
			name: "multiple partitions, all with pending",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](3)
				for i := int32(0); i < 3; i++ {
					op.PartitionMap[i] = makeTracker(i, int64(i*100), 0)
				}
				return op
			},
			expectedHasPending: true,
		},
		{
			name: "multiple partitions, all without pending (fallthrough to line 33)",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](5)
				for i := int32(0); i < 5; i++ {
					op.PartitionMap[i] = makeTracker(i, -1, 0)
				}
				return op
			},
			expectedHasPending: false,
		},
		{
			name: "mixed: first partition has pending (early return true)",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](3)
				op.PartitionMap[0] = makeTracker(0, 10, 0)
				op.PartitionMap[1] = makeTracker(1, -1, 0)
				op.PartitionMap[2] = makeTracker(2, -1, 0)
				return op
			},
			expectedHasPending: true,
		},
		{
			name: "mixed: last partition has pending",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](3)
				op.PartitionMap[0] = makeTracker(0, -1, 0)
				op.PartitionMap[1] = makeTracker(1, -1, 0)
				op.PartitionMap[2] = makeTracker(2, -1, 3) // gap buffer only
				return op
			},
			expectedHasPending: true,
		},
		{
			name: "mixed: middle partition has pending",
			setupPartitions: func() *OffsetsByPartition[string] {
				op := New[string](3)
				op.PartitionMap[0] = makeTracker(0, -1, 0)
				op.PartitionMap[1] = makeTracker(1, 50, 2) // Ready + gap buffer
				op.PartitionMap[2] = makeTracker(2, -1, 0)
				return op
			},
			expectedHasPending: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := tt.setupPartitions()
			result := op.HasPendingCommits()

			if result != tt.expectedHasPending {
				t.Errorf("expected HasPendingCommits=%v, got %v",
					tt.expectedHasPending, result)
			}
		})
	}
}

// Test_OffsetsByPartition_New validates the constructor
func Test_OffsetsByPartition_New(t *testing.T) {
	op := New[string](24)

	if op == nil {
		t.Fatal("New() returned nil")
	}

	if op.PartitionMap == nil {
		t.Fatal("PartitionMap is nil")
	}

	// Verify map is empty initially
	if len(op.PartitionMap) != 0 {
		t.Errorf("expected empty map, got length %d", len(op.PartitionMap))
	}

	// Verify map is usable
	op.PartitionMap[0] = &OffsetsTracker[string]{}
	if len(op.PartitionMap) != 1 {
		t.Errorf("map not usable: expected length 1 after insert, got %d",
			len(op.PartitionMap))
	}
}

// Test_clampPartitionsCount validates bounds enforcement for partition counts
func Test_clampPartitionsCount(t *testing.T) {
	tests := []struct {
		name     string
		input    int
		expected int
	}{
		// Below minimum
		{
			name:     "negative value clamped to minimum",
			input:    -10,
			expected: 12,
		},
		{
			name:     "zero clamped to minimum",
			input:    0,
			expected: 12,
		},
		{
			name:     "one below minimum",
			input:    11,
			expected: 12,
		},
		// At minimum
		{
			name:     "at minimum boundary",
			input:    12,
			expected: 12,
		},
		// Between min and max
		{
			name:     "typical small deployment (24 partitions)",
			input:    24,
			expected: 24,
		},
		{
			name:     "typical medium deployment (128 partitions)",
			input:    128,
			expected: 128,
		},
		{
			name:     "high-throughput deployment (1000 partitions)",
			input:    1000,
			expected: 1000,
		},
		// At maximum
		{
			name:     "at maximum boundary",
			input:    2048,
			expected: 2048,
		},
		// Beyond maximum
		{
			name:     "one above maximum",
			input:    2049,
			expected: 2048,
		},
		{
			name:     "far beyond maximum (5000)",
			input:    5000,
			expected: 2048,
		},
		{
			name:     "extreme value (10000)",
			input:    10000,
			expected: 2048,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := clampPartitionsCount(tt.input)
			if result != tt.expected {
				t.Errorf("clampPartitionsCount(%d) = %d, expected %d",
					tt.input, result, tt.expected)
			}
		})
	}
}

// FuzzClampPartitionsCount validates bounds invariants for all possible inputs
func FuzzClampPartitionsCount(f *testing.F) {
	// Seed with boundary values and typical cases
	f.Add(0)
	f.Add(12)
	f.Add(24)
	f.Add(128)
	f.Add(2048)
	f.Add(5000)
	f.Add(-100)

	f.Fuzz(func(t *testing.T, input int) {
		result := clampPartitionsCount(input)

		// Invariant 1: result must be within bounds [12, 2048]
		if result < 12 {
			t.Errorf("clampPartitionsCount(%d) = %d, below minimum 12", input, result)
		}
		if result > 2048 {
			t.Errorf("clampPartitionsCount(%d) = %d, above maximum 2048", input, result)
		}

		// Invariant 2: if input is within bounds, result equals input
		if input >= 12 && input <= 2048 {
			if result != input {
				t.Errorf("clampPartitionsCount(%d) = %d, expected %d (input in range)",
					input, result, input)
			}
		}

		// Invariant 3: if input below minimum, result is minimum
		if input < 12 {
			if result != 12 {
				t.Errorf("clampPartitionsCount(%d) = %d, expected 12 (below minimum)",
					input, result)
			}
		}

		// Invariant 4: if input above maximum, result is maximum
		if input > 2048 {
			if result != 2048 {
				t.Errorf("clampPartitionsCount(%d) = %d, expected 2048 (above maximum)",
					input, result)
			}
		}
	})
}
