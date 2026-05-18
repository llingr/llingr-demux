// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"testing"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

func Test_OffsetsTracker_HasPendingCommits(t *testing.T) {
	tests := []struct {
		name               string
		readyIsNil         bool
		gapBufferLen       int
		expectedHasPending bool
	}{
		{
			name:               "no pending: ready nil, gap buffer empty",
			readyIsNil:         true,
			gapBufferLen:       0,
			expectedHasPending: false,
		},
		{
			name:               "has pending: ready not nil, gap buffer empty",
			readyIsNil:         false,
			gapBufferLen:       0,
			expectedHasPending: true,
		},
		{
			name:               "has pending: ready nil, gap buffer has 1 item",
			readyIsNil:         true,
			gapBufferLen:       1,
			expectedHasPending: true,
		},
		{
			name:               "has pending: ready not nil, gap buffer has 1 item",
			readyIsNil:         false,
			gapBufferLen:       1,
			expectedHasPending: true,
		},
		{
			name:               "has pending: ready nil, gap buffer has 100 items",
			readyIsNil:         true,
			gapBufferLen:       100,
			expectedHasPending: true,
		},
		{
			name:               "has pending: ready not nil, gap buffer has 100 items",
			readyIsNil:         false,
			gapBufferLen:       100,
			expectedHasPending: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := &OffsetsTracker[string]{
				CommittedPlusOne: 0,
				Assignment:       nexus.Assign,
				MinOffsetSeen:    0,
				MaxOffsetSeen:    0,
				GapBuffer:        make([]*ports.WorkItem[string], tt.gapBufferLen),
			}

			if !tt.readyIsNil {
				tracker.Ready = &ports.WorkItem[string]{
					Message: &nexus.Message[string]{
						Partition: 0,
						Offset:    42,
					},
					Metrics: &nexus.Metrics{},
				}
			}

			result := tracker.HasPendingCommits()
			if result != tt.expectedHasPending {
				t.Errorf("expected HasPendingCommits=%v, got %v",
					tt.expectedHasPending, result)
			}
		})
	}
}

// Fuzz_OffsetsTracker_HasPendingCommits validates the invariant:
// HasPendingCommits() returns true if and only if (Ready != nil OR len(GapBuffer) > 0)
func Fuzz_OffsetsTracker_HasPendingCommits(f *testing.F) {
	f.Add(false, 0)   // no pending
	f.Add(true, 0)    // ready only
	f.Add(false, 1)   // gap buffer only
	f.Add(true, 1)    // both
	f.Add(false, 100) // large gap buffer
	f.Add(true, 1000) // both with large gap buffer
	f.Add(false, -1)  // negative length (invalid but test robustness)

	f.Fuzz(func(t *testing.T, hasReady bool, gapBufferLen int) {
		// Handle negative lengths gracefully
		if gapBufferLen < 0 {
			gapBufferLen = 0
		}

		tracker := &OffsetsTracker[string]{
			CommittedPlusOne: 0,
			Assignment:       nexus.Assign,
			MinOffsetSeen:    0,
			MaxOffsetSeen:    0,
			GapBuffer:        make([]*ports.WorkItem[string], 0, gapBufferLen),
		}

		if hasReady {
			tracker.Ready = &ports.WorkItem[string]{
				Message: &nexus.Message[string]{
					Partition: 0,
					Offset:    42,
				},
				Metrics: &nexus.Metrics{},
			}
		}

		result := tracker.HasPendingCommits()

		expectedResult := tracker.Ready != nil || len(tracker.GapBuffer) > 0

		if result != expectedResult {
			t.Errorf("invariant violation: hasReady=%v, gapBufferLen=%d, expected=%v, got=%v",
				hasReady, gapBufferLen, expectedResult, result)
		}
	})
}
