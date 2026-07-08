// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"sync"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// Test_Committer_CommitIngestChannelLen_Boundaries covers all interesting
// fill states for the commitsIn channel - empty, single item, cap-1, full -
// and verifies the cap is reported back faithfully across different sizes
func Test_Committer_CommitIngestChannelLen_Boundaries(t *testing.T) {
	tests := []struct {
		name     string
		capacity int
		fillTo   int
		wantLen  int
		wantCap  int
	}{
		{name: "empty channel", capacity: 16, fillTo: 0, wantLen: 0, wantCap: 16},
		{name: "single item", capacity: 16, fillTo: 1, wantLen: 1, wantCap: 16},
		{name: "near-full (cap-1)", capacity: 16, fillTo: 15, wantLen: 15, wantCap: 16},
		{name: "exactly full (cap)", capacity: 16, fillTo: 16, wantLen: 16, wantCap: 16},
		{name: "small cap empty", capacity: 1, fillTo: 0, wantLen: 0, wantCap: 1},
		{name: "small cap full", capacity: 1, fillTo: 1, wantLen: 1, wantCap: 1},
		{name: "large cap partial", capacity: 5000, fillTo: 1234, wantLen: 1234, wantCap: 5000},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			committer := &Committer[string]{
				commitsIn: make(chan *ports.WorkItem[string], tc.capacity),
			}
			for i := 0; i < tc.fillTo; i++ {
				committer.commitsIn <- &ports.WorkItem[string]{}
			}

			gotLen, gotCap := committer.CommitIngestChannelLen()
			if gotLen != tc.wantLen {
				t.Errorf("len = %d, want %d", gotLen, tc.wantLen)
			}
			if gotCap != tc.wantCap {
				t.Errorf("cap = %d, want %d", gotCap, tc.wantCap)
			}
		})
	}
}

// Test_FlushGapBuffers_EarlyContinue_OnEmptyOrNoReady covers the no-op paths
// of flushGapBuffers - a partition flagged for gap advance but with nothing
// actionable: an empty buffer (early continue), or Ready nil with a buffered
// item that is not below the baseline (prune) and cannot initialise Ready
// (its offset is not CommittedPlusOne and its predecessor is not the last
// committed offset),
// so the walk must break and leave the tracker untouched: the item is waiting
// for its true predecessor to complete.
func Test_FlushGapBuffers_EarlyContinue_OnEmptyOrNoReady(t *testing.T) {
	tests := []struct {
		name      string
		ready     *ports.WorkItem[string]
		gapBuffer []*ports.WorkItem[string]
	}{
		{
			// CommittedPlusOne is 10 (below): offset 12 with predecessor 10 cannot
			// initialise Ready (12 != 10, and 10 != 10-1); left buffered untouched
			name:  "Ready nil, buffer non-empty, cannot initialise Ready",
			ready: nil,
			gapBuffer: []*ports.WorkItem[string]{{
				Message:        &nexus.Message[string]{Offset: 12},
				Metrics:        &nexus.Metrics{},
				PreviousOffset: 10,
			}},
		},
		{
			name:      "Ready non-nil, buffer empty",
			ready:     &ports.WorkItem[string]{},
			gapBuffer: []*ports.WorkItem[string]{},
		},
		{
			name:      "Ready nil and buffer empty",
			ready:     nil,
			gapBuffer: []*ports.WorkItem[string]{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &OffsetsTracker[string]{
				NeedsGapAdvance:  true,
				Ready:            tc.ready,
				GapBuffer:        tc.gapBuffer,
				CommittedPlusOne: 10,
			}
			oc := &Committer[string]{
				offsetsByPartition: &OffsetsByPartition[string]{
					PartitionMap: map[int32]*OffsetsTracker[string]{0: tracker},
				},
				gapBufferSize: 16,
			}

			oc.flushGapBuffers(time.Now())

			if tracker.NeedsGapAdvance {
				t.Error("NeedsGapAdvance should be cleared even when nothing is actionable")
			}
			if tracker.Ready != tc.ready {
				t.Error("Ready should be untouched when nothing can initialise it")
			}
			if len(tracker.GapBuffer) != len(tc.gapBuffer) {
				t.Errorf("GapBuffer modified: len = %d, want %d",
					len(tracker.GapBuffer), len(tc.gapBuffer))
			}
		})
	}
}

// Test_PreCommitsSnapshot_ClampsSentinelCommittedOffset covers the
// `committedOffset < -1` clamp branch - when CommittedPlusOne carries a
// sentinel value (initial -1 or Kafka OffsetInvalid -1001), the snapshot
// must report -1 rather than the large-negative artefact
func Test_PreCommitsSnapshot_ClampsSentinelCommittedOffset(t *testing.T) {
	tests := []struct {
		name             string
		committedPlusOne int64
		wantClampedTo    int64
	}{
		{name: "initial sentinel (-1)", committedPlusOne: -1, wantClampedTo: -1},
		{name: "kafka OffsetInvalid (-1001)", committedPlusOne: -1001, wantClampedTo: -1},
		{name: "first message committed (0)", committedPlusOne: 0, wantClampedTo: -1},
		{name: "real commit (100)", committedPlusOne: 100, wantClampedTo: 99},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tracker := &OffsetsTracker[string]{
				CommittedPlusOne: tc.committedPlusOne,
			}
			oc := &Committer[string]{
				mu: &sync.Mutex{},
				offsetsByPartition: &OffsetsByPartition[string]{
					PartitionMap: map[int32]*OffsetsTracker[string]{0: tracker},
				},
			}

			ps := oc.PreCommitsSnapshot()

			if len(ps.Partitions) != 1 {
				t.Fatalf("expected 1 partition, got %d", len(ps.Partitions))
			}
			if got := ps.Partitions[0].CommittedOffset; got != tc.wantClampedTo {
				t.Errorf("CommittedOffset = %d, want %d", got, tc.wantClampedTo)
			}
		})
	}
}

// Test_FlushGapBuffers_NoMatchingPreviousOffset covers the
// `advancedOffsetIndex < 0` continue branch - tracker has Ready and a
// non-empty GapBuffer, but no entry's PreviousOffset matches Ready.Offset
// so the loop breaks on the first iteration without advancing
func Test_FlushGapBuffers_NoMatchingPreviousOffset(t *testing.T) {
	ready := &ports.WorkItem[string]{Message: &nexus.Message[string]{Offset: 100}}

	// PreviousOffset 999 deliberately does not match Ready's offset (100)
	disjoint := &ports.WorkItem[string]{
		PreviousOffset: 999,
		Message:        &nexus.Message[string]{Offset: 1000},
	}

	tracker := &OffsetsTracker[string]{
		NeedsGapAdvance: true,
		Ready:           ready,
		GapBuffer:       []*ports.WorkItem[string]{disjoint},
	}
	oc := &Committer[string]{
		offsetsByPartition: &OffsetsByPartition[string]{
			PartitionMap: map[int32]*OffsetsTracker[string]{0: tracker},
		},
		gapBufferSize: 16,
	}

	oc.flushGapBuffers(time.Now())

	if tracker.NeedsGapAdvance {
		t.Error("NeedsGapAdvance should be cleared")
	}
	if tracker.Ready != ready {
		t.Error("Ready should not have been swapped when no entry matched")
	}
	if len(tracker.GapBuffer) != 1 || tracker.GapBuffer[0] != disjoint {
		t.Error("GapBuffer should be untouched when no advance happened")
	}
}
