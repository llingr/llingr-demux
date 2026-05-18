// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package prev tracks the previous offset per partition for contiguity detection.
//
// [PartitionOffsets] records each message's predecessor, allowing the committer to detect
// contiguous sequences without assuming consecutive offset numbers. This handles gaps due
// to log compaction, transactions, and control records. It also detects first messages after
// rebalance and validates offset ordering to catch corruption or duplicate delivery.
package prev

import (
	"errors"
	"fmt"
	"sync"
)

// Offset validation errors.
var (
	ErrNegativeOffset   = errors.New("negative offset")
	ErrDuplicateOffset  = errors.New("duplicate offset")
	ErrOffsetRegression = errors.New("offset regression")
)

// PartitionOffsets tracks the previous offset for each partition to:
//
//   - Detect first messages after rebalance, committer needs to know the sequence start
//   - Handle non-contiguous sequences from control messages (transactions, log compaction)
//   - Validate offset ordering to detect corruption/duplicate delivery (very rare)
//
// This ensures at-least-once processing without requiring strict offset continuity.
type PartitionOffsets struct {
	prevOffsets map[int32]int64
	mu          sync.Mutex
}

// typical partition counts without rehashing, testing showed
// no performance benefit from making this configurable.
const defaultMapSize = 128

// NewPartitionOffsets creates a tracker used by offset.Committer for at-least-once guarantees.
func NewPartitionOffsets() *PartitionOffsets {
	return &PartitionOffsets{
		prevOffsets: make(map[int32]int64, defaultMapSize),
	}
}

// Init initialises an embedded PartitionOffsets in place, avoiding
// a heap allocation and keeping it on the same cache line as its parent struct.
func (p *PartitionOffsets) Init() {
	p.prevOffsets = make(map[int32]int64, defaultMapSize)
}

// GetPrevious offset for partition, updating the tracker with the
// current (provided) partitionOffset. Returns prevOffset, isFirst, error.
func (p *PartitionOffsets) GetPrevious(partition int32, partitionOffset int64) (int64, bool, error) {
	// broker offsets are always >= 0; negative values would break ascending-only assumption
	if partitionOffset < 0 {
		const errFmtNegativeOffset = "%w: %d on partition: %d"
		return -1, false, fmt.Errorf(errFmtNegativeOffset, ErrNegativeOffset, partitionOffset, partition)
	}

	p.mu.Lock()

	prevOffset, exists := p.prevOffsets[partition]

	if exists && partitionOffset > prevOffset {
		// not first message - most common
		p.prevOffsets[partition] = partitionOffset
		p.mu.Unlock()
		return prevOffset, false, nil

	} else if !exists {
		// first message - after rebalance
		p.prevOffsets[partition] = partitionOffset
		p.mu.Unlock()
		return partitionOffset - 1, true, nil
	}

	p.mu.Unlock()
	// duplicate or regression - very rare, triggers circuit-breaker
	if partitionOffset == prevOffset {
		const errFmtDuplicateOffset = "%w: %d on partition: %d"
		return -1, false, fmt.Errorf(errFmtDuplicateOffset, ErrDuplicateOffset, partitionOffset, partition)
	}

	const errFmtOffsetRegression = "%w: prevOffset: %d, offset: %d on partition: %d"
	return -1, false, fmt.Errorf(errFmtOffsetRegression, ErrOffsetRegression, prevOffset, partitionOffset, partition)
}

// Reset clears offset tracking for the given partitions, or all if none specified.
func (p *PartitionOffsets) Reset(partitions ...int32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(partitions) == 0 {
		p.prevOffsets = make(map[int32]int64, defaultMapSize)
		return
	}

	for _, partition := range partitions {
		delete(p.prevOffsets, partition)
	}
}
