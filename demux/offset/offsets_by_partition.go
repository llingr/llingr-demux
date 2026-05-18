// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

const (
	minPartitionMapSize = 12   // avoids immediate rehash for small deployments
	maxPartitionMapSize = 2048 // caps memory for extreme partition counts
)

// OffsetsByPartition maps each partition ID to its
// cached offset state, supporting per-partition and
// batch commits for high throughput message processing
type OffsetsByPartition[T any] struct {
	PartitionMap map[int32]*OffsetsTracker[T]
}

// New OffsetsByPartition is best initialized with the correct
// number of partitions on the message broker
func New[T any](partitionsCount int) *OffsetsByPartition[T] {
	partitionsCount = clampPartitionsCount(partitionsCount)
	return &OffsetsByPartition[T]{
		PartitionMap: make(map[int32]*OffsetsTracker[T], partitionsCount),
	}
}

// clampPartitionsCount ensures reasonable bounds for partition map pre-allocation,
// preventing small maps that immediately rehash and large maps that waste memory
func clampPartitionsCount(partitionsCount int) int {
	if partitionsCount < minPartitionMapSize {
		return minPartitionMapSize
	} else if partitionsCount > maxPartitionMapSize {
		return maxPartitionMapSize
	}
	return partitionsCount
}

// HasPendingCommits returns true if any partition has uncommitted messages.
//
// Used during async commit to determine whether to proceed, or throttle back
// to lower CPU utilisation as workloads drop - i.e. avoids mutex thrashing.
func (op *OffsetsByPartition[T]) HasPendingCommits() bool {
	if len(op.PartitionMap) == 0 {
		return false
	}
	for _, partition := range op.PartitionMap {
		if partition.HasPendingCommits() {
			return true
		}
	}
	return false
}
