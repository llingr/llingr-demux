// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package ports

import (
	"context"

	"github.com/llingr/llingr-nexus/nexus"
)

// WorkItem composite with message and metrics for observable
// processing. Allows Metrics to continue to the metrics sink
// while the Message can be reclaimed earlier.
type WorkItem[T any] struct {
	Message        *nexus.Message[T] // -> ProcessMessage
	Metrics        *nexus.Metrics    // -> MetricsSink after processing
	Ctx            context.Context   // from ExtractEnvelope and sent to ProcessMessage
	PreviousOffset int64             // for gap detection, handling log compaction and control records
	First          bool              // first message seen on partition since rebalance
	WorkerPool     uint32
}

// PartitionOffset accessor addressing info
func (w *WorkItem[T]) PartitionOffset() (int32, int64) {
	message := w.Message
	return message.Partition, message.Offset
}

// PartitionOffsetPrevious accessor addressing info and previous offset
func (w *WorkItem[T]) PartitionOffsetPrevious() (int32, int64, int64) {
	message := w.Message
	return message.Partition, message.Offset, w.PreviousOffset
}
