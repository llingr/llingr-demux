// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"sync"
	"sync/atomic"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// metricsCapture provides thread-safe metrics collection for tests.
type metricsCapture struct {
	mu          sync.Mutex
	byPartition map[int32][]int64 // partition → offsets
	count       atomic.Int64
}

func newMetricsCapture() *metricsCapture {
	return &metricsCapture{
		byPartition: make(map[int32][]int64),
	}
}

// Sink is a nexus.MetricsSink that captures metrics.
func (c *metricsCapture) Sink(_ nexus.SinkContext, m nexus.Metrics) error {
	c.mu.Lock()
	c.byPartition[m.Partition] = append(c.byPartition[m.Partition], m.Offset)
	c.mu.Unlock()
	c.count.Add(1)
	return nil
}

// Count returns the number of metrics captured.
func (c *metricsCapture) Count() int64 {
	return c.count.Load()
}

// ByPartition returns a copy of offsets grouped by partition.
func (c *metricsCapture) ByPartition() map[int32][]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[int32][]int64, len(c.byPartition))
	for k, v := range c.byPartition {
		result[k] = append([]int64{}, v...)
	}
	return result
}

// populateWorkItem sets up a work item with contiguous offset tracking.
// For offset 0, sets First=true. Otherwise sets PreviousOffset=offset-1.
func populateWorkItem[T any](w *ports.WorkItem[T], partition int32, offset int64) {
	w.Message.Partition = partition
	w.Message.Offset = offset
	w.Metrics.Partition = partition
	w.Metrics.Offset = offset
	if offset == 0 {
		w.First = true
	} else {
		w.PreviousOffset = offset - 1
	}
}
