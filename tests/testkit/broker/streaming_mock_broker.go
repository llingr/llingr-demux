// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package broker

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-demux/tests/testkit/scenario"
	"github.com/llingr/llingr-nexus/nexus"
)

const numPartitions = 100

// StreamingMockBroker generates an infinite stream of messages without pre-allocation.
// Designed for stress testing with minimal memory footprint.
//
// Memory usage: ~2.4KB (100 partitions × 3 counters × 8 bytes)
//
// Features:
//   - 100 partitions with deterministic key format: "{partition:02d}-{offset:016x}"
//   - Random control-record gaps (~1 in 5000 messages) to stress gap buffers
//   - Atomic tracking of generated vs committed offsets for verification
type StreamingMockBroker struct {
	// Generation tracking (atomic for lock-free Poll)
	globalCounter   atomic.Int64
	nextOffset      [numPartitions]atomic.Int64 // next offset to generate per partition
	highestReturned [numPartitions]atomic.Int64 // highest offset actually returned per partition

	// Commit tracking
	committedOffsets [numPartitions]atomic.Int64

	// Control
	stopped         atomic.Bool
	subscribed      atomic.Bool
	subscribeCalled chan struct{}

	// Callbacks
	rebalanceCallback func()
}

// NewStreamingMockBroker creates a streaming broker for stress testing.
func NewStreamingMockBroker() *StreamingMockBroker {
	return &StreamingMockBroker{
		subscribeCalled: make(chan struct{}),
	}
}

// SetRebalanceCallback configures a callback invoked when Subscribe succeeds.
func (b *StreamingMockBroker) SetRebalanceCallback(callback func()) {
	b.rebalanceCallback = callback
}

// Subscribe implementation.
func (b *StreamingMockBroker) Subscribe() error {
	if b.subscribed.Load() {
		return fmt.Errorf("already subscribed")
	}
	b.subscribed.Store(true)

	if b.rebalanceCallback != nil {
		go func() {
			b.rebalanceCallback()
			close(b.subscribeCalled)
		}()
	}

	return nil
}

// Unsubscribe implementation.
func (b *StreamingMockBroker) Unsubscribe() error {
	b.subscribed.Store(false)
	return nil
}

// Poll generates the next message on-the-fly.
// Returns immediately with a new message unless stopped.
// When stopped, simulates normal broker behavior: waits for timeout and returns no message.
// Occasionally creates offset gaps to simulate control records.
func (b *StreamingMockBroker) Poll(timeout time.Duration) (scenario.TestMessage, bool, error) {
	// Wait for subscribe to complete
	select {
	case <-b.subscribeCalled:
	default:
	}

	// When stopped, behave like a normal broker with no messages: wait timeout, return nothing
	if b.stopped.Load() {
		time.Sleep(timeout)
		return scenario.TestMessage{}, false, nil
	}

	id := b.globalCounter.Add(1) - 1
	partition := int32(id % numPartitions) //nolint:gosec // G115: numPartitions is small constant

	// Sequential offsets - no gaps
	offset := b.nextOffset[partition].Add(1) - 1

	// Track highest returned for verification
	updateAtomicMax(&b.highestReturned[partition], offset)

	// Deterministic key: no storage needed, can be reconstructed
	key := fmt.Sprintf("%02d-%016x", partition, offset)

	msg := scenario.TestMessage{
		ID:        int(id),
		Partition: partition,
		Offset:    offset,
		Key:       key,
	}

	return msg, true, nil
}

// Stop halts message generation. Poll will return no message (like a real broker with nothing to consume).
func (b *StreamingMockBroker) Stop() {
	b.stopped.Store(true)
}

// CommitOffsets tracks committed offsets per partition.
func (b *StreamingMockBroker) CommitOffsets(messages []*nexus.Message[scenario.TestMessage]) ([]*nexus.Message[scenario.TestMessage], error) {
	for _, msg := range messages {
		partition := msg.Partition
		offset := msg.Offset
		updateAtomicMax(&b.committedOffsets[partition], offset)
	}
	return messages, nil
}

// VerifyAllCommitted checks that all returned offsets were eventually committed.
// Returns nil if verification passes, error with details if not.
func (b *StreamingMockBroker) VerifyAllCommitted() error {
	var mismatches []string

	for p := int32(0); p < numPartitions; p++ {
		returned := b.highestReturned[p].Load()
		committed := b.committedOffsets[p].Load()

		if committed != returned {
			mismatches = append(mismatches,
				fmt.Sprintf("partition %d: committed=%d, returned=%d (diff=%d)",
					p, committed, returned, returned-committed))
		}
	}

	if len(mismatches) > 0 {
		return fmt.Errorf("commit verification failed:\n  %v", mismatches)
	}

	return nil
}

// GetStats returns current statistics.
func (b *StreamingMockBroker) GetStats() (polled int64, committed int64) {
	polled = b.globalCounter.Load()

	var totalCommitted int64
	for p := int32(0); p < numPartitions; p++ {
		totalCommitted += b.committedOffsets[p].Load()
	}

	return polled, totalCommitted
}

// PolledCount returns total messages polled.
func (b *StreamingMockBroker) PolledCount() int64 {
	return b.globalCounter.Load()
}

// ExtractEnvelope implements nexus.BrokerPort.
func (b *StreamingMockBroker) ExtractEnvelope(msg scenario.TestMessage) nexus.Envelope {
	return nexus.Envelope{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Ctx:       context.Background(),
	}
}

// AckRebalance implements nexus.BrokerPort.
func (b *StreamingMockBroker) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

// BrokerQuery implements nexus.BrokerPort.
func (b *StreamingMockBroker) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

// ConsumerGroup implements nexus.BrokerPort.
func (b *StreamingMockBroker) ConsumerGroup() string { return "" }

// updateAtomicMax atomically updates target to max(current, value).
func updateAtomicMax(target *atomic.Int64, value int64) {
	for {
		current := target.Load()
		if value <= current {
			return
		}
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}
