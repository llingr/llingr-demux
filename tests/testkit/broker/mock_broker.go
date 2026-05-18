// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package broker provides in-memory mock brokers for end-to-end testing without real
// infrastructure. [MockBroker] simulates subscribe, poll, commit, and rebalance operations.
package broker

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// MockBroker simulates an in-memory scenario broker for end-to-end testing.
// Thread-safe for concurrent access.
type MockBroker[T any] struct {
	messages          []*nexus.Message[T] // pre-defined messages to return from PollX
	polledIndex       atomic.Int64        // next scenario index to return
	mu                sync.RWMutex        // protects committedOffsets and error injectors
	committedOffsets  map[int32]int64     // partition → the highest committed offset
	commitError       error               // if set, CommitOffsets returns this error
	subscribed        atomic.Bool         // subscription state
	subscribeCalled   chan struct{}
	topicName         string // subscribed topic
	rebalanceCallback func() // called asynchronously when Subscribe succeeds
}

// NewMockBroker creates an in-memory broker with pre-defined messages.
// Topic name is provided via builder.WithTopicName(), not here.
func NewMockBroker[T any](messages []*nexus.Message[T],
	rebalanceCallback func()) *MockBroker[T] {
	return &MockBroker[T]{
		messages:          messages,
		committedOffsets:  make(map[int32]int64),
		rebalanceCallback: rebalanceCallback,
		subscribeCalled:   make(chan struct{}),
	}
}

// SetRebalanceCallback configures a callback that will be invoked asynchronously
// when Subscribe succeeds, simulating how Kafka triggers partition assignments.
func (m *MockBroker[T]) SetRebalanceCallback(callback func()) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rebalanceCallback = callback
}

// InjectCommitError causes CommitOffsets to return the specified error.
// Pass nil to clear the error injection.
func (m *MockBroker[T]) InjectCommitError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitError = err
}

// Subscribe implementation for nexus.Subscribe function type.
// Triggers the rebalance callback asynchronously if configured, simulating
// how Kafka automatically assigns partitions after subscription.
// Topic name is provided at broker construction, not here.
func (m *MockBroker[T]) Subscribe() error {
	if m.subscribed.Load() {
		return fmt.Errorf("already subscribed to topic: %s", m.topicName)
	}
	m.subscribed.Store(true)

	// Trigger rebalance callback asynchronously (simulates scenario broker behaviour)
	m.mu.RLock()
	rebalanceCallback := m.rebalanceCallback
	m.mu.RUnlock()

	if rebalanceCallback != nil {
		go func() {
			rebalanceCallback()
			close(m.subscribeCalled)
		}()
	}

	return nil
}

// Unsubscribe implementation for nexus.Unsubscribe function type.
func (m *MockBroker[T]) Unsubscribe() error {
	if !m.subscribed.Load() {
		return fmt.Errorf("not subscribed to any topic")
	}
	m.subscribed.Store(false)
	m.topicName = ""
	return nil
}

// Poll implementation for nexus.Poll[T] function type.
// Returns messages sequentially from the pre-defined slice.
// Returns (nil, false, nil) when no more messages available.
func (m *MockBroker[T]) Poll(timeout time.Duration) (T, bool, error) {

	select {
	case <-m.subscribeCalled:
	default:
	}

	// get next scenario
	idx := int(m.polledIndex.Add(1) - 1)
	if idx >= len(m.messages) {
		// no more messages
		time.Sleep(timeout) // simulate waiting
		var zero T
		return zero, false, nil
	}

	msg := m.messages[idx]
	return *msg.Payload, true, nil
}

// CommitOffsets implementation for nexus.CommitOffsets[T] function type.
// Tracks highest committed offset per partition for verification.
func (m *MockBroker[T]) CommitOffsets(messages []*nexus.Message[T]) ([]*nexus.Message[T], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// check for injected error
	if m.commitError != nil {
		return nil, m.commitError
	}

	// track committed offsets (highest per partition)
	for _, msg := range messages {
		partition := msg.Partition
		offset := msg.Offset

		if existing, ok := m.committedOffsets[partition]; ok {
			if offset <= existing {
				return nil, fmt.Errorf("commit offset %d <= existing %d for partition %d",
					offset, existing, partition)
			}
		}

		m.committedOffsets[partition] = offset
	}

	return messages, nil
}

// GetCommittedOffset returns the highest committed offset for a partition.
// Returns -1 if no commits have been made for that partition.
func (m *MockBroker[T]) GetCommittedOffset(partition int32) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if offset, ok := m.committedOffsets[partition]; ok {
		return offset
	}
	return -1
}

// GetCommittedOffsets returns a copy of all committed offsets.
func (m *MockBroker[T]) GetCommittedOffsets() map[int32]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[int32]int64, len(m.committedOffsets))
	for k, v := range m.committedOffsets {
		result[k] = v
	}
	return result
}

// PolledCount returns the number of messages that have been polled.
func (m *MockBroker[T]) PolledCount() int {
	return int(m.polledIndex.Load())
}

// TotalMessages returns the total number of messages in the broker.
func (m *MockBroker[T]) TotalMessages() int {
	return len(m.messages)
}

// IsSubscribed returns true if currently subscribed to a topic.
func (m *MockBroker[T]) IsSubscribed() bool {
	return m.subscribed.Load()
}

// ExtractEnvelope implements nexus.BrokerPort[T].
// For mocks, returns a basic envelope - tests can override via builder.WithExtractEnvelope.
func (m *MockBroker[T]) ExtractEnvelope(_ T) nexus.Envelope {
	return nexus.Envelope{}
}

// AckRebalance implements nexus.BrokerPort[T].
// No-op for mock broker.
func (m *MockBroker[T]) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

// BrokerQuery implements nexus.BrokerPort[T].
// No-op for mock broker.
func (m *MockBroker[T]) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

// ConsumerGroup implements nexus.BrokerPort[T].
// Returns empty string for mock broker (no consumer group).
func (m *MockBroker[T]) ConsumerGroup() string { return "" }

// Rebalancer is implemented by anything that can trigger a rebalance (e.g., demux.Consumer).
type Rebalancer interface {
	TriggerRebalance(nexus.RebalanceType, []nexus.RebalanceInfo) error
}

// MakeAssignAllPartitionsCallback creates a rebalance callback that assigns all partitions.
// Use with MockBroker.SetRebalanceCallback to simulate Kafka partition assignment.
func MakeAssignAllPartitionsCallback(t *testing.T, consumer Rebalancer, numPartitions int32) func() {
	return func() {
		time.Sleep(10 * time.Millisecond) //nolint:mnd // test timing constant
		rebalanceInfo := make([]nexus.RebalanceInfo, numPartitions)
		for i := int32(0); i < numPartitions; i++ {
			rebalanceInfo[i] = nexus.RebalanceInfo{Partition: i}
		}
		if err := consumer.TriggerRebalance(nexus.Assign, rebalanceInfo); err != nil {
			t.Errorf("TriggerRebalance failed: %v", err)
		}
	}
}
