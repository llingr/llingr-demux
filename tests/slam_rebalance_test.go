// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// TestSlamRebalanceChurn exercises the full real pipeline under the
// cooperative-rebalance topology the TLA+ models do not cover: revoke/assign
// cycles arrive on a SEPARATE goroutine (a broker client's callback goroutine)
// while the polling loop keeps running, every offset carries a broker gap
// (stride 5: the compaction/aborted-transaction shape, universally), processing
// is jittered across a small worker pool, and revoked partitions are
// redelivered from their committed offset exactly as a real broker would.
//
// Assignments carry the partition's real committed offset (an adapter's
// assignment offset lookup, the production default). Zero-duplicate
// PROCESSING is not asserted: a record dispatched in the razor between a
// revoke starting and the drain's worker scan is legitimate at-least-once
// redelivery. What must hold regardless of churn:
//
//	commit monotonicity: a partition's committed offset never moves backwards
//	eventual completeness: every partition commits to the end of its stream
//	                       despite continuous churn (no stall)
type churnBroker struct {
	t *testing.T

	mu            sync.Mutex
	streams       map[int32][]slamMessage
	assigned      map[int32]bool
	nextIndex     map[int32]int   // next stream index to deliver, per partition
	committedNext map[int32]int64 // next offset to read, per partition
	rotate        int32           // round-robin partition scan position

	subscribed    atomic.Bool
	rebalanceFunc func()
}

func newChurnBroker(t *testing.T, partitions, messagesPerPartition, keys int) *churnBroker {
	broker := &churnBroker{
		t:             t,
		streams:       make(map[int32][]slamMessage),
		assigned:      make(map[int32]bool),
		nextIndex:     make(map[int32]int),
		committedNext: make(map[int32]int64),
	}
	for p := 0; p < partitions; p++ {
		stream := make([]slamMessage, messagesPerPartition)
		offset := int64(p * 7) // partitions need not start at offset 0
		for i := 0; i < messagesPerPartition; i++ {
			stream[i] = slamMessage{
				ID:        p*messagesPerPartition + i,
				Key:       fmt.Sprintf("key-%d-%d", p, i%keys),
				Partition: int32(p),
				Offset:    offset,
			}
			// every offset jumps by five: no record is predecessor+1 anywhere
			// (log compaction / aborted transactions), so revoke tail commits
			// and re-assign baselines always land on non-contiguous boundaries
			offset += 5
		}
		broker.streams[int32(p)] = stream
	}
	return broker
}

func (b *churnBroker) Subscribe() error {
	b.subscribed.Store(true)
	if b.rebalanceFunc != nil {
		go b.rebalanceFunc()
	}
	return nil
}

func (b *churnBroker) Unsubscribe() error {
	b.subscribed.Store(false)
	return nil
}

// Poll delivers the next record of any assigned partition, round-robin.
func (b *churnBroker) Poll(timeout time.Duration) (slamMessage, bool, error) {
	b.mu.Lock()
	partitionCount := int32(len(b.streams))
	for scanned := int32(0); scanned < partitionCount; scanned++ {
		p := (b.rotate + scanned) % partitionCount
		if !b.assigned[p] {
			continue
		}
		index := b.nextIndex[p]
		if index >= len(b.streams[p]) {
			continue
		}
		b.nextIndex[p] = index + 1
		b.rotate = p + 1
		message := b.streams[p][index]
		b.mu.Unlock()
		return message, true, nil
	}
	b.mu.Unlock()
	time.Sleep(timeout)
	return slamMessage{}, false, nil
}

// CommitOffsets records commits and enforces commit monotonicity: a real
// broker accepts rewinds, so the guard must live in the engine and any
// backward commit reaching this mock is an engine failure.
func (b *churnBroker) CommitOffsets(messages []*nexus.Message[slamMessage]) ([]*nexus.Message[slamMessage], error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, message := range messages {
		next := message.Offset + 1
		if previous, ok := b.committedNext[message.Partition]; ok && next < previous {
			b.t.Errorf("commit monotonicity violated: partition %d committed offset moved backwards: %d -> %d",
				message.Partition, previous-1, message.Offset)
			continue
		}
		b.committedNext[message.Partition] = next
	}
	return messages, nil
}

func (b *churnBroker) ExtractEnvelope(message slamMessage) nexus.Envelope {
	return nexus.Envelope{
		Partition: message.Partition,
		Offset:    message.Offset,
		Key:       message.Key,
		Ctx:       context.Background(),
	}
}

func (b *churnBroker) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

func (b *churnBroker) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

func (b *churnBroker) ConsumerGroup() string { return "slam-rebalance-churn" }

// stopDelivery marks a partition unassigned; already-polled records may still
// be dispatching, which is exactly the at-least-once razor under test.
func (b *churnBroker) stopDelivery(p int32) {
	b.mu.Lock()
	b.assigned[p] = false
	b.mu.Unlock()
}

// resumeDelivery re-assigns a partition, resuming from its committed offset
// like a real broker: the uncommitted tail is redelivered.
func (b *churnBroker) resumeDelivery(p int32) (committedOffset int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	committed, ok := b.committedNext[p]
	if !ok {
		committed = -1
	}
	index := 0
	stream := b.streams[p]
	for index < len(stream) && stream[index].Offset < committed {
		index++
	}
	b.nextIndex[p] = index
	b.assigned[p] = true
	return committed
}

func (b *churnBroker) allComplete() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for p, stream := range b.streams {
		want := stream[len(stream)-1].Offset + 1
		if b.committedNext[p] != want {
			return false
		}
	}
	return true
}

func TestSlamRebalanceChurn(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	partitions := 8
	messagesPerPartition := 4000
	minChurnCycles := 100
	if testing.Short() {
		partitions = 4
		messagesPerPartition = 1000
		minChurnCycles = 25
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := newChurnBroker(t, partitions, messagesPerPartition, 40)

	var metricsCount atomic.Int64
	processMessage := func(_ context.Context, _ *nexus.Message[slamMessage]) error {
		time.Sleep(time.Duration(rand.Intn(500)) * time.Microsecond) //nolint:gosec // test jitter
		return nil
	}
	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}
	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		metricsCount.Add(1)
		return nil
	}

	cfg := config.DemuxConfig{
		ConcurrentKeys:     4,
		WorkerShardsCount:  4,
		AutoCommitInterval: 100 * time.Millisecond,
		PollTimeout:        2 * time.Millisecond,
	}

	consumer := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(mocklogger.NewRecordingLogger()).
		Build(broker)

	// initial assignment of every partition, from the broker's callback
	// goroutine, with real committed offsets (none yet: -1)
	initialAssignDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		time.Sleep(5 * time.Millisecond)
		info := make([]nexus.RebalanceInfo, partitions)
		for p := 0; p < partitions; p++ {
			committed := broker.resumeDelivery(int32(p))
			info[p] = nexus.RebalanceInfo{
				RebalanceType:   nexus.Assign,
				TopicName:       topicName,
				Partition:       int32(p),
				CommittedOffset: committed,
			}
		}
		if err := consumer.TriggerRebalance(nexus.Assign, info); err != nil {
			t.Errorf("initial TriggerRebalance failed: %v", err)
		}
		close(initialAssignDone)
	}

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	select {
	case <-initialAssignDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the initial assignment")
	}

	// The churn goroutine: a broker client's callback topology. Each cycle revokes
	// one partition (drain + tail commit run synchronously inside
	// TriggerRebalance on THIS goroutine, concurrent with the polling loop),
	// pauses, then re-assigns it with its real committed offset.
	churnDone := make(chan struct{})
	churnCtx, stopChurn := context.WithCancel(context.Background())
	defer stopChurn()
	var churnCycles atomic.Int64
	go func() {
		defer close(churnDone)
		random := rand.New(rand.NewSource(20260707)) //nolint:gosec // deterministic churn
		for churnCtx.Err() == nil {
			p := int32(random.Intn(partitions))
			broker.stopDelivery(p)
			revoke := []nexus.RebalanceInfo{{
				RebalanceType: nexus.Revoke, TopicName: topicName, Partition: p, CommittedOffset: -1,
			}}
			if err := consumer.TriggerRebalance(nexus.Revoke, revoke); err != nil {
				t.Errorf("TriggerRebalance(Revoke, p%d) failed: %v", p, err)
				return
			}
			time.Sleep(time.Duration(1+random.Intn(4)) * time.Millisecond)

			committed := broker.resumeDelivery(p)
			assign := []nexus.RebalanceInfo{{
				RebalanceType: nexus.Assign, TopicName: topicName, Partition: p, CommittedOffset: committed,
			}}
			if err := consumer.TriggerRebalance(nexus.Assign, assign); err != nil {
				t.Errorf("TriggerRebalance(Assign, p%d) failed: %v", p, err)
				return
			}
			churnCycles.Add(1)
			time.Sleep(time.Duration(1+random.Intn(5)) * time.Millisecond)
		}
	}()

	// eventual completeness: every partition reaches the end of its stream
	// while the churn keeps running
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) && !broker.allComplete() && !t.Failed() {
		time.Sleep(25 * time.Millisecond)
	}
	stopChurn()
	<-churnDone

	if !broker.allComplete() {
		broker.mu.Lock()
		for p, stream := range broker.streams {
			want := stream[len(stream)-1].Offset + 1
			if got := broker.committedNext[p]; got != want {
				t.Errorf("eventual completeness violated: partition %d committed next = %d, want %d (stalled under churn)",
					p, got, want)
			}
		}
		broker.mu.Unlock()
	}

	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}

	total := int64(partitions * messagesPerPartition)
	if got := metricsCount.Load(); got < total {
		t.Errorf("only %d of at least %d work item lifecycles completed", got, total)
	}
	// the test is only meaningful if the churn genuinely interleaved with
	// live traffic; guard against sizing drift making it a no-op
	if got := churnCycles.Load(); got < int64(minChurnCycles) {
		t.Errorf("only %d churn cycles interleaved with the stream, want >= %d (retune sizing)",
			got, minChurnCycles)
	}
	t.Logf("slam_rebalance_test.go: %d partitions x %d messages, %d revoke/assign cycles interleaved: %d lifecycles (>= %d expected, redeliveries included)",
		partitions, messagesPerPartition, churnCycles.Load(), metricsCount.Load(), total)
}
