// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

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

// slamMessage is the payload type for slam tests.
type slamMessage struct {
	ID          int
	Key         string
	Partition   int32
	Offset      int64
	DelayMicros int64 // processing delay in microseconds (0 = use callback default)
}

// commitRecord captures a single CommitOffsets call for verification.
type commitRecord struct {
	partition int32
	offset    int64
}

// slamBroker is a minimal BrokerPort implementation for slam testing.
// It feeds pre-generated messages via Poll and captures every CommitOffsets call
// for detailed post-test assertions.
type slamBroker struct {
	messages  []slamMessage
	pollIndex atomic.Int64

	mu             sync.Mutex
	commitHistory  []commitRecord // every individual offset committed, in order
	highestCommit  map[int32]int64
	duplicateFound bool

	subscribed      atomic.Bool
	subscribeCalled chan struct{}
	rebalanceFunc   func()
}

func newSlamBroker(messages []slamMessage) *slamBroker {
	return &slamBroker{
		messages:        messages,
		highestCommit:   make(map[int32]int64),
		subscribeCalled: make(chan struct{}),
	}
}

func (b *slamBroker) Subscribe() error {
	b.subscribed.Store(true)
	if b.rebalanceFunc != nil {
		go func() {
			b.rebalanceFunc()
			close(b.subscribeCalled)
		}()
	} else {
		// nothing to wait for: unblock delivery immediately so the Poll gate
		// cannot starve a subscriber that configured no rebalance callback
		close(b.subscribeCalled)
	}
	return nil
}

func (b *slamBroker) Unsubscribe() error {
	b.subscribed.Store(false)
	return nil
}

func (b *slamBroker) Poll(timeout time.Duration) (slamMessage, bool, error) {
	// No delivery before the assignment is processed: real broker clients only
	// fetch after assignment. Early delivery races the first commit tick, whose
	// ownership guard discards the completions; this mock never redelivers, so
	// the offset chain would break permanently (latent flake this gate closes).
	select {
	case <-b.subscribeCalled:
	default:
		time.Sleep(timeout)
		return slamMessage{}, false, nil
	}

	idx := int(b.pollIndex.Add(1) - 1)
	if idx >= len(b.messages) {
		time.Sleep(timeout)
		return slamMessage{}, false, nil
	}
	return b.messages[idx], true, nil
}

func (b *slamBroker) CommitOffsets(messages []*nexus.Message[slamMessage]) ([]*nexus.Message[slamMessage], error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, msg := range messages {
		rec := commitRecord{partition: msg.Partition, offset: msg.Offset}
		b.commitHistory = append(b.commitHistory, rec)

		if prev, ok := b.highestCommit[msg.Partition]; ok {
			if msg.Offset <= prev {
				b.duplicateFound = true
			}
		}
		b.highestCommit[msg.Partition] = msg.Offset
	}
	return messages, nil
}

func (b *slamBroker) ExtractEnvelope(msg slamMessage) nexus.Envelope {
	return nexus.Envelope{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Key:       msg.Key,
		Ctx:       context.Background(),
	}
}

func (b *slamBroker) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

func (b *slamBroker) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

func (b *slamBroker) ConsumerGroup() string { return "slam-test" }

// getCommitResults returns a snapshot of commit state for assertions.
func (b *slamBroker) getCommitResults() (history []commitRecord, highest map[int32]int64, hasDuplicate bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	h := make([]commitRecord, len(b.commitHistory))
	copy(h, b.commitHistory)

	m := make(map[int32]int64, len(b.highestCommit))
	for k, v := range b.highestCommit {
		m[k] = v
	}
	return h, m, b.duplicateFound
}

// generateSlamMessages creates N messages with K distinct keys on partition 0.
// Keys are distributed round-robin: key-0, key-1, ..., key-(K-1), key-0, ...
//
// EVERY offset jumps by five (0, 5, 10, ...), the non-contiguous shape of
// compacted topics and aborted transactions: no record is predecessor+1, so
// any "+1" offset arithmetic in the commit path breaks on the first record.
// Contiguity must come from predecessor linkage, and a commit tick that
// consumes Ready must not stall the committer (the successor's completion can
// never equal CommittedPlusOne, so Ready must be re-initialised from the gap
// buffer).
func generateSlamMessages(n, k int) []slamMessage {
	msgs := make([]slamMessage, n)
	offset := int64(0)
	for i := 0; i < n; i++ {
		msgs[i] = slamMessage{
			ID:        i,
			Key:       fmt.Sprintf("key-%d", i%k),
			Partition: 0,
			Offset:    offset,
		}
		offset += 5
	}
	return msgs
}

// TestSlam exercises the real demux pipeline with thousands of messages across varying
// key counts and processing delays. Every message must make it through the pipeline,
// offsets must be committed correctly with monotonic advances, and no races are tolerated.
//
// 7 key counts x 4 message counts = 28 scenarios, all run in parallel for maximum stress.
func TestSlam(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	keyCounts := []int{1, 2, 3, 5, 10, 50, 250}
	msgCounts := []int{10, 100, 1000, 10000}

	for _, keys := range keyCounts {
		for _, msgs := range msgCounts {
			keys, msgs := keys, msgs // capture loop vars
			t.Run(fmt.Sprintf("keys=%d_msgs=%d", keys, msgs), func(t *testing.T) {
				t.Parallel()
				runSlamScenario(t, keys, msgs)
			})
		}
	}
}

// TestSlamWild throws 2 million messages across multiple partitions with random keys,
// random processing delays, and everything fighting through guard=4, overflow=2.
// Verifies every message is committed with correct monotonic offsets per partition.
func TestSlamWild(t *testing.T) {
	t.Setenv(config.SkipValidationEnvVar, "true")

	const (
		totalMessages = 2_000_000
		numPartitions = 12
		numKeys       = 500
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Generate 2M messages spread across 12 partitions with 500 random keys.
	// Every offset jumps by five (compaction / aborted transactions): no
	// record is predecessor+1 on any partition.
	messages := make([]slamMessage, totalMessages)
	partitionOffsets := make([]int64, numPartitions)
	highestOffset := make([]int64, numPartitions)
	for i := 0; i < totalMessages; i++ {
		p := int32(i % numPartitions)
		messages[i] = slamMessage{
			ID:        i,
			Key:       fmt.Sprintf("key-%d", rand.Intn(numKeys)),
			Partition: p,
			Offset:    partitionOffsets[p],
		}
		highestOffset[p] = partitionOffsets[p]
		partitionOffsets[p] += 5
	}

	broker := newSlamBroker(messages)

	var metricsCount atomic.Int64

	processMessage := func(_ context.Context, _ *nexus.Message[slamMessage]) error {
		// Wild: 0-500µs random delay - fast enough to not take forever at 2M messages
		time.Sleep(time.Duration(rand.Intn(500)) * time.Microsecond) //nolint:gosec
		return nil
	}

	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		metricsCount.Add(1)
		return nil
	}

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:     4,
		WorkerShardsCount:  4,
		AutoCommitInterval: 100 * time.Millisecond,
		PollTimeout:        5 * time.Millisecond,
	}

	builder := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(logger)

	consumer := builder.Build(broker)

	// Assign all 12 partitions
	rebalanceDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		time.Sleep(10 * time.Millisecond)
		partitions := make([]nexus.RebalanceInfo, numPartitions)
		for i := 0; i < numPartitions; i++ {
			partitions[i] = nexus.RebalanceInfo{Partition: int32(i)}
		}
		if err := consumer.TriggerRebalance(nexus.Assign, partitions); err != nil {
			t.Errorf("TriggerRebalance failed: %v", err)
		}
		close(rebalanceDone)
	}

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	select {
	case <-rebalanceDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for rebalance")
	}

	// Wait for all 2M messages - allow up to 10 minutes
	startTime := time.Now()
	deadline := time.Now().Add(10 * time.Minute)
	lastLog := time.Now()
	for time.Now().Before(deadline) {
		count := metricsCount.Load()
		if count >= int64(totalMessages) {
			break
		}
		if time.Since(lastLog) > 5*time.Second {
			t.Logf("progress: %d / %d (%.1f%%)", count, totalMessages, float64(count)/float64(totalMessages)*100)
			lastLog = time.Now()
		}
		time.Sleep(50 * time.Millisecond)
	}

	if count := metricsCount.Load(); count < int64(totalMessages) {
		t.Fatalf("timed out: only %d of %d messages processed", count, totalMessages)
	}

	processingTime := time.Since(startTime)

	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}

	time.Sleep(500 * time.Millisecond) // final commit cycle

	// --- Assertions ---
	history, highest, hasDuplicate := broker.getCommitResults()

	// 1. Every partition's final offset matches the last generated offset
	// (offsets are strided, so take it from the stream, not a count)
	for p := int32(0); p < numPartitions; p++ {
		expected := highestOffset[p]
		if got, ok := highest[p]; !ok {
			t.Errorf("partition %d: no commits recorded", p)
		} else if got != expected {
			t.Errorf("partition %d: final committed offset = %d, want %d", p, got, expected)
		}
	}

	// 2. No duplicate or regressing commits
	if hasDuplicate {
		t.Errorf("duplicate or regressing commit detected")
	}

	// 3. Monotonic per partition
	prevByPartition := make(map[int32]int64)
	for i, rec := range history {
		if prev, ok := prevByPartition[rec.partition]; ok {
			if rec.offset <= prev {
				t.Errorf("commit %d: partition %d offset %d <= previous %d",
					i, rec.partition, rec.offset, prev)
				break
			}
		}
		prevByPartition[rec.partition] = rec.offset
	}

	// 4. All messages accounted for
	if count := metricsCount.Load(); count != int64(totalMessages) {
		t.Errorf("metrics count = %d, want %d", count, totalMessages)
	}

	msgsPerSec := float64(totalMessages) / processingTime.Seconds()
	t.Logf("slam_test.go: WILD - %d messages, %d partitions, %d keys, guard=4, overflow=2: completed in %v (%.0f msg/sec)",
		totalMessages, numPartitions, numKeys, processingTime.Round(time.Millisecond), msgsPerSec)
}

func runSlamScenario(t *testing.T, numKeys, numMessages int) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := generateSlamMessages(numMessages, numKeys)
	broker := newSlamBroker(messages)

	// Track processed count via metrics sink
	var metricsCount atomic.Int64

	processMessage := func(_ context.Context, _ *nexus.Message[slamMessage]) error {
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Microsecond) //nolint:gosec // G404: test jitter
		return nil
	}

	writeDeadLetter := func(_ context.Context, _ *nexus.Message[slamMessage], _ error) error {
		return nil
	}

	metricsSink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		metricsCount.Add(1)
		return nil
	}

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:     4,
		WorkerShardsCount:  4,
		AutoCommitInterval: 100 * time.Millisecond,
		PollTimeout:        5 * time.Millisecond,
	}

	builder := demux.NewBuilder[slamMessage](topicName, processMessage, writeDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(metricsSink).
		WithExtractEnvelope(broker.ExtractEnvelope).
		WithOverflowGuard(make(chan struct{}, 2)).
		WithLogger(logger)

	consumer := builder.Build(broker)

	// Configure rebalance: assign partition 0
	rebalanceDone := make(chan struct{})
	broker.rebalanceFunc = func() {
		time.Sleep(10 * time.Millisecond) //nolint:mnd // simulate broker delay
		if err := consumer.TriggerRebalance(nexus.Assign, []nexus.RebalanceInfo{
			{Partition: 0},
		}); err != nil {
			t.Errorf("TriggerRebalance failed: %v", err)
		}
		close(rebalanceDone)
	}

	// Subscribe and start processing
	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	select {
	case <-rebalanceDone:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for rebalance")
	}

	// Wait for all messages to be processed (metrics count reaches N)
	startTime := time.Now()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if metricsCount.Load() >= int64(numMessages) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if count := metricsCount.Load(); count < int64(numMessages) {
		t.Fatalf("timed out: only %d of %d messages processed", count, numMessages)
	}

	// Graceful shutdown: drains in-flight work and performs final commits
	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}

	// Brief pause for final commit cycle
	time.Sleep(200 * time.Millisecond)

	elapsed := time.Since(startTime)

	// --- Assertions ---
	history, highest, hasDuplicate := broker.getCommitResults()

	// 1. Final committed offset must equal highest offset in partition
	// (offsets are gapped, so take it from the generated stream, not numMessages-1)
	expectedHighest := messages[len(messages)-1].Offset
	if got, ok := highest[0]; !ok {
		t.Errorf("partition 0: no commits recorded")
	} else if got != expectedHighest {
		t.Errorf("partition 0: final committed offset = %d, want %d", got, expectedHighest)
	}

	// 2. No duplicate commits (offset never regresses or repeats)
	if hasDuplicate {
		t.Errorf("duplicate or regressing commit detected in partition 0")
	}

	// 3. Monotonic: per-partition committed offsets must strictly increase over time
	prevByPartition := make(map[int32]int64)
	for i, rec := range history {
		if prev, ok := prevByPartition[rec.partition]; ok {
			if rec.offset <= prev {
				t.Errorf("commit %d: partition %d offset %d <= previous %d (not monotonic)",
					i, rec.partition, rec.offset, prev)
				break // one failure is enough to diagnose
			}
		}
		prevByPartition[rec.partition] = rec.offset
	}

	// 4. All messages accounted for via metrics
	if count := metricsCount.Load(); count != int64(numMessages) {
		t.Errorf("metrics count = %d, want %d", count, numMessages)
	}

	t.Logf("slam_test.go: keys=%d msgs=%d: all %d messages committed in %v",
		numKeys, numMessages, numMessages, elapsed.Round(time.Millisecond))
}
