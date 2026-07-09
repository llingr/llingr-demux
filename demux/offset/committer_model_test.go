// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package offset

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-nexus/nexus"
)

// This file is a model-based test harness for the offset committer: a virtual
// broker plus a virtual delivery pipeline drive the REAL committer through
// randomized interleavings of gapped offset streams, out-of-order completions,
// commit ticks, broker commit failures, revoke/assign cycles, other-owner
// progress while unassigned, and orphan completions (work items stranded by a
// previous ownership epoch, the drain-timeout shape). Named invariants are
// checked on every broker commit and at quiescence; see the list below.
//
// Baseline modes mirror the adapters: "real" (assignment offset lookup
// succeeded: resets carry the broker's committed offset) and "unknown"
// (resets carry -1). In unknown mode, orphans carrying First=true are not
// injected: a stale First re-anchoring an unknown baseline is the documented,
// accepted at-least-once residue (see DUPLICATES-VERDICT.md) and is exactly
// the case the real-baseline mode exists to close.
//
// The same harness backs the randomized property test here and the fuzz
// target in committer_model_fuzz_test.go.
//
// The harness runs in two concurrency modes. Deterministic (live=false): the
// committer's context is cancelled at construction so the ingest and ticker
// goroutines exit, and the model drives processCommit/flushGapBuffers directly
// under the committer mutex. Live (live=true): completions travel the real
// path (CollectAndCommit -> commitsIn -> ingest goroutine -> batch flush ->
// drained signal) with the auto-commit ticker running; ticks are just extra
// CommitOffsets and the invariants must hold under them. The live mode is
// intended to run under -race.
//
// The invariants, by name (used verbatim in failure messages):
//
//	commit monotonicity:  the broker's committed offset never moves backwards
//	commit ownership:     commits only arrive while the partition is assigned
//	eventual completeness: at quiescence every partition is committed to the
//	                      end of its stream (no silent stall)
//	quiescent drain:      at quiescence the gap buffer is empty and Ready nil
//	work item accounting: every borrowed work item is returned exactly once,
//	                      tolerating only the accepted compact-drop of
//	                      duplicate-offset completions
//	baseline agreement:   once an epoch has anchored, the tracker's baseline
//	                      equals the broker's committed position

// modelCollector is a deterministic ports.MetricsPort: it ends each work
// item's lifecycle by returning it to the pool, counting returns. Atomic:
// in live mode the ingest goroutine calls Collect.
type modelCollector struct {
	pool    *alloc.WorkItemsPool[string]
	returns atomic.Int64
}

func (c *modelCollector) Collect(workItem *ports.WorkItem[string]) {
	c.returns.Add(1)
	c.pool.Return(workItem)
}

// modelBroker captures commits and enforces commit monotonicity and commit
// ownership on every call. Mutex-guarded: in live mode the ticker, a drain,
// and the driver can all commit concurrently.
type modelBroker struct {
	mu            sync.Mutex
	fail          func(format string, args ...any)
	committedNext map[int32]int64 // next offset to read, per partition
	assigned      map[int32]bool  // the model's view of current ownership
	failNext      bool            // one-shot injected broker failure
	commitCalls   int
}

func (b *modelBroker) commitOffsets(messages []*nexus.Message[string]) ([]*nexus.Message[string], error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failNext {
		b.failNext = false
		return nil, fmt.Errorf("injected broker failure")
	}
	b.commitCalls++
	for _, message := range messages {
		next := message.Offset + 1
		if !b.assigned[message.Partition] {
			b.fail("commit ownership violated: commit for partition %d offset %d while unassigned",
				message.Partition, message.Offset)
		}
		if previous, ok := b.committedNext[message.Partition]; ok && next < previous {
			b.fail("commit monotonicity violated: partition %d committed offset moved backwards: %d -> %d",
				message.Partition, previous-1, message.Offset)
			continue
		}
		b.committedNext[message.Partition] = next
	}
	return messages, nil
}

func (b *modelBroker) committedNextFor(p int32) (int64, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	next, ok := b.committedNext[p]
	return next, ok
}

func (b *modelBroker) setCommittedNext(p int32, next int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.committedNext[p] = next
}

func (b *modelBroker) setAssigned(p int32, assigned bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.assigned[p] = assigned
}

func (b *modelBroker) armFailNext() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failNext = true
}

// disarmFailNext clears an armed one-shot failure, reporting whether it was
// still armed (i.e. no commit consumed it).
func (b *modelBroker) disarmFailNext() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	armed := b.failNext
	b.failNext = false
	return armed
}

// pendingDelivery is a delivered-but-not-completed record: the model's view of
// a work item in flight through the (virtual) worker pool.
type pendingDelivery struct {
	offset int64
	prev   int64
	first  bool
}

// partitionModel is one partition's stream and epoch state.
type partitionModel struct {
	stream        []int64 // gapped, strictly increasing offsets
	assigned      bool
	deliverIndex  int   // next stream index to deliver in the current epoch
	firstPending  bool  // next delivery carries the First flag
	prevDelivered int64 // predecessor tracking, as the prev package would stamp it
	inFlight      []pendingDelivery
	orphans       []pendingDelivery // stranded by a revoke, replayable any time
	completed     map[int64]bool    // offsets already pushed through the committer
}

// committerModel wires the real committer to the virtual broker and pipeline.
type committerModel struct {
	t            *testing.T
	oc           *Committer[string]
	pool         *alloc.WorkItemsPool[string]
	collector    *modelCollector
	broker       *modelBroker
	partitions   []*partitionModel
	realBaseline bool       // true = assignment offset lookup succeeded (resets carry real offsets)
	live         bool       // true = completions travel the real ingest path, ticker running
	stateMu      sync.Mutex // guards opLog + failed: broker.fail can arrive from the ticker goroutine
	borrows      int
	// duplicateInjections counts completions for an offset the committer has
	// already seen (orphan + redelivery of the same offset). When two such
	// copies meet in the gap buffer, sortGapBuffer's compact keeps one and
	// drops the other WITHOUT the metrics/pool epilogue - reviewed and accepted
	// as defensive handling of a should-never-happen input, so the accounting
	// invariant tolerates at most this many unreturned items.
	duplicateInjections int
	opLog               []string
	failed              bool
}

// makeGappedStream builds n strictly increasing offsets with random gaps
// (compaction / transaction-marker shaped).
func makeGappedStream(random *rand.Rand, n int) []int64 {
	stream := make([]int64, n)
	offset := int64(random.Intn(3)) // streams need not start at 0
	for i := 0; i < n; i++ {
		stream[i] = offset
		offset++
		if random.Intn(4) == 0 {
			offset += int64(1 + random.Intn(3)) // broker gap
		}
	}
	return stream
}

func newCommitterModel(t *testing.T, random *rand.Rand, partitionCount, streamLen int,
	realBaseline, live bool) *committerModel {
	t.Helper()

	model := &committerModel{t: t, realBaseline: realBaseline, live: live}
	model.broker = &modelBroker{
		fail:          model.failf,
		committedNext: make(map[int32]int64),
		assigned:      make(map[int32]bool),
	}

	cfg := config.DemuxConfig{AutoCommitInterval: 250 * time.Millisecond}
	cfg.SetDemuxConfigDefaults()
	model.pool = alloc.NewWorkItemsPool[string](cfg)
	model.collector = &modelCollector{pool: model.pool}

	// deterministic mode: cancel immediately so the ticker and ingest
	// goroutines exit before their first action and the model drives every
	// transition itself. Live mode: leave them running.
	ctx, cancel := context.WithCancel(context.Background())
	logger := &resetZeroCapturingLogger{} // captures, never prints
	model.oc = NewCommitter[string](ctx, cfg, model.broker.commitOffsets, model.collector, logger)
	if live {
		t.Cleanup(cancel)
	} else {
		cancel()
	}

	for p := 0; p < partitionCount; p++ {
		model.partitions = append(model.partitions, &partitionModel{
			stream:    makeGappedStream(random, streamLen),
			completed: make(map[int64]bool, streamLen),
		})
	}
	return model
}

// failf records an invariant violation with the trailing op log for repro.
func (m *committerModel) failf(format string, args ...any) {
	m.stateMu.Lock()
	if !m.failed {
		m.failed = true
		tail := m.opLog
		if len(tail) > 60 {
			tail = tail[len(tail)-60:]
		}
		m.t.Logf("op log tail: %v", tail)
	}
	m.stateMu.Unlock()
	m.t.Errorf(format, args...)
}

func (m *committerModel) logOp(format string, args ...any) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	m.opLog = append(m.opLog, fmt.Sprintf(format, args...))
}

func (m *committerModel) isFailed() bool {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	return m.failed
}

// ---- operations ----

// opAssign starts a new ownership epoch: baseline per mode, delivery resumes
// from the broker's committed position.
func (m *committerModel) opAssign(p int32) {
	partition := m.partitions[p]
	if partition.assigned {
		return
	}
	baseline := int64(-1)
	if next, ok := m.broker.committedNextFor(p); ok && m.realBaseline {
		baseline = next
	}
	m.logOp("assign(p%d baseline=%d)", p, baseline)

	m.oc.ResetCommittedOffsets(map[int32]int64{p: baseline})
	m.oc.MarkPartitionAssigned(p)
	m.broker.setAssigned(p, true)
	partition.assigned = true
	partition.firstPending = true
	partition.prevDelivered = -1

	// resume delivery from the broker's committed position (a real client
	// fetches from the committed offset; a gap there resolves to the next
	// existing offset)
	resume, _ := m.broker.committedNextFor(p) // zero if never committed
	partition.deliverIndex = 0
	for partition.deliverIndex < len(partition.stream) &&
		partition.stream[partition.deliverIndex] < resume {
		partition.deliverIndex++
	}
}

// opDeliver hands the next n records of the epoch to the virtual pipeline.
func (m *committerModel) opDeliver(p int32, n int) {
	partition := m.partitions[p]
	if !partition.assigned {
		return
	}
	for i := 0; i < n && partition.deliverIndex < len(partition.stream); i++ {
		offset := partition.stream[partition.deliverIndex]
		partition.deliverIndex++
		delivery := pendingDelivery{offset: offset, prev: partition.prevDelivered}
		if partition.firstPending {
			delivery.first = true
			delivery.prev = offset - 1 // synthetic, as prev.GetPrevious stamps it
			partition.firstPending = false
		}
		partition.prevDelivered = offset
		partition.inFlight = append(partition.inFlight, delivery)
	}
}

// completeBatch pushes the given completions through the committer. In
// deterministic mode: as the ingest loop would, a batch of processCommit
// calls then the batch-end flush, all under the committer mutex. In live
// mode: through the real path via CollectAndCommit, letting the ingest
// goroutine batch and flush on its own schedule.
func (m *committerModel) completeBatch(p int32, deliveries []pendingDelivery) {
	if len(deliveries) == 0 {
		return
	}
	now := time.Now()
	partition := m.partitions[p]
	if !m.live {
		m.oc.mu.Lock()
	}
	for _, delivery := range deliveries {
		if partition.completed[delivery.offset] {
			m.duplicateInjections++
		}
		partition.completed[delivery.offset] = true
		item := m.pool.Borrow()
		m.borrows++
		item.Message.Partition = p
		item.Message.Offset = delivery.offset
		item.Metrics.Partition = p
		item.Metrics.Offset = delivery.offset
		item.First = delivery.first
		item.PreviousOffset = delivery.prev
		if m.live {
			m.oc.CollectAndCommit(item)
		} else {
			m.oc.processCommit(item, now)
		}
	}
	if !m.live {
		m.oc.flushGapBuffers(now)
		m.oc.mu.Unlock()
	}
}

// opComplete completes k random in-flight deliveries (out of order).
func (m *committerModel) opComplete(random *rand.Rand, p int32, k int) {
	partition := m.partitions[p]
	if len(partition.inFlight) == 0 {
		return
	}
	if k > len(partition.inFlight) {
		k = len(partition.inFlight)
	}
	random.Shuffle(len(partition.inFlight), func(i, j int) {
		partition.inFlight[i], partition.inFlight[j] = partition.inFlight[j], partition.inFlight[i]
	})
	batch := append([]pendingDelivery{}, partition.inFlight[:k]...)
	partition.inFlight = partition.inFlight[k:]
	m.logOp("complete(p%d %v)", p, offsetsOf(batch))
	m.completeBatch(p, batch)
}

// opTick is an auto-commit tick.
func (m *committerModel) opTick() {
	m.logOp("tick")
	if err := m.oc.CommitOffsets(); err != nil {
		m.failf("unexpected CommitOffsets error: %v", err)
	}
}

// opBrokerFailTick injects a one-shot broker failure under a tick; the engine
// must surface it and retain state for retry.
func (m *committerModel) opBrokerFailTick() {
	m.logOp("brokerFailTick")
	m.broker.armFailNext()
	err := m.oc.CommitOffsets()
	if m.broker.disarmFailNext() {
		// nothing was ready to commit; the injected failure was never consumed
		return
	}
	if err == nil && !m.live {
		// live mode cannot attribute the failure: the ticker may have consumed
		// it (and logged it) instead of this call. Surfacing is pinned by the
		// deterministic mode.
		m.failf("a failed broker commit must be surfaced to the caller")
	}
}

// opRevoke ends the epoch: a random subset of in-flight work is stranded as
// orphans (the drain-timeout shape), the rest completes (the drain), the tail
// commit runs, then the partition is revoked.
func (m *committerModel) opRevoke(random *rand.Rand, p int32) {
	partition := m.partitions[p]
	if !partition.assigned {
		return
	}
	strand := 0
	if len(partition.inFlight) > 0 && random.Intn(3) == 0 {
		strand = 1 + random.Intn(min(3, len(partition.inFlight)))
	}
	random.Shuffle(len(partition.inFlight), func(i, j int) {
		partition.inFlight[i], partition.inFlight[j] = partition.inFlight[j], partition.inFlight[i]
	})
	orphans := partition.inFlight[:strand]
	drained := partition.inFlight[strand:]
	m.logOp("revoke(p%d drained=%v orphans=%v)", p, offsetsOf(drained), offsetsOf(orphans))

	m.completeBatch(p, drained) // drainWorkers: in-flight completions land
	if m.live {
		// the drain's real committer step: wait for the ingest to empty, then
		// the final commit
		if err := m.oc.DrainCommitter(time.NewTimer(5 * time.Second)); err != nil {
			m.failf("revoke drain: DrainCommitter returned: %v", err)
		}
	} else {
		m.opTick() // the drain's tail commit
	}
	m.oc.MarkPartitionRevoked(p)
	m.broker.setAssigned(p, false)
	partition.assigned = false
	partition.inFlight = nil

	for _, orphan := range orphans {
		if !m.realBaseline && orphan.first {
			continue // documented residue: stale First vs unknown baseline (see file header)
		}
		partition.orphans = append(partition.orphans, orphan)
	}
}

// opReplayOrphans delivers stranded completions from a closed epoch, assigned
// or not: the committer must discard or inertly buffer them, never commit
// them backwards and never stall on them.
func (m *committerModel) opReplayOrphans(random *rand.Rand, p int32, k int) {
	partition := m.partitions[p]
	if len(partition.orphans) == 0 {
		return
	}
	if k > len(partition.orphans) {
		k = len(partition.orphans)
	}
	random.Shuffle(len(partition.orphans), func(i, j int) {
		partition.orphans[i], partition.orphans[j] = partition.orphans[j], partition.orphans[i]
	})
	batch := append([]pendingDelivery{}, partition.orphans[:k]...)
	partition.orphans = partition.orphans[k:]
	m.logOp("orphans(p%d %v)", p, offsetsOf(batch))
	m.completeBatch(p, batch)
}

// opOtherOwnerAdvance simulates another consumer processing and committing the
// partition while it is away from this consumer.
func (m *committerModel) opOtherOwnerAdvance(random *rand.Rand, p int32) {
	partition := m.partitions[p]
	if partition.assigned {
		return
	}
	current, _ := m.broker.committedNextFor(p)
	from := 0
	for from < len(partition.stream) && partition.stream[from] < current {
		from++
	}
	if from >= len(partition.stream) {
		return
	}
	target := from + random.Intn(min(20, len(partition.stream)-from))
	m.broker.setCommittedNext(p, partition.stream[target]+1)
	m.logOp("otherOwner(p%d ->%d)", p, partition.stream[target]+1)
}

// ---- quiescence ----

// quiesce drives every partition to the end of its stream, replays every
// orphan, then asserts eventual completeness, quiescent drain, baseline
// agreement, and work item accounting.
func (m *committerModel) quiesce(random *rand.Rand) {
	m.broker.disarmFailNext()
	for p := range m.partitions {
		partition := m.partitions[p]
		m.opAssign(int32(p))
		for partition.deliverIndex < len(partition.stream) ||
			len(partition.inFlight) > 0 || len(partition.orphans) > 0 {
			m.opDeliver(int32(p), 1+random.Intn(8))
			m.opReplayOrphans(random, int32(p), 1+random.Intn(2))
			m.opComplete(random, int32(p), 1+random.Intn(8))
		}
	}
	if m.live {
		// everything is enqueued; wait for the real ingest to finish before
		// the final commit and the state asserts
		if err := m.oc.DrainCommitter(time.NewTimer(5 * time.Second)); err != nil {
			m.failf("quiesce: DrainCommitter returned: %v", err)
		}
	}
	m.opTick()

	for p := range m.partitions {
		partition := m.partitions[p]
		wantNext := partition.stream[len(partition.stream)-1] + 1
		if got, _ := m.broker.committedNextFor(int32(p)); got != wantNext {
			m.failf("eventual completeness violated: partition %d committed next = %d, want %d (stalled)",
				p, got, wantNext)
		}
		m.oc.mu.Lock()
		tracker := m.oc.offsetsByPartition.PartitionMap[int32(p)]
		if len(tracker.GapBuffer) != 0 {
			m.failf("quiescent drain violated: partition %d gap buffer holds %d item(s) at quiescence",
				p, len(tracker.GapBuffer))
		}
		if tracker.Ready != nil {
			m.failf("quiescent drain violated: partition %d Ready non-nil at quiescence", p)
		}
		// An epoch that saw no completions (re-assigned after the stream ended)
		// legitimately keeps the unknown baseline: there is no First record to
		// anchor it. Internal/external agreement is only required once the
		// epoch has anchored.
		if tracker.CommittedPlusOne != wantNext && tracker.CommittedPlusOne != -1 {
			m.failf("baseline agreement violated: partition %d CommittedPlusOne=%d, broker=%d",
				p, tracker.CommittedPlusOne, wantNext)
		}
		m.oc.mu.Unlock()
	}

	returns := int(m.collector.returns.Load())
	unreturned := m.borrows - returns
	if unreturned < 0 {
		m.failf("work item accounting violated: %d returns exceed %d borrows (double-return)",
			returns, m.borrows)
	}
	if unreturned > m.duplicateInjections {
		m.failf("work item accounting violated: %d work items unreturned but only %d duplicate "+
			"injections (the accepted compact-drop cannot explain the rest: leak)",
			unreturned, m.duplicateInjections)
	}
}

// step performs one random operation.
func (m *committerModel) step(random *rand.Rand) {
	p := int32(random.Intn(len(m.partitions)))
	switch random.Intn(10) {
	case 0, 1:
		m.opDeliver(p, 1+random.Intn(6))
	case 2, 3, 4:
		m.opComplete(random, p, 1+random.Intn(6))
	case 5:
		m.opTick()
	case 6:
		m.opRevoke(random, p)
	case 7:
		m.opAssign(p)
	case 8:
		if random.Intn(4) == 0 {
			m.opBrokerFailTick()
		} else {
			m.opOtherOwnerAdvance(random, p)
		}
	case 9:
		m.opReplayOrphans(random, p, 1+random.Intn(2))
	}
}

func offsetsOf(deliveries []pendingDelivery) []int64 {
	offsets := make([]int64, len(deliveries))
	for i, d := range deliveries {
		offsets[i] = d.offset
	}
	return offsets
}

// runCommitterModel is the shared entry point for the property tests and the
// fuzz target: fixed shape, seeded ops, quiescence asserts.
func runCommitterModel(t *testing.T, seed int64, steps int, realBaseline, live bool) {
	t.Helper()
	random := rand.New(rand.NewSource(seed)) //nolint:gosec // deterministic model, not crypto
	model := newCommitterModel(t, random, 4, 120, realBaseline, live)
	for i := 0; i < steps && !model.isFailed(); i++ {
		model.step(random)
	}
	if !model.isFailed() {
		model.quiesce(random)
	}
	if model.isFailed() {
		t.Logf("reproduce with: seed=%d steps=%d realBaseline=%v live=%v", seed, steps, realBaseline, live)
	}
}

// TestCommitterModel_RandomizedInvariants is the model-based property test:
// many seeds, both baseline modes, hundreds of interleaved operations each.
//
//nolint:paralleltest // subtests capture loop vars explicitly
func TestCommitterModel_RandomizedInvariants(t *testing.T) {
	seeds := 32
	if testing.Short() {
		seeds = 6
	}
	for _, realBaseline := range []bool{true, false} {
		realBaseline := realBaseline
		mode := "unknownBaseline"
		if realBaseline {
			mode = "realBaseline"
		}
		t.Run(mode, func(t *testing.T) {
			for seed := int64(1); seed <= int64(seeds); seed++ {
				seed := seed
				t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
					t.Parallel()
					runCommitterModel(t, seed, 400, realBaseline, false)
				})
			}
		})
	}
}

// TestCommitterModel_LiveIngestInvariants runs the same model with completions
// travelling the real concurrent path: CollectAndCommit -> commitsIn -> ingest
// goroutine -> batch flush -> drained signal, with the auto-commit ticker
// running throughout. Intended to run under -race: it pins the ingest/tick/
// drain interleavings the deterministic mode cannot reach.
//
//nolint:paralleltest // subtests capture loop vars explicitly
func TestCommitterModel_LiveIngestInvariants(t *testing.T) {
	seeds := 16
	if testing.Short() {
		seeds = 4
	}
	for _, realBaseline := range []bool{true, false} {
		realBaseline := realBaseline
		mode := "unknownBaseline"
		if realBaseline {
			mode = "realBaseline"
		}
		t.Run(mode, func(t *testing.T) {
			for seed := int64(1); seed <= int64(seeds); seed++ {
				seed := seed
				t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
					t.Parallel()
					runCommitterModel(t, seed, 400, realBaseline, true)
				})
			}
		})
	}
}
