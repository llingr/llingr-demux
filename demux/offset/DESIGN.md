# Offset Committer Design

This document explains the offset commit pipeline philosophy, aimed at engineers new to the
codebase who need to understand how message ordering and commit safety works.

## Messages Polled and Rebalance Events are Sufficient

A naive approach to offset tracking might query the broker before each commit: "what's
currently committed?". This works but adds a synchronous round-trip to every commit cycle -
latency that compounds under load.

The broker *implicitly* tells you its committed position through the messages it delivers.
When the broker sends offset 100, it's saying: "offsets 0-99 are committed (or don't exist),
here's what's next". By tracking `previousOffset` as messages flow through the polling loop,
we maintain complete offset continuity without ever asking the broker.

## Message Flow: Poll to Commit

```text
Broker
  │
  ▼ Poll (delivers messages with offsets)
Adapter
  │ extracts: partition, offset, key, previousOffset
  ▼
Pipeline Processor
  │ routes by key to per-key workers
  ▼
Worker (N concurrent)
  │ calls ProcessMessage, handles errors
  ▼
Offset Committer
  │ re-multiplexes back to per-partition tracking
  │ advances high-watermark when contiguous
  ▼
Broker (commit)
```

### The previousOffset Field

Each `WorkItem` carries a `previousOffset` field set at poll time - the offset of the
message that preceded it in the broker's delivery. For a normal sequence:

```text
offset=100, previousOffset=99
offset=101, previousOffset=100
offset=102, previousOffset=101
```

This creates a linked chain. The committer only advances the high-watermark when a message's
`previousOffset` matches the current `Ready` offset - proving contiguous completion.

### Legitimate Gaps

Kafka doesn't guarantee consecutive offset numbers. Several scenarios create gaps:

- **Control records**: Internal Kafka bookkeeping (not delivered to consumers)
- **Log compaction**: Superseded records deleted, leaving gaps
- **Transaction markers**: Commit/abort markers consume offsets but aren't delivered
- **Aborted transactions**: Messages from rolled-back transactions are skipped

The broker handles this by setting `previousOffset` to the *logical* predecessor, not
`offset - 1`. Example with control records at offsets 3-9:

```text
offset=0,  previousOffset=-1 (first)
offset=1,  previousOffset=0
offset=2,  previousOffset=1
offset=10, previousOffset=2   ← gap is legitimate, previousOffset proves it
offset=11, previousOffset=10
```

The committer sees `previousOffset=2` and knows offset 10 is contiguous with offset 2 -
no gap buffer needed, immediate advancement.

## Committer Architecture

### Ingest Loop

The committer runs two concurrent goroutines:

1. **Ingest loop** (`startIngestLoop`) - receives WorkItems from the pipeline
2. **Async commit loop** (`startAsyncCommits`) - periodically sends commits to broker

The ingest loop processes in batches to amortise mutex access:

```go
const readBatchSize = 1000  // amortises mutex lock access

for {
    select {
    case workItem := <-oc.commitsIn:
        oc.processCommit(workItem, now)
        messageCount++
    default:
        // channel empty, release mutex and yield
    }
    if messageCount > readBatchSize {
        // batch complete, release mutex and yield to commit loop
    }
}
```

The `ingestGuard` channel coordinates between ingest and commit - only one can hold the
mutex at a time, preventing commit starvation under high ingest load.

### Per-Partition Tracking

Each partition has an `OffsetsTracker`:

```go
type OffsetsTracker[T any] struct {
    Ready               *nexus.WorkItem[T]   // next offset to commit
    CommittedPlusOne    int64                // broker's expected next offset
    LastCommittedOffset int64                // last record committed THIS epoch, -1 when none
    GapBuffer           []*nexus.WorkItem[T] // out-of-order completions
    NeedsGapAdvance     bool                 // deferred sort+walk flag, consumed by the flush
    Assignment          nexus.RebalanceType  // Assign or Revoke
}
```

**Ready**: The single WorkItem at the current high-watermark, awaiting the next commit tick.
When a commit succeeds, `Ready` is collected for metrics, `CommittedPlusOne` advances
(monotonically - a commit never lowers it), and `LastCommittedOffset` records the committed
record's offset.

**LastCommittedOffset**: The only anchor predecessor linkage may promote against when Ready
is empty. `CommittedPlusOne` is a *position*, not a record: after log compaction or an
aborted transaction the offset below a reset-derived baseline need not exist, so deriving a
linkage anchor arithmetically (`CommittedPlusOne - 1`) would promote stale items on
coincidence. The field is set only by an actual commit success this epoch and reset to -1
by the rebalance assign's baseline reset (the only caller of `ResetCommittedOffsets`).

**GapBuffer**: Holds WorkItems that completed out-of-order. When the gap fills, items are
sorted and walked through to advance `Ready`.

### Gap Buffer Strategy

The gap buffer uses flag-deferred resolution to avoid unnecessary sorting: the expensive
sort+walk (`flushGapBuffers`) runs only for trackers whose `NeedsGapAdvance` flag is set,
once per ingest batch and once per commit tick.

The flag is set only where it can matter, by the site that knows why:

- **Arrival** (Ready occupied): after a swap, a quick scan checks whether the new Ready's
  successor is already buffered.
- **Arrival** (Ready empty): only when the arriving item itself satisfies a
  Ready-initialisation rule (below). Other arrivals cannot unlock the walk, and flagging
  them made every batch-end flush sort the whole buffer while the baseline was unknown.
- **Commit success**: Ready was consumed with items still buffered.
- **Stale-Ready discard**: same shape, at commit time.
- **Baseline reset** (rebalance assign): items buffered before the reset may qualify
  against the new baseline with no further traffic ever arriving.

The commit tick consumes the flag too (a flag-gated flush at the top of `CommitOffsets`):
without that, a reset that flags pre-buffered items has no ingest batch to run the flush
on a quiet partition, and the chain strands until the next rebalance.

**Ready initialisation from the buffer**: with Ready empty, the walk promotes a buffered
item only when it *proves* contiguity with the committed position, one of two ways:

1. its offset equals `CommittedPlusOne` (the literal next offset), or
2. its `PreviousOffset` equals `LastCommittedOffset` (predecessor linkage against the
   record actually committed this epoch - the broker-gap case where the baseline offset
   does not exist).

An unprovable head stays buffered, waiting for its true predecessor: the committer stalls
exactly while a hole exists and resumes exactly when it fills, never lifting past a
missing offset (that would move the broker watermark over unprocessed work: loss, not
duplicates). Items below the baseline are pruned as orphaned during the walk so they can
never block the initialisation.

Resizing uses 70%/300% hysteresis thresholds:

- Grow when capacity drops below 70% of steady-state
- Shrink when capacity exceeds 300% (prevents memory leaks from transient spikes)

### Async Commits

The commit loop runs on a timer (default: 5s):

```go
for {
    select {
    case <-brokerCommitTicker.C:
        oc.CommitOffsets()  // collect all Ready items, send to broker
    }
}
```

Each commit:

1. Acquires `autoCommitGuard` (prevents concurrent commits)
2. Locks mutex
3. Runs the flag-gated gap-buffer flush (see Gap Buffer Strategy)
4. Collects all `Ready` messages across partitions (discarding orphaned trackers for
   unassigned partitions and any stale Ready below the baseline)
5. Sends batch to broker
6. On success: advances `CommittedPlusOne` (never backwards), records
   `LastCommittedOffset`, collects Ready for metrics, and flags a gap advance if items
   remain buffered
7. On broker failure: returns `ErrBrokerCommitFailed` (state untouched, retried next
   tick); the drain surfaces it loudly but does not escalate - a rejection at handoff is
   expected fencing and the uncommitted tail is an at-least-once re-read for the next
   owner

## The Zombie Problem

> **Terminology Note**: This document uses "zombie" (common in distributed systems literature) to
> describe work items that complete after their partition was revoked. In user-facing logs and
> error messages, these are called **"orphaned work items"** for clarity in production environments.
> See `COMMIT_GUARD_ANALYSIS.md` for the terminology mapping.

Consumer group rebalancing redistributes partitions across consumers. The protocol:

1. Broker signals rebalance
2. Consumer pauses polling
3. **Drain**: wait for in-flight messages to complete
4. Commit final offsets
5. Acknowledge rebalance
6. Resume with new partition assignments

### Drain Timeout and Abandoned Workers

Drain waits for workers to finish, but what if a worker is blocked on a slow downstream
system? Waiting indefinitely blocks the entire consumer group rebalance.

The solution is a drain timeout. If workers don't complete within the timeout (default: 20s),
we abandon them and proceed with rebalance. This keeps the consumer group healthy but creates
**zombie messages** - messages from abandoned workers that eventually complete after we've
moved on.

### The Zombie Scenario

```text
1. Partition 0 assigned to Consumer A
2. Consumer A polls offset 100, dispatches to worker
3. Worker blocks on slow downstream (database, API, etc.)
4. Rebalance triggered, drain times out after 20s
5. Consumer A abandons worker, acknowledges rebalance
6. Partition 0 reassigned to Consumer B
7. Consumer B processes offsets 100-150, commits up to 150
8. Another rebalance returns Partition 0 to Consumer A
9. Consumer A's zombie worker finally completes offset 100
```

At step 9, Consumer A has a completed `WorkItem` for offset 100, but the broker has already
committed up to 150. If we naively made offset 100 `Ready` and committed it, we'd move the
broker's position *backwards* - a serious correctness violation.

## Defence in Depth: Multi-Layer Zombie Protection

### Layer 1: Polling Boundary (prev package)

The `prev.PartitionOffsets` tracker validates offset sequences at poll time:

| Input                 | Output                   | Effect                    |
|-----------------------|--------------------------|---------------------------|
| First message         | prevOffset, isFirst=true | Signals sequence start    |
| Normal ascending      | Actual previous offset   | Normal contiguity         |
| Gap (log compaction)  | Actual previous offset   | Gaps allowed if ascending |
| Duplicate offset      | ErrDuplicateOffset       | Circuit breaker triggered |
| Offset regression     | ErrOffsetRegression      | Circuit breaker triggered |
| Negative offset       | ErrNegativeOffset        | Circuit breaker triggered |

This catches buggy adapters and corrupt streams before they enter the pipeline.

### Layer 2: CommittedPlusOne Guard

During rebalance assign, the broker reports its current committed position via
`RebalanceInfo.CommittedOffset`. We store this as `CommittedPlusOne` *before* polling
resumes:

```go
// subscription_rebalance.go - during assign
s.resetCommittedOffsetsFromRebalanceInfo(rebalanceInfo)

// committer.go (abridged; see ResetCommittedOffsets for the full form)
for partition, committedOffset := range partitionOffsets {
    // a re-assign AFTER A REVOKE starts a fresh epoch: everything still in
    // the tracker is stale and discarded (Ready AND gap buffer), gated on
    // the revoke stamp MarkPartitionRevoked left on the tracker
    if tracker.Assignment == nexus.Revoke { /* discard Ready + buffer as orphans */ }

    // otherwise reject only a Ready that is now behind the new baseline
    if tracker.Ready != nil && tracker.Ready.Message.Offset < committedOffset { /* discard */ }

    tracker.CommittedPlusOne = committedOffset
    tracker.LastCommittedOffset = -1 // nothing committed this epoch yet
    tracker.Assignment = nexus.Assign
    tracker.NeedsGapAdvance = len(tracker.GapBuffer) > 0
}
```

An adapter that cannot supply the real committed offset passes `-1` (unknown), never `0`:
zero is an achievable offset, and a zero baseline silently disarms the comparisons in
Layers 2-4.

### Layer 3: First Flag Protection

The `First` flag indicates a message is first after assignment. A zombie might carry a stale
`First=true` flag. The fix: only update `CommittedPlusOne` from `First` if the offset is
>= the current value:

```go
if workItem.First && offset >= offsetsTracker.CommittedPlusOne {
    offsetsTracker.CommittedPlusOne = offset
}
```

A zombie at offset 100 with `First=true` won't corrupt `CommittedPlusOne` if it's already 150.

### Layer 4: Advancement Rejection

Multiple paths in `checkAndAdvance` reject stale offsets:

```go
// Ready=nil case
if offsetsTracker.CommittedPlusOne > offset {
    // zombie detected - offset is behind broker's position
    oc.returnMessageAndCollectMetrics(workItem, now)  // reject
    return
}

// Ready≠nil case
if readyOffset > offset {
    // offset has already advanced past this message
    oc.returnMessageAndCollectMetrics(workItem, now)  // reject
    return
}
```

### Layer 5: Assigned Map Guard (Orphaned Work Item Protection)

The `assigned` map tracks which partitions are currently owned by this consumer. During
`CommitOffsets()`, any Ready items for partitions not in this map are discarded:

```go
// send_commits.go - during CommitOffsets(), BEFORE the Ready-nil skip so
// gap-buffer leftovers are cleaned even when no Ready exists
for partition, offsetTracker := range oc.offsetsByPartition.PartitionMap {
    if _, ok := oc.assigned[partition]; !ok {
        // discardOrphanedTracker: Ready AND every buffered item are marked
        // orphaned and returned through metrics (work-item accounting), the
        // buffer is cleared, the gap-advance flag reset
        oc.discardOrphanedTracker(partition, offsetTracker)
        continue
    }
    // ... stale-Ready guard, then:
    commits = append(commits, workItem.Message)
}
```

The map is updated during rebalance:

- `MarkPartitionAssigned(partition)` - called after ResetCommittedOffsets during Assign,
  BEFORE the ack
- `MarkPartitionRevoked(partition)` - called after the drain during Revoke, BEFORE the
  ack (marking after the ack is the ordering the TLA+ model proved unsafe: a commit tick
  in that window can commit a stale Ready and regress the broker). It also stamps
  `Assignment = Revoke` on the tracker, which is what gates the next assign's
  stale-epoch clear in Layer 2.

This matches the TLA+ model's `assignment = "Assigned"` guard on the `CommitReady` action.
See `COMMIT_GUARD_ANALYSIS.md` for the full analysis of this protection mechanism.

### Protection Summary

| Layer                 | Set When         | Protects Against                       |
|-----------------------|------------------|----------------------------------------|
| prev.PartitionOffsets | Poll time        | Buggy adapters, corrupt streams        |
| ResetCommittedOffsets | Rebalance assign | Race between zombie and new messages   |
| First flag check      | processCommit    | Stale First flag on zombies            |
| checkAndAdvance       | Every WorkItem   | Out-of-order zombies, already-advanced |
| assigned map          | CommitOffsets    | Orphaned work items after drain timeout|

## Edge Cases

### Synthetic previousOffset for First Messages

When `isFirst=true`, the returned `prevOffset` is synthetic (`offset - 1`), not from the
broker. This doesn't break anything because:

1. For first messages, `CommittedPlusOne` is set from broker via `ResetCommittedOffsets`
2. The gap buffer uses `PreviousOffset == Ready.Offset` for advancement - the synthetic
   value matches if offsets are truly contiguous

### Log Compaction Gaps

```text
GetPrevious(0, 100) → prevOffset=99, isFirst=true
GetPrevious(0, 500) → prevOffset=100, isFirst=false  // gap 101-499 compacted
```

Safe - gaps are allowed if ascending. The gap buffer handles non-contiguous completion.

### The Undefendable Case

The only way to break zombie protection is if the broker reports a wrong committed offset.
If broker says `CommittedPlusOne=50` when it's actually 150:

1. Zombie at offset 100 arrives
2. 100 >= 50, so it passes checks
3. Zombie becomes Ready, commits
4. Messages 51-99 potentially lost

This is outside our control - we must trust the broker's committed offset. This is the
fundamental assumption the system operates under.

## When Do Zombies Actually Happen?

Rarely. The drain timeout (20s default) is generous. Zombies only occur when:

1. A worker is blocked longer than the drain timeout
2. A rebalance occurs during that block
3. The partition returns to the same consumer after another consumer advanced it

This requires a perfect storm of slow downstream processing *and* rebalance timing. In
healthy systems with reasonable processing latencies, zombies essentially never happen.
But "essentially never" isn't "never", and correctness demands handling the edge case.

## Related Code

- `offsets_tracker.go` - `OffsetsTracker` struct: `CommittedPlusOne`, `LastCommittedOffset`
- `committer.go` - `ResetCommittedOffsets()`, `MarkPartitionAssigned/Revoked()`, `NewCommitter()`
- `committer_ingest.go` - `startIngestLoop()`, `CollectAndCommit()`
- `committer_process.go` - `processCommit()`, `checkAndAdvance()`, `flushGapBuffers()`
- `send_commits.go` - `startAsyncCommits()`, `CommitOffsets()`, `discardOrphanedTracker()`
- `committer_drain.go` - `DrainCommitter()` for rebalance coordination
- `committer_metrics.go` - Ring buffer for commit observability windows
- `../pipeline/prev/` - `PartitionOffsets` for polling boundary validation

### Tests

- `committer_process_test.go` - Happy path, CommittedPlusOne guards, Ready advancement
- `committer_process_missing_offsets_test.go` - Log compaction, transaction gaps
- `committer_process_duplicates_test.go` - Duplicate handling
- `committer_orphaned_race_test.go` - Orphaned work item scenarios, race verification
- `committer_reset_zero_orphan_test.go` - Unknown/zero baselines, stale epoch state,
  backward-commit and broker-error surfacing guards
- `committer_gap_stall_test.go` - Ready re-initialisation from the buffer: broker-gap
  commit boundaries, tick-alone re-initialisation, recorded-commit linkage (never
  baseline arithmetic), and never advancing past a missing offset
- `committer_fetch_discontinuity_test.go` - Fetch-position jumps without a rebalance
- `committer_drain_handshake_test.go` - Drain vs live ingest handshake
- `committer_model_test.go` / `committer_model_fuzz_test.go` - Model-based randomized
  interleavings and fuzz over the same harness, six named invariants
- `../../tests/stutter_test.go` - Universally strided offsets with producer quiet windows
  (every commit boundary non-contiguous, ticks against a silent pipeline)
