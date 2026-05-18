# Commit Guard Analysis: Drain Timeout Orphaned Work Item Scenario

**Status**: Solution Chosen
**Found During**: TLA+ Formal Verification (Phase 5 - Liveness)
**Date**: 2025-12-13

---

## Terminology

| User-Facing Term                  | Internal/TLA+ Term | Description                                                                     |
|-----------------------------------|--------------------|---------------------------------------------------------------------------------|
| **Orphaned work item**            | Zombie             | A work item that completes after its partition was revoked due to drain timeout |
| **Orphaned work item protection** | Zombie protection  | The mechanism that prevents commits for revoked partitions                      |

The TLA+ model and internal design discussions use "zombie" terminology (matching distributed systems
literature), but user-facing logs and comments use "orphaned" for clarity in production environments.

---

## Chosen Solution

Add an `assigned map[int32]struct{}` to track partition ownership at the application level.
Check this map during `CommitOffsets()` - skip and clean up any Ready items for partitions
not in the map. This matches the TLA+ model's `assignment = "Assigned"` guard.

---

## Summary

The TLA+ model assumes commits only occur when `assignment = "Assigned"`, but the real
implementation's `CommitOffsets()` does not check assignment state. After a drain timeout,
a zombie worker can complete, become Ready, and potentially have its offset committed for
a partition that has been revoked.

---

## The Scenario

### Timeline (Drain Timeout Leading to Zombie Commit)

```text
1. Revoke triggered (we're inside Poll() callback)
2. DrainWorkers() called - waiting for workers to complete
3. Worker W is slow (external system, network issue, etc.)
4. Drain timeout fires (default: 20s)
5. Circuit breaker triggered (emergency shutdown)
6. ackRebalance() called - partition revoked at broker
7. Poll() returns
8. --- We no longer own the partition ---
9. Worker W finally completes
10. W sends to commitsIn channel
11. Ingest loop processes W with STALE CommittedPlusOne
12. W becomes Ready (offset == stale CommittedPlusOne)
13. Async commit tick fires (before ctx.Done() stops the loop)
14. CommitOffsets() sends commit to broker for revoked partition
```

### Code Path

**drain_coordinator.go:57-60** - Drain timeout returns early:

```go
err = c.drainWorkers(timeoutTimer)
if err != nil {
    return  // drainCommitter NOT called, circuit breaker triggered in defer
}
```

**subscription_rebalance.go:41-42** - Revoke continues despite drain failure:

```go
s.drain(FromRebalance, rebalanceInfo...)  // May fail with timeout
if err = s.ackRebalance(...); err != nil { ... }  // Called regardless
```

**send_commits.go:51-55** - No assignment check:

```go
for _, offsetTracker := range oc.offsetsByPartition.PartitionMap {
    if workItem := offsetTracker.Ready; workItem != nil {
        commits = append(commits, workItem.Message)  // No assignment check!
    }
}
```

---

## Model vs Implementation Gap

### TLA+ Model (OffsetCommitterP5.tla)

```tla
CommitReady ==
    /\ mutex = "Free"
    /\ assignment = "Assigned"   \* Guard: only commit when assigned
    /\ ~IsNone(ready)
    /\ brokerCommitted' = ready.offset + 1
    /\ ready' = NoneWorkItem
    /\ UNCHANGED <<...>>
```

The model explicitly requires `assignment = "Assigned"` before committing.

### Real Implementation

The `OffsetsTracker.Assignment` field exists but:

1. Is only ever set to `nexus.Assign` (never `nexus.Revoke`)
2. Is never checked in `CommitOffsets()`

```go
// offsets_tracker.go:14
Assignment    nexus.RebalanceType // status after last rebalance event: Assigned or Revoked

// But only ever set to Assign:
// committer.go:106
tracker.Assignment = nexus.Assign

// And never checked in CommitOffsets()
```

---

## Current Protections

### 1. Broker-Level Protection (Primary)

Kafka (and most brokers) reject commits for partitions not assigned to the consumer:

- Consumer group coordinator tracks partition ownership
- Commits for unowned partitions return error or are ignored
- This is the de-facto protection currently relied upon

### 2. Circuit Breaker Context Cancellation

When circuit breaker fires:

- Context is cancelled
- `startAsyncCommits()` checks `ctx.Done()` and should exit
- But there's a race window between circuit breaker and next commit tick

### 3. Timing Window

- Commit interval is 5 seconds (configurable)
- Window between drain timeout and context cancellation is small
- But not zero - the race is theoretically possible

### 4. Zombie Offset Rejection (Partial)

If Assign happens before zombie completes:

- `ResetCommittedOffsets()` sets new CommittedPlusOne
- `processCommit()` rejects `offset < CommittedPlusOne`
- Stale Ready items are rejected (lines 100-103 of committer.go)

But if zombie completes and commits BEFORE Assign, this protection doesn't help.

---

## Risk Assessment

### Probability: Low

1. Drain timeout is rare (requires slow worker exceeding 20s default)
2. Circuit breaker stops most processing
3. Commit tick must fire in tiny window
4. Broker should reject invalid commits

### Impact: Low-Medium

1. Commit is rejected by broker - no data corruption
2. At-least-once semantics mean message will be redelivered
3. But relying on broker behaviour rather than application invariant
4. Violates principle of defence in depth

---

## Implementation

### Data Structure

Add to `Committer`:

```go
type Committer[T any] struct {
    // ... existing fields ...
    assigned map[int32]struct{}  // partitions currently assigned to this consumer
}
```

Initialise in `NewCommitter`:

```go
assigned: make(map[int32]struct{}),
```

### New Methods

```go
// MarkPartitionAssigned records that a partition is assigned to this consumer.
// Called during rebalance Assign, after ResetCommittedOffsets.
func (oc *Committer[T]) MarkPartitionAssigned(partition int32) {
    oc.mu.Lock()
    defer oc.mu.Unlock()
    oc.assigned[partition] = struct{}{}
}

// MarkPartitionRevoked records that a partition is no longer assigned.
// Called during rebalance Revoke, after ackRebalance completes.
func (oc *Committer[T]) MarkPartitionRevoked(partition int32) {
    oc.mu.Lock()
    defer oc.mu.Unlock()
    delete(oc.assigned, partition)
}
```

### CommitOffsets Guard

Update `CommitOffsets()` to check the map:

```go
func (oc *Committer[T]) CommitOffsets() error {
    // ... acquire guard and mutex ...

    now := time.Now()
    commits := make([]*nexus.Message[T], 0, len(oc.offsetsByPartition.PartitionMap))

    for partition, offsetTracker := range oc.offsetsByPartition.PartitionMap {
        workItem := offsetTracker.Ready
        if workItem == nil {
            continue
        }

        // Guard: only commit for assigned partitions
        if _, ok := oc.assigned[partition]; !ok {
            // Zombie for revoked partition - skip and clean up
            oc.logger.Warn(oc.ctx,
                "skipping commit for revoked partition %d, offset %d (zombie)",
                partition, workItem.Message.Offset)
            oc.returnMessageAndCollectMetrics(workItem, now)
            offsetTracker.Ready = nil
            offsetTracker.GapBuffer = offsetTracker.GapBuffer[:0]  // clear, keep capacity
            continue
        }

        commits = append(commits, workItem.Message)
    }

    // ... rest of commit logic ...
}
```

### Subscription Integration

In `subscription_rebalance.go`:

```go
case nexus.Assign:
    // ... existing reset logic ...
    s.resetCommittedOffsetsFromRebalanceInfo(rebalanceInfo)

    // Mark partitions as assigned
    for _, r := range rebalanceInfo {
        s.markPartitionAssigned(r.Partition)  // calls committer.MarkPartitionAssigned
    }

    if err = s.ackRebalance(rebalanceType, rebalanceInfo); err != nil { ... }

case nexus.Revoke:
    // ... existing drain logic ...
    s.drain(FromRebalance, rebalanceInfo...)

    // Mark partitions as revoked BEFORE ack to close race window
    // TLA+ verification (OffsetCommitterP3r.tla) proved that doing this AFTER ack
    // leaves a window where CommitTick can fire with committerAssigned=TRUE,
    // allowing zombie commits that violate monotonicity.
    for _, r := range rebalanceInfo {
        s.markPartitionRevoked(r.Partition)  // calls committer.MarkPartitionRevoked
    }

    if err = s.ackRebalance(rebalanceType, rebalanceInfo); err != nil { ... }
```

### Why Commit-Time Check is Sufficient

Checking at commit time (rather than in `processCommit`) is sufficient because:

1. **Ready items for revoked partitions are harmless until commit** - they just sit there
2. **Simpler implementation** - single check point in CommitOffsets
3. **Cleanup happens at detection** - no repeated warnings or memory leaks
4. **Post-Assign zombies handled by existing protection** - CommittedPlusOne rejects them

### Interaction with Existing Protections

| Scenario                                     | Protection                                          |
|----------------------------------------------|-----------------------------------------------------|
| Zombie completes after Revoke, before Assign | `assigned` map check in CommitOffsets               |
| Zombie completes after Assign                | `offset < CommittedPlusOne` in processCommit        |
| Zombie with First=true after Assign          | `offset >= CommittedPlusOne` guard in processCommit |
| Stale Ready after ResetCommittedOffsets      | Rejected in ResetCommittedOffsets (lines 100-103)   |

The `assigned` map fills the gap: zombies completing after Revoke but before Assign.

---

## Related Files

| File                                     | Relevance                                             |
|------------------------------------------|-------------------------------------------------------|
| `committer.go`                           | `ResetCommittedOffsets()` - sets Assignment to Assign |
| `send_commits.go`                        | `CommitOffsets()` - missing assignment check          |
| `offsets_tracker.go`                     | `Assignment` field definition                         |
| `committer_ingest.go`                    | Ingest loop processing zombies                        |
| `committer_process.go`                   | `processCommit()` - zombie offset rejection           |
| `drain/drain_coordinator.go`             | Drain timeout handling                                |
| `subscription/subscription_rebalance.go` | Revoke flow continues after drain failure             |

## TLA+ Model Reference

| File                     | Relevance                                                  |
|--------------------------|------------------------------------------------------------|
| `OffsetCommitterP3r.tla` | **Key model** - found race condition, verified fix         |
| `OffsetCommitterP5.tla`  | `CommitReady` action with `assignment = "Assigned"` guard  |
| `OffsetCommitterP4.tla`  | Mutex coordination model                                   |
| `OffsetCommitterP3.tla`  | Original zombie scenario model (before committerAssigned)  |

### TLC Verification Results (OffsetCommitterP3r.tla)

| Run | Model Variant                             | Result   | States      | Time    |
|-----|-------------------------------------------|----------|-------------|---------|
| 1   | Separate MarkPartitionRevoked (AFTER ack) | **FAIL** | 1,123       | 1.5s    |
| 2   | Atomic with RebalanceRevoke (BEFORE ack)  | **PASS** | 285,994,148 | 5m 24s  |

**Counterexample trace (Run 1)**: CommitTick fires while `assignment="Revoked"` but
`committerAssigned=TRUE`, committing zombie offset 0+1=1 when broker was at 2.

---

## Next Steps

- [x] Add `assigned map[int32]struct{}` to Committer struct
- [x] Initialise map in NewCommitter
- [x] Add `MarkPartitionAssigned(partition int32)` method
- [x] Add `MarkPartitionRevoked(partition int32)` method
- [x] Update `CommitOffsets()` with assigned check and cleanup
- [x] Wire up calls from subscription rebalance handler
- [x] Update existing tests to call MarkPartitionAssigned
- [x] Add unit test specifically for zombie-after-revoke scenario
- [x] Update DESIGN.md with new protection mechanism (Layer 5: Assigned Map Guard)
- [x] Verify TLA+ model alignment (OffsetCommitterP3r.tla - found and fixed race condition)
