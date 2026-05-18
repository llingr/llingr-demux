# Technical Deep Dive

## Introduction

Message processing systems often suffer from head-of-line blocking where one slow message delays
everything behind it. llingr-demux solves this using per-partition-key concurrency - messages with the
same key are strictly ordered (queued in channels), different keys run in parallel (executed in goroutines).

The benchmark results show near-linear scaling with 97-99% efficiency across a wide range of
configurations. This warrants explanation because concurrent systems typically exhibit sub-linear
scaling due to coordination overhead, lock contention, and resource exhaustion. This document explains
how the observed efficiency reflects deliberate, benchmark-driven engineering rather than measurement error.

Efficiency here measures how much throughput the framework 'loses' to coordination overhead.
Theoretical throughput assumes zero overhead, while real-world processing includes routing, synchronization
(mutex contention across CPU cores), and raw execution time - every instruction uses clock cycles,
and cache misses compound at high message rates.

```text
theoretical_tps = concurrent_keys × (1000ms / processor_latency_ms)
```

The benchmarks measure:

```text
efficiency = actual_tps / theoretical_tps
```

For example: 250 keys × (1000ms / 100ms) = 2,500 TPS theoretical

Observed Results:

- 250 concurrent partition keys @ 100ms latency: **2,477.8 TPS actual = 99.11% efficiency**
- 1000 concurrent partition keys @ 100ms latency: **9,798.7 TPS actual = 97.99% efficiency**
- 5000 concurrent partition keys @ 100ms latency: **46,825.5 TPS actual = 93.65% efficiency**

Efficiency drops at higher concurrency because mutex contention and cache line bouncing are
unavoidable costs that compound. The architecture keeps growth sub-linear because Go's primitives
have been carefully chosen to match access patterns - for example:

 - Object pools minimize memory pressure from system calls to allocate memory, and GC cycles to
   reclaim it. These typically require synchronization, but `sync.Pool` sidesteps this by exploiting
   the Go runtime's core-local affinity as each processor mostly uses its own pool and cross-core
   traffic is rare.
 - Shared maps protected by mutexes are generally more efficient where global visibility is needed.

Go is an excellent fit for this because it strikes a useful balance between high-level
primitives like channels, goroutines and the standard library sync package while remaining low-level
enough to control memory layout - e.g. contiguous 8-byte pointers aren't just cache-friendly, they let
CPU prefetchers detect striding patterns, fetch ahead and efficiently pipeline instructions.
This is harder to achieve in languages with managed runtimes where memory layout is opaque, or in
languages where the powerful concurrency primitives available in Go simply aren't available to the
programmer.

---

## Framework Overhead Analysis

### Statement Count

Tracing the hot path from broker poll to offset commit, the framework executes approximately
**142 statements per message** when processing normally (existing worker, contiguous offsets,
no errors).

| Component                  | File                                              | Statements  |
|----------------------------|---------------------------------------------------|-------------|
| Poll loop                  | `subscription/subscription_polling_loop.go:36-43` | 4           |
| Process entry              | `pipeline/processor.go:74-84`                     | 6           |
| WorkItem creation          | `pipeline/processor.go:113-135`                   | 15          |
| FNV-1a hash (36-char UUID) | `pipeline/fnv/fnv_hash.go:17-24`                  | 74          |
| WorkItem -> Worker routing | `pipeline/processor_demux.go:62-85`               | 11          |
| Worker receive             | `pipeline/worker.go:124-126`                      | 2           |
| Process wrapper            | `pipeline/worker.go:88-98`                        | 3           |
| Process call               | `pipeline/worker.go:157-179`                      | 11          |
| Commit send                | `offset/committer_ingest.go:15-17`                | 1           |
| Commit ingest              | `offset/committer_ingest.go:51-54`                | 3           |
| Commit process             | `offset/committer_process.go:23-51`               | 5           |
| Watermark advance          | `offset/committer_process.go:64-107`              | 7           |
| **Total**                  |                                                   | **~142**    |

### Measured Overhead

With zero processing latency (immediate return from ProcessMessage), the framework achieves
~800,000 messages per second on an 8-core developer laptop. This yields:

```text
1,000,000,000 ns / 850,000 messages ≈ 1,176 ns per message
```

At ~142 statements, this is approximately **8-9 nanoseconds per statement average**.

---

## Why Coordination Costs Remain Bounded

### 1. No Coordination Scaling

The critical operations do not scale with concurrency:

| Operation            | Scaling        | Location                               |
|----------------------|----------------|----------------------------------------|
| Channel send/receive | O(1)           | Guard channels in `processor.go:81-84` |
| Mutex lock/unlock    | O(1) per shard | Sharded in `processor_demux.go:72`     |
| Map lookup           | O(1) amortised | Worker map in `processor_demux.go:73`  |
| FNV hash             | O(key length)  | Fixed for UUID keys                    |

The sharded mutex design (`processor_demux.go:46-52`) divides workers across 16 independent locks.
Lock contention is distributed across worker shards, reducing pressure on any single mutex.

### 2. Hot Path Optimisation

The worker loop (`pipeline/worker.go:101-153`) uses a nil-as-state pattern:

```go
if workItem == nil {
    // Cold path: blocking select - goroutine parks with zero CPU
    select {
    case workItem = <-w.workItems:
        // ...
    }
} else {
    // Hot path: non-blocking drain
    select {
    case workItem = <-workItems:
        // ...
    default:
        // cleanup
    }
}
```

Workers either sleep efficiently (zero CPU) or drain at maximum throughput. There is no
intermediate state consuming resources.

### 3. Minimal Allocation Hot Path

The `alloc/work_items_pool.go` provides object pooling. WorkItems are borrowed and returned,
with memory allocations only needed when a pool is exhausted.

```go
workItem = p.workItemsPool.Borrow()  // processor.go:114
// ... processing ...
oc.metricsCollector.Collect(workItem)  // returns to pool after metrics
```

Avoiding per-message heap allocation means fewer system calls to obtain memory and less
GC pressure to reclaim it.

### 4. Amortised Time Syscalls

Wall clock time is expensive (~25ns per call). The framework amortises this:

```go
// subscription_polling_loop.go:37-41
var now = time.Now()
// ... loop code ...
delta = time.Since(now)
if delta > time.Second {
    now = time.Now()
    delta = 0
}
// ...
s.processor.Process(message, now.Add(delta))
```

```go
// committer_ingest.go:42-47
if delta := time.Since(lastUpdate); delta > time.Second {
    now = time.Now()
    lastUpdate = now
} else {
    now = lastUpdate.Add(delta)
}
```

One syscall per second instead of per message.

### 5. Contiguous Fast Path

The offset committer (`offset/committer_process.go:64-107`) has two paths:

**Fast path** (contiguous offset): Single pointer swap, immediate return

```go
if readyOffset == workItem.PreviousOffset {
    oc.returnMessageAndCollectMetrics(offsetsTracker.Ready, now)
    offsetsTracker.Ready = workItem
} else {
    // gap detected: insert into buffer and re-assess high-watermark
}

```

**Slow path** (gap detected): Sort and scan gap buffer

In normal operation, messages complete roughly in order. The fast path dominates. Gap buffer
processing only occurs during rebalance or extreme out-of-order completion.

---

## Why Efficiency Degrades Gracefully

The benchmarks show efficiency dropping from 99% to 91% as concurrent keys increase from
250 to 5000, which is predictable due to higher coordination costs.

At higher concurrency there are:

- More mutex acquisitions
- More channel operations competing for scheduler time
- More cache misses as working set exceeds L2/L3

Framework overhead ≈ 1,200 ns
Processing latency = 100,000,000 ns (100ms)

Efficiency = processing / (processing + overhead)
           = 100,000,000 / (100,000,000 + 1,200)
           = 99.9988%

The measured 99.11% at 250 keys includes additional costs:

- Goroutine scheduling variance
- Cache effects
- Channel contention

At 5000 keys, these effects compound but remain bounded because the architecture does not
introduce O(n) coordination.

---

## The FNV Hash Rationale

The custom FNV-1a implementation (`pipeline/fnv/fnv_hash.go`) includes detailed rationale:

```go
/**
  Benchmarked vs. stdlib (Go 1.24+)

  Why keep custom implementation:
    * Already optimal - compiles to minimal assembly (42 bytes on x86_64)
    * Fewer compiler dependencies - stdlib relies on multiple optimizations
    * More resilient to compiler regressions - only requires basic BCE
    * Explicit control over implementation - no abstraction layers
    * No stack frame needed - all variables fit in registers:
      AX: string pointer,  BX: string length,  DX: 'i',  SI: 'h'

  Simple loop fits in single cache line vs. 4 lines for unrolled version
**/
```

The 42-byte implementation fits in a single cache line. Loop unrolling was tested and rejected:
231 bytes across 4 cache lines performed worse due to instruction cache pressure.

---

## Variable Hoisting

The worker loop (`pipeline/worker.go:77-86`) hoists struct fields to local variables:

```go
var (
    workItems                             = w.workItems
    workItem           *nexus.WorkItem[T] = nil
    workers                               = w.workerShard.workers
    commit                                = w.committer.CollectAndCommit
    workerShardDone                       = w.workerShard.done.Load
)
```

This appears redundant but serves performance:

- Eliminates receiver pointer dereference per iteration
- Enables register allocation by the compiler
- `commit` becomes a direct function pointer, avoiding method dispatch

---

## Micro-Optimisations

Beyond the core architectural decisions that aim to reduce lock contention and improve core
affinity, the codebase contains numerous micro-optimisations that collectively shave nanoseconds
from every message. These are detailed below with file references.

### 1. Struct Field Ordering for Cache Line Affinity

Hot-path struct fields are positioned first to maximise cache line utilisation, and ordered
to assist the prefetcher with instruction pipelining. When the CPU fetches the struct pointer,
the first cache line (64 bytes on x86) contains all required fields.

**Committer** (`demux/offset/committer.go:19-34`):

```go
type Committer[T any] struct {
    commitsIn          chan *nexus.WorkItem[T] // hot: receives all work items
    ingestGuard        chan struct{}           // hot: contention guard
    mu                 *sync.Mutex             // hot: protects partition map
    offsetsByPartition *OffsetsByPartition[T]  // hot: partition lookup
    metricsCollector   *metrics.Collector[T]   // hot: metrics delivery
    commitOffsets      nexus.CommitOffsets[T]  // warm: periodic commits
    // ...
    ctx                context.Context         // cold: logging only
    logger             nexus.Logger            // cold: logging only
    // ... remaining cold fields
}
```

**Worker** (`demux/pipeline/worker.go:34-51`):

```go
type Worker[T any] struct {
    workItems      chan *nexus.WorkItem[T] // hot: message delivery channel
    processMessage nexus.ProcessMessage[T] // hot: application callback
    committer      *offset.Committer[T]    // hot: commit after process
    guard          <-chan struct{}         // hot: concurrency guard
    mu             *sync.Mutex             // hot: cleanup coordination
    returnWorker   func(*Worker[T])        // warm: pool return
    // ... remaining fields accessed less frequently
}
```

**Subscription** (`demux/subscription/subscription.go:27-49`):

```go
type Subscription[T, R, Q any] struct {
    mainCtxDone         <-chan struct{}          // hot: checked every poll
    poll                nexus.Poll[T]            // hot: called every iteration
    processor           *pipeline.Processor[T]   // hot: routes every message
    pausePollingTimeout time.Duration            // warm: rebalance only
    // ... remaining fields
}
```

**Metrics** (`llingr-nexus/nexus/metrics.go:18-28`):

```go
type Metrics struct {
    Traits                  Traits        // 8 bytes
    QueueDepth              int32         // 4 bytes - int32 not int64 to fit struct
    Partition               int32         // 4 bytes
    Offset                  int64         // 8 bytes
    ProcessDuration         time.Duration // 8 bytes
    WriteDeadLetterDuration time.Duration // 8 bytes
    ReadTime                time.Time     // 24 bytes
    ProcessStartTime        time.Time     // 24 bytes
    WatermarkAdvanceTime    time.Time     // 24 bytes
}   // Total: 112 bytes = exactly 2 x 64-byte cache lines (or 1 x 128-byte on ARM)
```

The comment at line 20 notes `int32` is used deliberately to fit the struct in 2 cache lines.

### 2. Cached Context.Done() Function

The circuit breaker (`demux/circuitbreaker/circuit_breaker.go:17-25`) caches the `Done()` method:

```go
type CircuitBreaker struct {
    mainCtx           context.Context        // cancels processing
    mainCtxDone       func() <-chan struct{} // cached Done() for hot path performance
    mainCancelFunc    context.CancelFunc
    // ...
}
```

And at construction (`circuit_breaker.go:34`):

```go
mainCtxDone: mainCtx.Done,  // function value, not method call
```

Every poll iteration checks `<-cb.mainCtxDone()`. Caching the function value avoids interface
method dispatch overhead (~2-3ns per call). At 800K messages/second, this saves ~2ms/second.

### 3. Hot Path Pointer Caching in Metrics Collector

The metrics collector (`demux/metrics/metrics_collector.go:97-99`) explicitly caches pointers:

```go
case workItem := <-c.workItems:
    // hot path pointers from cache to stack
    c.sendToMetricsSink(workItem, c.metricsSink, c.CollectedCount)
```

Passing `c.metricsSink` and `c.CollectedCount` as parameters rather than accessing `c.field`
inside `sendToMetricsSink` moves the pointer dereference outside the function, enabling the
compiler to keep values in registers across the call.

### 4. Pre-Allocated Zero Values

The object pool (`demux/alloc/work_items_pool.go:52-55`) pre-allocates zero values:

```go
var (
    zeroStr  = ""
    zeroTime time.Time
)
```

When returning WorkItems to the pool (`work_items_pool.go:59-83`), fields are reset using these
pre-allocated values. This avoids creating new zero values on each return, reducing allocation
pressure in the reset path.

### 5. Worker Pre-Warming

Workers are pre-started at initialisation (`demux/pipeline/worker_shard.go:69-75`):

```go
warmedWorkers := make([]*Worker[T], minIdleWorkers)
for i := 0; i < minIdleWorkers; i++ {
    warmedWorkers[i] = workerPool.BorrowWorker()
}
for j := 0; j < minIdleWorkers; j++ {
    workerPool.ReturnWorker(warmedWorkers[j])
}
```

This ensures goroutines are already running when the first messages arrive. Goroutine creation
costs ~2-5μs; pre-warming eliminates this from the message processing path.

### 6. Non-DRY Select for Branch Prediction

The processor (`demux/pipeline/processor.go:81-106`) duplicates code across select cases:

```go
select {
case p.guard <- struct{}{}:
    workItem.Metrics.ReadTime = readTime
    p.demux.SendToWorkerForProcessing(workItem.Message.Key, workItem)

default:
    p.timer.Reset(p.acquireWorkerTimeout)
    select {
    case p.guard <- struct{}{}:
        workItem.Metrics.ReadTime = readTime                              // duplicated
        p.demux.SendToWorkerForProcessing(workItem.Message.Key, workItem) // duplicated

    case p.overflowGuard <- struct{}{}:
        // ...
    }
}
```

The fast path (guard immediately available) and slow path (must race with overflow/timeout) both
contain the same assignment and call. This duplication ensures the fast path has minimal
instructions, optimising instruction cache usage. The slow path is rarely taken, so its
duplication cost is amortised.

### 7. Batched Ingest with Mutex Amortisation

The commit ingest loop (`demux/offset/committer_ingest.go:32-76`) batches reads to amortise
mutex acquisition:

```go
// ingest in batches to limit contention on ingestGuard and mutex
case oc.ingestGuard <- struct{}{}:
    oc.mu.Lock()
    const readBatchSize = 250 // amortizes mutex lock access
    // ... read up to 250 items before releasing
```

A single mutex acquisition handles up to 250 messages. At 100K TPS, this reduces mutex
operations from 100K/second to 400/second - a 250x reduction in lock/unlock traffic.

---

## Benchmark Methodology

**Test Environment**:

- CPU: Intel Core Ultra 7 258V (8 cores, no hyperthreading, 4.8GHz boost)
- Cache: 40KB L1d/core, 64KB L1i/core, 14MB L2, 12MB L3
- Memory: 32GB
- OS: Ubuntu 24.10, kernel 6.16.4

The benchmarks (`benchmarks/runner/main.go`) use a mock ProcessMessage that sleeps for
configurable latency:

```go
time.Sleep(latency)
return nil
```

This isolates framework overhead from application logic. The efficiency percentage measures
purely how much time the framework adds to the theoretical minimum.

Note that time.Sleep itself involves an unavoidable syscall, so the measured efficiency is
conservative.

---

## Conclusion

The benchmark efficiency is high because:

1. **Fixed overhead**: ~1,200 ns per message regardless of concurrency
2. **Sharded coordination**: 16 independent mutex domains
3. **Zero-allocation hot path**: Object pooling eliminates GC scaling
4. **Amortised syscalls**: Time calls reduced ~1000x
5. **Fast-path dominance**: Contiguous offsets skip sorting entirely
6. **Cache-aware design**: Critical code fits in L1 cache

At 100ms processing latency, adding 1.2μs overhead yields 99.999% theoretical efficiency. The
measured 97-99% reflects real-world scheduling and cache effects, which remain bounded because
the architecture avoids O(n) coordination patterns.

---

## File References

| Concern           | File                                              | Lines       |
|-------------------|---------------------------------------------------|-------------|
| Poll loop         | `demux/subscription/subscription_polling_loop.go` | 13-54       |
| Guard acquisition | `demux/pipeline/processor.go`                     | 74-110      |
| WorkItem pooling  | `demux/pipeline/processor.go`                     | 113-136     |
| FNV hash          | `demux/pipeline/fnv/fnv_hash.go`                  | 15-57       |
| Sharded mutex     | `demux/pipeline/processor_demux.go`               | 35-58       |
| Worker routing    | `demux/pipeline/processor_demux.go`               | 60-99       |
| Worker loop       | `demux/pipeline/worker.go`                        | 70-154      |
| Commit ingest     | `demux/offset/committer_ingest.go`                | 15-89       |
| Watermark advance | `demux/offset/committer_process.go`               | 23-107      |
| Object pool       | `demux/alloc/work_items_pool.go`                  | entire file |
| Benchmark runner  | `benchmarks/runner/main.go`                       | entire file |
