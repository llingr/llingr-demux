# llingr-demux

[![Pipeline](https://github.com/llingr/llingr-demux/actions/workflows/pipeline.yml/badge.svg)](https://github.com/llingr/llingr-demux/actions/workflows/pipeline.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/llingr/llingr-demux.svg)](https://pkg.go.dev/github.com/llingr/llingr-demux)
[![Tag](https://img.shields.io/github/v/tag/llingr/llingr-demux)](https://github.com/llingr/llingr-demux/tags)
[![License](https://img.shields.io/badge/License-Dual_Commercial%2FAGPL--3.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/llingr/llingr-demux)](go.mod)

Efficient message consumer engine for brokers with ordered partition/offset semantics.

Scale consumers vertically to improve compute utilization, keep costly broker/infrastructure
spend low and minimize operational complexity while significantly improving throughput and latency.

Reliable and observable: per-key ordering, clean rebalances, bounded at-least-once processing,
per-message telemetry, per-consumer bandwidth.

Built using standard Go with **no third-party dependencies**.

```bash
go get github.com/llingr/llingr-demux
```

Adapters bridge llingr-demux to specific brokers, currently:

- [llingr-adapter-kafka](https://github.com/llingr/llingr-adapter-kafka) -
  `confluent-kafka-go` (CGO/librdkafka, Apache 2.0)
- [llingr-adapter-franz](https://github.com/llingr/llingr-adapter-franz) -
  `franz-go` (pure Go, Apache 2.0)

---

## Quick Start

For overview see: [llingr.io/docs](https://llingr.io/docs)

Integration requires two callbacks - `ProcessMessage` and `WriteDeadLetter` -
alongside your existing broker client configuration. The llingr-demux engine handles
concurrency, offset management, rebalances and lifecycle coordination.

```go
func main() {
    // example broker adapter integration
    configMap := &kafka.ConfigMap{
        "bootstrap.servers": "broker-1:9092,broker-2:9092",
        "group.id":          "my-group",
    }

    adapter, err := kafkaadapter.New(configMap)
    if err != nil {
        panic(err)
    }

    // a reliable dead-letter is critical for at-least-once processing guarantees
    consumerBuilder := demux.NewBuilder[*kafka.Message]("orders", processMessageFn, writeDeadLetterFn).
        WithShutdownCallback(shutdownFn).       // for host application awareness of consumer shutdown
        WithMetricsSink(promSink.MetricsSink()) // for processing telemetry

    // bind consumer to adapter
    consumer := adapter.CreateConsumer(consumerBuilder)

    // optionally expose internal consumer state
    http.Handle("/snapshot", consumer.SnapshotHandler())

    defer consumer.Shutdown()
    if err := consumer.Subscribe(); err != nil {
        panic(err)
    }
}

// processMessageFn MUST be synchronous/blocking - llingr-demux PROVIDES concurrency.
// Do not spawn goroutines or async operations acting on the message that outlive the function call
func processMessageFn(ctx context.Context, msg *nexus.Message[*kafka.Message]) error {
    order := parseOrder(msg.Payload.Value)
    return db.Save(order)
}

// writeDeadLetterFn MUST be synchronous/blocking - llingr-demux PROVIDES concurrency.
// Do not spawn goroutines or async operations acting on the message that outlive the function call
func writeDeadLetterFn(ctx context.Context, msg *nexus.Message[*kafka.Message], reason error) error {
    return dlq.Publish(msg, reason)
}

// shutdownFn for incorporating application lifecycle management
func shutdownFn(ctx context.Context, reason error) {
    if reason != nil {
        log.Error(ctx, fmt.Sprintf("consumer emergency shutdown, reason: %v", reason))
    } else {
        log.Info(ctx, "consumer shutdown completed")
    }
}
```

> **Important:** The llingr-demux engine recycles message memory (see package
> `demux/alloc`), so `nexus.ProcessMessage` and `nexus.WriteDeadLetter` MUST be
> implemented as synchronous/blocking methods.

### Options

For overview see: [llingr.io/docs](https://llingr.io/docs#builder-options)

Consumer builder methods:

- **WithMetricsSink** - metrics collection (see [Observability](#observability))
- **WithLogger** - alternative logger back-end
- **WithContext** - static context for control plane logging (startup, rebalance,
  circuit-breaker, shutdown)
- **WithDemuxConfig** - pipeline tuning (see [demux/config/](demux/config/README.md))
- **WithOverflowGuard** - shared burst capacity between consumer instances
- **WithRateLimiter** - throttle message processing rate, rarely appropriate
- **WithShutdownCallback** - custom handler for graceful or emergency shutdown

The primary concurrency dial is `config.ConcurrentKeys` (default 250, max 5,000
per consumer instance), passed via `WithDemuxConfig`.

If no `WithShutdownCallback` is registered then a default handler logs errors
if present, waits 15 seconds (for log agents to ship), then sends `os.Interrupt`
to self.

---

## Fan-Out, Fan-In

For overview see: [llingr.io/correctness](https://llingr.io/correctness)

The `pipeline.Processor` demultiplexes (fans out) by partition key, delivering
messages into concurrent per-key workers that invoke the host application's
`ProcessMessage` callback. Each worker uses a Go channel to preserve per-key
message ordering.

The `offset.Committer` remultiplexes (fans in) by physical broker partition.
It resolves out-of-order completions before committing contiguous offset ranges
to the broker.

Client-specific configuration, auth, and other broker concerns are handled
outside this library. The [llingr-nexus](https://github.com/llingr/llingr-nexus)
interface layer acts as a broker-agnostic bridge to adapters.

### Configuration

For overview see: [llingr.io/docs](https://llingr.io/docs#config-concurrency)

- [demux/config/README.md](demux/config/README.md) - tuning guide
- [demux/config/demux_config_defaults.go](demux/config/demux_config_defaults.go) - defaults

---

## Processing Guarantees

**Bounded at-least-once** - messages are only considered ready to commit after
they have been processed by the host application. All commit logic is
encapsulated in the `offset.Committer`, which tracks contiguous offsets and
only commits safe ranges. The bound has two regimes. During planned lifecycle
events (deploys, scaling, rebalances) the drain coordinator completes in-flight
work and commits before partitions are released, so normal operations produce
zero duplicates and rolling updates cause no delivery spikes. After a
catastrophic crash (e.g. due to hardware failure), redelivery is strictly
bounded by the commit interval; only messages processed between commits are
potential duplicates. The default commit interval is 5s, configurable between
250ms and 15s.

**Circuit breakers** - the engine treats a failed ProcessMessage call followed
by a WriteDeadLetter error as a significant (likely infrastructure) issue: in
order to preserve at-least-once semantics, if a message cannot be processed
AND cannot be dead-lettered, the circuit-breaker is triggered and the consumer
stopped. Messages will not have been committed, and they will be re-routed to
a healthy replacement consumer. Bounded duplicates are possible; the engine's
shutdown and rebalance handling keep this to 'effectively-once' under normal
operations.

**Rebalance coordination** - polling stops immediately, active workers complete
their processing and the offset committer resolves contiguous messages before
making a final commit.

**Lifecycle coordination** - the shutdown callback integrates the engine with
the host application's lifecycle management. Worker completion and offset
commits use the same flow as rebalance coordination.

### Single Topic Per Consumer

A consumer instance is intended for exactly one topic. The engine's offset
tracking is keyed by partition number alone, so two topics that share partition
numbers would silently cross-contaminate committed offsets if they reached the same
consumer. The realistic way to hit this is handing an adapter a broker client
configured with a multi-topic or pattern subscription. Avoid this: to consume
multiple topics, build one consumer per topic, each scaling independently.

---

## Observability

```go
// fields ordered for cache-line alignment, not semantic grouping
type Metrics struct {
    Traits                  Traits
    QueueDepth              int32
    Partition               int32
    Offset                  int64
    ProcessDuration         time.Duration
    WriteDeadLetterDuration time.Duration
    ProcessStartTime        time.Time
    ReadTime                time.Time
    WatermarkAdvanceTime    time.Time
    WorkerPool              uint32
}
```

Partition keys are deliberately excluded from `MetricsSink` output to prevent
accidental PII disclosure in observability systems.

The [llingr-metrics-prometheus](https://github.com/llingr/llingr-metrics-prometheus)
adapter provides a ready-to-use `MetricsSink`.

`consumer.SnapshotHandler()` returns an `http.HandlerFunc` serving a JSON
snapshot of internal state including: concurrency utilization, sliding-window
throughput, partition offsets, and gap buffer depths. One important alert worth
considering could be on a partition's `gapBufferDepth` growing without bound: it
is the sign of a stuck handler invocation: completions accumulate behind necessarily
contiguous messages at full throughput, and commits for that partition will be
delayed until the handler returns and the contiguous message stream is resolved.
Thresholds are workload-specific, so this would be an operational alert.

---

## License & Copyright

**llingr-demux** is dual-licensed: `AGPL-3.0-only OR LicenseRef-Llingr-Commercial`
(every source file carries this SPDX expression; the package-level declaration is
[license.spdx](license.spdx)).

Patent pending.

For closed-source or proprietary use, contact [license@llingr.io](mailto:license@llingr.io)
for commercial licensing.

- [LICENSE](LICENSE) - AGPL-3.0-only (commercial licensing available)
- [LICENSES/LicenseRef-Llingr-Commercial.txt](LICENSES/LicenseRef-Llingr-Commercial.txt) - the commercial license reference
- [COPYRIGHT](COPYRIGHT) - Copyright
