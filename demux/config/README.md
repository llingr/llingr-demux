# Demux Configuration Guide

The llingr-demux library is a high-performance, broker-agnostic consumer that handles messages concurrently
while maintaining per-partition-key ordering. This guide explains some of the config options so that this
understanding can be applied to different operational scenarios, from low-throughput development environments
to high-volume production workloads.

## Understanding the Demux Architecture

Before diving into configuration, it is helpful to understand what llingr-demux actually does. Traditional
consumers process messages sequentially within each partition. The general approaches to increasing throughput
include reducing processing time (more compute, bigger/faster databases) or increasing parallelism in the
infrastructure layer - more physical partitions. Both options are increasingly expensive and will continue to
create head-of-line blocking when slow messages delay those behind them in a partition.

The llingr-demux approach solves this by demultiplexing messages to dedicated worker channels based on their
partition keys, so messages on a single partition can 'fan out' into concurrent threads, meaning each partition
can serve highly concurrent workloads without any changes to message broker infrastructure.

When a message arrives, it is routed to a processing channel specific to its partition key. This means
messages with different keys process concurrently, while messages with the same key maintain strict
ordering. Each partition key effectively gets its own sequential partition, eliminating blocking issues
that plague traditional consumers.

The system manages three main phases: polling messages from the broker, demultiplexing them to workers
based on partition keys, and collecting processed results back into per-partition commit streams. The
configuration options control how these phases coordinate and how resources are allocated across them.

## Configuration Validation

Early versions of llingr-demux included comprehensive startup config checks producing messages such as:

- PollTimeout (2s) too close to AutoCommitInterval (500ms), should be <25%
- ConcurrentKeys (1500) very high, ensure sufficient CPU cores and memory
- AutoCommitInterval (500ms) very aggressive, may cause excessive broker load
- AwaitAssignmentsTimeout (15s) should exceed DrainTimeout (20s) to allow for coordinated shutdowns during rebalance

On reflection this was removed because it introduced 'meta-complexity' - complexity about
complexity - and assumed too much about unknowable deployment specifics.

Our advice is to understand the architecture, read through the extensive comments on each config setting,
and proceed with care.

The defaults already support extremely high traffic levels. For those with the most demanding workloads, the
expertise required to operate at higher throughput necessitates deep understanding of every link in the chain,
so it is worth investing the time in understanding the impact of config settings so that you can make the
best choices for your deployments.

## Core Configuration Parameters

### Worker Concurrency and Buffering

**ConcurrentKeys** controls how many partition keys can be processed simultaneously. With a default
of 250, your consumer instance can handle up to 250 different partition keys at once. Workers are
created on-demand and self-terminate when their key's work channel empties.

For **low-throughput scenarios** (development, testing, light production workloads), you might reduce
this to 50-100 to conserve memory. For **high-throughput scenarios** with diverse partition keys,
increase to 500-1000, but ensure your host system has sufficient CPU, memory and (for example)
database connections to handle the increased parallelism.

**PerKeyBufferLen** sets the buffer size for each partition key's message channel. The default of 16
messages per key prevents "noisy neighbour" issues where slow processing of one key blocks others.
Each key gets its own buffered lane, so a slow payment processing operation won't delay fast
inventory updates. Most transactions might result in 5 or 6 events per partition, so 16 is normally
ample for the majority of workloads. One downside to having too many is that - because these are
virtual partitions - sequential processing can increase drain times, slowing rebalances.  16 messages
with a typical/conservative ~40ms per-message processing means up to 700ms drain time, which aligns
with the library's key objective of always supporting sub-second rebalances (normally much faster) for
even lumpy workloads. Memory is less of an issue here, its more the length of the channel dictates
the maximum drain time.

### Timing and Polling Configuration

**PollTimeout** determines how long the system waits for new messages in the broker client's
buffer. The default 100ms provides a good balance between processing latency and rebalancing
responsiveness. This timeout primarily affects how quickly the system detects rebalancing events, so
client-buffered messages are picked up immediately regardless of timeout. If there are no messages,
rebalances will be delayed by 100ms, for busy systems the delay is close to zero.

For **latency-sensitive applications**, consider reducing to 50ms for faster rebalancing detection.
For **batch-oriented workloads** where rebalancing speed is less critical, you might increase to
200-500ms.

**AutoCommitInterval** controls how often processed message offsets are committed back to the message
broker. The default 5 seconds ensures that offset commits don't overwhelm your brokers while keeping
the window of potential duplicate processing contained.

In **high-throughput scenarios**, resist the temptation to commit more frequently - this can reduce
overall system performance. For **low-throughput scenarios** where you want minimal duplicate
processing risk, you might reduce to 2-3 seconds, but rarely below 1 second as this can create
excessive broker load.

## Timeout Hierarchies and System Coordination

The various timeout settings form a hierarchy that ensures proper system coordination during normal
operation and graceful degradation during infrastructure failures. Understanding these relationships
is crucial for stable operation.

**PollTimeout** should be much smaller than **AutoCommitInterval** - one rule of thumb is no more
than 25% of the commit interval. This ensures the system polls frequently enough to maintain steady
throughput between commits. A 2-second poll timeout with a 5-second commit interval could create
stuttering message flow.

**DrainTimeout** defines how long the system waits for in-flight messages to complete during
rebalancing. This should align with your runtime orchestration platform's termination grace
period. The default 20 seconds works well with container orchestrators that typically give pods 30
seconds to shut down. In all cases, drain timeout should exceed your worst-case processing latency
when draining **PerKeyBufferLen** messages.

**AwaitAssignmentsTimeout** should exceed **DrainTimeout** to allow for coordinated shutdowns during
rebalancing. When consumers join or leave a group, existing consumers need time to drain their work
before partitions are reassigned. If assignment timeout is too short, consumers may repeatedly fail
to join during rolling deployments, and crash-loops will increase rebalance churn on active consumers.

## Resource Management and Memory Allocation

The system pre-allocates several buffers to maintain predictable performance under load.
**CommitIngestChannelLen** creates a large channel buffering between message processing and offset
commits. The default calculation provides roughly 5x the theoretical maximum message processing
capacity, preventing commit operations from creating backpressure in the processing pipeline.

For **memory-constrained environments**, you might reduce this, but monitor for processing delays
during commits. For **high-memory systems** prioritizing throughput, the default
calculation should suffice - increasing it further provides only marginal benefits.

**CommitPartitionSliceLen** pre-allocates space for tracking message gaps within each partition.
The default 400 assumes high jitter causing out-of-order message arrival. If your workload
is both fast *and* high jitter, consider increasing this to 800-1000. For more ordered or
low-throughput workloads, you might reduce to 50-200.

## Network Resilience and Fault Tolerance

**QueryTimeout** should provide enough headroom above **PollTimeout** to handle network variations.
The system may query broker metadata (separately from message polling), and these operations need
tolerance for occasional network delays or broker slowdowns.

For **reliable network environments** (same data center, dedicated connections), a 3x ratio
between query and poll timeouts usually suffices. For **variable network conditions** (some cloud
environments, cross-data-center setups), consider a 5-10x ratio to prevent spurious failures
during traffic spikes or maintenance windows.

**AcquireWorkerTimeoutCircuitBreaker** acts as the final safety valve. If all workers are
occupied for this duration, the system triggers emergency shutdown rather than blocking indefinitely.
This should be long enough to handle normal rebalancing and startup coordination but short enough to
prevent indefinite hangs. If this does trigger, it likely indicates a deadlock in your processing
code, so this is worth investigating to track down the source of the issue.

## Understanding System Behavior

The llingr-demux exhibits different performance characteristics as you adjust these parameters.
Increasing concurrency generally improves throughput until you hit resource constraints (CPU,
memory, or I/O limits). Each system's performance tends to be limited by the slowest or most
constrained component in your processing pipeline.

If your processing functions are CPU-bound, significant increases of **ConcurrentKeys** may
actually hurt performance due to context switching overhead. If they are I/O-bound (database
calls, API requests), you can often benefit from concurrency levels much higher than available
compute.

The system includes detailed monitoring via the metrics collector, which tracks processing
latencies and commit performance. Use these metrics to understand how configuration
changes affect your specific workloads rather than relying solely on theoretical guidelines.

Buffer sizes create a trade-off between memory usage and resilience to processing time variations.
Larger buffers smooth out temporary delays but consume more memory. Smaller buffers reduce resource
usage but make the system more sensitive to spikes in processing time.

## Thundering Herds

Remember that llingr-demux is designed to deliver messages to an application **extremely quickly**,
and this means that balancing configuration to ensure the application and its dependencies are not
overwhelmed is as important as providing this capability itself. The library does have protections
and will use circuit-breakers that will 'fail fast to restart cleanly' when necessary. Understanding
these is as important as understanding the config and the processing architecture itself.
