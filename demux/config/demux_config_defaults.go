// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package config

import (
	"fmt"
	"math/bits"
	"os"
	"strings"
	"time"
)

// DemuxConfig - for tuning the Message processing pipeline
//
// The most important defaults are towards the top, although it is
// worth understanding each, as these values work together to provide
// performant and reliable message processing.

const (
	// Core setting, controls maximum per-key concurrency (per-instance - e.g. k8s pod).
	// Higher settings reduce head-of-line blocking, improving latency and throughput,
	// but must be balanced to not overwhelm an application and its dependencies.
	concurrentKeys    = 250
	concurrentKeysMin = 1    // not recommended
	concurrentKeysMax = 5000 // consider all contention: downstream resources AND the Go runtime

	// Messages processed concurrently are still ordered using the partition key.
	// This setting defines the channel length to buffer messages. Consider the impact
	// on rebalances with sequential workers drain times from larger buffers
	perKeyBufferLen    = 16
	perKeyBufferLenMin = 1  // for one message per key scenarios, not normally recommended
	perKeyBufferLenMax = 64 // long chains of events in single transaction - very noisy neighbours

	// Wait time for polling loop when there are no messages in an assigned partition.
	// Longer timeouts impact average time to begin a rebalance: use shorter timeouts if
	// absolute transaction latency is a concern.
	pollTimeout        = 100 * time.Millisecond
	pollTimeoutMinimum = 20 * time.Millisecond // added compute but faster average rebalance time
	pollTimeoutMax     = 2 * time.Second       // lower compute but increases average rebalance lag

	// Async commit tick duration, commits acknowledge message(s) have been processed in
	// the broker. Longer intervals reduce chatter but can increase the number of duplicates
	// (multiple consumers given same message) after outages, for example an OOM or power cut.
	commitInterval        = 5 * time.Second
	commitIntervalMinimum = 250 * time.Millisecond // slightly more load on broker
	commitIntervalMax     = 15 * time.Second       // more duplicates in unstable systems

	// On rebalance, messages stop entering the pipeline allowing it to 'workers drain'.
	// For fast systems this will be minimal; this timeout aligns 'worst case' with standard
	// container orchestration platforms 30s termination. Drains taking >20s typically indicate
	// external system issues (payment providers, databases, etc.).
	drainTimeout        = 20 * time.Second
	drainTimeoutMinimum = 2 * time.Second  // for fast workloads (e.g. in-memory only changes)
	drainTimeoutMax     = 55 * time.Second // worst-case Kubernetes constraints

	// On startup, a new consumer needs to wait for others to rebalance before it is assigned
	// partitions. This setting should be higher than the (above) drainTimeout as it
	// proxies worst-case rebalance times. In rolling deployments, new consumer join times
	// are less critical than existing consumer rebalance times, within reason.
	awaitAssignmentsTimeout        = 50 * time.Second
	awaitAssignmentsTimeoutMinimum = 5 * time.Second // for fast brokers and efficient processors
	awaitAssignmentsTimeoutMax     = 5 * time.Minute // consider refactoring slow processors

	// The offset committer pauses message ingest while performing async commits.
	// While it does this, messages are queued to a buffered channel to avoid backpressure
	// in the pipeline.Processor; in very fast processors this can still be a concern.
	// The buffer length is calculated based on concurrentKeys, with memory conscious min/max.
	// Reducing commitInterval (above) helps contain buffered messages.
	commitIngestChannelLenCalcMin = 5000   // calculation minimum
	commitIngestChannelLenCalcMax = 100000 // calculation maximum
	commitIngestChannelLenMin     = 1000   // too low can cause pipeline.Processor backpressure
	commitIngestChannelLenMax     = 200000 // absolute maximum

	// Concurrently processed messages can complete out of order. When this happens,
	// non-contiguous offsets are 'gap-buffered' for a later sort and re-scan process.
	// The initial size of each partition gap buffer is there to avoid unnecessary
	// re-size allocations, it is not a limit on the number of out-of-order offsets cached.
	commitPartitionSliceLen    = 400
	commitPartitionSliceLenMin = 50   // plenty for low traffic deployments
	commitPartitionSliceLenMax = 2000 // for high-throughput, high jitter situations

	// Timeout for broker metadata queries (partition assignments, consumer group
	// coordination). Conservative setting for brokers under sustained load.
	queryTimeout        = 5 * time.Second
	queryTimeoutMinimum = 1 * time.Second  // lowest reasonable value for busy brokers
	queryTimeoutMax     = 10 * time.Second // consider if broker cluster is sufficiently resourced

	// For circuit breaker, if the pipeline is full and there is no worker availability for
	// a sustained period, the application is likely deadlocked. This 'safety valve' setting
	// normally won't trigger, but if it does it may warrant further investigation.
	acquireWorkerTimeoutCircuitBreaker        = time.Minute
	acquireWorkerTimeoutCircuitBreakerMinimum = 15 * time.Second // sufficient to avoid most false-positives
	acquireWorkerTimeoutCircuitBreakerMax     = 15 * time.Minute // essentially 'forever' in compute terms

	// For demultiplexer work sharding: the number of shards. Always power of 2 for fast
	// bitwise hash/index conversion. Each worker shard has its own mutex to reduce contention. Low
	// numbers increase contention, high numbers reduce contention but with more cache misses.
	// ** Change only after profiling and understanding workloads **
	workerShardsCount    = 16 // ~1.25Kb coordinators, high chance coordinator data stays in cache
	workerShardsCountMax = 64 // diminishing returns due to cache pressure

	// For workers drain before rebalance, if the pipeline is full and there is no worker availability
	// for a sustained period, the application is likely deadlocked. This 'safety valve' setting
	// normally won't trigger, but if it does it may warrant further investigation.
	rebalancePausePollingTimeout        = 30 * time.Second // highly contended workers with slow processing
	rebalancePausePollingTimeoutMinimum = 10 * time.Second // sufficient to avoid most false-positives
	rebalancePausePollingTimeoutMax     = 10 * time.Minute // essentially 'forever' in compute terms

	// Timeout for acquiring the commit guard during async offset commits. Controls how long
	// the committer will wait to acquire the guard before timing out and logging an error.
	// This safety valve prevents commit operations from blocking indefinitely when there's
	// contention on the commit guard.
	acquireCommitGuardTimeout        = 10 * time.Second       // reasonable timeout for typical usage
	acquireCommitGuardTimeoutMinimum = 100 * time.Millisecond // primarily useful for testing
	acquireCommitGuardTimeoutMax     = 30 * time.Second       // balance between patience and deadlock detection

	// SkipValidationEnvVar set to 'true' to disable validations. Rarely useful
	// for production code but provided for tests, research and more extreme deployments
	SkipValidationEnvVar = "LLINGR_DEMUX_SKIP_CONFIG_VALIDATION"
)

// SetDemuxConfigDefaults when not provided by the application hosting this library.
// These defaults already support high throughput systems, however for extreme cases
// they allow significant latitude for further tuning.
//
// Remember these are **per-instance** settings, for example with concurrentKeys=500 on
// 50 consumers subscribed to 50 partitions, the effective partition count becomes 25000,
// and if each message takes 100ms that means throughput up to 250,000 messages per second.
//
//nolint:gocyclo,gocognit // flat per-field validation manifest: same zero/min/max shape repeated, no nesting
func (c *DemuxConfig) SetDemuxConfigDefaults() DemuxConfig {

	skipValidation := isValidationSkipped()

	if c.ConcurrentKeys < concurrentKeysMin {
		c.ConcurrentKeys = concurrentKeys
	} else if c.ConcurrentKeys > concurrentKeysMax && !skipValidation {
		const validationError = "invalid ConcurrentKeys: %d, should be no more than %d"
		panic(fmt.Errorf(validationError, c.ConcurrentKeys, concurrentKeysMax))
	}

	if c.PerKeyBufferLen < perKeyBufferLenMin {
		c.PerKeyBufferLen = perKeyBufferLen
	} else if c.PerKeyBufferLen > perKeyBufferLenMax && !skipValidation {
		const validationError = "invalid PerKeyBufferLen: %d, should be no more than %d"
		panic(fmt.Errorf(validationError, c.PerKeyBufferLen, perKeyBufferLenMax))
	}

	switch {
	case c.PollTimeout < 1:
		c.PollTimeout = pollTimeout
	case c.PollTimeout < pollTimeoutMinimum && !skipValidation:
		const validationError = "invalid PollTimeout: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.PollTimeout, pollTimeoutMinimum))
	case c.PollTimeout > pollTimeoutMax && !skipValidation:
		const validationError = "invalid PollTimeout: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.PollTimeout, pollTimeoutMax))
	}

	switch {
	case c.AutoCommitInterval < 1:
		c.AutoCommitInterval = commitInterval
	case c.AutoCommitInterval < commitIntervalMinimum && !skipValidation:
		const validationError = "invalid AutoCommitInterval: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.AutoCommitInterval, commitIntervalMinimum))
	case c.AutoCommitInterval > commitIntervalMax && !skipValidation:
		const validationError = "invalid AutoCommitInterval: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.AutoCommitInterval, commitIntervalMax))
	}

	switch {
	case c.QueryTimeout < 1:
		c.QueryTimeout = queryTimeout
	case c.QueryTimeout < queryTimeoutMinimum && !skipValidation:
		const validationError = "invalid QueryTimeout: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.QueryTimeout, queryTimeoutMinimum))
	case c.QueryTimeout > queryTimeoutMax && !skipValidation:
		const validationError = "invalid QueryTimeout: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.QueryTimeout, queryTimeoutMax))
	}

	switch {
	case c.DrainTimeout < 1:
		c.DrainTimeout = drainTimeout
	case c.DrainTimeout < drainTimeoutMinimum && !skipValidation:
		const validationError = "invalid DrainTimeout: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.DrainTimeout, drainTimeoutMinimum))
	case c.DrainTimeout > drainTimeoutMax && !skipValidation:
		const validationError = "invalid DrainTimeout: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.DrainTimeout, drainTimeoutMax))
	}

	switch {
	case c.AwaitAssignmentsTimeout < 1:
		c.AwaitAssignmentsTimeout = awaitAssignmentsTimeout
	case c.AwaitAssignmentsTimeout < awaitAssignmentsTimeoutMinimum && !skipValidation:
		const validationError = "invalid AwaitAssignmentsTimeout: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.AwaitAssignmentsTimeout, awaitAssignmentsTimeoutMinimum))
	case c.AwaitAssignmentsTimeout > awaitAssignmentsTimeoutMax && !skipValidation:
		const validationError = "invalid AwaitAssignmentsTimeout: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.AwaitAssignmentsTimeout, awaitAssignmentsTimeoutMax))
	}

	switch {
	case c.CommitIngestChannelLen < 1:
		c.CommitIngestChannelLen = func() int {
			const (
				keysMultiplierMin = 50
				keysMultiplierMax = 1000
				multiplier        = 100
			)
			switch {
			case c.ConcurrentKeys < keysMultiplierMin:
				return commitIngestChannelLenCalcMin
			case c.ConcurrentKeys > keysMultiplierMax:
				return commitIngestChannelLenCalcMax
			default:
				return c.ConcurrentKeys * multiplier
			}
		}()
	case c.CommitIngestChannelLen < commitIngestChannelLenMin && !skipValidation:
		const validationError = "invalid CommitIngestChannelLen: %d, should be no less than %d"
		panic(fmt.Errorf(validationError, c.CommitIngestChannelLen, commitIngestChannelLenMin))
	case c.CommitIngestChannelLen > commitIngestChannelLenMax && !skipValidation:
		const validationError = "invalid CommitIngestChannelLen: %d, should be no more than %d"
		panic(fmt.Errorf(validationError, c.CommitIngestChannelLen, commitIngestChannelLenMax))
	}

	switch {
	case c.CommitPartitionSliceLen < 1:
		c.CommitPartitionSliceLen = commitPartitionSliceLen
	case c.CommitPartitionSliceLen < commitPartitionSliceLenMin && !skipValidation:
		const validationError = "invalid CommitPartitionSliceLen: %d, should be no less than %d"
		panic(fmt.Errorf(validationError, c.CommitPartitionSliceLen, commitPartitionSliceLenMin))
	case c.CommitPartitionSliceLen > commitPartitionSliceLenMax && !skipValidation:
		const validationError = "invalid CommitPartitionSliceLen: %d, should be no more than %d"
		panic(fmt.Errorf(validationError, c.CommitPartitionSliceLen, commitPartitionSliceLenMax))
	}

	switch {
	case c.AcquireWorkerTimeoutCircuitBreaker < 1:
		c.AcquireWorkerTimeoutCircuitBreaker = acquireWorkerTimeoutCircuitBreaker
	case c.AcquireWorkerTimeoutCircuitBreaker < acquireWorkerTimeoutCircuitBreakerMinimum && !skipValidation:
		const validationError = "invalid AcquireWorkerTimeoutCircuitBreaker: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.AcquireWorkerTimeoutCircuitBreaker, acquireWorkerTimeoutCircuitBreakerMinimum))
	case c.AcquireWorkerTimeoutCircuitBreaker > acquireWorkerTimeoutCircuitBreakerMax && !skipValidation:
		const validationError = "invalid AcquireWorkerTimeoutCircuitBreaker: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.AcquireWorkerTimeoutCircuitBreaker, acquireWorkerTimeoutCircuitBreakerMax))
	}

	switch {
	case c.WorkerShardsCount < 1:
		c.WorkerShardsCount = workerShardsCount
	case !isPowerOfTwo(c.WorkerShardsCount):
		const validationError = "invalid WorkerShardsCount: %d, must be a power of 2"
		panic(fmt.Errorf(validationError, c.WorkerShardsCount))
	case c.WorkerShardsCount > workerShardsCountMax && !skipValidation:
		const validationError = "invalid WorkerShardsCount: %d, should be no more than %d"
		panic(fmt.Errorf(validationError, c.WorkerShardsCount, workerShardsCountMax))
	}

	switch {
	case c.RebalancePausePollingTimeout < 1:
		c.RebalancePausePollingTimeout = rebalancePausePollingTimeout
	case c.RebalancePausePollingTimeout < rebalancePausePollingTimeoutMinimum && !skipValidation:
		const validationError = "invalid RebalancePausePollingTimeout: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.RebalancePausePollingTimeout, rebalancePausePollingTimeoutMinimum))
	case c.RebalancePausePollingTimeout > rebalancePausePollingTimeoutMax && !skipValidation:
		const validationError = "invalid RebalancePausePollingTimeout: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.RebalancePausePollingTimeout, rebalancePausePollingTimeoutMax))
	}

	switch {
	case c.AcquireCommitGuardTimeout < 1:
		c.AcquireCommitGuardTimeout = acquireCommitGuardTimeout
	case c.AcquireCommitGuardTimeout < acquireCommitGuardTimeoutMinimum && !skipValidation:
		const validationError = "invalid AcquireCommitGuardTimeout: %s, should be no less than %s"
		panic(fmt.Errorf(validationError, c.AcquireCommitGuardTimeout, acquireCommitGuardTimeoutMinimum))
	case c.AcquireCommitGuardTimeout > acquireCommitGuardTimeoutMax && !skipValidation:
		const validationError = "invalid AcquireCommitGuardTimeout: %s, should be no more than %s"
		panic(fmt.Errorf(validationError, c.AcquireCommitGuardTimeout, acquireCommitGuardTimeoutMax))
	}

	return *c
}

func isValidationSkipped() bool {
	return strings.EqualFold(os.Getenv(SkipValidationEnvVar), "true")
}

func isPowerOfTwo(value int) bool {
	return value >= 2 && bits.OnesCount(uint(value)) == 1
}
