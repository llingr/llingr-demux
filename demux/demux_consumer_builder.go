// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package demux

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/llingr/llingr-demux/demux/alloc"
	"github.com/llingr/llingr-demux/demux/bandwidth"
	"github.com/llingr/llingr-demux/demux/circuitbreaker"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/config/verify"
	"github.com/llingr/llingr-demux/demux/deadletter"
	"github.com/llingr/llingr-demux/demux/drain"
	"github.com/llingr/llingr-demux/demux/metrics"
	"github.com/llingr/llingr-demux/demux/metrics/snapshot"
	"github.com/llingr/llingr-demux/demux/offset"
	"github.com/llingr/llingr-demux/demux/pipeline"
	"github.com/llingr/llingr-demux/demux/pipeline/throttle"
	"github.com/llingr/llingr-demux/demux/subscription"
	"github.com/llingr/llingr-nexus/nexus"
)

// noOpMetricsSink discards metrics when no sink is configured.
var noOpMetricsSink = func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }

// ConsumerBuilder implements nexus.ConsumerBuilder[T].
//
// Create with NewBuilder(), configure with fluent methods,
// then pass to adapter.CreateConsumer() which calls Build().
//
// Example:
//
//	builder := demux.NewBuilder("orders", processOrder, handleDeadLetter).
//	    WithLogger(logger).
//	    WithEnrichContext(func(ctx context.Context, r *broker.Message) context.Context {
//	        return trace.ContextWithSpan(ctx, extractTraceParent(r.Headers))
//	    })
//
//	consumer := adapter.CreateConsumer(builder)
//	consumer.Subscribe()
type ConsumerBuilder[T any] struct {
	topicName              string                                   // required: topic/stream to consume from
	processMessage         nexus.ProcessMessage[T]                  // required: handles each message
	writeDeadLetter        nexus.WriteDeadLetter[T]                 // required: handles processing errors
	shutdownCallback       nexus.ShutdownCallback                   // recommended: graceful/emergency notification
	logger                 nexus.Logger                             // typically overridden
	metricsSink            nexus.MetricsSink                        // optional per-message observability
	extractEnvelope        nexus.ExtractEnvelope[T]                 // custom adapter extractor
	enrichContext          func(context.Context, T) context.Context // add tracing etc.
	demuxConfig            *config.DemuxConfig                      // concurrency control, timeouts etc.
	ctx                    context.Context                          // for control plane logging
	bandwidthMetricsSink   nexus.BandwidthMetricsSink               // optional bandwidth telemetry
	bandwidthFlushInterval time.Duration                            // optional: override aggregator flush interval
	service                *nexus.Service                           // optional: service identity for fleet routing
	overflowGuard          chan struct{}                            // capacity sharing across multiple consumer instances
	rateLimiter            throttle.RateLimiter[T]                  // optional rate limiting
	licenseKeyFn           verify.GetKeyFn                          // test seam; defaults to verify.GetPublicKey
}

// NewBuilder creates a ConsumerBuilder with required dependencies.
//
// Parameters:
//   - topicName: the topic (Kafka/Pulsar) or stream (NATS) to consume messages from
//   - processMessage: called for each message; must be a blocking call
//   - writeDeadLetter: called on processMessage failures; must be a blocking call
//
// Configure with optional fluent With* methods, then pass to adapter.CreateConsumer().
//
// Example:
//
//	builder := demux.NewBuilder("payment-events-topic", processPayment, handleDeadLetter).
//	    WithLogger(logger).
//	    WithShutdownCallback(onShutdown)
//
//	consumer, _ := adapter.CreateConsumer(builder)
//	consumer.Subscribe()
func NewBuilder[T any](topicName string, process nexus.ProcessMessage[T], deadLetter nexus.WriteDeadLetter[T]) *ConsumerBuilder[T] {
	switch {
	case strings.TrimSpace(topicName) == "":
		panic(errors.New("topicName must not be empty"))
	case process == nil:
		panic(errors.New("processMessage must not be nil"))
	case deadLetter == nil:
		panic(errors.New("deadLetter must not be nil"))
	}

	return &ConsumerBuilder[T]{
		topicName:       topicName,
		processMessage:  process,
		writeDeadLetter: deadLetter,
		rateLimiter:     &throttle.NoOpRateLimiter[T]{},
	}
}

// Build implements nexus.ConsumerBuilder[T].
//
// Called by the adapter to inject itself as BrokerPort and complete consumer construction.
// This wires together the polling, pipeline, offset commit, and metrics components.
//
//	                      CORE MESSAGE FLOW
//	                    ---------------------
//
//	[MESSAGE BROKER]  →  topicSubscription.PollAndForward
//	                                 ↓                        ╭→ [key1] → APPLICATION
//	                                 ↓                        ├→ [key2] → APPLICATION
//	                    pipeline.SendToWorkerForProcessing ───┼→ [key3] → APPLICATION
//	                                                          ├→ [key4] → APPLICATION
//	                                                          ├→  ...
//	                                                          └→ [keyN] → APPLICATION
//	                                                                  ↓
//	                                                                  ↓
//	[MESSAGE BROKER]  ← ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  commit ─ ─ ─ ─  offset.CollectAndCommit
//	                                                                  ↓
//	                                                          metrics.Collect → [MONITORING]
//
// The adapter stores AdaptedConsumer internally (for TriggerRebalance,
// Context, and Logger), and returns the narrower Consumer[T] interface
// to the host application.
func (b *ConsumerBuilder[T]) Build(brokerPort nexus.BrokerPort[T]) nexus.AdaptedConsumer[T] {
	if b.demuxConfig == nil {
		b.demuxConfig = &config.DemuxConfig{}
	}
	b.demuxConfig.SetDemuxConfigDefaults()

	if b.extractEnvelope == nil {
		b.extractEnvelope = brokerPort.ExtractEnvelope
	}
	if b.enrichContext != nil {
		baseExtract := b.extractEnvelope
		b.extractEnvelope = func(payload T) nexus.Envelope {
			env := baseExtract(payload)
			env.Ctx = b.enrichContext(env.Ctx, payload)
			return env
		}
	}

	if b.ctx == nil {
		b.ctx = context.Background()
	}

	if b.logger == nil {
		b.logger = nexus.NewDefaultLogger(slog.LevelInfo)
	}

	if b.licenseKeyFn == nil {
		b.licenseKeyFn = verify.GetPublicKey
	}

	if b.metricsSink == nil {
		b.metricsSink = noOpMetricsSink
	}

	// work items recycled to reduce allocations and GC pressure
	workItemsPool := alloc.NewWorkItemsPool[T](*b.demuxConfig)

	// safety valve: stops consumption and hands off to other instances
	// rather than operating in a degraded or indefinitely blocked state
	circuitBreaker := circuitbreaker.New(b.ctx, b.logger)

	// routes failed messages to the registered nexus.WriteDeadLetter
	deadLetterWriter := deadletter.New[T](b.writeDeadLetter, b.logger)

	// instance-level identity for metrics routing (set once, never changes)
	sinkCtx := nexus.SinkContext{
		Service:       b.service,
		TopicName:     b.topicName,
		ConsumerGroup: brokerPort.ConsumerGroup(),
	}

	// non-blocking, GC-friendly metrics collector
	metricsCollector := metrics.NewCollector[T](b.ctx, *b.demuxConfig, b.metricsSink, sinkCtx, workItemsPool, b.logger)

	// bandwidth telemetry side-channel: adapter → aggregator → sink
	var bandwidthAggregator *bandwidth.Aggregator
	if b.bandwidthMetricsSink != nil {
		if bp, ok := nexus.BrokerPort[T](brokerPort).(nexus.BandwidthPort[T]); ok {
			var aggOpts []bandwidth.AggregatorOption
			if b.bandwidthFlushInterval > 0 {
				aggOpts = append(aggOpts, bandwidth.WithFlushInterval(b.bandwidthFlushInterval))
			}
			if b.service != nil {
				aggOpts = append(aggOpts, bandwidth.WithService(b.service))
			}
			bandwidthAggregator = bandwidth.NewAggregator(b.ctx, b.bandwidthMetricsSink, b.topicName, b.logger, aggOpts...)
			bp.SetBandwidthCallback(bandwidthAggregator.Receive)
		} else {
			b.logger.Warn(b.ctx, "WithBandwidthMetricsSink configured but adapter does not implement BandwidthPort")
		}
	}

	// receives processed messages, commits offsets via brokerPort
	offsetCommitter := offset.NewCommitter[T](b.ctx, *b.demuxConfig, brokerPort.CommitOffsets, metricsCollector, b.logger)

	// capacity guard for concurrent keys
	guard := make(chan struct{}, b.demuxConfig.ConcurrentKeys)

	// demux: fan-out to per-key workers
	pipelineDemux := pipeline.NewDemux[T](*b.demuxConfig, b.processMessage, deadLetterWriter,
		offsetCommitter, circuitBreaker, guard, b.overflowGuard, b.logger, b.rateLimiter.Await)

	// processor: envelope extraction → worker dispatch
	pipelineProcessor := pipeline.NewProcessor[T](b.ctx, guard, b.overflowGuard, pipelineDemux, *b.demuxConfig,
		b.extractEnvelope, workItemsPool, b.logger)

	// drain coordinator: flush workers and commit offsets
	drainCoordinator := drain.NewDrainCoordinator(b.ctx, pipelineDemux, offsetCommitter, circuitBreaker,
		*b.demuxConfig, b.logger)

	// snapshot recorder: assembles point-in-time engine state from subsystem samplers
	overflowGuard := b.overflowGuard
	recorder := snapshot.NewRecorder(
		b.topicName,
		func() snapshot.ConcurrencySnapshot {
			c := snapshot.ConcurrencySnapshot{
				GuardActive:   len(guard),
				GuardCapacity: cap(guard),
			}
			if overflowGuard != nil {
				c.OverflowActive = len(overflowGuard)
				c.OverflowCapacity = cap(overflowGuard)
			}
			c.CommitIngestActive, c.CommitIngestCap = offsetCommitter.CommitIngestChannelLen()
			return c
		},
		pipelineDemux.ShardSnapshots,
		offsetCommitter.PreCommitsSnapshot,
		offsetCommitter.WindowData,
	)

	// subscription: poll loop, rebalance handling, shutdown coordination
	topicSubscription := subscription.New(b.ctx, *b.demuxConfig, circuitBreaker,
		pipelineProcessor, brokerPort.Poll, brokerPort.Subscribe, brokerPort.Unsubscribe, brokerPort.AckRebalance, brokerPort.BrokerQuery,
		drainCoordinator, offsetCommitter.ResetCommittedOffsets,
		offsetCommitter.MarkPartitionAssigned, offsetCommitter.MarkPartitionRevoked, b.topicName, b.logger)

	consumer := &Consumer[T]{
		ctx:                 b.ctx,
		subscription:        topicSubscription,
		pipelineProcessor:   pipelineProcessor,
		offsetCommitter:     offsetCommitter,
		metricsCollector:    metricsCollector,
		bandwidthAggregator: bandwidthAggregator,
		demuxConfig:         b.demuxConfig,
		circuitBreaker:      circuitBreaker,
		recorder:            recorder,
		topicName:           b.topicName,
		logger:              b.logger,
		stopRateLimit:       b.rateLimiter.Stop,
	}

	// set shutdown callback if provided
	if b.shutdownCallback != nil {
		consumer.shutdownCallback.Store(&b.shutdownCallback)
	}

	message, level, err := licenseStatus(time.Now(), b.licenseKeyFn)
	if err != nil {
		consumer.logger.Warn(consumer.ctx, err.Error())
	}
	if message != "" {
		if level == verify.Info {
			consumer.logger.Info(consumer.ctx, message)
		} else {
			consumer.logger.Debug(consumer.ctx, message)
		}
	}

	return consumer
}

// licenseStatus returns the licence message and level to log. The recover keeps
// licence checking strictly off the critical path: it can never fail a Build.
func licenseStatus(now time.Time, keyFn verify.GetKeyFn) (msg string, level verify.Level, err error) {
	defer func() {
		if r := recover(); r != nil {
			msg, level, err = "", verify.Debug, fmt.Errorf("verify: licence check failed: %v", r)
		}
	}()
	return verify.License(now, keyFn)
}

// WithMetricsSink sets the metrics sink for observability.
//
// The sink receives Metrics after each message is processed, enabling
// integration with Prometheus, StatsD, or custom monitoring systems.
//
// If not set, metrics are discarded.
func (b *ConsumerBuilder[T]) WithMetricsSink(sink nexus.MetricsSink) *ConsumerBuilder[T] {
	b.metricsSink = sink
	return b
}

// WithBandwidthMetricsSink sets the sink for bandwidth telemetry.
//
// The sink receives BandwidthMetrics packets aggregated from the broker adapter,
// enabling control-plane visibility into wire-level bytes in/out, broker topology,
// and compression efficiency.
//
// Requires the adapter to implement nexus.BandwidthPort[T]. If the adapter does not
// support bandwidth metrics, a warning is logged and the sink is not activated.
//
// If not set, bandwidth telemetry is not collected.
func (b *ConsumerBuilder[T]) WithBandwidthMetricsSink(sink nexus.BandwidthMetricsSink) *ConsumerBuilder[T] {
	b.bandwidthMetricsSink = sink
	return b
}

// WithBandwidthFlushInterval overrides how often the bandwidth aggregator
// forwards buffered packets to the sink. The default is 60 seconds.
// Shorter intervals reduce delivery latency at no cost to message processing
// (the aggregator runs off the hot path).
func (b *ConsumerBuilder[T]) WithBandwidthFlushInterval(d time.Duration) *ConsumerBuilder[T] {
	b.bandwidthFlushInterval = d
	return b
}

// WithLogger sets the logger for operational logging.
//
// If not set, a default slog-based logger at INFO level is used.
func (b *ConsumerBuilder[T]) WithLogger(logger nexus.Logger) *ConsumerBuilder[T] {
	b.logger = logger
	return b
}

// WithDemuxConfig sets configuration for worker pools, buffer sizes, and timeouts.
//
// Defaults are applied for any unset values.
func (b *ConsumerBuilder[T]) WithDemuxConfig(cfg config.DemuxConfig) *ConsumerBuilder[T] {
	b.demuxConfig = &cfg
	return b
}

// WithContext sets the control-plane context for the consumer lifecycle.
// This context is used for logging and internal coordination, not for
// message processing (each message gets its own context via ExtractEnvelope).
//
// If not set, context.Background() is used.
func (b *ConsumerBuilder[T]) WithContext(ctx context.Context) *ConsumerBuilder[T] {
	b.ctx = ctx
	return b
}

// WithOverflowGuard sets a channel for backpressure signaling.
//
// When the pipeline is at capacity, the guard channel is used to
// signal that new messages should be throttled. This provides
// shared burst capacity across multiple consumer instances.
//
// If not set (the default), only core config.ConcurrentKeys is used.
func (b *ConsumerBuilder[T]) WithOverflowGuard(guard chan struct{}) *ConsumerBuilder[T] {
	b.overflowGuard = guard
	return b
}

// WithExtractEnvelope overrides the adapter's default envelope extractor.
//
// The envelope extractor maps broker-specific messages to addressing info
// (partition, offset, key) and optionally provides a context for processing.
//
// Use this for full control when you need to:
//   - Optimize key extraction (e.g., skip UTF-8 validation for known UUID keys)
//   - Handle special message formats
//
// For just adding tracing to the context, prefer WithEnrichContext, which
// composes with the adapter's resolved Envelope extractor.
//
// Example:
//
//	builder.WithExtractEnvelope(func(r *broker.Message) nexus.Envelope {
//	    return nexus.Envelope{
//	        Partition: r.Partition,
//	        Offset:    r.Offset,
//	        Key:       string(r.Key),
//	        Ctx:       context.Background(),
//	    }
//	})
func (b *ConsumerBuilder[T]) WithExtractEnvelope(fn nexus.ExtractEnvelope[T]) *ConsumerBuilder[T] {
	b.extractEnvelope = fn
	return b
}

// WithEnrichContext adds context enrichment on top of envelope extraction.
//
// This makes it straightforward to add tracing, logging context, or request IDs
// without reimplementing envelope extraction.
//
// Resolves for both default and custom extractors - if you use both
// WithExtractEnvelope and WithEnrichContext, the enricher is resolved
// last since it is the preferred way to provide enrich per-message contexts.
//
// Example:
//
//	builder.WithEnrichContext(func(ctx context.Context, r *broker.Message) context.Context {
//	    traceParent := extractTraceParent(r.Headers)
//	    return trace.ContextWithSpan(ctx, traceParent)
//	})
func (b *ConsumerBuilder[T]) WithEnrichContext(fn func(context.Context, T) context.Context) *ConsumerBuilder[T] {
	b.enrichContext = fn
	return b
}

// WithShutdownCallback sets a callback for shutdown notification.
//
// The callback is invoked when the consumer exits:
//   - Graceful shutdown (reason nil): called after Shutdown() completes successfully
//   - Emergency shutdown (reason non-nil): called when circuit breaker triggers
//
// If not set, a default handler is used which logs completion for graceful
// shutdown, or waits 15s then sends os.Interrupt for emergency shutdown.
//
// Example:
//
//	builder.WithShutdownCallback(func(ctx context.Context, reason error) {
//	    if reason != nil {
//	        log.Error("consumer emergency shutdown", "error", reason)
//	        alertOps(reason)
//	    }
//	})
func (b *ConsumerBuilder[T]) WithShutdownCallback(fn nexus.ShutdownCallback) *ConsumerBuilder[T] {
	b.shutdownCallback = fn
	return b
}

// WithRateLimiter to throttle message forwarding in the pipeline, which
// can be used to protect downstream systems and/or control resource usage.
//
// Two built-in limiter types are provided:
//   - throttle.NewTokenBucket: allows bursting up to burst size, then steady rate
//   - throttle.NewTicker: steady, evenly spaced tokens (no bursting)
//
// Example:
//
//	builder.WithRateLimiter(
//	  throttle.NewTokenBucket[*broker.Message](messagesPerSec, burst)
//	)
func (b *ConsumerBuilder[T]) WithRateLimiter(limiter throttle.RateLimiter[T]) *ConsumerBuilder[T] {
	b.rateLimiter = limiter
	return b
}

// WithService identity sent to metrics sink callbacks
func (b *ConsumerBuilder[T]) WithService(service nexus.Service) *ConsumerBuilder[T] {
	b.service = &service
	return b
}

// TopicName returns the configured topic/stream name
func (b *ConsumerBuilder[T]) TopicName() string {
	return b.topicName
}
