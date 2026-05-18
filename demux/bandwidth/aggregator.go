// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Package bandwidth provides a side-channel aggregator for bandwidth telemetry
// from broker adapters. This operates independently of the core message processing
// pipeline - adapters push BandwidthMetrics packets at a configurable cadence,
// and the aggregator forwards them to the user's BandwidthMetricsSink.
package bandwidth

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

const (
	// defaultFlushInterval is how often the aggregator flushes buffered packets
	// to the sink, regardless of buffer depth.
	defaultFlushInterval = 60 * time.Second

	// defaultMaxBuffer is the packet count threshold that triggers an
	// immediate flush before the timer fires.
	defaultMaxBuffer = 50
)

// Stats is a point-in-time view of aggregator counters.
type Stats struct {
	Flushed int64 `json:"flushed"`
	Dropped int64 `json:"dropped"`
}

// Aggregator buffers BandwidthMetrics packets from an adapter and forwards
// them to a BandwidthMetricsSink. Flushing occurs every 60 seconds or when
// 50 or more packets have accumulated, whichever comes first.
//
// The aggregator is created by the builder when both a BandwidthMetricsSink
// and a BandwidthPort adapter are present. It is not on the message processing
// hot path.
type Aggregator struct {
	sink          nexus.BandwidthMetricsSink
	topicName     string
	team          *nexus.Team
	buffer        []nexus.BandwidthMetrics
	mu            sync.Mutex
	flushInterval time.Duration
	maxBuffer     int
	quit          chan struct{}
	once          sync.Once
	warnOnce      sync.Once
	wg            sync.WaitGroup
	logger        nexus.Logger
	ctx           context.Context
	flushedCount  atomic.Int64
	droppedCount  atomic.Int64
}

// AggregatorOption configures an Aggregator via functional options.
type AggregatorOption func(*Aggregator)

// WithFlushInterval overrides the default 60-second flush timer.
// Shorter intervals reduce delivery latency to the sink at the cost
// of more frequent lock acquisition (irrelevant off the hot path).
func WithFlushInterval(d time.Duration) AggregatorOption {
	return func(a *Aggregator) {
		if d > 0 {
			a.flushInterval = d
		}
	}
}

// WithTeam stamps team ownership metadata on every bandwidth packet.
func WithTeam(team *nexus.Team) AggregatorOption {
	return func(a *Aggregator) {
		a.team = team
	}
}

// NewAggregator creates a bandwidth aggregator that buffers adapter packets
// and forwards them to the sink.
func NewAggregator(ctx context.Context, sink nexus.BandwidthMetricsSink, topicName string, logger nexus.Logger, opts ...AggregatorOption) *Aggregator {
	a := &Aggregator{
		sink:          sink,
		topicName:     topicName,
		buffer:        make([]nexus.BandwidthMetrics, 0, defaultMaxBuffer),
		flushInterval: defaultFlushInterval,
		maxBuffer:     defaultMaxBuffer,
		quit:          make(chan struct{}),
		logger:        logger,
		ctx:           ctx,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Start spawns the flush timer goroutine.
func (a *Aggregator) Start() {
	a.wg.Add(1)
	go a.flushLoop()
}

// Receive is the callback registered on the adapter via SetBandwidthCallback.
// It appends the packet to the buffer and triggers an immediate flush when
// the buffer reaches the threshold.
func (a *Aggregator) Receive(packet nexus.BandwidthMetrics) {
	if a.team != nil {
		packet.Team = a.team
	}
	a.mu.Lock()
	a.buffer = append(a.buffer, packet)
	shouldFlush := len(a.buffer) >= a.maxBuffer
	a.mu.Unlock()

	if shouldFlush {
		a.flush()
	}
}

// Stop signals the flush goroutine to exit, waits for it, and performs
// a final flush of any remaining buffered packets.
func (a *Aggregator) Stop() {
	a.once.Do(func() {
		close(a.quit)
	})
	a.wg.Wait()
	a.flush() // final drain
}

// Stats returns a point-in-time view of aggregator counters.
func (a *Aggregator) Stats() Stats {
	return Stats{
		Flushed: a.flushedCount.Load(),
		Dropped: a.droppedCount.Load(),
	}
}

// flushLoop runs the periodic flush timer.
func (a *Aggregator) flushLoop() {
	defer a.wg.Done()
	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			a.flush()
		case <-a.quit:
			return
		}
	}
}

// flush copies the buffer under lock, releases the lock, then delivers
// each packet individually to the sink. Errors are logged and counted
// but do not propagate - fire-and-forget, consistent with MetricsSink.
func (a *Aggregator) flush() {
	a.mu.Lock()
	if len(a.buffer) == 0 {
		a.mu.Unlock()
		return
	}
	packets := a.buffer
	a.buffer = make([]nexus.BandwidthMetrics, 0, a.maxBuffer)
	a.mu.Unlock()

	if a.team == nil {
		a.warnOnce.Do(func() {
			a.logger.Warn(a.ctx, "WithTeam() has not been configured - set this for fleet self-reporting")
		})
	}

	for i := range packets {
		if err := a.sink(a.topicName, packets[i]); err != nil {
			a.logger.Warn(a.ctx, fmt.Sprintf("bandwidth metrics sink error: %s", err))
			a.droppedCount.Add(1)
		} else {
			a.flushedCount.Add(1)
		}
	}
}
