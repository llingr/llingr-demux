// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-demux/tests/testkit/broker"
	"github.com/llingr/llingr-nexus/nexus"
)

var (
	svcNoopProcess    = func(_ context.Context, _ *nexus.Message[string]) error { return nil }
	svcNoopDeadLetter = func(_ context.Context, _ *nexus.Message[string], _ error) error { return nil }
	svcTest           = nexus.Service{Name: "test-svc", Team: "test-team"}
)

// TestE2E_WithService_PropagatesToMetricsSink verifies that a Service
// configured via WithService() reaches the MetricsSink callback with both
// Name and Team intact, exercising the full builder -> SinkContext path
func TestE2E_WithService_PropagatesToMetricsSink(t *testing.T) {
	captured := make(chan nexus.SinkContext, 1)
	payload := "p"
	msgs := []*nexus.Message[string]{{Key: "k", Payload: &payload}}
	mockBroker := broker.NewMockBroker[string](msgs, nil)

	consumer := demux.NewBuilder("test-topic", svcNoopProcess, svcNoopDeadLetter).
		WithDemuxConfig(config.DemuxConfig{ConcurrentKeys: 1}).
		WithService(svcTest).
		WithMetricsSink(func(ctx nexus.SinkContext, _ nexus.Metrics) error {
			select {
			case captured <- ctx:
			default:
			}
			return nil
		}).
		WithExtractEnvelope(func(_ string) nexus.Envelope {
			return nexus.Envelope{Key: "k", Ctx: context.Background()}
		}).
		WithLogger(mocklogger.NewNoOpLogger()).
		Build(mockBroker)

	mockBroker.SetRebalanceCallback(broker.MakeAssignAllPartitionsCallback(t, consumer, 1))

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// give the poll loop a tick to pick up the single message, then Shutdown
	// forces the metrics collector to drain (1 message never fills the buffer)
	time.Sleep(200 * time.Millisecond)
	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}

	select {
	case ctx := <-captured:
		assertSvc(t, ctx.Service)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for sink callback")
	}
}

// svcBandwidthBroker wraps MockBroker with BandwidthPort capability so the
// builder wires up the bandwidth aggregator
type svcBandwidthBroker struct {
	*broker.MockBroker[string]
	cb nexus.BandwidthCallback
}

func (b *svcBandwidthBroker) SetBandwidthCallback(cb nexus.BandwidthCallback) { b.cb = cb }
func (b *svcBandwidthBroker) StatsInterval() time.Duration                    { return time.Hour }

// TestE2E_WithService_PropagatesToBandwidthSink verifies that a Service
// configured via WithService() reaches the BandwidthMetricsSink callback
// with both Name and Team intact, exercising the full builder -> aggregator
// -> packet stamping path
func TestE2E_WithService_PropagatesToBandwidthSink(t *testing.T) {
	captured := make(chan nexus.BandwidthMetrics, 1)
	bb := &svcBandwidthBroker{MockBroker: broker.NewMockBroker[string](nil, nil)}

	consumer := demux.NewBuilder("test-topic", svcNoopProcess, svcNoopDeadLetter).
		WithService(svcTest).
		WithBandwidthMetricsSink(func(_ string, m nexus.BandwidthMetrics) error {
			select {
			case captured <- m:
			default:
			}
			return nil
		}).
		WithBandwidthFlushInterval(time.Hour). // disable timed flush; Shutdown forces final flush
		WithLogger(mocklogger.NewNoOpLogger()).
		Build(bb)

	bb.SetRebalanceCallback(broker.MakeAssignAllPartitionsCallback(t, consumer, 1))

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// simulate adapter pumping a bandwidth packet; aggregator stamps Service
	bb.cb(nexus.BandwidthMetrics{TopicName: "test-topic"})

	// Shutdown drains the aggregator's final flush
	if err := consumer.Shutdown(); err != nil {
		t.Logf("shutdown: %v", err)
	}

	select {
	case p := <-captured:
		assertSvc(t, p.Service)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for bandwidth sink callback")
	}
}

func assertSvc(t *testing.T, s *nexus.Service) {
	t.Helper()
	if s == nil {
		t.Fatal("Service is nil")
	}
	if s.Name != svcTest.Name {
		t.Errorf("Service.Name = %q, want %q", s.Name, svcTest.Name)
	}
	if s.Team != svcTest.Team {
		t.Errorf("Service.Team = %q, want %q", s.Team, svcTest.Team)
	}
}
