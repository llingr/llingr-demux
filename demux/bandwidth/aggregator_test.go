// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package bandwidth

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// testLogger is a no-op logger for tests.
type testLogger struct{}

func (testLogger) Error(_ context.Context, _ string, _ ...any) {}
func (testLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (testLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (testLogger) Debug(_ context.Context, _ string, _ ...any) {}

func newTestAggregator(sink nexus.BandwidthMetricsSink) *Aggregator {
	a := NewAggregator(context.Background(), sink, "test-topic", testLogger{})
	a.flushInterval = 50 * time.Millisecond // fast for tests
	return a
}

func TestAggregator_FlushOnTimer(t *testing.T) {
	var received []nexus.BandwidthMetrics
	var mu sync.Mutex

	sink := nexus.BandwidthMetricsSink(func(_ string, m nexus.BandwidthMetrics) error {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
		return nil
	})

	a := newTestAggregator(sink)
	a.Start()

	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "timer-1"})
	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "timer-2"})

	// wait for flush timer to fire
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 2 {
		t.Errorf("expected 2 flushed packets, got %d", count)
	}

	a.Stop()

	stats := a.Stats()
	if stats.Flushed != 2 {
		t.Errorf("expected flushed count 2, got %d", stats.Flushed)
	}
}

func TestAggregator_FlushOnThreshold(t *testing.T) {
	var count atomic.Int64

	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		count.Add(1)
		return nil
	})

	a := newTestAggregator(sink)
	a.flushInterval = 10 * time.Minute // long timer - should not fire during test
	a.maxBuffer = 5
	a.Start()

	// send exactly the threshold
	for i := 0; i < 5; i++ {
		a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: fmt.Sprintf("thresh-%d", i)})
	}

	// threshold flush is synchronous in Receive, give a moment for delivery
	time.Sleep(20 * time.Millisecond)

	if got := count.Load(); got != 5 {
		t.Errorf("expected 5 flushed packets on threshold, got %d", got)
	}

	a.Stop()
}

func TestAggregator_FlushOnStop(t *testing.T) {
	var received []nexus.BandwidthMetrics
	var mu sync.Mutex

	sink := nexus.BandwidthMetricsSink(func(_ string, m nexus.BandwidthMetrics) error {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
		return nil
	})

	a := newTestAggregator(sink)
	a.flushInterval = 10 * time.Minute // long timer
	a.Start()

	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "stop-1"})
	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "stop-2"})
	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "stop-3"})

	a.Stop() // should perform final flush

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 3 {
		t.Errorf("expected 3 flushed packets on Stop, got %d", count)
	}
}

func TestAggregator_DropCountOnSinkError(t *testing.T) {
	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		return errors.New("sink unavailable")
	})

	a := newTestAggregator(sink)
	a.flushInterval = 10 * time.Minute
	a.Start()

	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "err-1"})
	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "err-2"})

	a.Stop()

	stats := a.Stats()
	if stats.Dropped != 2 {
		t.Errorf("expected dropped count 2, got %d", stats.Dropped)
	}
	if stats.Flushed != 0 {
		t.Errorf("expected flushed count 0, got %d", stats.Flushed)
	}
}

func TestAggregator_ConcurrentSafety(t *testing.T) {
	var count atomic.Int64

	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		count.Add(1)
		return nil
	})

	a := newTestAggregator(sink)
	a.maxBuffer = 10
	a.Start()

	// simulate concurrent adapter callbacks
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: fmt.Sprintf("concurrent-%d", id)})
		}(i)
	}
	wg.Wait()

	a.Stop()

	if got := count.Load(); got != 100 {
		t.Errorf("expected 100 flushed packets, got %d", got)
	}
}

func TestAggregator_EmptyFlush(t *testing.T) {
	callCount := 0
	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		callCount++
		return nil
	})

	a := newTestAggregator(sink)
	a.Start()

	// wait for timer to fire with empty buffer
	time.Sleep(150 * time.Millisecond)
	a.Stop()

	if callCount != 0 {
		t.Errorf("sink should not be called when buffer is empty, got %d calls", callCount)
	}
}

func TestAggregator_StatsAccuracy(t *testing.T) {
	callNum := 0
	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		callNum++
		if callNum%3 == 0 {
			return errors.New("every third fails")
		}
		return nil
	})

	a := newTestAggregator(sink)
	a.flushInterval = 10 * time.Minute
	a.Start()

	for i := 0; i < 9; i++ {
		a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: fmt.Sprintf("stats-%d", i)})
	}

	a.Stop()

	stats := a.Stats()
	if stats.Flushed != 6 {
		t.Errorf("expected 6 flushed, got %d", stats.Flushed)
	}
	if stats.Dropped != 3 {
		t.Errorf("expected 3 dropped, got %d", stats.Dropped)
	}
}

func TestAggregator_TopicNamePassedToSink(t *testing.T) {
	var receivedTopic string
	sink := nexus.BandwidthMetricsSink(func(topicName string, _ nexus.BandwidthMetrics) error {
		receivedTopic = topicName
		return nil
	})

	a := NewAggregator(context.Background(), sink, "my-topic", testLogger{})
	a.flushInterval = 10 * time.Minute
	a.Start()

	a.Receive(nexus.BandwidthMetrics{})

	a.Stop()

	if receivedTopic != "my-topic" {
		t.Errorf("expected topic 'my-topic', got %q", receivedTopic)
	}
}

func TestAggregator_StopIsIdempotent(t *testing.T) {
	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		return nil
	})

	a := newTestAggregator(sink)
	a.Start()

	// multiple stops should not panic
	a.Stop()
	a.Stop()
	a.Stop()

	stats := a.Stats()
	if stats.Dropped != 0 {
		t.Errorf("expected 0 drops after idempotent stop, got %d", stats.Dropped)
	}
}

func TestAggregator_WithFlushInterval(t *testing.T) {
	var received []nexus.BandwidthMetrics
	var mu sync.Mutex

	sink := nexus.BandwidthMetricsSink(func(_ string, m nexus.BandwidthMetrics) error {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
		return nil
	})

	a := NewAggregator(context.Background(), sink, "test-topic", testLogger{},
		WithFlushInterval(50*time.Millisecond),
	)
	a.Start()

	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "opt-1"})
	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "opt-2"})

	// wait for the 50ms flush to fire
	time.Sleep(150 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count != 2 {
		t.Errorf("expected 2 flushed packets with custom flush interval, got %d", count)
	}

	a.Stop()
}

// TestDefaultFlushInterval_Value is a mutation testing anchor.
// ARITHMETIC_BASE on `60 * time.Second` swaps * to +/-/÷ - this catches it.
func TestDefaultFlushInterval_Value(t *testing.T) {
	if defaultFlushInterval != 60*time.Second {
		t.Errorf("defaultFlushInterval should be 60s, got %v", defaultFlushInterval)
	}
}

func TestDefaultMaxBuffer_Value(t *testing.T) {
	if defaultMaxBuffer != 50 {
		t.Errorf("defaultMaxBuffer should be 50, got %d", defaultMaxBuffer)
	}
}

// TestAggregator_DefaultsFromNewAggregator verifies NewAggregator wires
// the default flush interval and buffer size - not overridden by test helpers.
func TestAggregator_DefaultsFromNewAggregator(t *testing.T) {
	sink := nexus.BandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error {
		return nil
	})

	a := NewAggregator(context.Background(), sink, "test-topic", testLogger{})

	if a.flushInterval != 60*time.Second {
		t.Errorf("expected flushInterval 60s, got %v", a.flushInterval)
	}
	if a.maxBuffer != 50 {
		t.Errorf("expected maxBuffer 50, got %d", a.maxBuffer)
	}
}

// mockBandwidthAdapter simulates an adapter implementing BandwidthPort[T].
// It pumps BandwidthMetrics packets into the registered callback on a
// configurable interval, mimicking the adapter-as-pump pattern.
type mockBandwidthAdapter struct {
	callback nexus.BandwidthCallback
	interval time.Duration
	quit     chan struct{}
	wg       sync.WaitGroup
}

func newMockAdapter(interval time.Duration) *mockBandwidthAdapter {
	return &mockBandwidthAdapter{
		interval: interval,
		quit:     make(chan struct{}),
	}
}

func (m *mockBandwidthAdapter) SetBandwidthCallback(cb nexus.BandwidthCallback) {
	m.callback = cb
}

func (m *mockBandwidthAdapter) StatsInterval() time.Duration {
	return m.interval
}

func (m *mockBandwidthAdapter) startPumping(topicName string) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()
		seq := 0
		for {
			select {
			case <-ticker.C:
				if m.callback != nil {
					seq++
					m.callback(nexus.BandwidthMetrics{
						Ts:                    time.Now(),
						StatsIntervalDuration: m.interval,
						BandwidthMetricsID:    fmt.Sprintf("mock-%d", seq),
						TopicName:             topicName,
						ConsumerGroup:         "mock-group",
						Brokers: []nexus.BrokerInfo{
							{ID: "1", Host: "mock-broker", Port: "9092"},
						},
						Partitions: []nexus.PartitionBandwidth{
							{ID: 0, ReceivedBytes: int64(seq * 1024), ReceivedMessageCount: int64(seq * 10)},
						},
					})
				}
			case <-m.quit:
				return
			}
		}
	}()
}

func (m *mockBandwidthAdapter) stop() {
	close(m.quit)
	m.wg.Wait()
}

// TestAggregator_WithTeam verifies that the WithTeam option stamps team
// ownership on every packet flushed to the sink, and that the no-team
// warning is suppressed when a service is configured.
func TestAggregator_WithService(t *testing.T) {
	var received []nexus.BandwidthMetrics
	var mu sync.Mutex

	sink := nexus.BandwidthMetricsSink(func(_ string, m nexus.BandwidthMetrics) error {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
		return nil
	})

	service := &nexus.Service{Name: "orders-api", Team: "orders-team"}

	a := NewAggregator(context.Background(), sink, "test-topic", testLogger{},
		WithService(service),
	)
	a.flushInterval = 10 * time.Minute
	a.Start()

	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "svc-1"})
	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "svc-2"})

	a.Stop()

	mu.Lock()
	got := received
	mu.Unlock()

	if len(got) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(got))
	}
	for i, p := range got {
		if p.Service == nil {
			t.Errorf("packet %d: Service was nil, expected stamped service", i)
			continue
		}
		if p.Service.Name != "orders-api" {
			t.Errorf("packet %d: expected Service.Name 'orders-api', got %q", i, p.Service.Name)
		}
		if p.Service.Team != "orders-team" {
			t.Errorf("packet %d: expected Service.Team 'orders-team', got %q", i, p.Service.Team)
		}
	}
}

// TestAggregator_WithService_NilIsNoOp ensures WithService(nil) does not panic
// and behaves like the no-service default (Receive leaves packet.Service alone)
func TestAggregator_WithService_NilIsNoOp(t *testing.T) {
	var received []nexus.BandwidthMetrics
	var mu sync.Mutex

	sink := nexus.BandwidthMetricsSink(func(_ string, m nexus.BandwidthMetrics) error {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
		return nil
	})

	a := NewAggregator(context.Background(), sink, "test-topic", testLogger{},
		WithService(nil),
	)
	a.flushInterval = 10 * time.Minute
	a.Start()

	a.Receive(nexus.BandwidthMetrics{BandwidthMetricsID: "nil-svc-1"})
	a.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(received))
	}
	if received[0].Service != nil {
		t.Errorf("expected Service to remain nil, got %+v", received[0].Service)
	}
}

// TestAggregator_AdapterPumpIntegration wires a mock adapter to an aggregator
// end-to-end: adapter pumps packets → aggregator buffers → sink receives.
// Uses short intervals to keep the test fast.
func TestAggregator_AdapterPumpIntegration(t *testing.T) {
	var mu sync.Mutex
	var received []nexus.BandwidthMetrics

	sink := nexus.BandwidthMetricsSink(func(topicName string, m nexus.BandwidthMetrics) error {
		mu.Lock()
		received = append(received, m)
		mu.Unlock()
		return nil
	})

	// create aggregator with short flush interval
	a := NewAggregator(context.Background(), sink, "orders", testLogger{})
	a.flushInterval = 80 * time.Millisecond

	// simulate builder wiring: register callback on adapter
	adapter := newMockAdapter(20 * time.Millisecond)
	adapter.SetBandwidthCallback(a.Receive)

	// start both - order mirrors production: aggregator first, then adapter
	a.Start()
	adapter.startPumping("orders")

	// let adapter pump ~10 packets and aggregator flush at least once
	time.Sleep(250 * time.Millisecond)

	// shutdown: adapter first (may emit final packet), then aggregator drains
	adapter.stop()
	a.Stop()

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count == 0 {
		t.Fatal("expected sink to receive packets from adapter pump")
	}

	// verify packet content came through intact
	mu.Lock()
	first := received[0]
	mu.Unlock()

	if first.TopicName != "orders" {
		t.Errorf("expected TopicName 'orders', got %q", first.TopicName)
	}
	if first.ConsumerGroup != "mock-group" {
		t.Errorf("expected ConsumerGroup 'mock-group', got %q", first.ConsumerGroup)
	}
	if len(first.Brokers) != 1 {
		t.Errorf("expected 1 broker, got %d", len(first.Brokers))
	}
	if len(first.Partitions) != 1 {
		t.Errorf("expected 1 partition, got %d", len(first.Partitions))
	}
	if first.Partitions[0].ReceivedBytes == 0 {
		t.Error("expected non-zero ReceivedBytes")
	}

	stats := a.Stats()
	if stats.Flushed != int64(count) {
		t.Errorf("flushed count %d should match received count %d", stats.Flushed, count)
	}
}
