// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package demux

import (
	"context"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/demux/pipeline/throttle"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

// builderTestBroker implements nexus.BrokerPort[string] for builder tests.
type builderTestBroker struct {
	extractEnvelopeCalled bool
}

func (m *builderTestBroker) Subscribe() error                           { return nil }
func (m *builderTestBroker) Unsubscribe() error                         { return nil }
func (m *builderTestBroker) Poll(_ time.Duration) (string, bool, error) { return "", false, nil }
func (m *builderTestBroker) CommitOffsets(msgs []*nexus.Message[string]) ([]*nexus.Message[string], error) {
	return msgs, nil
}
func (m *builderTestBroker) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}
func (m *builderTestBroker) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}
func (m *builderTestBroker) ExtractEnvelope(_ string) nexus.Envelope {
	m.extractEnvelopeCalled = true
	return nexus.Envelope{
		Partition: 0,
		Offset:    1,
		Key:       "test-key",
		Ctx:       context.Background(),
	}
}
func (m *builderTestBroker) ConsumerGroup() string { return "test-group" }

func TestNewBuilder(t *testing.T) {
	processCalled := false
	deadLetterCalled := false

	process := func(_ context.Context, _ *nexus.Message[string]) error {
		processCalled = true
		return nil
	}
	deadLetter := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		deadLetterCalled = true
		return nil
	}

	builder := NewBuilder("test-topic", process, deadLetter)

	if builder == nil {
		t.Fatal("NewBuilder returned nil")
	}
	if builder.topicName != "test-topic" {
		t.Error("topicName not set")
	}
	if builder.processMessage == nil {
		t.Error("processMessage not set")
	}
	if builder.writeDeadLetter == nil {
		t.Error("writeDeadLetter not set")
	}

	// verify functions are the ones we passed
	_ = builder.processMessage(context.Background(), nil)
	if !processCalled {
		t.Error("processMessage is not the function we passed")
	}
	_ = builder.writeDeadLetter(context.Background(), nil, nil)
	if !deadLetterCalled {
		t.Error("writeDeadLetter is not the function we passed")
	}
}

func TestNewBuilder_PanicsOnEmptyTopicName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty topicName")
		}
	}()
	NewBuilder[string]("", noopProcess, noopDeadLetter)
}

func TestNewBuilder_PanicsOnWhitespaceOnlyTopicName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for whitespace-only topicName")
		}
	}()
	NewBuilder[string]("   ", noopProcess, noopDeadLetter)
}

func TestNewBuilder_PanicsOnNilProcessMessage(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil processMessage")
		}
	}()
	NewBuilder[string]("test-topic", nil, noopDeadLetter)
}

func TestNewBuilder_PanicsOnNilDeadLetter(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil deadLetter")
		}
	}()
	NewBuilder[string]("test-topic", noopProcess, nil)
}

func TestWithMetricsSink(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	sinkCalled := false

	sink := func(_ nexus.SinkContext, _ nexus.Metrics) error {
		sinkCalled = true
		return nil
	}

	result := builder.WithMetricsSink(sink)

	if result != builder {
		t.Error("WithMetricsSink should return same builder for chaining")
	}
	if builder.metricsSink == nil {
		t.Error("metricsSink not set")
	}
	_ = builder.metricsSink(nexus.SinkContext{TopicName: "test"}, nexus.Metrics{})
	if !sinkCalled {
		t.Error("metricsSink is not the function we passed")
	}
}

func TestWithBandwidthMetricsSink(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	sinkCalled := false

	sink := func(_ string, _ nexus.BandwidthMetrics) error {
		sinkCalled = true
		return nil
	}

	result := builder.WithBandwidthMetricsSink(sink)

	if result != builder {
		t.Error("WithBandwidthMetricsSink should return same builder for chaining")
	}
	if builder.bandwidthMetricsSink == nil {
		t.Error("bandwidthMetricsSink not set")
	}
	_ = builder.bandwidthMetricsSink("test-topic", nexus.BandwidthMetrics{})
	if !sinkCalled {
		t.Error("bandwidthMetricsSink is not the function we passed")
	}
}

func TestWithBandwidthFlushInterval(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)

	result := builder.WithBandwidthFlushInterval(30 * time.Second)

	if result != builder {
		t.Error("WithBandwidthFlushInterval should return same builder for chaining")
	}
	if builder.bandwidthFlushInterval != 30*time.Second {
		t.Errorf("bandwidthFlushInterval = %v, want 30s", builder.bandwidthFlushInterval)
	}
}

func TestWithService(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)

	service := nexus.Service{Name: "payments-api", Team: "payments-team"}
	result := builder.WithService(service)

	if result != builder {
		t.Error("WithService should return same builder for chaining")
	}
	if builder.service == nil {
		t.Fatal("service should be set")
	}
	if builder.service.Name != "payments-api" {
		t.Errorf("service.Name = %q, want %q", builder.service.Name, "payments-api")
	}
	if builder.service.Team != "payments-team" {
		t.Errorf("service.Team = %q, want %q", builder.service.Team, "payments-team")
	}
}

func TestWithLogger(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	logger := mocklogger.NewNoOpLogger()

	result := builder.WithLogger(logger)

	if result != builder {
		t.Error("WithLogger should return same builder for chaining")
	}
	if builder.logger != logger {
		t.Error("logger not set correctly")
	}
}

func TestWithDemuxConfig(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	cfg := config.DemuxConfig{
		ConcurrentKeys: 42,
	}

	result := builder.WithDemuxConfig(cfg)

	if result != builder {
		t.Error("WithDemuxConfig should return same builder for chaining")
	}
	if builder.demuxConfig == nil {
		t.Error("demuxConfig not set")
	}
	if builder.demuxConfig.ConcurrentKeys != 42 {
		t.Errorf("demuxConfig.ConcurrentKeys = %d, want 42", builder.demuxConfig.ConcurrentKeys)
	}
}

func TestWithContext(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	ctx := context.WithValue(context.Background(), testKey{}, "test-value")

	result := builder.WithContext(ctx)

	if result != builder {
		t.Error("WithContext should return same builder for chaining")
	}
	if builder.ctx != ctx {
		t.Error("ctx not set correctly")
	}
}

type testKey struct{}

func TestWithOverflowGuard(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	guard := make(chan struct{}, 10)

	result := builder.WithOverflowGuard(guard)

	if result != builder {
		t.Error("WithOverflowGuard should return same builder for chaining")
	}
	if builder.overflowGuard != guard {
		t.Error("overflowGuard not set correctly")
	}
}

func TestWithExtractEnvelope(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	extractCalled := false

	extract := func(_ string) nexus.Envelope {
		extractCalled = true
		return nexus.Envelope{Key: "custom-key"}
	}

	result := builder.WithExtractEnvelope(extract)

	if result != builder {
		t.Error("WithExtractEnvelope should return same builder for chaining")
	}
	if builder.extractEnvelope == nil {
		t.Error("extractEnvelope not set")
	}
	env := builder.extractEnvelope("test")
	if !extractCalled {
		t.Error("extractEnvelope is not the function we passed")
	}
	if env.Key != "custom-key" {
		t.Errorf("extractEnvelope returned wrong key: %s", env.Key)
	}
}

func TestWithEnrichContext(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	enrichCalled := false

	enrich := func(ctx context.Context, _ string) context.Context {
		enrichCalled = true
		return context.WithValue(ctx, testKey{}, "enriched")
	}

	result := builder.WithEnrichContext(enrich)

	if result != builder {
		t.Error("WithEnrichContext should return same builder for chaining")
	}
	if builder.enrichContext == nil {
		t.Error("enrichContext not set")
	}
	ctx := builder.enrichContext(context.Background(), "test")
	if !enrichCalled {
		t.Error("enrichContext is not the function we passed")
	}
	if ctx.Value(testKey{}) != "enriched" {
		t.Error("enrichContext didn't enrich the context")
	}
}

func TestWithShutdownCallback(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	callbackCalled := false

	callback := func(_ context.Context, _ error) {
		callbackCalled = true
	}

	result := builder.WithShutdownCallback(callback)

	if result != builder {
		t.Error("WithShutdownCallback should return same builder for chaining")
	}
	if builder.shutdownCallback == nil {
		t.Error("shutdownCallback not set")
	}
	builder.shutdownCallback(context.Background(), nil)
	if !callbackCalled {
		t.Error("shutdownCallback is not the function we passed")
	}
}

func TestBuildWithDefaults(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	broker := &builderTestBroker{}

	consumer := builder.Build(broker)

	if consumer == nil {
		t.Fatal("Build returned nil consumer")
	}

	// verify it's the right type
	c, ok := consumer.(*Consumer[string])
	if !ok {
		t.Fatal("Build didn't return *Consumer[string]")
	}

	// verify defaults were applied
	if consumer.Context() == nil {
		t.Error("ctx should default to non-nil")
	}
	if c.demuxConfig == nil {
		t.Error("demuxConfig should be set")
	}
	if consumer.Logger() == nil {
		t.Error("logger should default to non-nil")
	}
}

func TestBuildWithAllOptions(t *testing.T) {
	ctx := context.WithValue(context.Background(), testKey{}, "test")
	logger := mocklogger.NewNoOpLogger()
	cfg := config.DemuxConfig{ConcurrentKeys: 100}
	guard := make(chan struct{}, 5)
	callbackCalled := false

	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithContext(ctx).
		WithLogger(logger).
		WithDemuxConfig(cfg).
		WithOverflowGuard(guard).
		WithMetricsSink(func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }).
		WithShutdownCallback(func(_ context.Context, _ error) {
			callbackCalled = true
		}).
		WithExtractEnvelope(func(_ string) nexus.Envelope {
			return nexus.Envelope{Key: "custom"}
		})

	broker := &builderTestBroker{}
	consumer := builder.Build(broker)

	if consumer == nil {
		t.Fatal("Build returned nil consumer")
	}

	c := consumer.(*Consumer[string]) //nolint:forcetypeassert // test: known type from builder

	// verify context was used
	if consumer.Context().Value(testKey{}) != "test" {
		t.Error("custom context not used")
	}

	// verify config was used (ConcurrentKeys should be 100)
	if c.demuxConfig.ConcurrentKeys != 100 {
		t.Errorf("ConcurrentKeys = %d, want 100", c.demuxConfig.ConcurrentKeys)
	}

	// verify shutdown callback was stored
	cb := c.shutdownCallback.Load()
	if cb == nil {
		t.Error("shutdownCallback not stored")
	} else {
		(*cb)(context.Background(), nil)
		if !callbackCalled {
			t.Error("stored callback is not the one we passed")
		}
	}

	// broker's ExtractEnvelope should NOT have been called (we provided custom)
	if broker.extractEnvelopeCalled {
		t.Error("broker's ExtractEnvelope should not be called when custom is provided")
	}
}

// builderBandwidthBroker implements both BrokerPort[string] and BandwidthPort[string]
// for testing the bandwidth-aggregator wiring path in Build
type builderBandwidthBroker struct {
	builderTestBroker
	callbackSet bool
}

func (m *builderBandwidthBroker) SetBandwidthCallback(_ nexus.BandwidthCallback) {
	m.callbackSet = true
}
func (m *builderBandwidthBroker) StatsInterval() time.Duration { return 5 * time.Second }

func TestBuild_BandwidthSink_AdapterImplementsBandwidthPort(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()

	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithLogger(logger).
		WithBandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error { return nil }).
		WithBandwidthFlushInterval(2 * time.Second).
		WithService(nexus.Service{Name: "test-service", Team: "test-team"})

	broker := &builderBandwidthBroker{}
	consumer := builder.Build(broker)
	c := consumer.(*Consumer[string]) //nolint:forcetypeassert // test: known type from builder

	if c.bandwidthAggregator == nil {
		t.Error("expected bandwidth aggregator to be wired when broker implements BandwidthPort")
	}
	if !broker.callbackSet {
		t.Error("expected SetBandwidthCallback to be called on the BandwidthPort adapter")
	}
	if logger.ContainsWarning("does not implement BandwidthPort") {
		t.Error("warn log should not fire when broker implements BandwidthPort")
	}
}

func TestBuild_BandwidthSink_AdapterDoesNotImplementBandwidthPort(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()

	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithLogger(logger).
		WithBandwidthMetricsSink(func(_ string, _ nexus.BandwidthMetrics) error { return nil })

	// builderTestBroker does NOT implement BandwidthPort
	broker := &builderTestBroker{}
	consumer := builder.Build(broker)
	c := consumer.(*Consumer[string]) //nolint:forcetypeassert // test: known type from builder

	if c.bandwidthAggregator != nil {
		t.Error("expected nil aggregator when adapter does not implement BandwidthPort")
	}
	if !logger.ContainsWarning("does not implement BandwidthPort") {
		t.Error("expected warn log when adapter does not implement BandwidthPort")
	}
}

func TestBuild_TakeSnapshot_ConcurrencyClosure(t *testing.T) {
	tests := []struct {
		name             string
		overflowCap      int
		wantOverflowCap  int
	}{
		{name: "with overflow guard", overflowCap: 7, wantOverflowCap: 7},
		{name: "without overflow guard", overflowCap: 0, wantOverflowCap: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
				WithLogger(mocklogger.NewNoOpLogger()).
				WithDemuxConfig(config.DemuxConfig{ConcurrentKeys: 13})
			if tc.overflowCap > 0 {
				builder = builder.WithOverflowGuard(make(chan struct{}, tc.overflowCap))
			}

			consumer := builder.Build(&builderTestBroker{})
			snap := consumer.(*Consumer[string]).TakeSnapshot() //nolint:forcetypeassert // test: known type from builder

			if snap.Concurrency.GuardCapacity != 13 {
				t.Errorf("GuardCapacity = %d, want 13", snap.Concurrency.GuardCapacity)
			}
			if snap.Concurrency.OverflowCapacity != tc.wantOverflowCap {
				t.Errorf("OverflowCapacity = %d, want %d",
					snap.Concurrency.OverflowCapacity, tc.wantOverflowCap)
			}
			if snap.Concurrency.CommitIngestCap == 0 {
				t.Error("CommitIngestCap should be non-zero (committer wires real channel)")
			}
		})
	}
}

func TestBuildWithEnrichContextWrapsExtractor(t *testing.T) {
	baseExtractCalled := false
	enrichCalled := false

	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithExtractEnvelope(func(payload string) nexus.Envelope {
			baseExtractCalled = true
			return nexus.Envelope{
				Key: payload,
				Ctx: context.Background(),
			}
		}).
		WithEnrichContext(func(ctx context.Context, payload string) context.Context {
			enrichCalled = true
			return context.WithValue(ctx, testKey{}, "enriched-"+payload)
		})

	broker := &builderTestBroker{}
	_ = builder.Build(broker)

	// After Build, extractEnvelope should be wrapped
	// Call the wrapped extractor
	env := builder.extractEnvelope("test-payload")

	if !baseExtractCalled {
		t.Error("base extractor should have been called")
	}
	if !enrichCalled {
		t.Error("enrichContext should have been called")
	}
	if env.Key != "test-payload" {
		t.Errorf("key = %s, want test-payload", env.Key)
	}
	if env.Ctx.Value(testKey{}) != "enriched-test-payload" {
		t.Error("context was not enriched correctly")
	}
}

func TestBuildWithEnrichContextUsesDefaultExtractor(t *testing.T) {
	enrichCalled := false

	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithEnrichContext(func(ctx context.Context, _ string) context.Context {
			enrichCalled = true
			return context.WithValue(ctx, testKey{}, "enriched")
		})

	broker := &builderTestBroker{}
	_ = builder.Build(broker)

	// Call the wrapped extractor (should use broker's default + enrichment)
	env := builder.extractEnvelope("test")

	if !broker.extractEnvelopeCalled {
		t.Error("broker's ExtractEnvelope should be used as base")
	}
	if !enrichCalled {
		t.Error("enrichContext should have been called")
	}
	if env.Ctx.Value(testKey{}) != "enriched" {
		t.Error("context was not enriched")
	}
}

func TestBuilderChaining(t *testing.T) {
	// Verify all methods can be chained in one expression
	consumer := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithContext(context.Background()).
		WithLogger(mocklogger.NewNoOpLogger()).
		WithDemuxConfig(config.DemuxConfig{}).
		WithOverflowGuard(make(chan struct{}, 1)).
		WithMetricsSink(func(_ nexus.SinkContext, _ nexus.Metrics) error { return nil }).
		WithExtractEnvelope(func(_ string) nexus.Envelope { return nexus.Envelope{} }).
		WithEnrichContext(func(ctx context.Context, _ string) context.Context { return ctx }).
		WithShutdownCallback(func(_ context.Context, _ error) {}).
		Build(&builderTestBroker{})

	if consumer == nil {
		t.Error("chained Build returned nil consumer")
	}
}

func TestWithRateLimiter(t *testing.T) {
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter)
	limiter := &throttle.NoOpRateLimiter[string]{}

	result := builder.WithRateLimiter(limiter)

	if result != builder {
		t.Error("WithRateLimiter should return same builder for chaining")
	}
	if builder.rateLimiter != limiter {
		t.Error("rateLimiter not set correctly")
	}
}

func TestNoOpMetricsSink(t *testing.T) {
	err := noOpMetricsSink(nexus.SinkContext{TopicName: "test-topic"}, nexus.Metrics{})
	if err != nil {
		t.Errorf("noOpMetricsSink returned error: %v", err)
	}
}

// noopProcess is a no-op process function for tests.
func noopProcess(_ context.Context, _ *nexus.Message[string]) error {
	return nil
}

// noopDeadLetter is a no-op dead letter function for tests.
func noopDeadLetter(_ context.Context, _ *nexus.Message[string], _ error) error {
	return nil
}
