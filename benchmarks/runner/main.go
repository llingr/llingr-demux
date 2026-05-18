// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

// Benchmark runner for llingr-demux performance testing.
// Runs the e2e pipeline with configurable parameters and appends results to a CSV file.
//
// Usage:
//
//	# Run all configs from file (default):
//	go run ./benchmarks/runner
//
//	# Run single benchmark:
//	go run ./benchmarks/runner -config="" -messages=50000 -keys=500 -latency=100ms
//
//nolint:tagliatelle,mnd // benchmark config uses snake_case for readability; magic numbers are CLI defaults
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/llingr/llingr-demux/demux"
	"github.com/llingr/llingr-demux/demux/config"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-demux/tests/testkit/broker"
	"github.com/llingr/llingr-demux/tests/testkit/hostapp"
	"github.com/llingr/llingr-demux/tests/testkit/scenario"
	"github.com/llingr/llingr-nexus/nexus"
)

func main() {
	// Parse command line flags
	configFile := flag.String("config", "benchmarks/runner/configs/benchmark_configs.json",
		"JSON config file with benchmark runs (use empty string for single run mode)")
	startAt := flag.Int("start", 1, "Start at this run number (1-indexed, for resuming)")
	messageCount := flag.Int("messages", 50_000, "Number of messages to process")
	concurrentKeys := flag.Int("keys", 500, "Number of concurrent keys")
	latencyMs := flag.Int("latency", 100, "Processor latency in milliseconds")
	jitter := flag.Float64("jitter", 0.0, "Jitter as fraction of latency (0.0 to 1.0)")
	numPartitions := flag.Int("partitions", 10, "Number of partitions")
	outputFile := flag.String("output", "benchmarks/benchmark_results.csv",
		"Output CSV file path")
	flag.Parse()

	if *configFile != "" {
		// Run from config file
		if err := runFromConfig(*configFile, *outputFile, *startAt); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Run single benchmark
		processorLatency := time.Duration(*latencyMs) * time.Millisecond
		result, err := runSingleBenchmark(
			*messageCount, *concurrentKeys, processorLatency, *jitter, *numPartitions,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Benchmark failed: %v\n", err)
			os.Exit(1)
		}
		printResult(result)
		if err := appendToCSV(*outputFile, result); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write CSV: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Results appended to %s\n", *outputFile)
	}
}

// ConfigFile represents the JSON config structure.
type ConfigFile struct {
	Defaults RunConfig   `json:"defaults"`
	Runs     []RunConfig `json:"runs"`
}

// RunConfig holds parameters for a single benchmark run.
type RunConfig struct {
	MessageCount       *int     `json:"message_count,omitempty"`
	ConcurrentKeys     *int     `json:"concurrent_keys,omitempty"`
	ProcessorLatencyMs *int     `json:"processor_latency_ms,omitempty"`
	Jitter             *float64 `json:"jitter,omitempty"`
	NumPartitions      *int     `json:"num_partitions,omitempty"`
}

func runFromConfig(configPath, outputPath string, startAt int) error {
	data, err := os.ReadFile(configPath) //nolint:gosec // G304: CLI tool reads user-specified config
	if err != nil {
		return fmt.Errorf("failed to read config: %w", err)
	}

	var cfg ConfigFile
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}

	totalRuns := len(cfg.Runs)
	fmt.Printf("Loaded %d benchmark configurations\n", totalRuns)
	if startAt > 1 {
		fmt.Printf("Starting at run #%d (skipping %d runs)\n", startAt, startAt-1)
	}
	fmt.Printf("Output: %s\n\n", outputPath)

	var totalElapsed time.Duration
	completed := 0

	for i, run := range cfg.Runs {
		runNum := i + 1 // 1-indexed run number

		// Skip runs before startAt
		if runNum < startAt {
			continue
		}
		// Merge with defaults
		merged := mergeConfig(cfg.Defaults, run)

		messageCount := valueOr(merged.MessageCount, 50_000)
		concurrentKeys := valueOr(merged.ConcurrentKeys, 500)
		latencyMs := valueOr(merged.ProcessorLatencyMs, 100)
		jitter := valueOrFloat(merged.Jitter, 0.0)
		partitions := valueOr(merged.NumPartitions, 10)

		processorLatency := time.Duration(latencyMs) * time.Millisecond

		// Progress and ETA
		var etaStr string
		remaining := totalRuns - runNum + 1
		if completed > 0 {
			avgDuration := totalElapsed / time.Duration(completed)
			eta := avgDuration * time.Duration(remaining)
			etaStr = fmt.Sprintf(" (ETA: %v)", eta.Round(time.Second))
		}

		fmt.Printf("[#%d %d/%d]%s keys=%d, latency=%dms, jitter=%.1f, msgs=%d\n",
			runNum, runNum-startAt+1, remaining+completed, etaStr,
			concurrentKeys, latencyMs, jitter, messageCount)

		runStart := time.Now()
		result, err := runSingleBenchmark(
			messageCount, concurrentKeys, processorLatency, jitter, partitions,
		)
		runDuration := time.Since(runStart)
		totalElapsed += runDuration

		if err != nil {
			fmt.Printf("        FAILED: %v (took %v)\n\n", err, runDuration.Round(time.Second))
			completed++
			continue
		}

		fmt.Printf("        TPS: %.1f (%.2f%% eff) took %v\n\n",
			result.ActualTPS, result.Efficiency, runDuration.Round(time.Second))

		if err := appendToCSV(outputPath, result); err != nil {
			return fmt.Errorf("failed to write CSV: %w", err)
		}
		completed++
	}

	fmt.Printf("Completed %d runs in %v (runs #%d - #%d)\n",
		completed, totalElapsed.Round(time.Second), startAt, totalRuns)
	fmt.Printf("Results written to %s\n", outputPath)
	return nil
}

func mergeConfig(defaults, override RunConfig) RunConfig {
	result := defaults
	if override.MessageCount != nil {
		result.MessageCount = override.MessageCount
	}
	if override.ConcurrentKeys != nil {
		result.ConcurrentKeys = override.ConcurrentKeys
	}
	if override.ProcessorLatencyMs != nil {
		result.ProcessorLatencyMs = override.ProcessorLatencyMs
	}
	if override.Jitter != nil {
		result.Jitter = override.Jitter
	}
	if override.NumPartitions != nil {
		result.NumPartitions = override.NumPartitions
	}
	return result
}

func valueOr(ptr *int, def int) int {
	if ptr != nil {
		return *ptr
	}
	return def
}

func valueOrFloat(ptr *float64, def float64) float64 {
	if ptr != nil {
		return *ptr
	}
	return def
}

// BenchmarkResult holds the outcome of a single benchmark run.
type BenchmarkResult struct {
	Timestamp        time.Time
	MessageCount     int
	ConcurrentKeys   int
	ProcessorLatency time.Duration
	Jitter           float64
	NumPartitions    int
	Duration         time.Duration
	StartupDuration  time.Duration
	RunDuration      time.Duration
	ShutdownDuration time.Duration
	ActualTPS        float64
	TheoreticalTPS   float64
	Efficiency       float64
}

func printResult(r *BenchmarkResult) {
	fmt.Printf("Results:\n")
	fmt.Printf("  Messages:     %d\n", r.MessageCount)
	fmt.Printf("  Concurrent:   %d keys\n", r.ConcurrentKeys)
	fmt.Printf("  Latency:      %v\n", r.ProcessorLatency)
	fmt.Printf("  Duration:     %v (startup: %v, run: %v, shutdown: %v)\n",
		r.Duration, r.StartupDuration.Round(time.Millisecond),
		r.RunDuration.Round(time.Millisecond), r.ShutdownDuration.Round(time.Millisecond))
	fmt.Printf("  Actual TPS:   %.1f\n", r.ActualTPS)
	fmt.Printf("  Theo TPS:     %.1f\n", r.TheoreticalTPS)
	fmt.Printf("  Efficiency:   %.2f%%\n", r.Efficiency)
	fmt.Println()
}

func runSingleBenchmark(messageCount, concurrentKeys int, processorLatency time.Duration,
	jitter float64, numPartitions int) (*BenchmarkResult, error) {

	// Skip config validation for benchmark flexibility
	if err := os.Setenv(config.SkipValidationEnvVar, "true"); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages := scenario.GenerateMessages(messageCount, int32(numPartitions)) //nolint:gosec // G115: numPartitions bounded by CLI

	broker := broker.NewMockBroker(messages, func() {})

	hostApp := hostapp.NewHostApp(processorLatency, jitter)

	logger := mocklogger.NewRecordingLogger()

	cfg := config.DemuxConfig{
		ConcurrentKeys:     concurrentKeys,
		AutoCommitInterval: 250 * time.Millisecond, // fast commits for benchmark accuracy (production default: 5s)
	}

	// Build consumer using the builder pattern
	builder := demux.NewBuilder("benchmark-topic", hostApp.ProcessMessage, hostApp.WriteDeadLetter).
		WithContext(ctx).
		WithDemuxConfig(cfg).
		WithMetricsSink(hostApp.MetricsSink).
		WithExtractEnvelope(hostapp.SimpleEnvelopeExtractor).
		WithLogger(logger)

	consumer := builder.Build(broker)

	// Set up rebalance callback
	rebalanceDone := make(chan struct{}, 1)
	broker.SetRebalanceCallback(func() {
		defer func() {
			rebalanceDone <- struct{}{}
		}()
		time.Sleep(10 * time.Millisecond)
		rebalanceInfo := make([]nexus.RebalanceInfo, numPartitions)
		for i := int32(0); i < int32(numPartitions); i++ { //nolint:gosec // G115: numPartitions bounded by CLI
			rebalanceInfo[i] = nexus.RebalanceInfo{
				Partition: i,
			}
		}
		_ = consumer.TriggerRebalance(nexus.Assign, rebalanceInfo)
	})

	// Subscribe and start
	startTime := time.Now()

	if err := consumer.Subscribe(); err != nil {
		return nil, fmt.Errorf("subscribe failed: %w", err)
	}

	// Wait for rebalance
	select {
	case <-rebalanceDone:
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("timed out waiting for rebalance")
	}

	// Wait for all messages to be processed
	deadline := time.Now().Add(10 * time.Minute)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastCount := int64(0)
	stuckCount := 0

	for time.Now().Before(deadline) {
		count := hostApp.GetMetricsCount()
		if count >= int64(messageCount) {
			break
		}

		// Detect if stuck
		if count == lastCount {
			stuckCount++
			if stuckCount > 20 { // 10 seconds stuck
				return nil, fmt.Errorf("processing appears stuck at %d/%d messages",
					count, messageCount)
			}
		} else {
			stuckCount = 0
			lastCount = count
		}

		<-ticker.C
	}

	endTime := time.Now()

	// Verify completion
	metricsCount := hostApp.GetMetricsCount()
	if metricsCount != int64(messageCount) {
		return nil, fmt.Errorf("incomplete: got %d/%d messages", metricsCount, messageCount)
	}

	// Calculate efficiency
	actualTPS, theoreticalTPS, efficiency := hostApp.CalculateEfficiency(messageCount, concurrentKeys)

	// Calculate phase durations
	procStart, procFinish := hostApp.GetProcessingWindow()
	totalDuration := endTime.Sub(startTime)
	startupDuration := procStart.Sub(startTime)
	runDuration := procFinish.Sub(procStart)
	shutdownDuration := endTime.Sub(procFinish)

	return &BenchmarkResult{
		Timestamp:        time.Now(),
		MessageCount:     messageCount,
		ConcurrentKeys:   concurrentKeys,
		ProcessorLatency: processorLatency,
		Jitter:           jitter,
		NumPartitions:    numPartitions,
		Duration:         totalDuration,
		StartupDuration:  startupDuration,
		RunDuration:      runDuration,
		ShutdownDuration: shutdownDuration,
		ActualTPS:        actualTPS,
		TheoreticalTPS:   theoreticalTPS,
		Efficiency:       efficiency,
	}, nil
}

func appendToCSV(filename string, result *BenchmarkResult) error {
	// Check if file exists to determine if we need headers
	writeHeader := false
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		writeHeader = true
	}

	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) //nolint:gosec // G302: benchmark CSV output
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header if new file
	if writeHeader {
		header := []string{
			"timestamp",
			"message_count",
			"concurrent_keys",
			"processor_latency_ms",
			"jitter",
			"num_partitions",
			"duration_ms",
			"actual_tps",
			"theoretical_tps",
			"efficiency_pct",
		}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	// Write data row
	row := []string{
		result.Timestamp.Format(time.RFC3339),
		strconv.Itoa(result.MessageCount),
		strconv.Itoa(result.ConcurrentKeys),
		strconv.FormatInt(result.ProcessorLatency.Milliseconds(), 10),
		strconv.FormatFloat(result.Jitter, 'f', 2, 64),
		strconv.Itoa(result.NumPartitions),
		strconv.FormatInt(result.Duration.Milliseconds(), 10),
		strconv.FormatFloat(result.ActualTPS, 'f', 1, 64),
		strconv.FormatFloat(result.TheoreticalTPS, 'f', 1, 64),
		strconv.FormatFloat(result.Efficiency, 'f', 2, 64),
	}
	if err := writer.Write(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	return nil
}
