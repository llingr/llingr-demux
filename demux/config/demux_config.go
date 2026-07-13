// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package config for the [DemuxConfig] type which controls concurrency limits, buffer
// sizes, timeouts, and tuning parameters. Zero values trigger production-ready defaults.
// Explicit values are validated against safe bounds.
//
// Key settings:
//
//	ConcurrentKeys (default 250) bounds parallel workers
//	DrainTimeout (default 20s) caps rebalance wait.
package config

import (
	"cmp"
	"encoding/json"
	"fmt"
	"time"
)

// DemuxConfig - for tuning the Message processing pipeline
//
// Configuration can be loaded from JSON/YAML files using the supplied
// struct tags. Duration fields accept string values like "30s" or "5m"
//
// Start with zero values to get production-ready defaults, then tune
// specific fields based on your workloads.
//
// See SetDemuxConfigDefaults() for more detail of each setting, default
// values also shown on the right.
type DemuxConfig struct {
	ConcurrentKeys                     int           `json:"concurrentKeys"`                     // 250 (default)
	PerKeyBufferLen                    int           `json:"perKeyBufferLen"`                    // 16
	PollTimeout                        time.Duration `json:"pollTimeout"`                        // 100ms
	AutoCommitInterval                 time.Duration `json:"autoCommitInterval"`                 // 5s
	DrainTimeout                       time.Duration `json:"drainTimeout"`                       // 20s
	AwaitAssignmentsTimeout            time.Duration `json:"awaitAssignmentsTimeout"`            // 50s
	CommitIngestChannelLen             int           `json:"commitIngestChannelLen"`             // 25000 (calculated, see: SetDemuxConfigDefaults)
	CommitPartitionSliceLen            int           `json:"commitPartitionSliceLen"`            // 400
	QueryTimeout                       time.Duration `json:"queryTimeout"`                       // 5s
	AcquireWorkerTimeoutCircuitBreaker time.Duration `json:"acquireWorkerTimeoutCircuitBreaker"` // 1m
	WorkerShardsCount                  int           `json:"workerShardsCount"`                  // 16
	RebalancePausePollingTimeout       time.Duration `json:"rebalancePausePollingTimeout"`       // 45s
	AcquireCommitGuardTimeout          time.Duration `json:"acquireCommitGuardTimeout"`          // 10s
}

// UnmarshalJSON handles parsing time.Duration from strings.
// Also works with YAML libraries that support JSON struct tags.
//
//nolint:gocyclo,gocognit // json.Unmarshaler for duration fields - each field adds parse+error check but logic is linear
func (c *DemuxConfig) UnmarshalJSON(data []byte) error {
	type Alias DemuxConfig
	alias := &struct {
		PollTimeout                        string `json:"pollTimeout"`
		AutoCommitInterval                 string `json:"autoCommitInterval"`
		DrainTimeout                       string `json:"drainTimeout"`
		AwaitAssignmentsTimeout            string `json:"awaitAssignmentsTimeout"`
		QueryTimeout                       string `json:"queryTimeout"`
		AcquireWorkerTimeoutCircuitBreaker string `json:"acquireWorkerTimeoutCircuitBreaker"`
		RebalancePausePollingTimeout       string `json:"rebalancePausePollingTimeout"`
		AcquireCommitGuardTimeout          string `json:"acquireCommitGuardTimeout"`
		*Alias
	}{
		Alias: (*Alias)(c),
	}

	var err error
	if err = json.Unmarshal(data, &alias); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if alias.PollTimeout != "" {
		c.PollTimeout, err = time.ParseDuration(alias.PollTimeout)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal pollTimeout: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.PollTimeout, err)
		}
	}

	if alias.AutoCommitInterval != "" {
		c.AutoCommitInterval, err = time.ParseDuration(alias.AutoCommitInterval)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal autoCommitInterval: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.AutoCommitInterval, err)
		}
	}

	if alias.QueryTimeout != "" {
		c.QueryTimeout, err = time.ParseDuration(alias.QueryTimeout)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal queryTimeout: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.QueryTimeout, err)
		}
	}

	if alias.DrainTimeout != "" {
		c.DrainTimeout, err = time.ParseDuration(alias.DrainTimeout)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal drainTimeout: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.DrainTimeout, err)
		}
	}

	if alias.AwaitAssignmentsTimeout != "" {
		c.AwaitAssignmentsTimeout, err = time.ParseDuration(alias.AwaitAssignmentsTimeout)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal awaitAssignmentsTimeout: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.AwaitAssignmentsTimeout, err)
		}
	}

	if alias.AcquireWorkerTimeoutCircuitBreaker != "" {
		c.AcquireWorkerTimeoutCircuitBreaker, err = time.ParseDuration(alias.AcquireWorkerTimeoutCircuitBreaker)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal acquireWorkerTimeoutCircuitBreaker: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.AcquireWorkerTimeoutCircuitBreaker, err)
		}
	}

	if alias.RebalancePausePollingTimeout != "" {
		c.RebalancePausePollingTimeout, err = time.ParseDuration(alias.RebalancePausePollingTimeout)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal rebalancePausePollingTimeout: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.RebalancePausePollingTimeout, err)
		}
	}

	if alias.AcquireCommitGuardTimeout != "" {
		c.AcquireCommitGuardTimeout, err = time.ParseDuration(alias.AcquireCommitGuardTimeout)
		if err != nil {
			const unmarshalFailed = "failed to unmarshal acquireCommitGuardTimeout: %s - %w"
			return fmt.Errorf(unmarshalFailed, alias.AcquireCommitGuardTimeout, err)
		}
	}

	return nil
}

// CalcWorkerChannelsMapSize with additional capacity for lumpy
// partition key shard allocations (even chaos is clumpy), then
// rounding up to next power-of-2 to align with faster bitwise
// operations accessing hash items, plus per-bucket depth is
// reduced with larger maps.
//
// This trades a modest amount of memory for better performance
// and reduced allocation pressure on hot paths.
func (c *DemuxConfig) CalcWorkerChannelsMapSize() int {
	//nolint:mnd // 2x headroom for hash map sizing - reduces collision chains and rehashing
	count := 2 * (c.ConcurrentKeys / c.WorkerShardsCount)
	//nolint - power of 2 numbers
	switch {
	case count <= 64:
		return 128
	case count <= 128:
		return 256
	case count <= 256:
		return 512
	case count <= 512:
		return 1024
	case count <= 1024:
		return 2048
	default:
		return 4096
	}
}

// CalcMinimumIdleWorkers returns the minimum number of per-shard
// idle workers awaiting messages. This prioritises optimal
// process start latency for evenly distributed workloads.
//
// Bursts are not limited by this, but to control resources the
// workers will be gradually pruned back to this level afterward.
func (c *DemuxConfig) CalcMinimumIdleWorkers() int {
	shardsCount := cmp.Or(c.WorkerShardsCount, workerShardsCount)
	keys := cmp.Or(c.ConcurrentKeys, concurrentKeys)

	// ceiling division to ensure reasonable baseline capacity
	return (shardsCount + keys - 1) / shardsCount
}
