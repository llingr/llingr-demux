// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package config

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func Test_DemuxConfig_OverrideValues_JsonUnMarshal(t *testing.T) {
	// example values deviate from defaults, this test is for
	// unmarshal correctness, not the specific values themselves
	exampleJSON := `{
		"concurrentKeys": 300,
		"perKeyBufferLen": 12,
		"pollTimeout": "110ms",
		"autoCommitInterval": "6s",
		"drainTimeout": "16s",
		"awaitAssignmentsTimeout": "31s",
		"commitIngestChannelLen": 20001,
		"commitPartitionSliceLen": 700,
		"queryTimeout": "6s",
		"acquireWorkerTimeoutCircuitBreaker": "1m",
		"workerShardsCount": 64,
		"rebalancePausePollingTimeout": "46s",
		"acquireCommitGuardTimeout": "11s"
	}`

	// using hash to ensure test values are stable and not, for
	// example, inadvertently altered using an IDE find/replace
	h := sha256.New()
	h.Write([]byte(exampleJSON))
	expectedHash := "499cb7add497fbe9139d89b05c366e150aaac81100c0abc8b4ce77e3d80b4292"
	actualHash := fmt.Sprintf("%x", h.Sum(nil))
	if actualHash != expectedHash {
		t.Errorf("json tags inadvertently altered: expected: %s, got: %s", expectedHash, actualHash)
	}

	var demuxConfig DemuxConfig
	err := json.Unmarshal([]byte(exampleJSON), &demuxConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if demuxConfig.ConcurrentKeys != 300 {
		t.Errorf("expected ConcurrentKeys=300, got %d", demuxConfig.ConcurrentKeys)
	}
	if demuxConfig.PerKeyBufferLen != 12 {
		t.Errorf("expected PerKeyBufferLen=12, got %d", demuxConfig.PerKeyBufferLen)
	}
	if demuxConfig.PollTimeout != 110*time.Millisecond {
		t.Errorf("expected PollTimeout=110ms, got %v", demuxConfig.PollTimeout)
	}
	if demuxConfig.AutoCommitInterval != 6*time.Second {
		t.Errorf("expected AutoCommitInterval=6s, got %v", demuxConfig.AutoCommitInterval)
	}
	if demuxConfig.DrainTimeout != 16*time.Second {
		t.Errorf("expected DrainTimeout=16s, got %v", demuxConfig.DrainTimeout)
	}
	if demuxConfig.AwaitAssignmentsTimeout != 31*time.Second {
		t.Errorf("expected AwaitAssignmentsTimeout=31s, got %v", demuxConfig.AwaitAssignmentsTimeout)
	}
	if demuxConfig.CommitIngestChannelLen != 20001 {
		t.Errorf("expected CommitIngestChannelLen=20001, got %d", demuxConfig.CommitIngestChannelLen)
	}
	if demuxConfig.CommitPartitionSliceLen != 700 {
		t.Errorf("expected CommitPartitionSliceLen=700, got %d", demuxConfig.CommitPartitionSliceLen)
	}
	if demuxConfig.QueryTimeout != 6*time.Second {
		t.Errorf("expected QueryTimeout=6s, got %v", demuxConfig.QueryTimeout)
	}
	if demuxConfig.AcquireWorkerTimeoutCircuitBreaker != time.Minute {
		t.Errorf("expected AcquireWorkerTimeoutCircuitBreaker=1m, got %v",
			demuxConfig.AcquireWorkerTimeoutCircuitBreaker)
	}
	if demuxConfig.WorkerShardsCount != 64 {
		t.Errorf("expected WorkerShardsCount=64, got %d", demuxConfig.WorkerShardsCount)
	}
	if demuxConfig.RebalancePausePollingTimeout != 46*time.Second {
		t.Errorf("expected RebalancePausePollingTimeout=46s, got %s", demuxConfig.RebalancePausePollingTimeout)
	}
	if demuxConfig.AcquireCommitGuardTimeout != 11*time.Second {
		t.Errorf("expected AcquireCommitGuardTimeout=11s, got %s", demuxConfig.AcquireCommitGuardTimeout)
	}
}

// Test_DemuxConfig_Empty_JsonUnMarshal should return zero amounts
// so that the default overrides can be applied on startup
func Test_DemuxConfig_Empty_JsonUnMarshal(t *testing.T) {
	var demuxConfig DemuxConfig
	err := json.Unmarshal([]byte(`{}`), &demuxConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if demuxConfig.ConcurrentKeys != 0 {
		t.Errorf("expected ConcurrentKeys=0, got %d", demuxConfig.ConcurrentKeys)
	}
	if demuxConfig.PerKeyBufferLen != 0 {
		t.Errorf("expected PerKeyBufferLen=0, got %d", demuxConfig.PerKeyBufferLen)
	}
	if demuxConfig.PollTimeout != 0 {
		t.Errorf("expected PollTimeout=0, got %v", demuxConfig.PollTimeout)
	}
	if demuxConfig.AwaitAssignmentsTimeout != 0 {
		t.Errorf("expected AwaitAssignmentsTimeout=0, got %v", demuxConfig.AwaitAssignmentsTimeout)
	}
	if demuxConfig.AutoCommitInterval != 0 {
		t.Errorf("expected AutoCommitInterval=0, got %v", demuxConfig.AutoCommitInterval)
	}
	if demuxConfig.QueryTimeout != 0 {
		t.Errorf("expected QueryTimeout=0, got %v", demuxConfig.QueryTimeout)
	}
	if demuxConfig.DrainTimeout != 0 {
		t.Errorf("expected DrainTimeout=0, got %v", demuxConfig.DrainTimeout)
	}
	if demuxConfig.CommitIngestChannelLen != 0 {
		t.Errorf("expected CommitIngestChannelLen=0, got %d", demuxConfig.CommitIngestChannelLen)
	}
	if demuxConfig.AcquireWorkerTimeoutCircuitBreaker != 0 {
		t.Errorf("expected AcquireWorkerTimeoutCircuitBreaker=0, got %v", demuxConfig.AcquireWorkerTimeoutCircuitBreaker)
	}
	if demuxConfig.WorkerShardsCount != 0 {
		t.Errorf("expected WorkerShardsCount=0, got %v", demuxConfig.WorkerShardsCount)
	}
	if demuxConfig.RebalancePausePollingTimeout != 0 {
		t.Errorf("expected RebalancePausePollingTimeout=0, got %v", demuxConfig.RebalancePausePollingTimeout)
	}
	if demuxConfig.AcquireCommitGuardTimeout != 0 {
		t.Errorf("expected AcquireCommitGuardTimeout=0, got %v", demuxConfig.AcquireCommitGuardTimeout)
	}
}

func Test_DemuxConfig_BrokenJson_UnMarshal(t *testing.T) {
	var demuxConfig DemuxConfig
	err := demuxConfig.UnmarshalJSON([]byte(`{`))
	if err == nil {
		t.Fatal("expected error for broken JSON, got nil")
	}
	if !strings.Contains(err.Error(), "unexpected end of JSON input") {
		t.Errorf("expected error containing 'unexpected end of JSON input', got: %v", err)
	}
}

// Test_DemuxConfig_UnmarshalJSON confirms invalid Go time.Duration strings
// cause an error rather than silently failing and defaulting to zero value
func Test_DemuxConfig_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name         string
		jsonFragment string
		wantErr      string
	}{
		{
			name:         "invalid pollDuration time.Duration",
			jsonFragment: `{"pollTimeout": "INVALID_POLL_TIMEOUT"}`,
			wantErr:      "failed to unmarshal pollTimeout: INVALID_POLL_TIMEOUT - time: invalid duration",
		},
		{
			name:         "invalid awaitAssignmentsTimeout time.Duration",
			jsonFragment: `{"awaitAssignmentsTimeout": "INVALID_WAIT_DUR"}`,
			wantErr:      "failed to unmarshal awaitAssignmentsTimeout: INVALID_WAIT_DUR - time: invalid duration",
		},
		{
			name:         "invalid autoCommitInterval time.Duration",
			jsonFragment: `{"autoCommitInterval": "INVALID_COMMIT_INTERVAL"}`,
			wantErr:      "failed to unmarshal autoCommitInterval: INVALID_COMMIT_INTERVAL - time: invalid duration",
		},
		{
			name:         "invalid brokerClientQueryTimeout time.Duration",
			jsonFragment: `{"queryTimeout": "BROKER_QUERY_TIMEOUT"}`,
			wantErr:      "failed to unmarshal queryTimeout: BROKER_QUERY_TIMEOUT - time: invalid duration",
		},
		{
			name:         "invalid drainTimeout time.Duration",
			jsonFragment: `{"drainTimeout": "INVALID_DRAIN_TIMEOUT"}`,
			wantErr:      "failed to unmarshal drainTimeout: INVALID_DRAIN_TIMEOUT - time: invalid duration",
		},
		{
			name:         "invalid acquireWorkerTimeoutCircuitBreaker time.Duration",
			jsonFragment: `{"acquireWorkerTimeoutCircuitBreaker": "INVALID_DRAIN_TIMEOUT"}`,
			wantErr:      "failed to unmarshal acquireWorkerTimeoutCircuitBreaker: INVALID_DRAIN_TIMEOUT - time: invalid duration",
		},
		{
			name:         "invalid rebalancePausePollingTimeout time.Duration",
			jsonFragment: `{"rebalancePausePollingTimeout": "INVALID_REBALANCE_TIMEOUT"}`,
			wantErr:      "failed to unmarshal rebalancePausePollingTimeout: INVALID_REBALANCE_TIMEOUT - time: invalid duration",
		},
		{
			name:         "invalid acquireCommitGuardTimeout time.Duration",
			jsonFragment: `{"acquireCommitGuardTimeout": "INVALID_ACQUIRE_COMMIT_GUARD_TIMEOUT"}`,
			wantErr:      "failed to unmarshal acquireCommitGuardTimeout: INVALID_ACQUIRE_COMMIT_GUARD_TIMEOUT - time: invalid duration",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var demuxConfig DemuxConfig
			err := json.Unmarshal([]byte(tt.jsonFragment), &demuxConfig)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErr, err)
			}
		})
	}
}

func Test_DemuxConfig_SetDefaults(t *testing.T) {
	// Reusable expected configs to avoid duplication
	defaultConfig := DemuxConfig{
		ConcurrentKeys:                     250,
		PerKeyBufferLen:                    16,
		PollTimeout:                        100 * time.Millisecond,
		AwaitAssignmentsTimeout:            50 * time.Second,
		AutoCommitInterval:                 5 * time.Second,
		QueryTimeout:                       5 * time.Second,
		DrainTimeout:                       20 * time.Second,
		CommitIngestChannelLen:             25000,
		CommitPartitionSliceLen:            400,
		WorkerShardsCount:                  16,
		AcquireWorkerTimeoutCircuitBreaker: time.Minute,
		AcquireCommitGuardTimeout:          10 * time.Second,
	}
	maxValuesConfig := DemuxConfig{
		ConcurrentKeys:                     5000,
		PerKeyBufferLen:                    20,
		PollTimeout:                        2 * time.Second,
		AwaitAssignmentsTimeout:            time.Minute,
		AutoCommitInterval:                 15 * time.Second,
		QueryTimeout:                       10 * time.Second,
		DrainTimeout:                       55 * time.Second,
		CommitIngestChannelLen:             200000,
		CommitPartitionSliceLen:            2000,
		WorkerShardsCount:                  16,
		AcquireWorkerTimeoutCircuitBreaker: 10 * time.Minute,
	}

	tests := []struct {
		name        string
		demuxConfig DemuxConfig
		wantErr     string
		expect      DemuxConfig
	}{
		{
			name:        "defaults from zero value",
			demuxConfig: DemuxConfig{},
			wantErr:     "",
			expect:      defaultConfig,
		},
		{
			name:        "maximum values",
			demuxConfig: maxValuesConfig,
			wantErr:     "",
			expect:      maxValuesConfig,
		},
		{
			name: "too high ConcurrentKeys",
			demuxConfig: DemuxConfig{
				ConcurrentKeys: 5001,
			},
			wantErr: "invalid ConcurrentKeys: 5001, should be no more than 5000",
		},
		{
			name: "too high PerKeyBufferLen",
			demuxConfig: DemuxConfig{
				PerKeyBufferLen: 65,
			},
			wantErr: "invalid PerKeyBufferLen: 65, should be no more than 64",
		},
		{
			name: "too low PollTimeout",
			demuxConfig: DemuxConfig{
				PollTimeout: 19 * time.Millisecond,
			},
			wantErr: "invalid PollTimeout: 19ms, should be no less than 20ms",
		},
		{
			name: "too high PollTimeout",
			demuxConfig: DemuxConfig{
				PollTimeout: 2001 * time.Millisecond,
			},
			wantErr: "invalid PollTimeout: 2.001s, should be no more than 2s",
		},
		{
			name: "too low AutoCommitInterval",
			demuxConfig: DemuxConfig{
				AutoCommitInterval: 249 * time.Millisecond,
			},
			wantErr: "invalid AutoCommitInterval: 249ms, should be no less than 250ms",
		},
		{
			name: "too high AutoCommitInterval",
			demuxConfig: DemuxConfig{
				AutoCommitInterval: 15001 * time.Millisecond,
			},
			wantErr: "invalid AutoCommitInterval: 15.001s, should be no more than 15s",
		},
		{
			name: "too low QueryTimeout",
			demuxConfig: DemuxConfig{
				QueryTimeout: 999 * time.Millisecond,
			},
			wantErr: "invalid QueryTimeout: 999ms, should be no less than 1s",
		},
		{
			name: "too high QueryTimeout",
			demuxConfig: DemuxConfig{
				QueryTimeout: 10001 * time.Millisecond,
			},
			wantErr: "invalid QueryTimeout: 10.001s, should be no more than 10s",
		},
		{
			name: "too low DrainTimeout",
			demuxConfig: DemuxConfig{
				DrainTimeout: 1999 * time.Millisecond,
			},
			wantErr: "invalid DrainTimeout: 1.999s, should be no less than 2s",
		},
		{
			name: "too high DrainTimeout",
			demuxConfig: DemuxConfig{
				DrainTimeout: 55001 * time.Millisecond,
			},
			wantErr: "invalid DrainTimeout: 55.001s, should be no more than 55s",
		},
		{
			name: "too low AwaitAssignmentsTimeout",
			demuxConfig: DemuxConfig{
				AwaitAssignmentsTimeout: 4999 * time.Millisecond,
			},
			wantErr: "invalid AwaitAssignmentsTimeout: 4.999s, should be no less than 5s",
		},
		{
			name: "too high AwaitAssignmentsTimeout",
			demuxConfig: DemuxConfig{
				AwaitAssignmentsTimeout: 300001 * time.Millisecond,
			},
			wantErr: "invalid AwaitAssignmentsTimeout: 5m0.001s, should be no more than 5m0s",
		},
		{
			name: "too high CommitIngestChannelLen",
			demuxConfig: DemuxConfig{
				CommitIngestChannelLen: 200001,
			},
			wantErr: "invalid CommitIngestChannelLen: 200001, should be no more than 200000",
		},
		{
			name: "too low CommitIngestChannelLen",
			demuxConfig: DemuxConfig{
				CommitIngestChannelLen: 999,
			},
			wantErr: "invalid CommitIngestChannelLen: 999, should be no less than 1000",
		},
		{
			name: "too high CommitPartitionSliceLen",
			demuxConfig: DemuxConfig{
				CommitPartitionSliceLen: 2001,
			},
			wantErr: "invalid CommitPartitionSliceLen: 2001, should be no more than 2000",
		},
		{
			name: "too low CommitPartitionSliceLen",
			demuxConfig: DemuxConfig{
				CommitPartitionSliceLen: 49,
			},
			wantErr: "invalid CommitPartitionSliceLen: 49, should be no less than 50",
		},
		{
			name: "too high AcquireWorkerTimeoutCircuitBreaker",
			demuxConfig: DemuxConfig{
				AcquireWorkerTimeoutCircuitBreaker: 15*time.Minute + time.Second,
			},
			wantErr: "invalid AcquireWorkerTimeoutCircuitBreaker: 15m1s, should be no more than 15m0s",
		},
		{
			name: "too low AcquireWorkerTimeoutCircuitBreaker",
			demuxConfig: DemuxConfig{
				AcquireWorkerTimeoutCircuitBreaker: 14 * time.Second,
			},
			wantErr: "invalid AcquireWorkerTimeoutCircuitBreaker: 14s, should be no less than 15s",
		},
		{
			name: "not power of 2 WorkerShardsCount",
			demuxConfig: DemuxConfig{
				WorkerShardsCount: 15,
			},
			wantErr: "invalid WorkerShardsCount: 15, must be a power of 2",
		},
		{
			name: "WorkerShardsCount 1 not power of 2",
			demuxConfig: DemuxConfig{
				WorkerShardsCount: 1,
			},
			wantErr: "invalid WorkerShardsCount: 1, must be a power of 2",
		},
		{
			name:        "WorkerShardsCount 0 defaults to 16",
			demuxConfig: DemuxConfig{WorkerShardsCount: 0},
			wantErr:     "",
			expect:      defaultConfig,
		},
		{
			name:        "WorkerShardsCount -2 defaults to 16",
			demuxConfig: DemuxConfig{WorkerShardsCount: -2},
			wantErr:     "",
			expect:      defaultConfig,
		},
		{
			name:        "WorkerShardsCount -4 defaults to 16",
			demuxConfig: DemuxConfig{WorkerShardsCount: -4},
			wantErr:     "",
			expect:      defaultConfig,
		},
		{
			name: "WorkerShardsCount too large",
			demuxConfig: DemuxConfig{
				WorkerShardsCount: 256,
			},
			wantErr: "invalid WorkerShardsCount: 256, should be no more than 64",
		},
		{
			name: "too low RebalancePausePollingTimeout",
			demuxConfig: DemuxConfig{
				RebalancePausePollingTimeout: 9 * time.Second,
			},
			wantErr: "invalid RebalancePausePollingTimeout: 9s, should be no less than 10s",
		},
		{
			name: "too high RebalancePausePollingTimeout",
			demuxConfig: DemuxConfig{
				RebalancePausePollingTimeout: 10*time.Minute + time.Second,
			},
			wantErr: "invalid RebalancePausePollingTimeout: 10m1s, should be no more than 10m0s",
		},
		{
			name: "too low AcquireCommitGuardTimeout",
			demuxConfig: DemuxConfig{
				AcquireCommitGuardTimeout: 99 * time.Millisecond,
			},
			wantErr: "invalid AcquireCommitGuardTimeout: 99ms, should be no less than 100ms",
		},
		{
			name: "too high AcquireCommitGuardTimeout",
			demuxConfig: DemuxConfig{
				AcquireCommitGuardTimeout: 31 * time.Second,
			},
			wantErr: "invalid AcquireCommitGuardTimeout: 31s, should be no more than 30s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//nolint:nestif // table-driven test - nested validation of error paths is clearer than extracting helpers
			if tt.wantErr != "" {
				// Expect panic
				defer func() {
					r := recover()
					if r == nil {
						t.Fatalf("expected panic with error %q, got no panic", tt.wantErr)
					}
					panicErr, ok := r.(error)
					if !ok {
						t.Fatalf("expected panic with error, got: %v", r)
					}
					if panicErr.Error() != tt.wantErr {
						t.Errorf("expected panic error %q, got %q", tt.wantErr, panicErr.Error())
					}
				}()
				tt.demuxConfig.SetDemuxConfigDefaults()
			} else {
				demuxConfig := tt.demuxConfig.SetDemuxConfigDefaults()
				if demuxConfig.ConcurrentKeys != tt.expect.ConcurrentKeys {
					t.Errorf("expected ConcurrentKeys=%d, got %d",
						tt.expect.ConcurrentKeys, demuxConfig.ConcurrentKeys)
				}
				if demuxConfig.PerKeyBufferLen != tt.expect.PerKeyBufferLen {
					t.Errorf("expected PerKeyBufferLen=%d, got %d",
						tt.expect.PerKeyBufferLen, demuxConfig.PerKeyBufferLen)
				}
				if demuxConfig.PollTimeout != tt.expect.PollTimeout {
					t.Errorf("expected PollTimeout=%v, got %v",
						tt.expect.PollTimeout, demuxConfig.PollTimeout)
				}
				if demuxConfig.AwaitAssignmentsTimeout != tt.expect.AwaitAssignmentsTimeout {
					t.Errorf("expected AwaitAssignmentsTimeout=%v, got %v",
						tt.expect.AwaitAssignmentsTimeout, demuxConfig.AwaitAssignmentsTimeout)
				}
				if demuxConfig.AutoCommitInterval != tt.expect.AutoCommitInterval {
					t.Errorf("expected AutoCommitInterval=%v, got %v",
						tt.expect.AutoCommitInterval, demuxConfig.AutoCommitInterval)
				}
				if demuxConfig.QueryTimeout != tt.expect.QueryTimeout {
					t.Errorf("expected QueryTimeout=%v, got %v",
						tt.expect.QueryTimeout, demuxConfig.QueryTimeout)
				}
				if demuxConfig.DrainTimeout != tt.expect.DrainTimeout {
					t.Errorf("expected DrainTimeout=%v, got %v",
						tt.expect.DrainTimeout, demuxConfig.DrainTimeout)
				}
				if demuxConfig.CommitIngestChannelLen != tt.expect.CommitIngestChannelLen {
					t.Errorf("expected CommitIngestChannelLen=%d, got %d",
						tt.expect.CommitIngestChannelLen, demuxConfig.CommitIngestChannelLen)
				}
				if demuxConfig.CommitPartitionSliceLen != tt.expect.CommitPartitionSliceLen {
					t.Errorf("expected CommitPartitionSliceLen=%d, got %d",
						tt.expect.CommitPartitionSliceLen, demuxConfig.CommitPartitionSliceLen)
				}
				if demuxConfig.WorkerShardsCount != tt.expect.WorkerShardsCount {
					t.Errorf("expected WorkerShardsCount=%d, got %d",
						tt.expect.WorkerShardsCount, demuxConfig.WorkerShardsCount)
				}
				if demuxConfig.AcquireWorkerTimeoutCircuitBreaker != tt.expect.AcquireWorkerTimeoutCircuitBreaker {
					t.Errorf("expected AcquireWorkerTimeoutCircuitBreaker=%v, got %v",
						tt.expect.AcquireWorkerTimeoutCircuitBreaker, demuxConfig.AcquireWorkerTimeoutCircuitBreaker)
				}
			}
		})
	}
}

func Test_DemuxConfig_calcIngestChannelSize(t *testing.T) {
	tests := []struct {
		name        string
		demuxConfig *DemuxConfig
		want        int
	}{
		{
			name:        "default config",
			demuxConfig: &DemuxConfig{},
			want:        25_000,
		},
		{
			name: "fairly aggressive vertical scaling in each consumer instance",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys: 900,
			},
			want: 90_000,
		},
		{
			name: "dumb to avoid divide by zeros",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys: 1,
			},
			want: 5_000,
		},
		{
			name: "unlikely per-consumer setting",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys: 5_000,
			},
			want: 100_000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxConfig := tt.demuxConfig.SetDemuxConfigDefaults()
			if demuxConfig.CommitIngestChannelLen != tt.want {
				t.Errorf("expected CommitIngestChannelLen=%d, got %d",
					tt.want, demuxConfig.CommitIngestChannelLen)
			}
		})
	}
}

func Test_DemuxConfig_CalcWorkerChannelsMapSize(t *testing.T) {
	tests := []struct {
		name        string
		demuxConfig *DemuxConfig
		want        int
	}{
		{
			name:        "default config",
			demuxConfig: &DemuxConfig{},
			want:        128,
		},
		{
			name: "5000 concurrent keys, 64 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    5000,
				WorkerShardsCount: 64,
			},
			want: 512,
		},
		{
			name: "5000 concurrent keys, 2 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    5000,
				WorkerShardsCount: 2,
			},
			want: 4096,
		},
		{
			name: "5000 concurrent keys, 4 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    5000,
				WorkerShardsCount: 4,
			},
			want: 4096,
		},
		{
			name: "2500 concurrent keys, 8 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    2500,
				WorkerShardsCount: 8,
			},
			want: 2048,
		},
		{
			name: "2400 concurrent keys, 16 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    2400,
				WorkerShardsCount: 16,
			},
			want: 1024,
		},
		{
			name: "1200 concurrent keys, 16 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    1200,
				WorkerShardsCount: 16,
			},
			want: 512,
		},
		{
			name: "600 concurrent keys, 16 shards",
			demuxConfig: &DemuxConfig{
				ConcurrentKeys:    600,
				WorkerShardsCount: 16,
			},
			want: 256,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxConfig := tt.demuxConfig.SetDemuxConfigDefaults()
			if got := demuxConfig.CalcWorkerChannelsMapSize(); got != tt.want {
				t.Errorf("expected CalcWorkerChannelsMapSize()=%d, got %d", tt.want, got)
			}
		})
	}
}

func Test_DisableValidationEnvironmentVariables(t *testing.T) {
	// using hash to environment variable does not get inadvertently
	// altered in a find and replace.
	h := sha256.New()
	h.Write([]byte(SkipValidationEnvVar))
	expectedHash := "c624063e76d9893c0acf5e2ca4f82896f503ac7b1720385707eb64f86a54871e"
	actualHash := fmt.Sprintf("%x", h.Sum(nil))
	if actualHash != expectedHash {
		t.Errorf("LLINGR_DEMUX_SKIP_CONFIG_VALIDATION inadvertently altered")
	}
}

func Test_IsValidationSkipped(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{"lowercase", "true", true},
		{"uppercase", "TRUE", true},
		{"mixed case", "True", true},

		{"empty", "", false},
		{"t", "t", false},   // must provide 'true' word, not stdlib parseBool behaviour
		{"one", "1", false}, // must provide 'true' word, not stdlib parseBool behaviour
		{"lowercase false", "false", false},
		{"uppercase false", "FALSE", false},
		{"whitespace padded", " true ", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("LLINGR_DEMUX_SKIP_CONFIG_VALIDATION", tt.value)
			if got := isValidationSkipped(); got != tt.want {
				t.Errorf("isValidationSkipped(%s) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func Test_isPowerOfTwo(t *testing.T) {
	tests := []struct {
		name  string
		value int
		want  bool
	}{
		{"negative value", -4, false},
		{"negative value", -2, false},
		{"negative value", -1, false},
		{"zero", 0, false},
		{"one", 1, false},
		{"two", 2, true},
		{"three", 3, false},
		{"four", 4, true},
		{"five", 5, false},
		{"six", 6, false},
		{"seven", 7, false},
		{"eight", 8, true},
		{"nine", 9, false},
		{"fifteen", 15, false},
		{"sixteen", 16, true},
		{"thirty-one", 31, false},
		{"thirty-two", 32, true},
		{"sixty-three", 63, false},
		{"sixty-four", 64, true},
		{"one-twenty-seven", 127, false},
		{"one-twenty-eight", 128, true},
		{"two-fifty-five", 255, false},
		{"two-fifty-six", 256, true},
		{"large power of two", 1024, true},
		{"large non-power of two", 1023, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isPowerOfTwo(tt.value); got != tt.want {
				t.Errorf("isPowerOfTwo(%d) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func Test_DemuxConfig_CalcMinimumIdleWorkers(t *testing.T) {
	tests := []struct {
		name              string
		concurrentKeys    int
		workerShardsCount int
		want              int
	}{
		// Zero values - should use 1 as fallback (cmp.Or)
		{
			name:              "both zero - uses 1/1",
			concurrentKeys:    0,
			workerShardsCount: 0,
			want:              16,
		},
		{
			name:              "keys zero, shards 16",
			concurrentKeys:    0,
			workerShardsCount: 16,
			want:              16,
		},
		{
			name:              "keys 250, shards zero",
			concurrentKeys:    250,
			workerShardsCount: 0,
			want:              16,
		},

		{
			name:              "keys 1, shards 1",
			concurrentKeys:    1,
			workerShardsCount: 1,
			want:              1,
		},
		{
			name:              "keys 1, shards 2",
			concurrentKeys:    1,
			workerShardsCount: 2,
			want:              1,
		},
		{
			name:              "keys 2, shards 1",
			concurrentKeys:    2,
			workerShardsCount: 1,
			want:              2,
		},
		{
			name:              "keys 2, shards 2",
			concurrentKeys:    2,
			workerShardsCount: 2,
			want:              1,
		},

		{
			name:              "keys 16, shards 16",
			concurrentKeys:    16,
			workerShardsCount: 16,
			want:              1,
		},
		{
			name:              "keys 32, shards 16",
			concurrentKeys:    32,
			workerShardsCount: 16,
			want:              2,
		},
		{
			name:              "keys 64, shards 16",
			concurrentKeys:    64,
			workerShardsCount: 16,
			want:              4,
		},
		{
			name:              "keys 128, shards 16",
			concurrentKeys:    128,
			workerShardsCount: 16,
			want:              8,
		},
		{
			name:              "keys 256, shards 16",
			concurrentKeys:    256,
			workerShardsCount: 16,
			want:              16,
		},

		{
			name:              "keys 15, shards 16",
			concurrentKeys:    15,
			workerShardsCount: 16,
			want:              1,
		},
		{
			name:              "keys 17, shards 16",
			concurrentKeys:    17,
			workerShardsCount: 16,
			want:              2,
		},
		{
			name:              "keys 31, shards 16",
			concurrentKeys:    31,
			workerShardsCount: 16,
			want:              2, // (16 + 31 - 1) / 16 = 2
		},
		{
			name:              "keys 33, shards 16",
			concurrentKeys:    33,
			workerShardsCount: 16,
			want:              3,
		},

		// Typical production values
		{
			name:              "keys 250, shards 16 (default)",
			concurrentKeys:    250,
			workerShardsCount: 16,
			want:              16,
		},
		{
			name:              "keys 500, shards 16",
			concurrentKeys:    500,
			workerShardsCount: 16,
			want:              32,
		},
		{
			name:              "keys 1000, shards 16",
			concurrentKeys:    1000,
			workerShardsCount: 16,
			want:              63,
		},
		{
			name:              "keys 2000, shards 16",
			concurrentKeys:    2000,
			workerShardsCount: 16,
			want:              125,
		},

		// low shards count (not recommended)
		{
			name:              "keys 250, shards 2",
			concurrentKeys:    250,
			workerShardsCount: 2,
			want:              125,
		},
		{
			name:              "keys 250, shards 4",
			concurrentKeys:    250,
			workerShardsCount: 4,
			want:              63,
		},
		{
			name:              "keys 250, shards 8",
			concurrentKeys:    250,
			workerShardsCount: 8,
			want:              32,
		},
		{
			name:              "keys 250, shards 32",
			concurrentKeys:    250,
			workerShardsCount: 32,
			want:              8,
		},
		{
			name:              "keys 250, shards 64",
			concurrentKeys:    250,
			workerShardsCount: 64,
			want:              4,
		},

		// extreme concurrency
		{
			name:              "keys 5000, shards 2",
			concurrentKeys:    5000,
			workerShardsCount: 2,
			want:              2500,
		},
		{
			name:              "keys 5000, shards 4",
			concurrentKeys:    5000,
			workerShardsCount: 4,
			want:              1250,
		},
		{
			name:              "keys 5000, shards 8",
			concurrentKeys:    5000,
			workerShardsCount: 8,
			want:              625,
		},
		{
			name:              "keys 5000, shards 32",
			concurrentKeys:    5000,
			workerShardsCount: 32,
			want:              157,
		},
		{
			name:              "keys 5000, shards 64",
			concurrentKeys:    5000,
			workerShardsCount: 64,
			want:              79,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &DemuxConfig{
				ConcurrentKeys:    tt.concurrentKeys,
				WorkerShardsCount: tt.workerShardsCount,
			}
			got := config.CalcMinimumIdleWorkers()
			if got != tt.want {
				t.Errorf("CalcMinimumIdleWorkers() with keys=%d, shards=%d = %d, want %d, got: %d",
					tt.concurrentKeys, tt.workerShardsCount, got, tt.want, got)
			}
		})
	}
}

// Test_DefaultConstants verifies the exact values of default constants,
// and exists specifically to kill mutation testing survivors.
func Test_DefaultConstants(t *testing.T) {
	tests := []struct {
		name     string
		got      time.Duration
		expected time.Duration
	}{
		// pollTimeout family (lines 38-40)
		{"pollTimeout default", pollTimeout, 100 * time.Millisecond},
		{"pollTimeoutMinimum", pollTimeoutMinimum, 20 * time.Millisecond},
		{"pollTimeoutMax", pollTimeoutMax, 2 * time.Second},

		// commitInterval family (lines 45-47)
		{"commitInterval default", commitInterval, 5 * time.Second},
		{"commitIntervalMinimum", commitIntervalMinimum, 250 * time.Millisecond},
		{"commitIntervalMax", commitIntervalMax, 15 * time.Second},

		// drainTimeout family (lines 53-55)
		{"drainTimeout default", drainTimeout, 20 * time.Second},
		{"drainTimeoutMinimum", drainTimeoutMinimum, 2 * time.Second},
		{"drainTimeoutMax", drainTimeoutMax, 55 * time.Second},

		// awaitAssignmentsTimeout family (lines 61-63)
		{"awaitAssignmentsTimeout default", awaitAssignmentsTimeout, 50 * time.Second},
		{"awaitAssignmentsTimeoutMinimum", awaitAssignmentsTimeoutMinimum, 5 * time.Second},
		{"awaitAssignmentsTimeoutMax", awaitAssignmentsTimeoutMax, 5 * time.Minute},

		// queryTimeout family (lines 85-87)
		{"queryTimeout default", queryTimeout, 5 * time.Second},
		{"queryTimeoutMinimum", queryTimeoutMinimum, 1 * time.Second},
		{"queryTimeoutMax", queryTimeoutMax, 10 * time.Second},

		// acquireWorkerTimeoutCircuitBreaker family
		{"acquireWorkerTimeoutCircuitBreaker default", acquireWorkerTimeoutCircuitBreaker, 1 * time.Minute},
		{"acquireWorkerTimeoutCircuitBreakerMinimum", acquireWorkerTimeoutCircuitBreakerMinimum, 15 * time.Second},
		{"acquireWorkerTimeoutCircuitBreakerMax", acquireWorkerTimeoutCircuitBreakerMax, 15 * time.Minute},

		// rebalancePausePollingTimeout family
		{"rebalancePausePollingTimeout default", rebalancePausePollingTimeout, 30 * time.Second},
		{"rebalancePausePollingTimeoutMinimum", rebalancePausePollingTimeoutMinimum, 10 * time.Second},
		{"rebalancePausePollingTimeoutMax", rebalancePausePollingTimeoutMax, 10 * time.Minute},

		// acquireCommitGuardTimeout family
		{"acquireCommitGuardTimeout default", acquireCommitGuardTimeout, 10 * time.Second},
		{"acquireCommitGuardTimeoutMinimum", acquireCommitGuardTimeoutMinimum, 100 * time.Millisecond},
		{"acquireCommitGuardTimeoutMax", acquireCommitGuardTimeoutMax, 30 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s: got %v, expected %v", tt.name, tt.got, tt.expected)
			}
		})
	}
}

// Test_MutationKiller_IntConstants verifies exact values of integer constants.
func Test_MutationKiller_IntConstants(t *testing.T) {
	tests := []struct {
		name     string
		got      int
		expected int
	}{
		{"concurrentKeys default", concurrentKeys, 250},
		{"concurrentKeysMin", concurrentKeysMin, 1},
		{"concurrentKeysMax", concurrentKeysMax, 5000},

		{"perKeyBufferLen default", perKeyBufferLen, 16},
		{"perKeyBufferLenMin", perKeyBufferLenMin, 1},
		{"perKeyBufferLenMax", perKeyBufferLenMax, 64},

		{"commitIngestChannelLenCalcMin", commitIngestChannelLenCalcMin, 5000},
		{"commitIngestChannelLenCalcMax", commitIngestChannelLenCalcMax, 100000},
		{"commitIngestChannelLenMin", commitIngestChannelLenMin, 1000},
		{"commitIngestChannelLenMax", commitIngestChannelLenMax, 200000},

		{"commitPartitionSliceLen default", commitPartitionSliceLen, 400},
		{"commitPartitionSliceLenMin", commitPartitionSliceLenMin, 50},
		{"commitPartitionSliceLenMax", commitPartitionSliceLenMax, 2000},

		{"workerShardsCount default", workerShardsCount, 16},
		{"workerShardsCountMax", workerShardsCountMax, 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("%s: got %d, expected %d", tt.name, tt.got, tt.expected)
			}
		})
	}
}

// Test_MutationKiller_ExactMinimumBoundaries verifies that values exactly at
// the minimum boundary are ACCEPTED (no panic, no default override).
//
// This kills mutations that change `<` to `<=` in validation logic.
// If `c.Field < minimum` becomes `c.Field <= minimum`, then value == minimum
// would incorrectly trigger default assignment or panic.
func Test_MutationKiller_ExactMinimumBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		config DemuxConfig
		check  func(DemuxConfig) bool
	}{
		{
			name:   "ConcurrentKeys at minimum (1)",
			config: DemuxConfig{ConcurrentKeys: 1},
			check:  func(c DemuxConfig) bool { return c.ConcurrentKeys == 1 },
		},
		{
			name:   "PerKeyBufferLen at minimum (1)",
			config: DemuxConfig{PerKeyBufferLen: 1},
			check:  func(c DemuxConfig) bool { return c.PerKeyBufferLen == 1 },
		},
		{
			name:   "PollTimeout at minimum (20ms)",
			config: DemuxConfig{PollTimeout: 20 * time.Millisecond},
			check:  func(c DemuxConfig) bool { return c.PollTimeout == 20*time.Millisecond },
		},
		{
			name:   "AutoCommitInterval at minimum (250ms)",
			config: DemuxConfig{AutoCommitInterval: 250 * time.Millisecond},
			check:  func(c DemuxConfig) bool { return c.AutoCommitInterval == 250*time.Millisecond },
		},
		{
			name:   "QueryTimeout at minimum (1s)",
			config: DemuxConfig{QueryTimeout: 1 * time.Second},
			check:  func(c DemuxConfig) bool { return c.QueryTimeout == 1*time.Second },
		},
		{
			name:   "DrainTimeout at minimum (2s)",
			config: DemuxConfig{DrainTimeout: 2 * time.Second},
			check:  func(c DemuxConfig) bool { return c.DrainTimeout == 2*time.Second },
		},
		{
			name:   "AwaitAssignmentsTimeout at minimum (5s)",
			config: DemuxConfig{AwaitAssignmentsTimeout: 5 * time.Second},
			check:  func(c DemuxConfig) bool { return c.AwaitAssignmentsTimeout == 5*time.Second },
		},
		{
			name:   "CommitIngestChannelLen at minimum (1000)",
			config: DemuxConfig{CommitIngestChannelLen: 1000},
			check:  func(c DemuxConfig) bool { return c.CommitIngestChannelLen == 1000 },
		},
		{
			name:   "CommitPartitionSliceLen at minimum (50)",
			config: DemuxConfig{CommitPartitionSliceLen: 50},
			check:  func(c DemuxConfig) bool { return c.CommitPartitionSliceLen == 50 },
		},
		{
			name:   "AcquireWorkerTimeoutCircuitBreaker at minimum (15s)",
			config: DemuxConfig{AcquireWorkerTimeoutCircuitBreaker: 15 * time.Second},
			check:  func(c DemuxConfig) bool { return c.AcquireWorkerTimeoutCircuitBreaker == 15*time.Second },
		},
		{
			name:   "WorkerShardsCount at minimum valid (2)",
			config: DemuxConfig{WorkerShardsCount: 2},
			check:  func(c DemuxConfig) bool { return c.WorkerShardsCount == 2 },
		},
		{
			name:   "RebalancePausePollingTimeout at minimum (10s)",
			config: DemuxConfig{RebalancePausePollingTimeout: 10 * time.Second},
			check:  func(c DemuxConfig) bool { return c.RebalancePausePollingTimeout == 10*time.Second },
		},
		{
			name:   "AcquireCommitGuardTimeout at minimum (100ms)",
			config: DemuxConfig{AcquireCommitGuardTimeout: 100 * time.Millisecond},
			check:  func(c DemuxConfig) bool { return c.AcquireCommitGuardTimeout == 100*time.Millisecond },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.SetDemuxConfigDefaults()
			if !tt.check(result) {
				t.Errorf("value at exact minimum boundary was modified or rejected")
			}
		})
	}
}

// Test_MutationKiller_ExactMaximumBoundaries verifies that values exactly at
// the maximum boundary are ACCEPTED (no panic).
//
// This kills mutations that change `>` to `>=` in validation logic.
func Test_MutationKiller_ExactMaximumBoundaries(t *testing.T) {
	tests := []struct {
		name   string
		config DemuxConfig
		check  func(DemuxConfig) bool
	}{
		{
			name:   "ConcurrentKeys at maximum (5000)",
			config: DemuxConfig{ConcurrentKeys: 5000},
			check:  func(c DemuxConfig) bool { return c.ConcurrentKeys == 5000 },
		},
		{
			name:   "PerKeyBufferLen at maximum (64)",
			config: DemuxConfig{PerKeyBufferLen: 64},
			check:  func(c DemuxConfig) bool { return c.PerKeyBufferLen == 64 },
		},
		{
			name:   "PollTimeout at maximum (2s)",
			config: DemuxConfig{PollTimeout: 2 * time.Second},
			check:  func(c DemuxConfig) bool { return c.PollTimeout == 2*time.Second },
		},
		{
			name:   "AutoCommitInterval at maximum (15s)",
			config: DemuxConfig{AutoCommitInterval: 15 * time.Second},
			check:  func(c DemuxConfig) bool { return c.AutoCommitInterval == 15*time.Second },
		},
		{
			name:   "QueryTimeout at maximum (10s)",
			config: DemuxConfig{QueryTimeout: 10 * time.Second},
			check:  func(c DemuxConfig) bool { return c.QueryTimeout == 10*time.Second },
		},
		{
			name:   "DrainTimeout at maximum (55s)",
			config: DemuxConfig{DrainTimeout: 55 * time.Second},
			check:  func(c DemuxConfig) bool { return c.DrainTimeout == 55*time.Second },
		},
		{
			name:   "AwaitAssignmentsTimeout at maximum (5m)",
			config: DemuxConfig{AwaitAssignmentsTimeout: 5 * time.Minute},
			check:  func(c DemuxConfig) bool { return c.AwaitAssignmentsTimeout == 5*time.Minute },
		},
		{
			name:   "CommitIngestChannelLen at maximum (200000)",
			config: DemuxConfig{CommitIngestChannelLen: 200000},
			check:  func(c DemuxConfig) bool { return c.CommitIngestChannelLen == 200000 },
		},
		{
			name:   "CommitPartitionSliceLen at maximum (2000)",
			config: DemuxConfig{CommitPartitionSliceLen: 2000},
			check:  func(c DemuxConfig) bool { return c.CommitPartitionSliceLen == 2000 },
		},
		{
			name:   "AcquireWorkerTimeoutCircuitBreaker at maximum (15m)",
			config: DemuxConfig{AcquireWorkerTimeoutCircuitBreaker: 15 * time.Minute},
			check:  func(c DemuxConfig) bool { return c.AcquireWorkerTimeoutCircuitBreaker == 15*time.Minute },
		},
		{
			name:   "WorkerShardsCount at maximum (64)",
			config: DemuxConfig{WorkerShardsCount: 64},
			check:  func(c DemuxConfig) bool { return c.WorkerShardsCount == 64 },
		},
		{
			name:   "RebalancePausePollingTimeout at maximum (10m)",
			config: DemuxConfig{RebalancePausePollingTimeout: 10 * time.Minute},
			check:  func(c DemuxConfig) bool { return c.RebalancePausePollingTimeout == 10*time.Minute },
		},
		{
			name:   "AcquireCommitGuardTimeout at maximum (30s)",
			config: DemuxConfig{AcquireCommitGuardTimeout: 30 * time.Second},
			check:  func(c DemuxConfig) bool { return c.AcquireCommitGuardTimeout == 30*time.Second },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.SetDemuxConfigDefaults()
			if !tt.check(result) {
				t.Errorf("value at exact maximum boundary was modified or rejected")
			}
		})
	}
}

// Test_MutationKiller_OneBelowMinimum_Defaults verifies that values one unit
// below the minimum trigger default assignment (for fields that default rather
// than panic).
//
// This kills mutations on the `< minimum` comparisons that trigger defaults.
func Test_MutationKiller_OneBelowMinimum_Defaults(t *testing.T) {
	// ConcurrentKeys: 0 should get default 250 (not remain 0)
	t.Run("ConcurrentKeys zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{ConcurrentKeys: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.ConcurrentKeys != 250 {
			t.Errorf("ConcurrentKeys=0 should default to 250, got %d", result.ConcurrentKeys)
		}
	})

	// PerKeyBufferLen: 0 should get default 16
	t.Run("PerKeyBufferLen zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{PerKeyBufferLen: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.PerKeyBufferLen != 16 {
			t.Errorf("PerKeyBufferLen=0 should default to 16, got %d", result.PerKeyBufferLen)
		}
	})

	// PollTimeout: 0 should get default 100ms
	t.Run("PollTimeout zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{PollTimeout: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.PollTimeout != 100*time.Millisecond {
			t.Errorf("PollTimeout=0 should default to 100ms, got %v", result.PollTimeout)
		}
	})

	// AutoCommitInterval: 0 should get default 5s
	t.Run("AutoCommitInterval zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{AutoCommitInterval: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.AutoCommitInterval != 5*time.Second {
			t.Errorf("AutoCommitInterval=0 should default to 5s, got %v", result.AutoCommitInterval)
		}
	})

	// QueryTimeout: 0 should get default 5s
	t.Run("QueryTimeout zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{QueryTimeout: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.QueryTimeout != 5*time.Second {
			t.Errorf("QueryTimeout=0 should default to 5s, got %v", result.QueryTimeout)
		}
	})

	// DrainTimeout: 0 should get default 20s
	t.Run("DrainTimeout zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{DrainTimeout: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.DrainTimeout != 20*time.Second {
			t.Errorf("DrainTimeout=0 should default to 20s, got %v", result.DrainTimeout)
		}
	})

	// AwaitAssignmentsTimeout: 0 should get default 50s
	t.Run("AwaitAssignmentsTimeout zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{AwaitAssignmentsTimeout: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.AwaitAssignmentsTimeout != 50*time.Second {
			t.Errorf("AwaitAssignmentsTimeout=0 should default to 50s, got %v", result.AwaitAssignmentsTimeout)
		}
	})

	// CommitPartitionSliceLen: 0 should get default 400
	t.Run("CommitPartitionSliceLen zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{CommitPartitionSliceLen: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.CommitPartitionSliceLen != 400 {
			t.Errorf("CommitPartitionSliceLen=0 should default to 400, got %d", result.CommitPartitionSliceLen)
		}
	})

	// AcquireWorkerTimeoutCircuitBreaker: 0 should get default 1m
	t.Run("AcquireWorkerTimeoutCircuitBreaker zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{AcquireWorkerTimeoutCircuitBreaker: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.AcquireWorkerTimeoutCircuitBreaker != 1*time.Minute {
			t.Errorf("AcquireWorkerTimeoutCircuitBreaker=0 should default to 1m, got %v",
				result.AcquireWorkerTimeoutCircuitBreaker)
		}
	})

	// WorkerShardsCount: 0 should get default 16
	t.Run("WorkerShardsCount zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{WorkerShardsCount: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.WorkerShardsCount != 16 {
			t.Errorf("WorkerShardsCount=0 should default to 16, got %d", result.WorkerShardsCount)
		}
	})

	// RebalancePausePollingTimeout: 0 should get default 30s
	t.Run("RebalancePausePollingTimeout zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{RebalancePausePollingTimeout: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.RebalancePausePollingTimeout != 30*time.Second {
			t.Errorf("RebalancePausePollingTimeout=0 should default to 30s, got %v",
				result.RebalancePausePollingTimeout)
		}
	})

	// AcquireCommitGuardTimeout: 0 should get default 10s
	t.Run("AcquireCommitGuardTimeout zero gets default", func(t *testing.T) {
		cfg := DemuxConfig{AcquireCommitGuardTimeout: 0}
		result := cfg.SetDemuxConfigDefaults()
		if result.AcquireCommitGuardTimeout != 10*time.Second {
			t.Errorf("AcquireCommitGuardTimeout=0 should default to 10s, got %v",
				result.AcquireCommitGuardTimeout)
		}
	})
}

// Test_MutationKiller_CommitIngestChannelLen_Calculation verifies the
// calculated default for CommitIngestChannelLen based on ConcurrentKeys.
//
// The calculation uses thresholds at 50 and 1000 concurrent keys.
func Test_MutationKiller_CommitIngestChannelLen_Calculation(t *testing.T) {
	tests := []struct {
		name           string
		concurrentKeys int
		expectedLen    int
	}{
		// Below 50: uses commitIngestChannelLenCalcMin (5000)
		{"keys=1 uses calcMin", 1, 5000},
		{"keys=49 uses calcMin", 49, 5000},
		{"keys=50 uses calculation", 50, 5000}, // 50*100=5000, same as min

		// Between 50 and 1000: uses ConcurrentKeys * 100
		{"keys=51 uses calculation", 51, 5100},
		{"keys=100 uses calculation", 100, 10000},
		{"keys=250 uses calculation", 250, 25000},
		{"keys=500 uses calculation", 500, 50000},
		{"keys=999 uses calculation", 999, 99900},
		{"keys=1000 uses calculation", 1000, 100000},

		// Above 1000: uses commitIngestChannelLenCalcMax (100000)
		{"keys=1001 uses calcMax", 1001, 100000},
		{"keys=2000 uses calcMax", 2000, 100000},
		{"keys=5000 uses calcMax", 5000, 100000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DemuxConfig{ConcurrentKeys: tt.concurrentKeys}
			result := cfg.SetDemuxConfigDefaults()
			if result.CommitIngestChannelLen != tt.expectedLen {
				t.Errorf("ConcurrentKeys=%d: expected CommitIngestChannelLen=%d, got %d",
					tt.concurrentKeys, tt.expectedLen, result.CommitIngestChannelLen)
			}
		})
	}
}

// Test_MutationKiller_ValidMidRangeValues verifies that values in the middle
// of valid ranges are preserved without modification.
//
// This ensures the validation logic doesn't incorrectly modify valid values.
func Test_MutationKiller_ValidMidRangeValues(t *testing.T) {
	cfg := DemuxConfig{
		ConcurrentKeys:                     500,                    // mid-range (1-5000)
		PerKeyBufferLen:                    32,                     // mid-range (1-64)
		PollTimeout:                        500 * time.Millisecond, // mid-range (20ms-2s)
		AutoCommitInterval:                 3 * time.Second,        // mid-range (250ms-15s)
		QueryTimeout:                       3 * time.Second,        // mid-range (1s-10s)
		DrainTimeout:                       30 * time.Second,       // mid-range (2s-55s)
		AwaitAssignmentsTimeout:            2 * time.Minute,        // mid-range (5s-5m)
		CommitIngestChannelLen:             50000,                  // mid-range (1000-200000)
		CommitPartitionSliceLen:            1000,                   // mid-range (50-2000)
		AcquireWorkerTimeoutCircuitBreaker: 5 * time.Minute,        // mid-range (15s-15m)
		WorkerShardsCount:                  32,                     // valid power of 2
		RebalancePausePollingTimeout:       1 * time.Minute,        // mid-range (10s-10m)
		AcquireCommitGuardTimeout:          15 * time.Second,       // mid-range (100ms-30s)
	}

	result := cfg.SetDemuxConfigDefaults()

	// All values should be preserved exactly
	if result.ConcurrentKeys != 500 {
		t.Errorf("ConcurrentKeys changed from 500 to %d", result.ConcurrentKeys)
	}
	if result.PerKeyBufferLen != 32 {
		t.Errorf("PerKeyBufferLen changed from 32 to %d", result.PerKeyBufferLen)
	}
	if result.PollTimeout != 500*time.Millisecond {
		t.Errorf("PollTimeout changed from 500ms to %v", result.PollTimeout)
	}
	if result.AutoCommitInterval != 3*time.Second {
		t.Errorf("AutoCommitInterval changed from 3s to %v", result.AutoCommitInterval)
	}
	if result.QueryTimeout != 3*time.Second {
		t.Errorf("QueryTimeout changed from 3s to %v", result.QueryTimeout)
	}
	if result.DrainTimeout != 30*time.Second {
		t.Errorf("DrainTimeout changed from 30s to %v", result.DrainTimeout)
	}
	if result.AwaitAssignmentsTimeout != 2*time.Minute {
		t.Errorf("AwaitAssignmentsTimeout changed from 2m to %v", result.AwaitAssignmentsTimeout)
	}
	if result.CommitIngestChannelLen != 50000 {
		t.Errorf("CommitIngestChannelLen changed from 50000 to %d", result.CommitIngestChannelLen)
	}
	if result.CommitPartitionSliceLen != 1000 {
		t.Errorf("CommitPartitionSliceLen changed from 1000 to %d", result.CommitPartitionSliceLen)
	}
	if result.AcquireWorkerTimeoutCircuitBreaker != 5*time.Minute {
		t.Errorf("AcquireWorkerTimeoutCircuitBreaker changed from 5m to %v",
			result.AcquireWorkerTimeoutCircuitBreaker)
	}
	if result.WorkerShardsCount != 32 {
		t.Errorf("WorkerShardsCount changed from 32 to %d", result.WorkerShardsCount)
	}
	if result.RebalancePausePollingTimeout != 1*time.Minute {
		t.Errorf("RebalancePausePollingTimeout changed from 1m to %v",
			result.RebalancePausePollingTimeout)
	}
	if result.AcquireCommitGuardTimeout != 15*time.Second {
		t.Errorf("AcquireCommitGuardTimeout changed from 15s to %v",
			result.AcquireCommitGuardTimeout)
	}
}
