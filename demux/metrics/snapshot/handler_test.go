// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package snapshot

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// failingResponseWriter forces Encode to fail by returning an error
// from Write - used to exercise the handler's error fallback branch
type failingResponseWriter struct {
	headers http.Header
	status  int
}

func (f *failingResponseWriter) Header() http.Header {
	if f.headers == nil {
		f.headers = http.Header{}
	}
	return f.headers
}

func (f *failingResponseWriter) Write(_ []byte) (int, error) {
	return 0, errors.New("write failed")
}

func (f *failingResponseWriter) WriteHeader(status int) {
	f.status = status
}

func TestNewHandler_EncodeError_FallsBackToHTTPError(t *testing.T) {
	handler := NewHandler(func() Snapshot { return Snapshot{} })

	req := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	rec := &failingResponseWriter{}

	// must not panic - the Encode error path runs http.Error, which also
	// invokes Write on our failing writer (silently dropped). We only need
	// to confirm the handler completes and tagged InternalServerError
	handler(rec, req)

	if rec.status != http.StatusInternalServerError {
		t.Errorf("expected status 500 on encode failure, got %d", rec.status)
	}
}

func TestNewHandler_ReturnsJSON(t *testing.T) {
	rebalanceTime := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)

	snap := Snapshot{
		Summary: SummarySnapshot{
			TopicName:              "orders.completed",
			AssignedPartitionCount: 1,
			TotalProcessed:         48291,
			LastRebalanceTime:      rebalanceTime,
		},
		Concurrency: ConcurrencySnapshot{
			GuardActive:      5,
			GuardCapacity:    250,
			OverflowActive:   2,
			OverflowCapacity: 100,
		},
		Shards: []ShardSnapshot{
			{Shard: 0, ActiveWorkers: 3, PooledWorkers: 5},
		},
		Throughput: ThroughputSnapshot{
			Windows: []ThroughputEntry{
				{
					FromTime:        time.Date(2025, 6, 15, 14, 32, 7, 0, time.UTC),
					ToTime:          time.Date(2025, 6, 15, 14, 32, 7, 999_000_000, time.UTC),
					ProcessedCount:  310,
					DeadLetterCount: 3,
				},
				{
					FromTime:        time.Date(2025, 6, 15, 14, 32, 6, 0, time.UTC),
					ToTime:          time.Date(2025, 6, 15, 14, 32, 6, 999_000_000, time.UTC),
					ProcessedCount:  298,
					DeadLetterCount: 0,
				},
			},
			TotalProcessed:      608,
			TotalDeadLettered:   3,
			AvgProcessDuration:  10 * time.Millisecond,
			MaxProcessDuration:  50 * time.Millisecond,
			AvgEndToEndDuration: 25 * time.Millisecond,
			MaxEndToEndDuration: 200 * time.Millisecond,
		},
		PreCommits: PreCommitsSnapshot{
			LastRebalanceTime: rebalanceTime,
			Partitions: []PartitionSnapshot{
				{Partition: 0, CommittedOffset: 1049, HighestReadyOffset: 1052, GapBufferDepth: 2, Assigned: true},
			},
		},
	}

	handler := NewHandler(func() Snapshot { return snap })

	req := httptest.NewRequest(http.MethodGet, "/snapshot", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	contentType := rec.Header().Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	var result map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// verify top-level keys
	expectedKeys := []string{
		"summary", "concurrency", "shards", "throughput",
		"preCommits",
	}
	for _, key := range expectedKeys {
		if _, ok := result[key]; !ok {
			t.Errorf("expected key %q in JSON response", key)
		}
	}

	// verify summary
	summary, ok := result["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary object, got %v", result["summary"])
	}
	if summary["topicName"] != "orders.completed" {
		t.Errorf("summary.topicName: expected \"orders.completed\", got %v", summary["topicName"])
	}
	if summary["totalProcessed"] != float64(48291) {
		t.Errorf("summary.totalProcessed: expected 48291, got %v", summary["totalProcessed"])
	}
	if summary["lastRebalanceTime"] != "2025-06-15T14:30:00.000Z" {
		t.Errorf("summary.lastRebalanceTime: expected \"2025-06-15T14:30:00.000Z\", got %v", summary["lastRebalanceTime"])
	}
	if summary["assignedPartitionCount"] != float64(1) {
		t.Errorf("summary.assignedPartitionCount: expected 1, got %v", summary["assignedPartitionCount"])
	}

	// verify concurrency object
	concurrency, ok := result["concurrency"].(map[string]any)
	if !ok {
		t.Fatalf("expected concurrency object, got %v", result["concurrency"])
	}
	if concurrency["guardActive"] != float64(5) {
		t.Errorf("guardActive: expected 5, got %v", concurrency["guardActive"])
	}
	if concurrency["guardCapacity"] != float64(250) {
		t.Errorf("guardCapacity: expected 250, got %v", concurrency["guardCapacity"])
	}

	// verify throughput object
	throughput, ok := result["throughput"].(map[string]any)
	if !ok {
		t.Fatalf("expected throughput object, got %v", result["throughput"])
	}
	if throughput["totalProcessed"] != float64(608) {
		t.Errorf("throughput.totalProcessed: expected 608, got %v", throughput["totalProcessed"])
	}
	if throughput["totalDeadLettered"] != float64(3) {
		t.Errorf("throughput.totalDeadLettered: expected 3, got %v", throughput["totalDeadLettered"])
	}
	if throughput["avgProcessDuration"] != "10ms" {
		t.Errorf("avgProcessDuration: expected \"10ms\", got %v", throughput["avgProcessDuration"])
	}
	if throughput["maxProcessDuration"] != "50ms" {
		t.Errorf("maxProcessDuration: expected \"50ms\", got %v", throughput["maxProcessDuration"])
	}
	if throughput["avgEndToEndDuration"] != "25ms" {
		t.Errorf("avgEndToEndDuration: expected \"25ms\", got %v", throughput["avgEndToEndDuration"])
	}
	if throughput["maxEndToEndDuration"] != "200ms" {
		t.Errorf("maxEndToEndDuration: expected \"200ms\", got %v", throughput["maxEndToEndDuration"])
	}

	// verify throughput windows
	windows, ok := throughput["windows"].([]any)
	if !ok || len(windows) != 2 {
		t.Fatalf("expected 2 windows, got %v", throughput["windows"])
	}
	entry := windows[0].(map[string]any)
	if entry["fromTime"] != "2025-06-15T14:32:07.000Z" {
		t.Errorf("windows[0].fromTime: expected \"2025-06-15T14:32:07.000Z\", got %v", entry["fromTime"])
	}
	if entry["processedCount"] != float64(310) {
		t.Errorf("windows[0].processedCount: expected 310, got %v", entry["processedCount"])
	}
	if entry["deadLetterCount"] != float64(3) {
		t.Errorf("windows[0].deadLetterCount: expected 3, got %v", entry["deadLetterCount"])
	}

	// verify timestamp is NOT in JSON output
	if _, hasTimestamp := result["timestamp"]; hasTimestamp {
		t.Error("timestamp should not appear in JSON output")
	}

	// verify preCommits (lastRebalanceTime should NOT appear here)
	preCommits, ok := result["preCommits"].(map[string]any)
	if !ok {
		t.Fatalf("expected preCommits object, got %v", result["preCommits"])
	}
	if _, hasRebalance := preCommits["lastRebalanceTime"]; hasRebalance {
		t.Error("preCommits should not contain lastRebalanceTime (it belongs in summary)")
	}
	partitions, ok := preCommits["partitions"].([]any)
	if !ok || len(partitions) != 1 {
		t.Fatalf("expected 1 partition, got %v", preCommits["partitions"])
	}
	partition := partitions[0].(map[string]any)
	if partition["partition"] != float64(0) {
		t.Errorf("partition: expected 0, got %v", partition["partition"])
	}
	if partition["committedOffset"] != float64(1049) {
		t.Errorf("committedOffset: expected 1049, got %v", partition["committedOffset"])
	}
	if partition["highestReadyOffset"] != float64(1052) {
		t.Errorf("highestReadyOffset: expected 1052, got %v", partition["highestReadyOffset"])
	}
	if partition["gapBufferDepth"] != float64(2) {
		t.Errorf("gapBufferDepth: expected 2, got %v", partition["gapBufferDepth"])
	}
	if partition["assigned"] != true {
		t.Errorf("assigned: expected true, got %v", partition["assigned"])
	}
}
