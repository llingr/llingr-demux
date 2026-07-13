// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package pipeline

import (
	"testing"
)

func TestShardSnapshots_ReturnsPerShardState(t *testing.T) {
	shards := make([]*WorkerShard[string], 4)
	for i := range shards {
		shards[i] = &WorkerShard[string]{
			//activeCount: &atomic.Int32{},
			pooledCount: func() int { return 0 },
		}
	}
	shards[0].activeCount = 3
	shards[0].pooledCount = func() int { return 5 }
	shards[1].activeCount = 1
	shards[1].pooledCount = func() int { return 7 }
	shards[2].activeCount = 4
	shards[2].pooledCount = func() int { return 4 }
	shards[3].activeCount = 0
	shards[3].pooledCount = func() int { return 8 }

	dmx := &Demux[string]{workerShards: shards}
	snapshots := dmx.ShardSnapshots()

	if len(snapshots) != 4 {
		t.Fatalf("expected 4 shards, got %d", len(snapshots))
	}

	expected := []struct {
		shard, active, pooled int
	}{
		{0, 3, 5},
		{1, 1, 7},
		{2, 4, 4},
		{3, 0, 8},
	}
	for i, exp := range expected {
		if snapshots[i].Shard != exp.shard {
			t.Errorf("shard %d: Shard expected %d, got %d", i, exp.shard, snapshots[i].Shard)
		}
		if snapshots[i].ActiveWorkers != exp.active {
			t.Errorf("shard %d: ActiveWorkers expected %d, got %d", i, exp.active, snapshots[i].ActiveWorkers)
		}
		if snapshots[i].PooledWorkers != exp.pooled {
			t.Errorf("shard %d: PooledWorkers expected %d, got %d", i, exp.pooled, snapshots[i].PooledWorkers)
		}
	}
}
