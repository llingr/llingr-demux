// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package subscription

import (
	"strings"
	"testing"

	"github.com/llingr/llingr-nexus/nexus"
)

// A consumer serves exactly one topic: offset trackers are keyed by partition
// alone, so accepting rebalance events for a second topic would silently
// cross-contaminate committed offsets between topics that share partition
// numbers. A misconfigured broker client (e.g. a multi-topic or pattern
// subscription) must be rejected fail-fast, before any state is touched.

func TestHandleRebalance_ForeignTopicAssign_RejectsAndTriggersCircuitBreaker(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	err := sub.HandleRebalance(nexus.Assign, []nexus.RebalanceInfo{{
		RebalanceType: nexus.Assign, TopicName: "other-topic", Partition: 0, CommittedOffset: 100,
	}})

	if err == nil {
		t.Fatal("a foreign-topic assign must be rejected with an error")
	}
	if !strings.Contains(err.Error(), "other-topic") || !strings.Contains(err.Error(), "test-topic") {
		t.Errorf("error should name both topics, got: %v", err)
	}
	if got := h.circuitBreaker.shutdownCount.Load(); got != 1 {
		t.Errorf("TriggerEmergencyShutdown calls = %d, want 1", got)
	}
	if calls := h.getResetCommittedCalls(); len(calls) != 0 {
		t.Errorf("no committed-offset reset may happen for a rejected event, got %d", len(calls))
	}
	if calls := h.getMarkAssignedCalls(); len(calls) != 0 {
		t.Errorf("no partition may be marked assigned for a rejected event, got %d", len(calls))
	}
	if calls := h.getAckRebalanceCalls(); len(calls) != 0 {
		t.Errorf("a rejected event must not be acked, got %d ack(s)", len(calls))
	}
}

func TestHandleRebalance_ForeignTopicRevoke_RejectsWithoutDraining(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	err := sub.HandleRebalance(nexus.Revoke, []nexus.RebalanceInfo{{
		RebalanceType: nexus.Revoke, TopicName: "other-topic", Partition: 0, CommittedOffset: -1,
	}})

	if err == nil {
		t.Fatal("a foreign-topic revoke must be rejected with an error")
	}
	if got := h.circuitBreaker.shutdownCount.Load(); got != 1 {
		t.Errorf("TriggerEmergencyShutdown calls = %d, want 1", got)
	}
	if got := h.drainCoordinator.drainCalls.Load(); got != 0 {
		t.Errorf("no drain may run for a rejected event, got %d", got)
	}
}

func TestHandleRebalance_MixedTopics_RejectsWholeEvent(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	err := sub.HandleRebalance(nexus.Assign, []nexus.RebalanceInfo{
		{RebalanceType: nexus.Assign, TopicName: "test-topic", Partition: 0, CommittedOffset: 10},
		{RebalanceType: nexus.Assign, TopicName: "other-topic", Partition: 1, CommittedOffset: 20},
	})

	if err == nil {
		t.Fatal("an event mixing topics must be rejected whole, not partially applied")
	}
	if calls := h.getMarkAssignedCalls(); len(calls) != 0 {
		t.Errorf("partial application: %d partition(s) marked assigned", len(calls))
	}
}

// Adapters that do not populate TopicName remain supported: an empty name is
// unspecified, not foreign.
func TestHandleRebalance_EmptyTopicName_IsAccepted(t *testing.T) {
	h := newTestHarness[string]()
	sub := h.createSubscription("test-topic")

	err := sub.HandleRebalance(nexus.Assign, []nexus.RebalanceInfo{{
		RebalanceType: nexus.Assign, Partition: 0, CommittedOffset: 100,
	}})

	if err != nil {
		t.Fatalf("empty TopicName must be accepted, got: %v", err)
	}
	if got := h.circuitBreaker.shutdownCount.Load(); got != 0 {
		t.Errorf("TriggerEmergencyShutdown calls = %d, want 0", got)
	}
	if calls := h.getMarkAssignedCalls(); len(calls) != 1 {
		t.Errorf("the assign should have been processed, marked %d partition(s)", len(calls))
	}
}
