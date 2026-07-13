// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package hostapp provides test harness components that simulate host application callbacks.
// [DeadLetterCollector] captures dead-letter entries for verification in tests.
package hostapp

import (
	"context"
	"sync"

	"github.com/llingr/llingr-demux/tests/testkit/scenario"
	"github.com/llingr/llingr-nexus/nexus"
)

// DeadLetterCollector collects dead letter messages for verification.
// Thread-safe for concurrent access.
type DeadLetterCollector struct {
	deadLetters []DeadLetterEntry
	mu          sync.Mutex
}

// DeadLetterEntry records a dead letter scenario and its reason.
type DeadLetterEntry struct {
	Message *nexus.Message[scenario.TestMessage]
	Reason  error
}

// NewDeadLetterCollector creates a collector for dead letter messages.
func NewDeadLetterCollector() *DeadLetterCollector {
	return &DeadLetterCollector{
		deadLetters: make([]DeadLetterEntry, 0),
	}
}

// WriteDeadLetter is a nexus.WriteDeadLetter[T] implementation.
func (dlc *DeadLetterCollector) WriteDeadLetter(_ context.Context,
	msg *nexus.Message[scenario.TestMessage], reason error) error {

	dlc.mu.Lock()
	dlc.deadLetters = append(dlc.deadLetters, DeadLetterEntry{
		Message: msg,
		Reason:  reason,
	})
	dlc.mu.Unlock()

	return nil
}

// GetDeadLetters returns a copy of all collected dead letter entries.
func (dlc *DeadLetterCollector) GetDeadLetters() []DeadLetterEntry {
	dlc.mu.Lock()
	defer dlc.mu.Unlock()
	result := make([]DeadLetterEntry, len(dlc.deadLetters))
	copy(result, dlc.deadLetters)
	return result
}

// Count returns the number of dead letters collected.
func (dlc *DeadLetterCollector) Count() int {
	dlc.mu.Lock()
	defer dlc.mu.Unlock()
	return len(dlc.deadLetters)
}
