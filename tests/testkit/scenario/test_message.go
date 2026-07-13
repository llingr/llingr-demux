// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package scenario

import (
	"fmt"
	"strings"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// TestMessage is a self-describing test scenario that carries its own behaviour controls
// and expected outcomes. The test harness callbacks read the behaviour flags during
// processing, and verification walks the messages checking expectations.
type TestMessage struct {
	// identity
	ID int // monotonically increasing from the generator

	Key       string
	Partition int32
	Offset    int64 // for the partition

	// legacy, to delete
	Data string

	Tag        string    // e.g. should cause DL panic and circuit-breaker to trigger
	TimePolled time.Time // from extract envelope

	// control what happens during processing
	BrokerPollDelay time.Duration
	BrokerPollError error

	ProcessingDelay time.Duration // simulate slow processing
	DeadLetterDelay time.Duration // simulate time to commit a dead letter
	FailProcessing  bool          // ProcessMessage returns error
	PanicProcessing bool          // ProcessMessage panics
	FailDeadLetter  bool          // WriteDeadLetter returns error
	PanicDeadLetter bool          // DeadLetter panics

	// assertions - what should happen
	ExpectCommit       bool // should this offset be committed
	ExpectDeadLetter   bool // should this end up in dead letter
	ExpectCircuitBreak bool // should this trigger circuit breaker

	ExpectTraits nexus.Traits // expected traits on metrics
}

// String returns a human-readable summary of the TestMessage.
func (m *TestMessage) String() string {
	var sb strings.Builder

	// identity line
	if m.Tag != "" {
		fmt.Fprintf(&sb, "%q [p:%d o:%d key:%q]", m.Tag, m.Partition, m.Offset, m.Key)
	} else {
		fmt.Fprintf(&sb, "[p:%d o:%d key:%q]", m.Partition, m.Offset, m.Key)
	}

	// behaviours (only show non-defaults)
	var behaviours []string
	if m.ProcessingDelay > 0 {
		behaviours = append(behaviours, fmt.Sprintf("delay:%v", m.ProcessingDelay))
	}
	if m.DeadLetterDelay > 0 {
		behaviours = append(behaviours, fmt.Sprintf("dlDelay:%v", m.DeadLetterDelay))
	}
	if m.FailProcessing {
		behaviours = append(behaviours, "failProc")
	}
	if m.FailDeadLetter {
		behaviours = append(behaviours, "failDL")
	}
	if m.PanicProcessing {
		behaviours = append(behaviours, "panicProc")
	}
	if m.PanicDeadLetter {
		behaviours = append(behaviours, "panicDL")
	}
	if len(behaviours) > 0 {
		sb.WriteString(" behave:{")
		sb.WriteString(strings.Join(behaviours, ", "))
		sb.WriteString("}")
	}

	// expectations (only show non-defaults)
	var expects []string
	if m.ExpectCommit {
		expects = append(expects, "commit")
	}
	if m.ExpectDeadLetter {
		expects = append(expects, "deadLetter")
	}
	if m.ExpectCircuitBreak {
		expects = append(expects, "circuitBreak")
	}
	if m.ExpectTraits != 0 {
		expects = append(expects, fmt.Sprintf("traits:0x%x", m.ExpectTraits))
	}
	if len(expects) > 0 {
		sb.WriteString(" expect:{")
		sb.WriteString(strings.Join(expects, ", "))
		sb.WriteString("}")
	}

	return sb.String()
}
