// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

// Package mocklogger provides test implementations of nexus.Logger.
package mocklogger

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/llingr/llingr-nexus/nexus"
)

// Level represents a log level.
type Level string

// Log level constants.
const (
	LevelError Level = "ERROR"
	LevelWarn  Level = "WARN"
	LevelInfo  Level = "INFO"
	LevelDebug Level = "DEBUG"
)

// LogEntry represents a captured log message.
type LogEntry struct {
	Level   Level
	Message string
}

// NoOpLogger implements nexus.Logger and discards all output.
// Use this for tests that don't need to verify logging behavior.
type NoOpLogger struct{}

var _ nexus.Logger = (*NoOpLogger)(nil)

// NewNoOpLogger creates a new NoOpLogger.
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

// Error discards the message.
func (NoOpLogger) Error(_ context.Context, _ string, _ ...any) {}

// Warn discards the message.
func (NoOpLogger) Warn(_ context.Context, _ string, _ ...any) {}

// Info discards the message.
func (NoOpLogger) Info(_ context.Context, _ string, _ ...any) {}

// Debug discards the message.
func (NoOpLogger) Debug(_ context.Context, _ string, _ ...any) {}

// RecordingLogger implements nexus.Logger and captures all log output.
// Use this for tests that need to verify logging behavior.
type RecordingLogger struct {
	mu       sync.Mutex
	messages map[Level][]string
}

var _ nexus.Logger = (*RecordingLogger)(nil)

// NewRecordingLogger creates a new RecordingLogger.
func NewRecordingLogger() *RecordingLogger {
	return &RecordingLogger{
		messages: make(map[Level][]string),
	}
}

func (l *RecordingLogger) log(level Level, format string, args ...any) {
	l.mu.Lock()
	l.messages[level] = append(l.messages[level], fmt.Sprintf(format, args...))
	l.mu.Unlock()
}

func (l *RecordingLogger) Error(_ context.Context, format string, args ...any) {
	l.log(LevelError, format, args...)
}

// Warn records the message at warn level.
func (l *RecordingLogger) Warn(_ context.Context, format string, args ...any) {
	l.log(LevelWarn, format, args...)
}

// Info records the message at info level.
func (l *RecordingLogger) Info(_ context.Context, format string, args ...any) {
	l.log(LevelInfo, format, args...)
}

// Debug records the message at debug level.
func (l *RecordingLogger) Debug(_ context.Context, format string, args ...any) {
	l.log(LevelDebug, format, args...)
}

// ErrorCount returns the number of Error calls.
func (l *RecordingLogger) ErrorCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages[LevelError])
}

// WarnCount returns the number of Warn calls.
func (l *RecordingLogger) WarnCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages[LevelWarn])
}

// InfoCount returns the number of Info calls.
func (l *RecordingLogger) InfoCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages[LevelInfo])
}

// DebugCount returns the number of Debug calls.
func (l *RecordingLogger) DebugCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages[LevelDebug])
}

// Messages returns a copy of all captured log entries.
// Note: entries are grouped by level, not in chronological order across levels.
func (l *RecordingLogger) Messages() []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	total := 0
	for _, msgs := range l.messages {
		total += len(msgs)
	}
	result := make([]LogEntry, 0, total)
	for level, msgs := range l.messages {
		for _, msg := range msgs {
			result = append(result, LogEntry{Level: level, Message: msg})
		}
	}
	return result
}

// Errors returns a copy of all error messages.
func (l *RecordingLogger) Errors() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return copySlice(l.messages[LevelError])
}

// Warnings returns a copy of all warning messages.
func (l *RecordingLogger) Warnings() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return copySlice(l.messages[LevelWarn])
}

// Infos returns a copy of all info messages.
func (l *RecordingLogger) Infos() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return copySlice(l.messages[LevelInfo])
}

// Debugs returns a copy of all debug messages.
func (l *RecordingLogger) Debugs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return copySlice(l.messages[LevelDebug])
}

func copySlice(s []string) []string {
	if s == nil {
		return nil
	}
	result := make([]string, len(s))
	copy(result, s)
	return result
}

// Reset clears all captured messages.
func (l *RecordingLogger) Reset() {
	l.mu.Lock()
	l.messages = make(map[Level][]string)
	l.mu.Unlock()
}

// HasErrors returns true if any errors were logged.
func (l *RecordingLogger) HasErrors() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages[LevelError]) > 0
}

// HasNoErrors returns true if no errors were logged.
func (l *RecordingLogger) HasNoErrors() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.messages[LevelError]) == 0
}

// HasError returns true if any error message exactly matches the given string.
func (l *RecordingLogger) HasError(exact string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return hasExact(l.messages[LevelError], exact)
}

// HasWarning returns true if any warning message exactly matches the given string.
func (l *RecordingLogger) HasWarning(exact string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return hasExact(l.messages[LevelWarn], exact)
}

// HasInfo returns true if any info message exactly matches the given string.
func (l *RecordingLogger) HasInfo(exact string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return hasExact(l.messages[LevelInfo], exact)
}

// HasDebug returns true if any debug message exactly matches the given string.
func (l *RecordingLogger) HasDebug(exact string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return hasExact(l.messages[LevelDebug], exact)
}

func hasExact(messages []string, exact string) bool {
	for _, msg := range messages {
		if msg == exact {
			return true
		}
	}
	return false
}

// ContainsError returns true if any error message contains the given substring.
func (l *RecordingLogger) ContainsError(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return containsSubstr(l.messages[LevelError], substr)
}

// ContainsWarning returns true if any warning message contains the given substring.
func (l *RecordingLogger) ContainsWarning(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return containsSubstr(l.messages[LevelWarn], substr)
}

// ContainsInfo returns true if any info message contains the given substring.
func (l *RecordingLogger) ContainsInfo(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return containsSubstr(l.messages[LevelInfo], substr)
}

// ContainsDebug returns true if any debug message contains the given substring.
func (l *RecordingLogger) ContainsDebug(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return containsSubstr(l.messages[LevelDebug], substr)
}

func containsSubstr(messages []string, substr string) bool {
	for _, msg := range messages {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}
