// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0

package mocklogger

import (
	"context"
	"sync"
	"testing"
)

func TestNoOpLogger(_ *testing.T) {
	var logger NoOpLogger
	ctx := context.Background()

	// Should not panic
	logger.Error(ctx, "error %s", "test")
	logger.Warn(ctx, "warn %s", "test")
	logger.Info(ctx, "info %s", "test")
	logger.Debug(ctx, "debug %s", "test")
}

func TestRecordingLogger(t *testing.T) {
	logger := NewRecordingLogger()
	ctx := context.Background()

	logger.Error(ctx, "error %d", 1)
	logger.Warn(ctx, "warn %d", 2)
	logger.Info(ctx, "info %d", 3)
	logger.Debug(ctx, "debug %d", 4)

	if logger.ErrorCount() != 1 {
		t.Errorf("expected 1 error, got %d", logger.ErrorCount())
	}
	if logger.WarnCount() != 1 {
		t.Errorf("expected 1 warn, got %d", logger.WarnCount())
	}
	if logger.InfoCount() != 1 {
		t.Errorf("expected 1 info, got %d", logger.InfoCount())
	}
	if logger.DebugCount() != 1 {
		t.Errorf("expected 1 debug, got %d", logger.DebugCount())
	}

	messages := logger.Messages()
	if len(messages) != 4 {
		t.Errorf("expected 4 messages, got %d", len(messages))
	}

	errors := logger.Errors()
	if len(errors) != 1 || errors[0] != "error 1" {
		t.Errorf("unexpected errors: %v", errors)
	}

	warnings := logger.Warnings()
	if len(warnings) != 1 || warnings[0] != "warn 2" {
		t.Errorf("unexpected warnings: %v", warnings)
	}
}

func TestRecordingLoggerReset(t *testing.T) {
	logger := NewRecordingLogger()
	ctx := context.Background()

	logger.Error(ctx, "error")
	logger.Warn(ctx, "warn")

	if logger.ErrorCount() != 1 {
		t.Errorf("expected 1 error before reset, got %d", logger.ErrorCount())
	}

	logger.Reset()

	if logger.ErrorCount() != 0 {
		t.Errorf("expected 0 errors after reset, got %d", logger.ErrorCount())
	}
	if logger.WarnCount() != 0 {
		t.Errorf("expected 0 warns after reset, got %d", logger.WarnCount())
	}
	if len(logger.Messages()) != 0 {
		t.Errorf("expected 0 messages after reset, got %d", len(logger.Messages()))
	}
}

func TestRecordingLoggerConcurrency(t *testing.T) {
	logger := NewRecordingLogger()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(4)
		go func() { defer wg.Done(); logger.Error(ctx, "e") }()
		go func() { defer wg.Done(); logger.Warn(ctx, "w") }()
		go func() { defer wg.Done(); logger.Info(ctx, "i") }()
		go func() { defer wg.Done(); logger.Debug(ctx, "d") }()
	}
	wg.Wait()

	if logger.ErrorCount() != 100 {
		t.Errorf("expected 100 errors, got %d", logger.ErrorCount())
	}
	if logger.WarnCount() != 100 {
		t.Errorf("expected 100 warns, got %d", logger.WarnCount())
	}
	if len(logger.Messages()) != 400 {
		t.Errorf("expected 400 messages, got %d", len(logger.Messages()))
	}
}
