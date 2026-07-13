// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package deadletter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/llingr/llingr-demux/ports"
	"github.com/llingr/llingr-demux/tests/mocklogger"
	"github.com/llingr/llingr-nexus/nexus"
)

func createTestWorkItem() *ports.WorkItem[string] {
	return &ports.WorkItem[string]{
		Message: &nexus.Message[string]{
			Partition: 5,
			Offset:    100,
		},
		Metrics: &nexus.Metrics{
			Partition: 5,
			Offset:    100,
		},
		Ctx: context.Background(),
	}
}

func TestWrite_Success(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()
	originalError := errors.New("processing failed")
	var writeDeadLetterCalled bool
	var receivedMessage *nexus.Message[string]
	var receivedError error

	writeDeadLetter := func(_ context.Context, msg *nexus.Message[string], err error) error {
		writeDeadLetterCalled = true
		receivedMessage = msg
		receivedError = err
		return nil
	}

	dl := New(writeDeadLetter, logger)
	workItem := createTestWorkItem()

	err := dl.Write(workItem, originalError)

	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}

	if !writeDeadLetterCalled {
		t.Error("expected writeDeadLetter to be called")
	}

	if receivedMessage != workItem.Message {
		t.Error("expected writeDeadLetter to receive the work item's message")
	}

	if !errors.Is(receivedError, originalError) {
		t.Errorf("expected writeDeadLetter to receive original error, got: %v", receivedError)
	}

	warns := logger.Warnings()
	if len(warns) != 1 {
		t.Fatalf("expected 1 warning log, got %d", len(warns))
	}

	expectedPrefix := "dead-letter - partition: 5, offset: 100 - caused by error:" //nolint:goconst // test assertion
	if !strings.HasPrefix(warns[0], expectedPrefix) {
		t.Errorf("expected warning to start with %q, got: %q", expectedPrefix, warns[0])
	}

	if !strings.Contains(warns[0], originalError.Error()) {
		t.Errorf("expected warning to contain original error %q, got: %q",
			originalError.Error(), warns[0])
	}

	errorLogs := logger.Errors()
	if len(errorLogs) != 0 {
		t.Errorf("expected no error logs on success, got %d: %v", len(errorLogs), errorLogs)
	}
}

func TestWrite_ReturnsError(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()
	originalError := errors.New("processing failed")
	deadLetterError := errors.New("dead letter write failed")
	var writeDeadLetterCalled bool

	writeDeadLetter := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		writeDeadLetterCalled = true
		return deadLetterError
	}

	dl := New(writeDeadLetter, logger)
	workItem := createTestWorkItem()

	err := dl.Write(workItem, originalError)

	if err == nil {
		t.Fatal("expected error to be returned")
	}

	if !writeDeadLetterCalled {
		t.Error("expected writeDeadLetter to be called")
	}

	warns := logger.Warnings()
	if len(warns) != 1 {
		t.Fatalf("expected 1 warning log, got %d", len(warns))
	}

	expectedPrefix := "dead-letter - partition: 5, offset: 100 - caused by error:" //nolint:goconst // test assertion
	if !strings.HasPrefix(warns[0], expectedPrefix) {
		t.Errorf("expected warning to start with %q, got: %q", expectedPrefix, warns[0])
	}

	expectedErrorPrefix := "dead-letter - partition: 5, offset: 100 - write failed"
	if !strings.HasPrefix(err.Error(), expectedErrorPrefix) {
		t.Errorf("expected error to start with %q, got: %q", expectedErrorPrefix, err.Error())
	}

	if !strings.Contains(err.Error(), deadLetterError.Error()) {
		t.Errorf("expected error to contain dead letter error %q, got: %q",
			deadLetterError.Error(), err.Error())
	}

	errorLogs := logger.Errors()
	if len(errorLogs) != 1 {
		t.Fatalf("expected 1 error log, got %d", len(errorLogs))
	}

	if !strings.HasPrefix(errorLogs[0], expectedErrorPrefix) {
		t.Errorf("expected error log to start with %q, got: %q", expectedErrorPrefix, errorLogs[0])
	}

	if !strings.Contains(errorLogs[0], deadLetterError.Error()) {
		t.Errorf("expected error log to contain dead letter error %q, got: %q",
			deadLetterError.Error(), errorLogs[0])
	}
}

func TestWrite_Panics(t *testing.T) {
	logger := mocklogger.NewRecordingLogger()
	originalError := errors.New("processing failed")
	panicMessage := "something went terribly wrong"
	var writeDeadLetterCalled bool

	writeDeadLetter := func(_ context.Context, _ *nexus.Message[string], _ error) error {
		writeDeadLetterCalled = true
		panic(panicMessage)
	}

	dl := New(writeDeadLetter, logger)
	workItem := createTestWorkItem()

	err := dl.Write(workItem, originalError)

	if err == nil {
		t.Fatal("expected error to be returned after panic")
	}

	if !writeDeadLetterCalled {
		t.Error("expected writeDeadLetter to be called before panic")
	}

	warns := logger.Warnings()
	if len(warns) != 1 {
		t.Fatalf("expected 1 warning log, got %d", len(warns))
	}

	expectedPrefix := "dead-letter - partition: 5, offset: 100 - caused by error:" //nolint:goconst // test assertion
	if !strings.HasPrefix(warns[0], expectedPrefix) {
		t.Errorf("expected warning to start with %q, got: %q", expectedPrefix, warns[0])
	}

	expectedErrorPrefix := "dead-letter - partition: 5, offset: 100 - panic recovered"
	if !strings.HasPrefix(err.Error(), expectedErrorPrefix) {
		t.Errorf("expected error to start with %q, got: %q", expectedErrorPrefix, err.Error())
	}

	if !strings.Contains(err.Error(), panicMessage) {
		t.Errorf("expected error to contain panic message %q, got: %q",
			panicMessage, err.Error())
	}

	errorLogs := logger.Errors()
	if len(errorLogs) != 1 {
		t.Fatalf("expected 1 error log, got %d", len(errorLogs))
	}

	if !strings.HasPrefix(errorLogs[0], expectedErrorPrefix) {
		t.Errorf("expected error log to start with %q, got: %q", expectedErrorPrefix, errorLogs[0])
	}

	if !strings.Contains(errorLogs[0], panicMessage) {
		t.Errorf("expected error log to contain panic message %q, got: %q",
			panicMessage, errorLogs[0])
	}
}

func TestWrite_FormatStrings(t *testing.T) {
	// Verify the format strings used match the constants
	partition := int32(5)
	offset := int64(100)
	testErr := errors.New("test error")
	panicValue := "panic value"

	expectedWriting := fmt.Sprintf(writing, partition, offset, testErr)
	if !strings.Contains(expectedWriting, "dead-letter - partition: 5, offset: 100") {
		t.Errorf("writing format unexpected: %s", expectedWriting)
	}
	if !strings.Contains(expectedWriting, "caused by error:") {
		t.Errorf("writing format should contain 'caused by error:', got: %s", expectedWriting)
	}

	expectedPanicRecovered := fmt.Sprintf(panicRecovered, partition, offset, panicValue)
	if !strings.Contains(expectedPanicRecovered, "dead-letter - partition: 5, offset: 100") {
		t.Errorf("panicRecovered format unexpected: %s", expectedPanicRecovered)
	}
	if !strings.Contains(expectedPanicRecovered, "panic recovered") {
		t.Errorf("panicRecovered format should contain 'panic recovered', got: %s",
			expectedPanicRecovered)
	}

	expectedWriteFailed := fmt.Errorf(writeFailed, partition, offset, testErr)
	if !strings.Contains(expectedWriteFailed.Error(), "dead-letter - partition: 5, offset: 100") {
		t.Errorf("writeFailed format unexpected: %s", expectedWriteFailed.Error())
	}
	if !strings.Contains(expectedWriteFailed.Error(), "write failed") {
		t.Errorf("writeFailed format should contain 'write failed', got: %s",
			expectedWriteFailed.Error())
	}
}
