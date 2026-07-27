// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package tests

import (
	"context"
	"testing"

	"github.com/llingr/llingr-nexus/nexus"
)

// shutdownCallbackRegistrar is the one capability failOnEmergency needs;
// satisfied by *demux.Consumer[T] for any T.
type shutdownCallbackRegistrar interface {
	RegisterShutdownCallback(nexus.ShutdownCallback)
}

// failOnEmergency replaces the default emergency callback, which sleeps 15s
// and then interrupts the whole process: one unexpected trip in a consumer
// with no registered callback kills the test binary and every test queued
// behind it, with the reason swallowed by the recording logger. Registered
// here instead, an unexpected trip fails the owning test and names the
// reason. Tests that expect a trip register their own capture.
func failOnEmergency(t *testing.T, consumer any) {
	t.Helper()
	registrar, ok := consumer.(shutdownCallbackRegistrar)
	if !ok {
		t.Fatalf("consumer %T does not expose RegisterShutdownCallback", consumer)
	}
	registrar.RegisterShutdownCallback(func(_ context.Context, reason error) {
		if reason != nil {
			t.Errorf("unexpected emergency shutdown: %v", reason)
		}
	})
}
