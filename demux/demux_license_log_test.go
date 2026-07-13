// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package demux

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/llingr/llingr-demux/tests/mocklogger"
)

// These pin Build's licence logging: AGPL notice at Debug (token or not),
// invalid token also a Warn, valid token VERIFIED at Info.

const licenseEnvVar = "LLINGR_DEMUX_LICENSE_TOKEN"

// signLicenseToken builds and signs a token in the wire format (signature over
// the base64url payload segment), so a test can mint without the mint package.
func signLicenseToken(t *testing.T, privateKey ed25519.PrivateKey, values url.Values) string {
	t.Helper()
	segment := base64.RawURLEncoding.EncodeToString([]byte(values.Encode()))
	signature := ed25519.Sign(privateKey, []byte(segment))
	return segment + "." + base64.RawURLEncoding.EncodeToString(signature)
}

func TestBuild_LogsLicenseTerms_WhenNoToken(t *testing.T) {
	t.Setenv(licenseEnvVar, "")
	logger := mocklogger.NewRecordingLogger()

	NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithLogger(logger).
		Build(&builderTestBroker{})

	if !logger.ContainsDebug("licenced under AGPL-3.0") {
		t.Errorf("AGPL terms not logged at Debug; debugs=%v", logger.Debugs())
	}
	if logger.WarnCount() != 0 {
		t.Errorf("no token should not log a warning: %v", logger.Warnings())
	}
}

func TestBuild_LogsWarnAndTerms_WhenInvalidToken(t *testing.T) {
	t.Setenv(licenseEnvVar, "this-is-not-a-valid-token")
	logger := mocklogger.NewRecordingLogger()

	NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).
		WithLogger(logger).
		Build(&builderTestBroker{})

	if logger.WarnCount() == 0 {
		t.Error("invalid token should log a warning")
	}
	if !logger.ContainsDebug("licenced under AGPL-3.0") {
		t.Errorf("AGPL terms not logged at Debug even on invalid token; debugs=%v", logger.Debugs())
	}
}

func TestBuild_LogsVerified_WhenValidToken(t *testing.T) {
	const kid = "test-kid"
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	values := url.Values{}
	values.Set("kid", kid)
	values.Set("iss", "https://llingr.io")
	values.Set("sub", "Acme Investments LLP")
	values.Set("typ", "EVALUATION")
	values.Set("iat", strconv.FormatInt(now.Add(-time.Hour).Unix(), 10))
	values.Set("exp", strconv.FormatInt(now.Add(60*24*time.Hour).Unix(), 10))
	t.Setenv(licenseEnvVar, signLicenseToken(t, privateKey, values))

	logger := mocklogger.NewRecordingLogger()
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).WithLogger(logger)
	// Inject the matching public key so the embedded production map isn't needed.
	builder.licenseKeyFn = func(requestedKID string) ed25519.PublicKey {
		if requestedKID == kid {
			return publicKey
		}
		return nil
	}
	builder.Build(&builderTestBroker{})

	if !logger.ContainsInfo("[VERIFIED]") {
		t.Errorf("verified message not logged at Info; infos=%v", logger.Infos())
	}
	if !logger.ContainsInfo("Acme Investments LLP") {
		t.Errorf("licensee not logged in verified message; infos=%v", logger.Infos())
	}
	if logger.WarnCount() != 0 {
		t.Errorf("valid token should not log a warning: %v", logger.Warnings())
	}
}

// A panic in the licence check must not take down Build.
func TestBuild_RecoversFromLicenseKeyPanic(t *testing.T) {
	// Well-formed token so the key lookup (which panics) is reached.
	values := url.Values{}
	values.Set("kid", "boom")
	payload := base64.RawURLEncoding.EncodeToString([]byte(values.Encode()))
	token := payload + "." + base64.RawURLEncoding.EncodeToString([]byte("ignored"))
	t.Setenv(licenseEnvVar, token)

	logger := mocklogger.NewRecordingLogger()
	builder := NewBuilder[string]("test-topic", noopProcess, noopDeadLetter).WithLogger(logger)
	builder.licenseKeyFn = func(string) ed25519.PublicKey {
		panic("simulated panic in licence key lookup")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Build must not panic when the licence check panics; got: %v", r)
		}
	}()

	consumer := builder.Build(&builderTestBroker{})
	if consumer == nil {
		t.Fatal("Build must still return a consumer when the licence check panics")
	}
	if !logger.ContainsWarning("licence check failed") {
		t.Errorf("a recovered panic should be logged at Warn; warnings=%v", logger.Warnings())
	}
}
