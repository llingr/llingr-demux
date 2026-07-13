// SPDX-FileCopyrightText: Copyright (c) 2026 The llingr-demux Authors
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Llingr-Commercial

package verify

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// Claims are the validated contents of a
// token. Times are UTC, second-precision.
type Claims struct {
	KeyID     string
	Issuer    string
	Subject   string
	Type      string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// maxTokenLen is the size limit for a token (a real one is under ~4.5KB).
const maxTokenLen = 8192

func getClaims(token string, getKey GetKeyFn) (*Claims, error) {
	if len(token) > maxTokenLen {
		const tooLong = "verify: token too long: %d bytes (max %d)"
		return nil, fmt.Errorf(tooLong, len(token), maxTokenLen)
	}

	payload, signature, err := split(token)
	if err != nil {
		return nil, err
	}

	payloadRaw, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		const decodeFailed = "verify: cannot base64-decode payload %q - %v"
		return nil, fmt.Errorf(decodeFailed, payload, err)
	}
	values, err := url.ParseQuery(string(payloadRaw))
	if err != nil {
		const invalidPayload = "verify: token payload %q is not valid url-encoded data - %v"
		return nil, fmt.Errorf(invalidPayload, payload, err)
	}

	keyId := values.Get("kid")
	pub := getKey(keyId)
	if len(pub) != ed25519.PublicKeySize {
		const invalidKey = "verify: invalid kid: %q"
		return nil, fmt.Errorf(invalidKey, keyId)
	}

	sig, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil {
		const malformedSignature = "verify: signature %q is not valid url-encoded data - %w"
		return nil, fmt.Errorf(malformedSignature, signature, err)
	}

	if len(sig) != ed25519.SignatureSize || !ed25519.Verify(pub, []byte(payload), sig) {
		const badSignature = "verify: invalid signature %q"
		return nil, fmt.Errorf(badSignature, signature)
	}

	// Signature is valid: the claims are now trustworthy.
	claims, err := parseClaims(values)
	if err != nil {
		const badClaims = "verify: unable to parse claims - %w"
		return nil, fmt.Errorf(badClaims, err)
	}

	return claims, nil
}

func parseClaims(v url.Values) (*Claims, error) {
	c := &Claims{
		KeyID:   v.Get("kid"),
		Issuer:  v.Get("iss"),
		Subject: v.Get("sub"),
		Type:    v.Get("typ"),
	}

	var err error
	c.IssuedAt, err = tsClaim(v.Get("iat"))
	if err != nil {
		return nil, err
	}
	c.ExpiresAt, err = tsClaim(v.Get("exp"))
	if err != nil {
		return nil, err
	}
	return c, nil
}

func tsClaim(s string) (time.Time, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		const invalidClaim = "verify: invalid ts claim %q - %w"
		return time.Time{}, fmt.Errorf(invalidClaim, s, err)
	}
	return time.Unix(n, 0).UTC(), nil
}
