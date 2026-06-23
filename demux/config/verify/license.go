package verify

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	licenseAGPL  = "The llingr-demux consumer engine is licenced under AGPL-3.0. "
	licenseTerms = licenseAGPL + "For full license terms see: https://github.com/llingr/llingr-demux/blob/main/LICENSE"
	licenseToken = "LLINGR_DEMUX_LICENSE_TOKEN" // for commercial licenses
)

// License check (if present) otherwise
// AGPL as detailed in LICENSE file
func License(now time.Time, keyFn GetKeyFn) (string, Level, error) {
	token := os.Getenv(licenseToken)
	if token == "" {
		return licenseTerms, Debug, nil // see LICENSE file
	}

	claims, err := getClaims(token, keyFn)
	if err != nil {
		return licenseTerms, Debug, err
	}
	if !now.Before(claims.ExpiresAt) {
		const expired = "verify: %q license for %q expired %s"
		return licenseTerms, Info, fmt.Errorf(expired, claims.Type, claims.Subject, claims.ExpiresAt)
	}
	if now.Before(claims.IssuedAt) {
		const notYetValid = "verify: %q license for %q will be valid from %s"
		return licenseTerms, Debug, fmt.Errorf(notYetValid, claims.Type, claims.Subject, claims.IssuedAt)
	}

	const licensed = "[VERIFIED] llingr-demux instance is licensed to %q"
	return fmt.Sprintf(licensed, claims.Subject), Info, nil
}

// split returns the two segments "<non-empty>.<non-empty>" with a single dot.
func split(token string) (payload, sig string, err error) {
	i := strings.IndexByte(token, '.')

	const invalidToken = "verify: token: %q is not <payload>.<signature>"

	if i <= 0 || i == len(token)-1 {
		return "", "", fmt.Errorf(invalidToken, token)
	}
	if strings.IndexByte(token[i+1:], '.') != -1 {
		return "", "", fmt.Errorf(invalidToken, token)
	}
	return token[:i], token[i+1:], nil
}

// Level for logging verification output
type Level int

const (
	Debug Level = iota
	Info
)
