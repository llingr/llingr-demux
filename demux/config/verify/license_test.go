package verify

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// --- Fixed test key pair (deterministic; NEVER a production key) ---
//
// Derived from a frozen seed so tokens minted in these tests are reproducible
// and no private key has to be stored. This pair is injected through GetKeyFn;
// it is unrelated to the hard-wired key in license_keys.go, which is checksummed
// separately in TestEmbeddedKey_Unchanged.

var testSeed = [ed25519.SeedSize]byte{
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
	0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
	0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20,
}

const (
	testKID = "3176e"
	testISS = "https://llingr.io"
	testSUB = "Acme Investments LLP"
	testTYP = "EVALUATION"
)

func testKey() (ed25519.PrivateKey, ed25519.PublicKey) {
	privateKey := ed25519.NewKeyFromSeed(testSeed[:])
	return privateKey, privateKey.Public().(ed25519.PublicKey)
}

// keyResolverFor returns a GetKeyFn that resolves kid to publicKey and anything
// else to nil, the same shape as the embedded map's GetPublicKey.
func keyResolverFor(kid string, publicKey ed25519.PublicKey) GetKeyFn {
	return func(requestedKID string) ed25519.PublicKey {
		if requestedKID == kid {
			return publicKey
		}
		return nil
	}
}

// signToken serialises and signs a claim set exactly as the wire format
// requires: the signature is over the base64url payload segment.
func signToken(privateKey ed25519.PrivateKey, values url.Values) string {
	payloadSegment := base64.RawURLEncoding.EncodeToString([]byte(values.Encode()))
	signature := ed25519.Sign(privateKey, []byte(payloadSegment))
	return payloadSegment + "." + base64.RawURLEncoding.EncodeToString(signature)
}

// validValues returns a claim set valid in [now-1h, now+60d) for testKID.
func validValues(now time.Time) url.Values {
	values := url.Values{}
	values.Set("kid", testKID)
	values.Set("iss", testISS)
	values.Set("sub", testSUB)
	values.Set("typ", testTYP)
	values.Set("iat", strconv.FormatInt(now.Add(-time.Hour).Unix(), 10))
	values.Set("exp", strconv.FormatInt(now.Add(60*24*time.Hour).Unix(), 10))
	return values
}

// --- License: top-level flow (reads the env token) ---

func TestLicense_Valid(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	t.Setenv(licenseToken, signToken(privateKey, validValues(now)))

	message, err := License(now, keyResolverFor(testKID, publicKey))
	if err != nil {
		t.Fatalf("License: %v", err)
	}
	if !strings.Contains(message, "[VERIFIED]") || !strings.Contains(message, testSUB) {
		t.Errorf("message = %q", message)
	}
}

func TestLicense_NoToken(t *testing.T) {
	_, publicKey := testKey()
	t.Setenv(licenseToken, "")

	message, err := License(time.Now().UTC(), keyResolverFor(testKID, publicKey))
	if err != nil {
		t.Fatalf("no token should not error: %v", err)
	}
	if message != licenseTerms {
		t.Errorf("message = %q, want licenseTerms", message)
	}
}

func TestLicense_Expired(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	values := validValues(now)
	values.Set("iat", strconv.FormatInt(now.Add(-48*time.Hour).Unix(), 10))
	values.Set("exp", strconv.FormatInt(now.Add(-time.Hour).Unix(), 10))
	t.Setenv(licenseToken, signToken(privateKey, values))

	message, err := License(now, keyResolverFor(testKID, publicKey))
	if err == nil {
		t.Fatalf("expired token verified: %q", message)
	}
	if message != licenseTerms {
		t.Errorf("expired message = %q, want licenseTerms", message)
	}
}

func TestLicense_NotYetValid(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	values := validValues(now)
	values.Set("iat", strconv.FormatInt(now.Add(time.Hour).Unix(), 10))
	t.Setenv(licenseToken, signToken(privateKey, values))

	if message, err := License(now, keyResolverFor(testKID, publicKey)); err == nil {
		t.Fatalf("not-yet-valid token verified: %q", message)
	}
}

// A token that fails verification surfaces the error AND returns the AGPL terms
// to log, so the licence notice is never silently dropped.
func TestLicense_BadToken(t *testing.T) {
	_, publicKey := testKey()
	t.Setenv(licenseToken, "this-token-has-no-dot")

	message, err := License(time.Now().UTC(), keyResolverFor(testKID, publicKey))
	if err == nil {
		t.Fatalf("bad token verified: %q", message)
	}
	if message != licenseTerms {
		t.Errorf("bad token message = %q, want licenseTerms", message)
	}
}

// The AGPL terms must be the returned message in EVERY unlicensed situation, so
// the builder's logger.Info(message) (demux_consumer_builder.go) always logs the
// notice. There is one case per distinct error-return path in License /
// getClaims / split / parseClaims, plus the no-token, expired and not-yet-valid
// paths.
func TestLicense_TermsLoggedWhenUnlicensed(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	resolveKey := keyResolverFor(testKID, publicKey)

	// A valid token's payload segment, reused to build signature-side failures.
	validPayloadSeg, _, _ := strings.Cut(signToken(privateKey, validValues(now)), ".")

	expired := validValues(now)
	expired.Set("iat", strconv.FormatInt(now.Add(-48*time.Hour).Unix(), 10))
	expired.Set("exp", strconv.FormatInt(now.Add(-time.Hour).Unix(), 10))

	notYetValid := validValues(now)
	notYetValid.Set("iat", strconv.FormatInt(now.Add(time.Hour).Unix(), 10))

	unknownKid := validValues(now)
	unknownKid.Set("kid", "no-such-kid")

	badIat := validValues(now) // signature valid, reaches parseClaims; iat unparseable
	badIat.Set("iat", "soon")

	missingExp := url.Values{} // signature valid, reaches parseClaims; exp absent
	missingExp.Set("kid", testKID)
	missingExp.Set("iat", "1781604660")

	cases := []struct {
		name  string
		token string
	}{
		{"no-token", ""},                                         // License: token == ""
		{"malformed-no-dot", "no-dot"},                           // split: missing/edge dot
		{"malformed-multiple-dots", "a.b.c"},                     // split: more than one dot
		{"too-long", strings.Repeat("A", maxTokenLen+1)},         // getClaims: size guard
		{"bad-base64-payload", "!!!." + strings.Repeat("A", 86)}, // getClaims: payload decode
		{"bad-query-payload", base64.RawURLEncoding.EncodeToString([]byte("%zz")) + "." + strings.Repeat("A", 86)}, // getClaims: ParseQuery
		{"unknown-kid", signToken(privateKey, unknownKid)},                                                         // getClaims: kid resolves to nil
		{"bad-signature-encoding", validPayloadSeg + ".@@@"},                                                       // getClaims: signature decode
		{"signature-wrong-length", validPayloadSeg + "." + base64.RawURLEncoding.EncodeToString([]byte("short"))},  // getClaims: len != SignatureSize
		{"bad-signature", tamperFirstByte(signToken(privateKey, validValues(now)))},                                // getClaims: ed25519.Verify fails
		{"bad-claims-iat", signToken(privateKey, badIat)},                                                          // parseClaims: iat unparseable
		{"bad-claims-exp", signToken(privateKey, missingExp)},                                                      // parseClaims: exp absent
		{"expired", signToken(privateKey, expired)},                                                                // License: now >= exp
		{"not-yet-valid", signToken(privateKey, notYetValid)},                                                      // License: now < iat
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv(licenseToken, c.token)
			message, _ := License(now, resolveKey)
			if message != licenseTerms {
				t.Errorf("message = %q, want licenseTerms", message)
			}
		})
	}
}

// tamperFirstByte flips the first byte so the signature no longer matches while
// the token stays structurally valid.
func tamperFirstByte(token string) string {
	b := []byte(token)
	if b[0] == 'A' {
		b[0] = 'B'
	} else {
		b[0] = 'A'
	}
	return string(b)
}

// Subject full of metacharacters must round-trip through verification intact.
func TestLicense_SpecialSubject(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	const subject = "Smith & Jones = Partners, Ltd ☎ Größe 100%+"
	values := validValues(now)
	values.Set("sub", subject)
	t.Setenv(licenseToken, signToken(privateKey, values))

	message, err := License(now, keyResolverFor(testKID, publicKey))
	if err != nil {
		t.Fatalf("License: %v", err)
	}
	if !strings.Contains(message, subject) {
		t.Errorf("subject not round-tripped: %q", message)
	}
}

// --- getClaims: signature / structural / claim errors ---

func TestGetClaims_Valid(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	claims, err := getClaims(signToken(privateKey, validValues(now)), keyResolverFor(testKID, publicKey))
	if err != nil {
		t.Fatalf("getClaims: %v", err)
	}
	if claims.KeyID != testKID || claims.Issuer != testISS || claims.Subject != testSUB || claims.Type != testTYP {
		t.Errorf("claims = %+v", claims)
	}
}

func TestGetClaims_Malformed(t *testing.T) {
	_, publicKey := testKey()
	resolveKey := keyResolverFor(testKID, publicKey)
	for _, token := range []string{"", "nodot", ".leading", "trailing.", "a.b.c", "."} {
		if _, err := getClaims(token, resolveKey); err == nil {
			t.Errorf("getClaims(%q): expected error", token)
		}
	}
}

func TestGetClaims_BadBase64Payload(t *testing.T) {
	_, publicKey := testKey()
	// "!!!" is not base64url; the signature segment shape is irrelevant, decode fails first.
	if _, err := getClaims("!!!."+strings.Repeat("A", 86), keyResolverFor(testKID, publicKey)); err == nil {
		t.Error("expected error for non-base64 payload")
	}
}

func TestGetClaims_BadQueryPayload(t *testing.T) {
	_, publicKey := testKey()
	// Decodes cleanly but is not valid url-encoded data (bad % escape).
	payloadSegment := base64.RawURLEncoding.EncodeToString([]byte("%zz"))
	if _, err := getClaims(payloadSegment+"."+strings.Repeat("A", 86), keyResolverFor(testKID, publicKey)); err == nil {
		t.Error("expected error for invalid url-encoded payload")
	}
}

func TestGetClaims_UnknownKid(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	token := signToken(privateKey, validValues(now))
	// The resolver only knows a different kid.
	if _, err := getClaims(token, keyResolverFor("deadbeef", publicKey)); err == nil {
		t.Error("expected error for unknown kid")
	}
}

func TestGetClaims_BadSignatureEncoding(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	payloadSegment, _, _ := strings.Cut(signToken(privateKey, validValues(now)), ".")
	if _, err := getClaims(payloadSegment+".@@@", keyResolverFor(testKID, publicKey)); err == nil {
		t.Error("expected error for non-base64 signature")
	}
}

func TestGetClaims_SignatureWrongLength(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	payloadSegment, _, _ := strings.Cut(signToken(privateKey, validValues(now)), ".")
	shortSignature := base64.RawURLEncoding.EncodeToString([]byte("too short"))
	if _, err := getClaims(payloadSegment+"."+shortSignature, keyResolverFor(testKID, publicKey)); err == nil {
		t.Error("expected error for wrong-length signature")
	}
}

func TestGetClaims_TamperedPayload(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	token := signToken(privateKey, validValues(now))

	// Flip the first payload byte: still base64url, still decodes, but the
	// signature no longer matches.
	corrupted := []byte(token)
	if corrupted[0] == 'A' {
		corrupted[0] = 'B'
	} else {
		corrupted[0] = 'A'
	}
	if _, err := getClaims(string(corrupted), keyResolverFor(testKID, publicKey)); err == nil {
		t.Error("tampered payload accepted")
	}
}

// A token over the size guard is rejected before any decode/parse work.
func TestGetClaims_TooLong(t *testing.T) {
	_, publicKey := testKey()
	oversized := strings.Repeat("A", maxTokenLen+1)
	if _, err := getClaims(oversized, keyResolverFor(testKID, publicKey)); err == nil {
		t.Fatal("oversized token accepted")
	}
}

// A legitimate worst case (256-rune non-ASCII subject) stays under the guard and
// still verifies: the cap must not clip reality.
func TestGetClaims_MaxRealisticSubject(t *testing.T) {
	privateKey, publicKey := testKey()
	now := time.Now().UTC()
	values := validValues(now)
	values.Set("sub", strings.Repeat("é", 256)) // 256 runes, 512 bytes
	token := signToken(privateKey, values)

	if len(token) > maxTokenLen {
		t.Fatalf("worst-case token is %d bytes, exceeds guard %d", len(token), maxTokenLen)
	}
	claims, err := getClaims(token, keyResolverFor(testKID, publicKey))
	if err != nil {
		t.Fatalf("worst-case token rejected: %v", err)
	}
	if claims.Subject != strings.Repeat("é", 256) {
		t.Error("subject did not round-trip")
	}
}

// --- Claim parsing ---

func TestGetClaims_BadClaims(t *testing.T) {
	privateKey, publicKey := testKey()
	resolveKey := keyResolverFor(testKID, publicKey)
	validClaims := func() url.Values { // a fully valid set we then break
		values := url.Values{}
		values.Set("kid", testKID)
		values.Set("iat", "1781604660")
		values.Set("exp", "1786788600")
		return values
	}
	cases := []struct {
		name    string
		corrupt func(url.Values)
	}{
		{"missing-iat", func(values url.Values) { values.Del("iat") }},
		{"missing-exp", func(values url.Values) { values.Del("exp") }},
		{"non-numeric-iat", func(values url.Values) { values.Set("iat", "soon") }},
		{"non-numeric-exp", func(values url.Values) { values.Set("exp", "later") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			values := validClaims()
			tc.corrupt(values)
			// Sign so the signature passes and we reach claim parsing.
			if _, err := getClaims(signToken(privateKey, values), resolveKey); err == nil {
				t.Errorf("%s: expected error", tc.name)
			}
		})
	}
}

func TestTsClaim(t *testing.T) {
	got, err := tsClaim("1781604660")
	if err != nil {
		t.Fatalf("tsClaim: %v", err)
	}
	if want := time.Unix(1781604660, 0).UTC(); !got.Equal(want) {
		t.Errorf("tsClaim = %v, want %v", got, want)
	}
	if _, err := tsClaim(""); err == nil {
		t.Error("tsClaim(empty): expected error")
	}
	if _, err := tsClaim("notanumber"); err == nil {
		t.Error("tsClaim(garbage): expected error")
	}
}

// --- The hard-wired production key ---

// embeddedKID is the kid committed in license_keys.go (the production trust map).
const embeddedKID = "fd57299352df11ba"

func TestGetPublicKey(t *testing.T) {
	if publicKey := GetPublicKey(embeddedKID); len(publicKey) != ed25519.PublicKeySize {
		t.Errorf("GetPublicKey(%q): len %d, want %d", embeddedKID, len(publicKey), ed25519.PublicKeySize)
	}
	if publicKey := GetPublicKey("no-such-kid"); publicKey != nil {
		t.Errorf("GetPublicKey(unknown) = %v, want nil", publicKey)
	}
}

// Tripwire: the committed public key must not change silently. If a key is
// rotated on purpose, update want to the new checksum (printed in the failure);
// an unexpected change means the trust anchor was altered.
func TestEmbeddedKey_Unchanged(t *testing.T) {
	const want = "a97cc04d699d232e913172a31216e9a0bec9d9c030e74d3001cc6e7e7e7abfd0"

	publicKey := GetPublicKey(embeddedKID)
	if len(publicKey) != ed25519.PublicKeySize {
		t.Fatalf("embedded key %q: len %d, want %d", embeddedKID, len(publicKey), ed25519.PublicKeySize)
	}
	checksum := sha256.Sum256(publicKey)
	if got := hex.EncodeToString(checksum[:]); got != want {
		t.Fatalf("embedded key %q changed: sha256=%s\n(update want only if this rotation is intentional)", embeddedKID, got)
	}
}

// FuzzGetClaims: arbitrary bytes must never panic; (claims, err) stays consistent.
func FuzzGetClaims(f *testing.F) {
	privateKey, publicKey := testKey()
	resolveKey := keyResolverFor(testKID, publicKey)
	now := time.Now().UTC()
	for _, seed := range []string{"", ".", "a.b", "a.b.c", signToken(privateKey, validValues(now)), "%zz." + strings.Repeat("A", 86)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, token string) {
		claims, err := getClaims(token, resolveKey)
		if (err == nil) == (claims == nil) {
			t.Fatalf("inconsistent result for %q: claims=%v err=%v", token, claims, err)
		}
	})
}
