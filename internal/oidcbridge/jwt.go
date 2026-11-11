package oidcbridge

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
)

var safeActorSegmentPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9._-]{0,61}[a-z0-9])?$`)

type trustedKey struct {
	kid       string
	alg       string
	publicKey crypto.PublicKey
}

type jwtHeader struct {
	Typ string `json:"typ"`
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

type jwtRawClaims struct {
	Issuer         string          `json:"iss"`
	Subject        string          `json:"sub"`
	Audiences      json.RawMessage `json:"aud"`
	Expiration     *float64        `json:"exp"`
	NotBefore      *float64        `json:"nbf"`
	Actor          json.RawMessage `json:"actor.ate.dev,omitempty"`
	WorkerForActor json.RawMessage `json:"worker-for-actor.ate.dev,omitempty"`
}

type substrateActorClaim struct {
	Atespace  string `json:"atespace"`
	ActorName string `json:"actorName"`
	ActorUID  string `json:"actorUID"`
}

func (s *Server) verifyActorJWT(ctx context.Context, rawToken string) (actorIdentity, int, error) {
	parts := strings.Split(rawToken, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("malformed actor JWT")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("invalid JWT header encoding")
	}
	var hdr jwtHeader
	if err := json.Unmarshal(headerBytes, &hdr); err != nil {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("invalid JWT header JSON")
	}
	if hdr.Typ != "" && hdr.Typ != "JWT" {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("unsupported JWT typ header")
	}
	if hdr.Alg != "ES256" && hdr.Alg != "RS256" {
		return actorIdentity{}, http.StatusUnauthorized, fmt.Errorf("unsupported JWT alg %q", hdr.Alg)
	}
	if hdr.Kid == "" || len(hdr.Kid) > 256 {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("JWT kid header is required")
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("invalid JWT payload encoding")
	}
	var claims jwtRawClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("invalid JWT payload JSON")
	}
	if claims.Issuer != s.issuerURL {
		return actorIdentity{}, http.StatusUnauthorized, fmt.Errorf("unexpected JWT issuer %q", claims.Issuer)
	}

	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sigBytes) == 0 {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("invalid JWT signature encoding")
	}

	// Reject wrong-audience, expired, or not-yet-valid tokens before touching
	// the JWKS cache so invalid tokens with random kids cannot consume the
	// unknown-kid refresh rate-limit budget.
	now := s.now()
	audiences, err := parseAudiences(claims.Audiences)
	if err != nil {
		return actorIdentity{}, http.StatusUnauthorized, err
	}
	if !slices.ContainsFunc(s.audiences, func(expected string) bool {
		return slices.Contains(audiences, expected)
	}) {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("actor JWT audience is not accepted")
	}

	if claims.Expiration == nil || math.IsNaN(*claims.Expiration) || math.IsInf(*claims.Expiration, 0) {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("actor JWT exp claim is required")
	}
	expTime := time.Unix(int64(*claims.Expiration), 0)
	if !now.Add(-s.clockSkew).Before(expTime) {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("actor JWT has expired")
	}

	if claims.NotBefore == nil || math.IsNaN(*claims.NotBefore) || math.IsInf(*claims.NotBefore, 0) {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("actor JWT nbf claim is required")
	}
	nbfTime := time.Unix(int64(*claims.NotBefore), 0)
	if now.Add(s.clockSkew).Before(nbfTime) {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("actor JWT is not valid yet")
	}
	if claims.Subject == "" {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("actor JWT sub claim is required")
	}

	key, err := s.lookupTrustedKey(ctx, hdr.Kid, now)
	if err != nil {
		return actorIdentity{}, http.StatusUnauthorized, err
	}
	if hdr.Alg != key.alg {
		return actorIdentity{}, http.StatusUnauthorized, fmt.Errorf("JWT alg %q does not match trusted JWK algorithm %q", hdr.Alg, key.alg)
	}

	if err := verifyJWS(key, []byte(parts[0]+"."+parts[1]), sigBytes); err != nil {
		return actorIdentity{}, http.StatusUnauthorized, err
	}

	if len(claims.WorkerForActor) > 0 && string(claims.WorkerForActor) != "null" {
		return actorIdentity{}, http.StatusForbidden, errors.New("worker-for-actor tokens are forbidden")
	}
	if strings.HasPrefix(claims.Subject, "worker-for-actor/") {
		return actorIdentity{}, http.StatusForbidden, errors.New("worker-for-actor subject is forbidden")
	}
	if !strings.HasPrefix(claims.Subject, "actor/") {
		return actorIdentity{}, http.StatusForbidden, errors.New("JWT subject must begin with actor/")
	}

	atespace, actorName, ok := strings.Cut(strings.TrimPrefix(claims.Subject, "actor/"), "/")
	if !ok || strings.Contains(actorName, "/") || !safeActorSegmentPattern.MatchString(atespace) || !safeActorSegmentPattern.MatchString(actorName) {
		return actorIdentity{}, http.StatusUnauthorized, errors.New("malformed actor/<atespace>/<actor> subject")
	}

	if len(claims.Actor) > 0 && string(claims.Actor) != "null" {
		var subClaim substrateActorClaim
		if err := json.Unmarshal(claims.Actor, &subClaim); err != nil {
			return actorIdentity{}, http.StatusUnauthorized, errors.New("malformed actor.ate.dev claim")
		}
		if (subClaim.Atespace != "" && subClaim.Atespace != atespace) || (subClaim.ActorName != "" && subClaim.ActorName != actorName) {
			return actorIdentity{}, http.StatusUnauthorized, errors.New("actor.ate.dev claim does not match sub")
		}
	}

	return actorIdentity{
		atespace:  atespace,
		actorName: actorName,
	}, http.StatusOK, nil
}

func parseTrustedJWK(entry jwkEntry) (trustedKey, error) {
	if entry.Kid == "" {
		return trustedKey{}, errors.New("missing kid")
	}
	switch entry.Kty {
	case "EC":
		if entry.Crv != "P-256" {
			return trustedKey{}, fmt.Errorf("unsupported EC curve %q", entry.Crv)
		}
		if entry.Alg != "" && entry.Alg != "ES256" {
			return trustedKey{}, fmt.Errorf("unsupported EC alg %q", entry.Alg)
		}
		xBytes, err := base64.RawURLEncoding.DecodeString(entry.X)
		if err != nil || len(xBytes) == 0 || len(xBytes) > 32 {
			return trustedKey{}, errors.New("invalid EC x coordinate")
		}
		yBytes, err := base64.RawURLEncoding.DecodeString(entry.Y)
		if err != nil || len(yBytes) == 0 || len(yBytes) > 32 {
			return trustedKey{}, errors.New("invalid EC y coordinate")
		}
		uncompressed := make([]byte, 65)
		uncompressed[0] = 0x04
		copy(uncompressed[1+(32-len(xBytes)):33], xBytes)
		copy(uncompressed[33+(32-len(yBytes)):65], yBytes)
		pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), uncompressed)
		if err != nil {
			return trustedKey{}, err
		}
		return trustedKey{kid: entry.Kid, alg: "ES256", publicKey: pub}, nil

	case "RSA":
		if entry.Alg != "" && entry.Alg != "RS256" {
			return trustedKey{}, fmt.Errorf("unsupported RSA alg %q", entry.Alg)
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(entry.N)
		if err != nil || len(nBytes) == 0 {
			return trustedKey{}, errors.New("invalid RSA modulus n")
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(entry.E)
		if err != nil || len(eBytes) == 0 {
			return trustedKey{}, errors.New("invalid RSA exponent e")
		}
		n := new(big.Int).SetBytes(nBytes)
		if n.BitLen() < 2048 {
			return trustedKey{}, errors.New("RSA modulus smaller than 2048 bits")
		}
		e := new(big.Int).SetBytes(eBytes)
		if !e.IsInt64() || e.Int64() < 3 || e.Bit(0) == 0 || int64(int(e.Int64())) != e.Int64() {
			return trustedKey{}, errors.New("invalid RSA public exponent")
		}
		return trustedKey{
			kid: entry.Kid,
			alg: "RS256",
			publicKey: &rsa.PublicKey{
				N: n,
				E: int(e.Int64()),
			},
		}, nil

	default:
		return trustedKey{}, fmt.Errorf("unsupported JWK kty %q", entry.Kty)
	}
}

func verifyJWS(key trustedKey, signingInput, sigBytes []byte) error {
	digest := sha256.Sum256(signingInput)
	switch key.alg {
	case "ES256":
		ecKey, ok := key.publicKey.(*ecdsa.PublicKey)
		if !ok || ecKey.Curve != elliptic.P256() {
			return errors.New("JWK key is not an ECDSA P-256 public key")
		}
		if len(sigBytes) != 64 {
			return errors.New("invalid ES256 signature length")
		}
		r := new(big.Int).SetBytes(sigBytes[:32])
		s := new(big.Int).SetBytes(sigBytes[32:])
		if !ecdsa.Verify(ecKey, digest[:], r, s) {
			return errors.New("invalid ES256 JWT signature")
		}
		return nil

	case "RS256":
		rsaKey, ok := key.publicKey.(*rsa.PublicKey)
		if !ok {
			return errors.New("JWK key is not an RSA public key")
		}
		if err := rsa.VerifyPKCS1v15(rsaKey, crypto.SHA256, digest[:], sigBytes); err != nil {
			return errors.New("invalid RS256 JWT signature")
		}
		return nil

	default:
		return fmt.Errorf("unsupported signing algorithm %q", key.alg)
	}
}

func parseAudiences(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("actor JWT aud claim is required")
	}
	var single string
	if err := json.Unmarshal(raw, &single); err == nil {
		if single == "" {
			return nil, errors.New("actor JWT aud claim is empty")
		}
		return []string{single}, nil
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err == nil && len(list) > 0 {
		return list, nil
	}
	return nil, errors.New("invalid actor JWT aud claim")
}

func extractBearerToken(r *http.Request) (string, error) {
	vals := r.Header.Values("Authorization")
	if len(vals) == 0 {
		return "", errors.New("missing Authorization bearer header")
	}
	if len(vals) > 1 {
		return "", errors.New("multiple Authorization headers are not allowed")
	}
	scheme, token, ok := strings.Cut(strings.TrimSpace(vals[0]), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", errors.New("Authorization header must use Bearer scheme")
	}
	token = strings.TrimSpace(token)
	if token == "" || len(token) > maxBearerTokenBytes || strings.ContainsAny(token, " \t\r\n") {
		return "", errors.New("invalid bearer token format")
	}
	return token, nil
}
