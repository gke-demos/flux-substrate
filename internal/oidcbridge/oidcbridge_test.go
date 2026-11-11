package oidcbridge_test

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/rest"

	"github.com/gke-demos/flux-substrate/internal/oidcbridge"
)

type testJWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	Crv string `json:"crv,omitempty"`
	X   string `json:"x,omitempty"`
	Y   string `json:"y,omitempty"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
}

type testIDP struct {
	mu            sync.RWMutex
	keys          []testJWK
	failDiscovery atomic.Bool
	discoveryHits atomic.Int64
	jwksHits      atomic.Int64
	server        *httptest.Server
	caPEM         []byte
}

func newTestIDP(t *testing.T, initialKeys ...testJWK) *testIDP {
	t.Helper()
	idp := &testIDP{
		keys: append([]testJWK(nil), initialKeys...),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		idp.discoveryHits.Add(1)
		if idp.failDiscovery.Load() {
			http.Error(w, "idp unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   idp.server.URL,
			"jwks_uri": idp.server.URL + "/openid/v1/jwks",
		})
	})
	mux.HandleFunc("GET /openid/v1/jwks", func(w http.ResponseWriter, _ *http.Request) {
		idp.jwksHits.Add(1)
		idp.mu.RLock()
		keys := append([]testJWK(nil), idp.keys...)
		idp.mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": keys,
		})
	})

	idp.server = httptest.NewTLSServer(mux)
	t.Cleanup(idp.server.Close)
	idp.caPEM = pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: idp.server.Certificate().Raw,
	})
	return idp
}

func (idp *testIDP) setKeys(keys ...testJWK) {
	idp.mu.Lock()
	defer idp.mu.Unlock()
	idp.keys = append([]testJWK(nil), keys...)
}

func generateES256Key(t *testing.T, kid string) (*ecdsa.PrivateKey, testJWK) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	uncompressed, err := priv.PublicKey.Bytes()
	if err != nil {
		t.Fatalf("PublicKey.Bytes: %v", err)
	}
	return priv, testJWK{
		Kty: "EC",
		Kid: kid,
		Use: "sig",
		Alg: "ES256",
		Crv: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(uncompressed[1:33]),
		Y:   base64.RawURLEncoding.EncodeToString(uncompressed[33:65]),
	}
}

func generateRS256Key(t *testing.T, kid string) (*rsa.PrivateKey, testJWK) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return priv, testJWK{
		Kty: "RSA",
		Kid: kid,
		Use: "sig",
		Alg: "RS256",
		N:   base64.RawURLEncoding.EncodeToString(priv.PublicKey.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(priv.PublicKey.E)).Bytes()),
	}
}

func signES256JWT(t *testing.T, priv *ecdsa.PrivateKey, header map[string]any, claims map[string]any) string {
	t.Helper()
	hdrB64 := mustBase64JSON(t, header)
	payB64 := mustBase64JSON(t, claims)
	signingInput := hdrB64 + "." + payB64
	digest := sha256.Sum256([]byte(signingInput))

	r, s, err := ecdsa.Sign(rand.Reader, priv, digest[:])
	if err != nil {
		t.Fatalf("ecdsa.Sign: %v", err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func signRS256JWT(t *testing.T, priv *rsa.PrivateKey, header map[string]any, claims map[string]any) string {
	t.Helper()
	hdrB64 := mustBase64JSON(t, header)
	payB64 := mustBase64JSON(t, claims)
	signingInput := hdrB64 + "." + payB64
	digest := sha256.Sum256([]byte(signingInput))

	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatalf("rsa.SignPKCS1v15: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func mustBase64JSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestIdentityBridgeVerifiesES256ActorJWTAndImpersonatesUpstream(t *testing.T) {
	ecPriv, ecJWK := generateES256Key(t, "es256-key-1")
	idp := newTestIDP(t, ecJWK)

	const bridgeSA = "bridge-sa-token-xyz"
	var gotAuthHeader string
	var gotImpersonateUser string
	var gotPath string

	upstreamKube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("Authorization")
		gotImpersonateUser = r.Header.Get("Impersonate-User")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"kind": "ConfigMapList"})
	}))
	t.Cleanup(upstreamKube.Close)

	upstreamCAPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: upstreamKube.Certificate().Raw,
	})

	const audience = "https://kube-bridge.flux-substrate.svc"
	now := time.Unix(1_760_400_000, 0)

	bridge, err := oidcbridge.NewServer(oidcbridge.Config{
		IssuerURL:   idp.server.URL,
		Audiences:   []string{audience},
		IssuerCAPEM: idp.caPEM,
		UpstreamRestConfig: &rest.Config{
			Host:        upstreamKube.URL,
			BearerToken: bridgeSA,
			TLSClientConfig: rest.TLSClientConfig{
				CAData: upstreamCAPEM,
			},
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("oidcbridge.NewServer: %v", err)
	}

	bridgeTS := httptest.NewTLSServer(bridge.Handler())
	t.Cleanup(bridgeTS.Close)

	// Verify /readyz works unauthenticated with security headers and blocks TRACE.
	readyResp, err := bridgeTS.Client().Get(bridgeTS.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	readyResp.Body.Close()
	if readyResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /readyz status = %d, want 200", readyResp.StatusCode)
	}
	if got := readyResp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}

	traceReq, _ := http.NewRequest(http.MethodTrace, bridgeTS.URL+"/api/v1/namespaces", nil)
	traceResp, err := bridgeTS.Client().Do(traceReq)
	if err != nil {
		t.Fatalf("TRACE: %v", err)
	}
	traceResp.Body.Close()
	if traceResp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("TRACE status = %d, want 405", traceResp.StatusCode)
	}

	token := signES256JWT(t, ecPriv, map[string]any{
		"typ": "JWT",
		"alg": "ES256",
		"kid": "es256-key-1",
	}, map[string]any{
		"iss": idp.server.URL,
		"sub": "actor/flux-substrate/tenant-01",
		"aud": []string{audience},
		"iat": now.Unix(),
		"nbf": now.Add(-5 * time.Minute).Unix(),
		"exp": now.Add(10 * time.Minute).Unix(),
		"actor.ate.dev": map[string]any{
			"atespace":  "flux-substrate",
			"actorName": "tenant-01",
			"actorUID":  "uid-tenant-01",
		},
	})

	req, err := http.NewRequest(http.MethodGet, bridgeTS.URL+"/api/v1/namespaces/tenant-01/configmaps", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := bridgeTS.Client().Do(req)
	if err != nil {
		t.Fatalf("GET through Identity Bridge: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("proxied GET status = %d, want 200", resp.StatusCode)
	}
	if gotPath != "/api/v1/namespaces/tenant-01/configmaps" {
		t.Errorf("upstream path = %q, want /api/v1/namespaces/tenant-01/configmaps", gotPath)
	}
	if gotImpersonateUser != "ate:actor:flux-substrate:tenant-01" {
		t.Errorf("upstream Impersonate-User = %q, want %q", gotImpersonateUser, "ate:actor:flux-substrate:tenant-01")
	}
	if gotAuthHeader != "Bearer "+bridgeSA {
		t.Errorf("upstream Authorization = %q, want bridge ServiceAccount token (actorJwt stripped)", gotAuthHeader)
	}
}

func TestIdentityBridgeSupportsRS256AndRejectsInvalidOrWorkerTokens(t *testing.T) {
	ecPriv, ecJWK := generateES256Key(t, "es256-key-1")
	rsaPriv, rsaJWK := generateRS256Key(t, "rsa256-key-1")
	idp := newTestIDP(t, ecJWK, rsaJWK)

	var upstreamCalls atomic.Int64
	var lastImpersonateUser atomic.Value

	upstreamKube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		lastImpersonateUser.Store(r.Header.Get("Impersonate-User"))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	}))
	t.Cleanup(upstreamKube.Close)

	upstreamCAPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: upstreamKube.Certificate().Raw,
	})

	const audience = "https://kube-bridge.flux-substrate.svc"
	now := time.Unix(1_760_400_000, 0)

	bridge, err := oidcbridge.NewServer(oidcbridge.Config{
		IssuerURL:   idp.server.URL,
		Audiences:   []string{audience},
		IssuerCAPEM: idp.caPEM,
		UpstreamRestConfig: &rest.Config{
			Host:        upstreamKube.URL,
			BearerToken: "bridge-sa",
			TLSClientConfig: rest.TLSClientConfig{
				CAData: upstreamCAPEM,
			},
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("oidcbridge.NewServer: %v", err)
	}

	bridgeTS := httptest.NewTLSServer(bridge.Handler())
	t.Cleanup(bridgeTS.Close)

	// 1. Valid RS256 actorJwt succeeds and impersonates tenant-02.
	validRS256 := signRS256JWT(t, rsaPriv, map[string]any{
		"typ": "JWT",
		"alg": "RS256",
		"kid": "rsa256-key-1",
	}, map[string]any{
		"iss": idp.server.URL,
		"sub": "actor/flux-substrate/tenant-02",
		"aud": []string{audience},
		"iat": now.Unix(),
		"nbf": now.Add(-5 * time.Minute).Unix(),
		"exp": now.Add(10 * time.Minute).Unix(),
		"actor.ate.dev": map[string]any{
			"atespace":  "flux-substrate",
			"actorName": "tenant-02",
			"actorUID":  "uid-tenant-02",
		},
	})

	req, _ := http.NewRequest(http.MethodGet, bridgeTS.URL+"/api/v1/namespaces/tenant-02/pods", nil)
	req.Header.Set("Authorization", "Bearer "+validRS256)
	resp, err := bridgeTS.Client().Do(req)
	if err != nil {
		t.Fatalf("RS256 request failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("RS256 actorJwt status = %d, want 200", resp.StatusCode)
	}
	if got, _ := lastImpersonateUser.Load().(string); got != "ate:actor:flux-substrate:tenant-02" {
		t.Fatalf("RS256 Impersonate-User = %q, want ate:actor:flux-substrate:tenant-02", got)
	}

	baseClaims := func() map[string]any {
		return map[string]any{
			"iss": idp.server.URL,
			"sub": "actor/flux-substrate/tenant-01",
			"aud": []string{audience},
			"iat": now.Unix(),
			"nbf": now.Add(-1 * time.Minute).Unix(),
			"exp": now.Add(10 * time.Minute).Unix(),
		}
	}

	// Build a tampered token (valid signature over tenant-01, payload swapped to tenant-02).
	validES256 := signES256JWT(t, ecPriv, map[string]any{
		"typ": "JWT",
		"alg": "ES256",
		"kid": "es256-key-1",
	}, baseClaims())
	tamperedClaims := baseClaims()
	tamperedClaims["sub"] = "actor/flux-substrate/tenant-02"
	splitValid := split3(t, validES256)
	tamperedToken := splitValid[0] + "." + mustBase64JSON(t, tamperedClaims) + "." + splitValid[2]

	// Build alg:none token.
	algNoneToken := mustBase64JSON(t, map[string]any{
		"typ": "JWT",
		"alg": "none",
		"kid": "es256-key-1",
	}) + "." + mustBase64JSON(t, baseClaims()) + ".fakesig"

	// Build alg mismatch token (RSA signature claiming ES256 kid).
	algMismatchToken := signRS256JWT(t, rsaPriv, map[string]any{
		"typ": "JWT",
		"alg": "RS256",
		"kid": "es256-key-1",
	}, baseClaims())

	expiredClaims := baseClaims()
	expiredClaims["exp"] = now.Add(-1 * time.Second).Unix()

	futureNBFClaims := baseClaims()
	futureNBFClaims["nbf"] = now.Add(2 * time.Minute).Unix()

	missingNBFClaims := baseClaims()
	delete(missingNBFClaims, "nbf")

	wrongIssClaims := baseClaims()
	wrongIssClaims["iss"] = "https://rogue-idp.ate-system.svc"

	wrongAudClaims := baseClaims()
	wrongAudClaims["aud"] = []string{"https://other-service.flux-substrate.svc"}

	workerSubClaims := baseClaims()
	workerSubClaims["sub"] = "worker-for-actor/flux-substrate/tenant-01"
	workerSubClaims["worker-for-actor.ate.dev"] = map[string]any{
		"atespace":  "flux-substrate",
		"actorName": "tenant-01",
		"actorUID":  "uid-1",
	}

	dualClaim := baseClaims()
	dualClaim["worker-for-actor.ate.dev"] = map[string]any{
		"atespace":  "flux-substrate",
		"actorName": "tenant-01",
	}

	rejectCases := []struct {
		name       string
		authHeader string
		wantStatus int
	}{
		{
			name:       "missing Authorization header",
			authHeader: "",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "alg none rejected",
			authHeader: "Bearer " + algNoneToken,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "algorithm mismatch with trusted JWK rejected",
			authHeader: "Bearer " + algMismatchToken,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "tampered payload signature rejected",
			authHeader: "Bearer " + tamperedToken,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "expired exp rejected",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, expiredClaims),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "future nbf rejected",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, futureNBFClaims),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "missing nbf rejected",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, missingNBFClaims),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong issuer rejected",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, wrongIssClaims),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "wrong audience rejected",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, wrongAudClaims),
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "worker-for-actor subject rejected with 403",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, workerSubClaims),
			wantStatus: http.StatusForbidden,
		},
		{
			name: "worker-for-actor claim rejected with 403",
			authHeader: "Bearer " + signES256JWT(t, ecPriv, map[string]any{
				"typ": "JWT", "alg": "ES256", "kid": "es256-key-1",
			}, dualClaim),
			wantStatus: http.StatusForbidden,
		},
	}

	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			before := upstreamCalls.Load()
			r, _ := http.NewRequest(http.MethodGet, bridgeTS.URL+"/api/v1/namespaces/tenant-01/configmaps", nil)
			if tc.authHeader != "" {
				r.Header.Set("Authorization", tc.authHeader)
			}
			res, doErr := bridgeTS.Client().Do(r)
			if doErr != nil {
				t.Fatalf("Do: %v", doErr)
			}
			res.Body.Close()
			if res.StatusCode != tc.wantStatus {
				t.Errorf("status = %d, want %d", res.StatusCode, tc.wantStatus)
			}
			if after := upstreamCalls.Load(); after != before {
				t.Errorf("upstream received request on rejected token (%s)", tc.name)
			}
		})
	}
}

func split3(t *testing.T, jwt string) [3]string {
	t.Helper()
	var out [3]string
	p1 := -1
	p2 := -1
	for i := range len(jwt) {
		if jwt[i] == '.' {
			if p1 == -1 {
				p1 = i
			} else {
				p2 = i
				break
			}
		}
	}
	if p1 <= 0 || p2 <= p1 {
		t.Fatalf("invalid jwt %q", jwt)
	}
	out[0] = jwt[:p1]
	out[1] = jwt[p1+1 : p2]
	out[2] = jwt[p2+1:]
	return out
}

func TestIdentityBridgeCachesJWKSAndRateLimitsUnknownKIDRefreshes(t *testing.T) {
	k1Priv, k1JWK := generateES256Key(t, "kid-1")
	k2Priv, k2JWK := generateES256Key(t, "kid-2")
	k3Priv, k3JWK := generateES256Key(t, "kid-3")
	bogusPriv, _ := generateES256Key(t, "kid-bogus")

	idp := newTestIDP(t, k1JWK)

	upstreamKube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"ok": "true"})
	}))
	t.Cleanup(upstreamKube.Close)
	upstreamCAPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: upstreamKube.Certificate().Raw,
	})

	caFile := filepath.Join(t.TempDir(), "idp-ca.pem")
	if err := os.WriteFile(caFile, idp.caPEM, 0o600); err != nil {
		t.Fatalf("WriteFile caFile: %v", err)
	}

	const audience = "https://kube-bridge.flux-substrate.svc"
	var nowMu sync.Mutex
	curNow := time.Unix(1_760_500_000, 0)
	getNow := func() time.Time {
		nowMu.Lock()
		defer nowMu.Unlock()
		return curNow
	}
	advance := func(d time.Duration) {
		nowMu.Lock()
		curNow = curNow.Add(d)
		nowMu.Unlock()
	}

	bridge, err := oidcbridge.NewServer(oidcbridge.Config{
		IssuerURL:    idp.server.URL,
		Audiences:    []string{audience},
		IssuerCAFile: caFile,
		UpstreamRestConfig: &rest.Config{
			Host:        upstreamKube.URL,
			BearerToken: "bridge-sa",
			TLSClientConfig: rest.TLSClientConfig{
				CAData: upstreamCAPEM,
			},
		},
		RefreshInterval:    5 * time.Minute,
		MinRefreshInterval: 1 * time.Minute,
		Now:                getNow,
	})
	if err != nil {
		t.Fatalf("oidcbridge.NewServer: %v", err)
	}
	bridgeTS := httptest.NewTLSServer(bridge.Handler())
	t.Cleanup(bridgeTS.Close)

	mint := func(priv *ecdsa.PrivateKey, kid string) string {
		now := getNow()
		return signES256JWT(t, priv, map[string]any{
			"typ": "JWT",
			"alg": "ES256",
			"kid": kid,
		}, map[string]any{
			"iss": idp.server.URL,
			"sub": "actor/flux-substrate/tenant-01",
			"aud": []string{audience},
			"iat": now.Unix(),
			"nbf": now.Add(-1 * time.Minute).Unix(),
			"exp": now.Add(30 * time.Minute).Unix(),
		})
	}

	callBridge := func(tok string) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, bridgeTS.URL+"/api/v1/namespaces", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		resp, doErr := bridgeTS.Client().Do(req)
		if doErr != nil {
			t.Fatalf("Do: %v", doErr)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	// 1. Multiple warm calls with kid-1 hit OIDC discovery/JWKS only once.
	for range 5 {
		if status := callBridge(mint(k1Priv, "kid-1")); status != http.StatusOK {
			t.Fatalf("kid-1 status = %d, want 200", status)
		}
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("warm discoveryHits = %d, want 1", got)
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Fatalf("warm jwksHits = %d, want 1", got)
	}

	// 2. Unauthenticated/expired or unsupported-alg tokens with random kids must NOT trigger
	// an IdP fetch or poison the unknown-kid rate-limit window before a real key rotation.
	hs256UnknownKID := mustBase64JSON(t, map[string]any{
		"typ": "JWT", "alg": "HS256", "kid": "poison-kid-hs256",
	}) + "." + mustBase64JSON(t, map[string]any{
		"iss": idp.server.URL,
		"sub": "actor/flux-substrate/tenant-01",
		"aud": []string{audience},
		"nbf": getNow().Add(-1 * time.Minute).Unix(),
		"exp": getNow().Add(10 * time.Minute).Unix(),
	}) + ".fakesig"
	if status := callBridge(hs256UnknownKID); status != http.StatusUnauthorized {
		t.Fatalf("HS256 unknown kid status = %d, want 401", status)
	}
	expiredUnknownKID := signES256JWT(t, bogusPriv, map[string]any{
		"typ": "JWT", "alg": "ES256", "kid": "poison-kid-expired",
	}, map[string]any{
		"iss": idp.server.URL,
		"sub": "actor/flux-substrate/tenant-01",
		"aud": []string{audience},
		"nbf": getNow().Add(-10 * time.Minute).Unix(),
		"exp": getNow().Add(-1 * time.Minute).Unix(),
	})
	if status := callBridge(expiredUnknownKID); status != http.StatusUnauthorized {
		t.Fatalf("expired unknown kid status = %d, want 401", status)
	}
	if got := idp.discoveryHits.Load(); got != 1 {
		t.Fatalf("discoveryHits after invalid pre-checked tokens = %d, want 1", got)
	}

	// Rotate key to kid-2: unknown kid triggers 1 immediate refresh and succeeds.
	idp.setKeys(k1JWK, k2JWK)
	if status := callBridge(mint(k2Priv, "kid-2")); status != http.StatusOK {
		t.Fatalf("rotated kid-2 status = %d, want 200", status)
	}
	if got := idp.discoveryHits.Load(); got != 2 {
		t.Fatalf("post-rotation discoveryHits = %d, want 2", got)
	}

	// 3. Flood of unknown kids within MinRefreshInterval is rate-limited (0 extra IdP fetches).
	for i := range 5 {
		bogusTok := mint(bogusPriv, fmt.Sprintf("unknown-kid-%d", i))
		if status := callBridge(bogusTok); status != http.StatusUnauthorized {
			t.Fatalf("unknown kid %d status = %d, want 401", i, status)
		}
	}
	if got := idp.discoveryHits.Load(); got != 2 {
		t.Fatalf("rate-limited unknown kid discoveryHits = %d, want 2", got)
	}
	if got := idp.jwksHits.Load(); got != 2 {
		t.Fatalf("rate-limited unknown kid jwksHits = %d, want 2", got)
	}

	// 4. Advance past MinRefreshInterval: kid-3 rotation succeeds with 1 refresh.
	advance(65 * time.Second)
	idp.setKeys(k2JWK, k3JWK)
	if status := callBridge(mint(k3Priv, "kid-3")); status != http.StatusOK {
		t.Fatalf("kid-3 after rate-limit window status = %d, want 200", status)
	}
	if got := idp.discoveryHits.Load(); got != 3 {
		t.Fatalf("kid-3 discoveryHits = %d, want 3", got)
	}

	// 5. Periodic refresh expires while IdP is transiently unavailable: cached kid-3 keeps
	// working and subsequent calls within MinRefreshInterval back off without hammering IdP.
	idp.failDiscovery.Store(true)
	advance(6 * time.Minute)
	for range 4 {
		if status := callBridge(mint(k3Priv, "kid-3")); status != http.StatusOK {
			t.Fatalf("cached kid-3 during transient IdP outage status = %d, want 200", status)
		}
	}
	if got := idp.discoveryHits.Load(); got != 4 {
		t.Fatalf("transient outage discoveryHits = %d, want 4 (1 failed refresh + backoff)", got)
	}

	// 6. Untrusted TLS certificate on OIDC issuer fails closed (401), then succeeds after CA file rotation.
	idp.failDiscovery.Store(false)
	unrelatedCAPEM := generateEphemeralCAPEM(t)
	rotatableCAFile := filepath.Join(t.TempDir(), "rotatable-ca.pem")
	if err := os.WriteFile(rotatableCAFile, unrelatedCAPEM, 0o600); err != nil {
		t.Fatalf("WriteFile rotatableCAFile: %v", err)
	}
	untrustedBridge, err := oidcbridge.NewServer(oidcbridge.Config{
		IssuerURL:    idp.server.URL,
		Audiences:    []string{audience},
		IssuerCAFile: rotatableCAFile,
		UpstreamRestConfig: &rest.Config{
			Host:        upstreamKube.URL,
			BearerToken: "bridge-sa",
			TLSClientConfig: rest.TLSClientConfig{
				CAData: upstreamCAPEM,
			},
		},
		MinRefreshInterval: 1 * time.Minute,
		Now:                getNow,
	})
	if err != nil {
		t.Fatalf("NewServer untrusted: %v", err)
	}
	untrustedTS := httptest.NewTLSServer(untrustedBridge.Handler())
	t.Cleanup(untrustedTS.Close)

	req, _ := http.NewRequest(http.MethodGet, untrustedTS.URL+"/api/v1/namespaces", nil)
	req.Header.Set("Authorization", "Bearer "+mint(k3Priv, "kid-3"))
	resp, err := untrustedTS.Client().Do(req)
	if err != nil {
		t.Fatalf("Do untrusted: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("untrusted IdP TLS cert status = %d, want 401", resp.StatusCode)
	}

	// Rotate CA file to trust idp.caPEM and advance past backoff window.
	if err := os.WriteFile(rotatableCAFile, idp.caPEM, 0o600); err != nil {
		t.Fatalf("rotate CA file: %v", err)
	}
	advance(65 * time.Second)
	req2, _ := http.NewRequest(http.MethodGet, untrustedTS.URL+"/api/v1/namespaces", nil)
	req2.Header.Set("Authorization", "Bearer "+mint(k3Priv, "kid-3"))
	resp2, err := untrustedTS.Client().Do(req2)
	if err != nil {
		t.Fatalf("Do post-CA-rotation: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("post-CA-rotation status = %d, want 200", resp2.StatusCode)
	}
}

func generateEphemeralCAPEM(t *testing.T) []byte {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey CA: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ephemeral-untrusted-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestIdentityBridgeStripsClientImpersonationAndHopByHopHeaders(t *testing.T) {
	ecPriv, ecJWK := generateES256Key(t, "es256-key-1")
	idp := newTestIDP(t, ecJWK)

	var capturedHeaders http.Header
	upstreamKube := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	t.Cleanup(upstreamKube.Close)

	upstreamCAPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: upstreamKube.Certificate().Raw,
	})

	const audience = "https://kube-bridge.flux-substrate.svc"
	now := time.Unix(1_760_600_000, 0)

	bridge, err := oidcbridge.NewServer(oidcbridge.Config{
		IssuerURL:   idp.server.URL,
		Audiences:   []string{audience},
		IssuerCAPEM: idp.caPEM,
		UpstreamRestConfig: &rest.Config{
			Host:        upstreamKube.URL,
			BearerToken: "bridge-own-sa-token",
			TLSClientConfig: rest.TLSClientConfig{
				CAData: upstreamCAPEM,
			},
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("oidcbridge.NewServer: %v", err)
	}

	bridgeTS := httptest.NewTLSServer(bridge.Handler())
	t.Cleanup(bridgeTS.Close)

	tok := signES256JWT(t, ecPriv, map[string]any{
		"typ": "JWT",
		"alg": "ES256",
		"kid": "es256-key-1",
	}, map[string]any{
		"iss": idp.server.URL,
		"sub": "actor/flux-substrate/tenant-01",
		"aud": []string{audience},
		"iat": now.Unix(),
		"nbf": now.Add(-1 * time.Minute).Unix(),
		"exp": now.Add(10 * time.Minute).Unix(),
	})

	req, err := http.NewRequest(http.MethodPatch, bridgeTS.URL+"/api/v1/namespaces/tenant-01/configmaps/app-config?fieldManager=flux-substrate", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Impersonate-User", "kubernetes-admin")
	req.Header.Set("Impersonate-Group", "system:masters")
	req.Header.Set("Impersonate-Uid", "0000-admin")
	req.Header.Set("Impersonate-Extra-Scopes", "cluster-admin")
	req.Header.Set("X-Remote-User", "cluster-admin")
	req.Header.Set("X-Remote-Group", "system:masters")
	req.Header.Set("Connection", "Impersonate-User")

	resp, err := bridgeTS.Client().Do(req)
	if err != nil {
		t.Fatalf("PATCH through bridge: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200", resp.StatusCode)
	}

	if got := capturedHeaders.Values("Impersonate-User"); len(got) != 1 || got[0] != "ate:actor:flux-substrate:tenant-01" {
		t.Fatalf("upstream Impersonate-User = %v, want [ate:actor:flux-substrate:tenant-01]", got)
	}
	for _, forbidden := range []string{
		"Impersonate-Group",
		"Impersonate-Uid",
		"Impersonate-Extra-Scopes",
		"X-Remote-User",
		"X-Remote-Group",
	} {
		if got := capturedHeaders.Values(forbidden); len(got) != 0 {
			t.Errorf("upstream leaked client header %s = %v, want stripped", forbidden, got)
		}
	}
	if got := capturedHeaders.Get("Authorization"); got != "Bearer bridge-own-sa-token" {
		t.Errorf("upstream Authorization = %q, want bridge ServiceAccount token", got)
	}
}
