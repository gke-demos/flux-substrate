package oidcbridge

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"sync"
	"time"
)

type oidcDiscoveryDoc struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

type jwkSetDoc struct {
	Keys []jwkEntry `json:"keys"`
}

type jwkEntry struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use,omitempty"`
	Alg string `json:"alg,omitempty"`
	Crv string `json:"crv,omitempty"`
	X   string `json:"x,omitempty"`
	Y   string `json:"y,omitempty"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
}

func (s *Server) lookupTrustedKey(ctx context.Context, kid string, now time.Time) (trustedKey, error) {
	s.mu.RLock()
	if k, ok := s.cachedKeyLocked(kid, now); ok {
		s.mu.RUnlock()
		return k, nil
	}
	if s.isRefreshRateLimitedLocked(kid, now) {
		s.mu.RUnlock()
		return trustedKey{}, fmt.Errorf("unknown signing key ID %q (JWKS refresh rate-limited)", kid)
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	if k, ok := s.cachedKeyLocked(kid, now); ok {
		return k, nil
	}
	if s.isRefreshRateLimitedLocked(kid, now) {
		return trustedKey{}, fmt.Errorf("unknown signing key ID %q (JWKS refresh rate-limited)", kid)
	}
	if _, ok := s.keys[kid]; !ok && len(s.keys) > 0 {
		s.lastUnknownKIDRefresh = now
	}

	// Detach from caller cancellation so one aborted client request cannot cancel
	// a shared JWKS refresh or strand key rotation in backoff.
	fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), oidcFetchTimeout)
	defer cancel()

	fetched, err := s.fetchKeysFromIssuer(fetchCtx)
	if err != nil {
		s.lastFailedRefresh = now
		if cached, ok := s.keys[kid]; ok {
			return cached, nil
		}
		return trustedKey{}, fmt.Errorf("fetch OIDC signing keys: %w", err)
	}
	s.keys = fetched
	s.lastRefresh = now
	s.lastFailedRefresh = time.Time{}

	k, ok := s.keys[kid]
	if !ok {
		return trustedKey{}, fmt.Errorf("unknown signing key ID %q", kid)
	}
	return k, nil
}

func (s *Server) cachedKeyLocked(kid string, now time.Time) (trustedKey, bool) {
	k, ok := s.keys[kid]
	if !ok {
		return trustedKey{}, false
	}
	if now.Sub(s.lastRefresh) < s.refreshInterval {
		return k, true
	}
	if !s.lastFailedRefresh.IsZero() && now.Sub(s.lastFailedRefresh) < s.minRefreshInterval {
		return k, true
	}
	return trustedKey{}, false
}

func (s *Server) isRefreshRateLimitedLocked(kid string, now time.Time) bool {
	if _, ok := s.keys[kid]; ok {
		return false
	}
	if !s.lastFailedRefresh.IsZero() && now.Sub(s.lastFailedRefresh) < s.minRefreshInterval {
		return true
	}
	return len(s.keys) > 0 && !s.lastUnknownKIDRefresh.IsZero() && now.Sub(s.lastUnknownKIDRefresh) < s.minRefreshInterval
}

func (s *Server) fetchKeysFromIssuer(ctx context.Context) (map[string]trustedKey, error) {
	discoveryURL := s.issuerURL + "/.well-known/openid-configuration"
	var doc oidcDiscoveryDoc
	if err := fetchHTTPSJSON(ctx, s.oidcClient, discoveryURL, &doc); err != nil {
		return nil, fmt.Errorf("OIDC discovery failed: %w", err)
	}
	if doc.Issuer != s.issuerURL {
		return nil, fmt.Errorf("discovered issuer %q does not match trusted issuer %q", doc.Issuer, s.issuerURL)
	}
	jwksURL, err := url.Parse(doc.JWKSURI)
	if err != nil || jwksURL.Scheme != "https" || jwksURL.Hostname() == "" {
		return nil, fmt.Errorf("invalid non-HTTPS jwks_uri %q", doc.JWKSURI)
	}

	var set jwkSetDoc
	if err := fetchHTTPSJSON(ctx, s.oidcClient, doc.JWKSURI, &set); err != nil {
		return nil, fmt.Errorf("JWKS fetch failed: %w", err)
	}

	out := make(map[string]trustedKey, len(set.Keys))
	for _, entry := range set.Keys {
		if entry.Use != "" && entry.Use != "sig" {
			continue
		}
		parsed, parseErr := parseTrustedJWK(entry)
		if parseErr != nil {
			continue
		}
		if _, exists := out[parsed.kid]; exists {
			return nil, fmt.Errorf("duplicate JWK kid %q", parsed.kid)
		}
		out[parsed.kid] = parsed
	}
	if len(out) == 0 {
		return nil, errors.New("JWKS contains no usable signing keys")
	}
	return out, nil
}

func buildOIDCHTTPClient(caPEM []byte, caFile string) (*http.Client, error) {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	switch {
	case len(caPEM) > 0:
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("parse IssuerCAPEM failed")
		}
		base.TLSClientConfig.RootCAs = pool
		return &http.Client{Timeout: oidcFetchTimeout, Transport: base}, nil
	case caFile != "":
		rt, err := newDynamicCAFileTransport(base, caFile)
		if err != nil {
			return nil, err
		}
		return &http.Client{Timeout: oidcFetchTimeout, Transport: rt}, nil
	default:
		return &http.Client{Timeout: oidcFetchTimeout, Transport: base}, nil
	}
}

type dynamicCAFileTransport struct {
	base      *http.Transport
	caFile    string
	mu        sync.Mutex
	lastBytes []byte
	transport *http.Transport
}

func newDynamicCAFileTransport(base *http.Transport, caFile string) (*dynamicCAFileTransport, error) {
	t := &dynamicCAFileTransport{base: base, caFile: caFile}
	if _, err := t.current(); err != nil {
		return nil, err
	}
	return t, nil
}

func (t *dynamicCAFileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt, err := t.current()
	if err != nil {
		return nil, err
	}
	return rt.RoundTrip(req)
}

func (t *dynamicCAFileTransport) current() (*http.Transport, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	pemBytes, err := os.ReadFile(t.caFile)
	if err != nil {
		if t.transport != nil {
			return t.transport, nil
		}
		return nil, fmt.Errorf("read IssuerCAFile %s: %w", t.caFile, err)
	}
	if t.transport != nil && slices.Equal(pemBytes, t.lastBytes) {
		return t.transport, nil
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		if t.transport != nil {
			return t.transport, nil
		}
		return nil, fmt.Errorf("parse certificates from IssuerCAFile %s failed", t.caFile)
	}
	cloned := t.base.Clone()
	cloned.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}
	if t.transport != nil {
		t.transport.CloseIdleConnections()
	}
	t.lastBytes = pemBytes
	t.transport = cloned
	return t.transport, nil
}

func fetchHTTPSJSON(ctx context.Context, client *http.Client, targetURL string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxDiscoveryBodyBytes))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
