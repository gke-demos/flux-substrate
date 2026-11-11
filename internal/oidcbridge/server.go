// Package oidcbridge implements the Identity Bridge (ADR-0002) that verifies
// Agent Substrate actorJwt bearer tokens against an OIDC issuer and reverse-proxies
// Kubernetes API calls using Impersonate-User: ate:actor:<atespace>:<actor>.
package oidcbridge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	fshttp "github.com/gke-demos/flux-substrate/internal/httputil"
)

const (
	defaultRefreshInterval    = 5 * time.Minute
	defaultMinRefreshInterval = time.Minute
	oidcFetchTimeout          = 10 * time.Second
	maxDiscoveryBodyBytes     = 1 << 20 // 1 MiB OIDC/JWKS JSON response cap
	maxBearerTokenBytes       = 16 << 10
	bridgeCSP                 = "default-src 'self'; frame-ancestors 'none'; object-src 'none'; base-uri 'self'"
)

type actorIdentityContextKey struct{}

type actorIdentity struct {
	atespace  string
	actorName string
}

// Config configures the Identity Bridge OIDC verifier and Kubernetes API reverse proxy.
type Config struct {
	IssuerURL          string
	Audiences          []string
	IssuerCAPEM        []byte
	IssuerCAFile       string
	UpstreamRestConfig *rest.Config
	KubeconfigPath     string
	UpstreamKubeURL    string
	RefreshInterval    time.Duration
	MinRefreshInterval time.Duration
	ClockSkew          time.Duration
	Now                func() time.Time
}

// Server verifies actorJwt tokens and reverse-proxies requests to kube-apiserver.
type Server struct {
	issuerURL          string
	audiences          []string
	oidcClient         *http.Client
	upstreamURL        *url.URL
	proxy              *httputil.ReverseProxy
	refreshInterval    time.Duration
	minRefreshInterval time.Duration
	clockSkew          time.Duration
	now                func() time.Time

	mu                    sync.RWMutex
	keys                  map[string]trustedKey
	lastRefresh           time.Time
	lastFailedRefresh     time.Time
	lastUnknownKIDRefresh time.Time

	handler http.Handler
}

// NewServer constructs an Identity Bridge server.
func NewServer(cfg Config) (*Server, error) {
	issuerURL := strings.TrimSpace(cfg.IssuerURL)
	if err := validateHTTPSIssuerURL(issuerURL); err != nil {
		return nil, err
	}

	var audiences []string
	for _, aud := range cfg.Audiences {
		if trimmed := strings.TrimSpace(aud); trimmed != "" && !slices.Contains(audiences, trimmed) {
			audiences = append(audiences, trimmed)
		}
	}
	if len(audiences) == 0 {
		return nil, errors.New("at least one expected OIDC audience is required")
	}

	oidcClient, err := buildOIDCHTTPClient(cfg.IssuerCAPEM, strings.TrimSpace(cfg.IssuerCAFile))
	if err != nil {
		return nil, err
	}

	upstreamCfg, err := resolveUpstreamRestConfig(cfg)
	if err != nil {
		return nil, err
	}
	upstreamURL, err := parseUpstreamURL(upstreamCfg.Host)
	if err != nil {
		return nil, err
	}
	upstreamTransport, err := rest.TransportFor(upstreamCfg)
	if err != nil {
		return nil, fmt.Errorf("build upstream Kubernetes transport: %w", err)
	}

	refreshInterval := cfg.RefreshInterval
	if refreshInterval <= 0 {
		refreshInterval = defaultRefreshInterval
	}
	minRefreshInterval := cfg.MinRefreshInterval
	if minRefreshInterval <= 0 {
		minRefreshInterval = defaultMinRefreshInterval
	}
	nowFn := cfg.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	s := &Server{
		issuerURL:          issuerURL,
		audiences:          audiences,
		oidcClient:         oidcClient,
		upstreamURL:        upstreamURL,
		refreshInterval:    refreshInterval,
		minRefreshInterval: minRefreshInterval,
		clockSkew:          cfg.ClockSkew,
		now:                nowFn,
	}

	s.proxy = &httputil.ReverseProxy{
		Transport: upstreamTransport,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(s.upstreamURL)
			pr.Out.Header.Del("Authorization")
			for headerName := range pr.Out.Header {
				lower := strings.ToLower(headerName)
				if strings.HasPrefix(lower, "impersonate-") || strings.HasPrefix(lower, "x-remote-") {
					delete(pr.Out.Header, headerName)
				}
			}
			if ident, ok := pr.In.Context().Value(actorIdentityContextKey{}).(actorIdentity); ok {
				pr.Out.Header.Set("Impersonate-User", fmt.Sprintf("ate:actor:%s:%s", ident.atespace, ident.actorName))
			}
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, proxyErr error) {
			writeKubeStatusError(w, http.StatusBadGateway, metav1.StatusReasonInternalError, fmt.Sprintf("upstream kube-apiserver proxy failed: %v", proxyErr))
		},
	}

	s.handler = http.HandlerFunc(s.serveHTTP)
	return s, nil
}

// Handler returns the HTTP handler for the Identity Bridge.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// TODO(security): Per-IP rate limiting on proxied Kubernetes API calls is intentionally
// omitted because 24 Workflow Actors multiplex rapid Server-Side Apply turns through
// atenet-egress NAT; JWKS refetch rate limiting (MinRefreshInterval) protects ate-idp-server.
func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	h.Set("Content-Security-Policy", bridgeCSP)
	h.Set("Cache-Control", "no-store")

	if r.URL.Path == "/readyz" {
		if r.Method != http.MethodGet {
			h.Set("Allow", "GET")
			writeKubeStatusError(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed, "method not allowed")
			return
		}
		fshttp.WriteJSON(w, http.StatusOK, map[string]string{
			"status": "ready",
			"mode":   "oidc-bridge",
			"issuer": s.issuerURL,
		})
		return
	}

	if !isAllowedKubeMethod(r.Method) {
		h.Set("Allow", "GET, HEAD, POST, PUT, PATCH, DELETE")
		writeKubeStatusError(w, http.StatusMethodNotAllowed, metav1.StatusReasonMethodNotAllowed, "method not allowed")
		return
	}

	rawToken, err := extractBearerToken(r)
	if err != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeKubeStatusError(w, http.StatusUnauthorized, metav1.StatusReasonUnauthorized, err.Error())
		return
	}

	ident, statusCode, verifyErr := s.verifyActorJWT(r.Context(), rawToken)
	if verifyErr != nil {
		reason := metav1.StatusReasonUnauthorized
		if statusCode == http.StatusForbidden {
			reason = metav1.StatusReasonForbidden
		} else {
			w.Header().Set("WWW-Authenticate", "Bearer")
		}
		writeKubeStatusError(w, statusCode, reason, verifyErr.Error())
		return
	}

	ctx := context.WithValue(r.Context(), actorIdentityContextKey{}, ident)
	s.proxy.ServeHTTP(w, r.WithContext(ctx))
}

func validateHTTPSIssuerURL(issuer string) error {
	u, err := url.Parse(issuer)
	if err != nil {
		return fmt.Errorf("invalid IssuerURL: %w", err)
	}
	if u.Hostname() == "" || issuer != "https://"+u.Host {
		return fmt.Errorf("IssuerURL %q must be https://<host>[:port] with no path, query, or fragment", issuer)
	}
	return nil
}

func parseUpstreamURL(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("invalid upstream Kubernetes API URL %q", raw)
	}
	return u, nil
}

func resolveUpstreamRestConfig(cfg Config) (*rest.Config, error) {
	var out *rest.Config
	switch {
	case cfg.UpstreamRestConfig != nil:
		out = rest.CopyConfig(cfg.UpstreamRestConfig)
	case strings.TrimSpace(cfg.KubeconfigPath) != "":
		loaded, err := clientcmd.BuildConfigFromFlags("", strings.TrimSpace(cfg.KubeconfigPath))
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %s: %w", cfg.KubeconfigPath, err)
		}
		out = loaded
	default:
		inCluster, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
		}
		out = inCluster
	}
	if override := strings.TrimSpace(cfg.UpstreamKubeURL); override != "" {
		out.Host = override
	}
	return out, nil
}

func isAllowedKubeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func writeKubeStatusError(w http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	fshttp.WriteJSON(w, code, metav1.Status{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Status",
		},
		Status:  metav1.StatusFailure,
		Message: message,
		Reason:  reason,
		Code:    int32(code),
	})
}
