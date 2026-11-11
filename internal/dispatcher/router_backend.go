package dispatcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gke-demos/flux-substrate/internal/actor"
)

// HTTPRouterBackendConfig configures routing Reconcile Turns over HTTP(S) to
// Agent Substrate's atenet-router using dynamic per-handshake podidentity mTLS.
type HTTPRouterBackendConfig struct {
	RouterURL    string
	RouterCAFile string
	RouterCAPEM  []byte
	PodCertFile  string
	PodKeyFile   string
	TurnTimeout  time.Duration
}

// HTTPRouterActorBackend forwards POST /reconcile requests through atenet-router
// with the ate-target-actor header while recording desired actor/egress state.
type HTTPRouterActorBackend struct {
	routerURL  string
	httpClient *http.Client

	mu       sync.Mutex
	actors   map[string]ActorSpec
	policies map[string]EgressPolicySpec
}

// NewHTTPRouterActorBackend constructs an HTTP(S) router ActorBackend for --mode=dispatcher.
func NewHTTPRouterActorBackend(cfg HTTPRouterBackendConfig) (*HTTPRouterActorBackend, error) {
	routerURL := strings.TrimRight(strings.TrimSpace(cfg.RouterURL), "/")
	if routerURL == "" {
		return nil, errors.New("RouterURL is required for HTTPRouterActorBackend")
	}

	caPEM := append([]byte(nil), cfg.RouterCAPEM...)
	if len(caPEM) == 0 && strings.TrimSpace(cfg.RouterCAFile) != "" {
		raw, err := os.ReadFile(strings.TrimSpace(cfg.RouterCAFile))
		if err != nil {
			return nil, fmt.Errorf("read RouterCAFile %s: %w", cfg.RouterCAFile, err)
		}
		caPEM = raw
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if len(caPEM) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("failed to parse RouterCA PEM")
		}
		tlsCfg.RootCAs = pool
	}
	certFile := strings.TrimSpace(cfg.PodCertFile)
	keyFile := strings.TrimSpace(cfg.PodKeyFile)
	if certFile != "" && keyFile != "" {
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pair, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, err
			}
			return &pair, nil
		}
	}

	timeout := cfg.TurnTimeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}

	return &HTTPRouterActorBackend{
		routerURL: routerURL,
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig: tlsCfg,
			},
		},
		actors:   make(map[string]ActorSpec),
		policies: make(map[string]EgressPolicySpec),
	}, nil
}

func (b *HTTPRouterActorBackend) EnsureActor(_ context.Context, spec ActorSpec) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.actors[spec.ActorName] = spec
	return nil
}

func (b *HTTPRouterActorBackend) EnsureEgressPolicy(_ context.Context, policy EgressPolicySpec) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.policies[policy.ActorName] = policy
	return nil
}

func (b *HTTPRouterActorBackend) InvokeReconcile(ctx context.Context, inv TurnInvocation) (actor.ReconcileResult, int, error) {
	body, err := json.Marshal(inv.Request)
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.routerURL+"/reconcile", bytes.NewReader(body))
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", inv.Atespace, inv.ActorName))

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return actor.ReconcileResult{}, http.StatusBadGateway, err
	}
	defer resp.Body.Close()

	var result actor.ReconcileResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return actor.ReconcileResult{}, resp.StatusCode, fmt.Errorf("decode router ReconcileResult: %w", err)
	}
	return result, resp.StatusCode, nil
}

func (b *HTTPRouterActorBackend) SuspendActor(_ context.Context, _, _ string) error {
	return nil
}

func (b *HTTPRouterActorBackend) RevertActor(_ context.Context, _, _ string) error {
	return nil
}
