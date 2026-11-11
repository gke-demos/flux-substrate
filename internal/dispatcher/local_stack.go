package dispatcher

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
	"github.com/gke-demos/flux-substrate/manifests/crds"
)

// LocalStackConfig configures the standalone single-process --mode=local runtime.
type LocalStackConfig struct {
	BaseDir        string
	Atespace       string
	TenantCount    int
	RestConfig     *rest.Config
	SyncInterval   time.Duration
	RetryInterval  time.Duration
	WebhookSecret  string
	PresenterToken string
}

// LocalStack bundles the in-process Smart HTTP TLS Git server, capacity-bounded
// 9-slot LocalActorBackend, target Kubernetes store, and Dispatcher for --mode=local.
type LocalStack struct {
	dispatcher    *Dispatcher
	localBackend  *LocalActorBackend
	gitServer     *gitserver.Server
	kubeClient    client.Client
	localKube     *LocalKubeServer
	gitLn         net.Listener
	gitHTTPSrv    *http.Server
	ownedTempRoot string
}

// NewLocalStack initializes and boots the complete 24-tenant local GitOps fleet.
func NewLocalStack(ctx context.Context, cfg LocalStackConfig) (*LocalStack, error) {
	baseDir := strings.TrimSpace(cfg.BaseDir)
	var ownedTempRoot string
	if baseDir == "" {
		tmp, err := os.MkdirTemp("", "flux-substrate-local-*")
		if err != nil {
			return nil, fmt.Errorf("create local runtime directory: %w", err)
		}
		baseDir = tmp
		ownedTempRoot = tmp
	} else if err := os.MkdirAll(baseDir, 0o750); err != nil {
		return nil, fmt.Errorf("create local data directory: %w", err)
	}

	cleanupOnErr := func(ls *LocalStack) {
		if ls != nil {
			ls.Close()
		} else if ownedTempRoot != "" {
			_ = os.RemoveAll(ownedTempRoot)
		}
	}

	atespace := strings.TrimSpace(cfg.Atespace)
	if atespace == "" {
		atespace = defaultAtespace
	}
	tenantCount := cfg.TenantCount
	if tenantCount <= 0 {
		tenantCount = DefaultFleetTenantCount
	}

	stack := &LocalStack{ownedTempRoot: ownedTempRoot}

	restCfg := cfg.RestConfig
	if restCfg == nil {
		lk, err := NewLocalKubeServer()
		if err != nil {
			cleanupOnErr(stack)
			return nil, err
		}
		stack.localKube = lk
		restCfg = lk.RestConfig()
	}

	scheme, err := crds.NewScheme()
	if err != nil {
		cleanupOnErr(stack)
		return nil, err
	}
	kc, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		cleanupOnErr(stack)
		return nil, fmt.Errorf("create local Kubernetes client: %w", err)
	}
	stack.kubeClient = kc

	gitToken, err := httputil.RandomToken(32)
	if err != nil {
		cleanupOnErr(stack)
		return nil, err
	}
	adminToken, err := httputil.RandomToken(32)
	if err != nil {
		cleanupOnErr(stack)
		return nil, err
	}

	gs, err := gitserver.New(gitserver.Config{
		ReposDir:     filepath.Join(baseDir, "repos"),
		DefaultToken: gitToken,
		AdminToken:   adminToken,
	})
	if err != nil {
		cleanupOnErr(stack)
		return nil, fmt.Errorf("start local git-server: %w", err)
	}
	stack.gitServer = gs

	tlsCert, certPEM, err := generateLoopbackTLSCert()
	if err != nil {
		cleanupOnErr(stack)
		return nil, err
	}

	gitLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cleanupOnErr(stack)
		return nil, fmt.Errorf("listen 127.0.0.1:0 for local Git TLS server: %w", err)
	}
	stack.gitLn = gitLn

	// Simulate atenet-egress replaceHeaders so Workflow Actors carry zero Git tokens.
	egressGitHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		if r2.Header.Get("Authorization") == "Bearer actor-placeholder" {
			r2.Header.Set("Authorization", "Bearer "+gitToken)
		}
		gs.Handler().ServeHTTP(w, r2)
	})

	stack.gitHTTPSrv = &http.Server{
		Handler:           egressGitHandler,
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{tlsCert},
		},
	}
	go func() {
		_ = stack.gitHTTPSrv.ServeTLS(gitLn, "", "")
	}()

	gitBaseURL := "https://" + gitLn.Addr().String()

	localBackend, err := NewLocalActorBackend(LocalActorBackendConfig{
		Atespace:           atespace,
		BaseDurableDir:     filepath.Join(baseDir, "actors"),
		RestConfig:         restCfg,
		TrustBundlePEM:     certPEM,
		MaxConcurrentSlots: defaultWorkerCount * defaultSlotsPerWorker,
	})
	if err != nil {
		cleanupOnErr(stack)
		return nil, err
	}
	stack.localBackend = localBackend

	if err := SeedFleet(ctx, SeedFleetOptions{
		KubeClient:       kc,
		GitServer:        gs,
		GitBaseURL:       gitBaseURL,
		ControlNamespace: atespace,
		TenantCount:      tenantCount,
		SyncInterval:     cfg.SyncInterval,
		RetryInterval:    cfg.RetryInterval,
	}); err != nil {
		cleanupOnErr(stack)
		return nil, fmt.Errorf("seed %d-tenant local fleet: %w", tenantCount, err)
	}

	disp, err := New(Config{
		Mode:           "local",
		Atespace:       atespace,
		WatchNamespace: atespace,
		KubeClient:     kc,
		Backend:        localBackend,
		GitServer:      gs,
		KubeBridgeURL:  "https://kube-bridge.flux-substrate.svc",
		WebhookSecret:  cfg.WebhookSecret,
		PresenterToken: cfg.PresenterToken,
	})
	if err != nil {
		cleanupOnErr(stack)
		return nil, err
	}
	stack.dispatcher = disp

	if err := disp.Start(ctx); err != nil {
		cleanupOnErr(stack)
		return nil, err
	}

	return stack, nil
}

// Handler returns the Dispatcher HTTP handler serving /api/* and webhook routes.
func (s *LocalStack) Handler() http.Handler {
	return s.dispatcher.Handler()
}

// Dispatcher returns the underlying Dispatcher instance.
func (s *LocalStack) Dispatcher() *Dispatcher {
	return s.dispatcher
}

// LocalBackend returns the capacity-bounded LocalActorBackend instance.
func (s *LocalStack) LocalBackend() *LocalActorBackend {
	return s.localBackend
}

// GitServer returns the in-process Smart HTTP GitServer.
func (s *LocalStack) GitServer() *gitserver.Server {
	return s.gitServer
}

// KubeClient returns the Kubernetes client connected to the local target store.
func (s *LocalStack) KubeClient() client.Client {
	return s.kubeClient
}

// Close shuts down the Dispatcher, loopback Git server, and loopback Kube store.
func (s *LocalStack) Close() {
	if s.dispatcher != nil {
		s.dispatcher.Close()
	}
	if s.gitHTTPSrv != nil {
		_ = s.gitHTTPSrv.Close()
	}
	if s.gitLn != nil {
		_ = s.gitLn.Close()
	}
	if s.localKube != nil {
		s.localKube.Close()
	}
	if s.ownedTempRoot != "" {
		_ = os.RemoveAll(s.ownedTempRoot)
	}
}

func generateLoopbackTLSCert() (tls.Certificate, []byte, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	now := time.Now().UTC()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	return pair, certPEM, nil
}
