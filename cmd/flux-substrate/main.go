// Command flux-substrate is the multi-mode Go entrypoint for Multi-Tenant
// FluxCD on Agent Substrate (exposing actor, git-server, oidc-bridge, dispatcher, and local modes).
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gke-demos/flux-substrate/internal/actor"
	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
	"github.com/gke-demos/flux-substrate/internal/oidcbridge"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("flux-substrate exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("flux-substrate", flag.ContinueOnError)

	mode := fs.String("mode", envOrDefault("FLUX_SUBSTRATE_MODE", "actor"), "Operating mode: actor | git-server | oidc-bridge | dispatcher | local")
	addr := fs.String("addr", envOrDefault("LISTEN_ADDR", ":8080"), "HTTP(S) listen address")
	tlsCertFile := fs.String("tls-cert-file", os.Getenv("TLS_CERT_FILE"), "Optional TLS certificate PEM path (reloaded per handshake)")
	tlsKeyFile := fs.String("tls-key-file", os.Getenv("TLS_KEY_FILE"), "Optional TLS private key PEM path (reloaded per handshake)")

	// Workflow Actor mode flags
	actorName := fs.String("actor-name", os.Getenv("ACTOR_NAME"), "Actor name override (defaults to /run/ate/actor-name)")
	atespace := fs.String("atespace", os.Getenv("ATESPACE"), "Atespace override (defaults to /run/ate/atespace)")
	durableDir := fs.String("durable-dir", envOrDefault("DURABLE_DIR", "/var/lib/flux"), "DurableDir path for warm Git checkouts")
	systemInfoDir := fs.String("system-info-dir", envOrDefault("SYSTEM_INFO_DIR", "/run/ate"), "Substrate SystemInfo mount directory")
	trustBundleFile := fs.String("trust-bundle-file", envOrDefault("TRUST_BUNDLE_FILE", "/run/ate/trust-bundle.pem"), "Projected trustBundle PEM path")
	kubeBridgeURL := fs.String("kube-bridge-url", os.Getenv("KUBE_BRIDGE_URL"), "In-cluster Identity Bridge URL")
	kubeconfig := fs.String("kubeconfig", os.Getenv("KUBECONFIG"), "Optional kubeconfig path")

	// Smart HTTP git-server mode flags
	reposDir := fs.String("repos-dir", envOrDefault("GIT_REPOS_DIR", "/var/lib/git-server/repos"), "Directory storing bare tenant Git repositories")
	seedTenants := fs.String("seed-tenants", os.Getenv("GIT_SEED_TENANTS"), "Comma-separated tenant IDs to seed on startup (e.g. tenant-01,tenant-02)")
	seedCount := fs.Int("seed-count", 0, "Seed tenant-01..tenant-NN on startup when > 0")
	tenantTokensDir := fs.String("tenant-tokens-dir", os.Getenv("GIT_TENANT_TOKENS_DIR"), "Optional directory containing per-tenant token files")
	dispFlags := registerDispatcherFlags(fs)

	// Identity Bridge (oidc-bridge) mode flags
	oidcIssuerURL := fs.String("oidc-issuer-url", os.Getenv("OIDC_ISSUER_URL"), "Trusted OIDC issuer URL (e.g. https://idp.ate-system.svc)")
	oidcAudience := fs.String("oidc-audience", os.Getenv("OIDC_AUDIENCE"), "Comma-separated expected JWT audiences for Identity Bridge")
	oidcCAFile := fs.String("oidc-ca-file", os.Getenv("OIDC_CA_FILE"), "Optional CA PEM bundle path for trusting the OIDC issuer")
	upstreamKubeURL := fs.String("upstream-kube-url", os.Getenv("UPSTREAM_KUBE_URL"), "Optional upstream kube-apiserver URL override")

	if err := fs.Parse(args); err != nil {
		return err
	}

	var (
		handler http.Handler
		cleanup func()
	)
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	switch strings.TrimSpace(*mode) {
	case "actor":
		srv, err := actor.NewServer(actor.Config{
			ActorName:       *actorName,
			Atespace:        *atespace,
			DurableDir:      *durableDir,
			SystemInfoDir:   *systemInfoDir,
			TrustBundleFile: *trustBundleFile,
			GitBearerToken:  os.Getenv("GIT_BEARER_TOKEN"),
			KubeBridgeURL:   *kubeBridgeURL,
			KubeBearerToken: os.Getenv("KUBE_BEARER_TOKEN"),
			KubeconfigPath:  *kubeconfig,
		})
		if err != nil {
			return fmt.Errorf("initialize Workflow Actor: %w", err)
		}
		slog.Info("starting Workflow Actor HTTP server",
			slog.String("addr", *addr),
			slog.String("durable_dir", *durableDir))
		handler = srv.Handler()

	case "git-server":
		tokenMap, err := loadTenantTokensDir(*tenantTokensDir)
		if err != nil {
			return fmt.Errorf("load tenant tokens dir: %w", err)
		}
		gs, err := gitserver.New(gitserver.Config{
			ReposDir:     *reposDir,
			DefaultToken: os.Getenv("GIT_SERVER_TOKEN"),
			AdminToken:   os.Getenv("GIT_ADMIN_TOKEN"),
			TenantTokens: tokenMap,
		})
		if err != nil {
			return fmt.Errorf("initialize Smart HTTP git-server: %w", err)
		}
		for _, tenantID := range resolveSeedTenants(*seedTenants, *seedCount) {
			rev, seedErr := gs.SeedDefaultTenant(tenantID, tenantID)
			if seedErr != nil {
				return fmt.Errorf("seed tenant %s: %w", tenantID, seedErr)
			}
			slog.Info("seeded tenant Git repository",
				slog.String("tenant", tenantID),
				slog.String("sha", rev.SHA))
		}
		slog.Info("starting Smart HTTP git-server",
			slog.String("addr", *addr),
			slog.String("repos_dir", *reposDir))
		handler = gs.Handler()

	case "oidc-bridge":
		bridge, err := oidcbridge.NewServer(oidcbridge.Config{
			IssuerURL:       *oidcIssuerURL,
			Audiences:       strings.Split(*oidcAudience, ","),
			IssuerCAFile:    *oidcCAFile,
			KubeconfigPath:  *kubeconfig,
			UpstreamKubeURL: *upstreamKubeURL,
		})
		if err != nil {
			return fmt.Errorf("initialize Identity Bridge: %w", err)
		}
		slog.Info("starting Identity Bridge HTTP server",
			slog.String("addr", *addr),
			slog.String("issuer_url", *oidcIssuerURL))
		handler = bridge.Handler()

	case "dispatcher", "local":
		h, closeFn, err := buildDispatcherModeHandler(ctx, strings.TrimSpace(*mode), *addr, *atespace, *kubeconfig, *kubeBridgeURL, dispFlags)
		if err != nil {
			return err
		}
		handler = h
		cleanup = closeFn

	default:
		return fmt.Errorf("unsupported --mode %q (supported modes: actor, git-server, oidc-bridge, dispatcher, local)", *mode)
	}

	return serveHTTP(ctx, *addr, *tlsCertFile, *tlsKeyFile, handler)
}

func serveHTTP(ctx context.Context, addr, certFile, keyFile string, handler http.Handler) error {
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		if certFile != "" && keyFile != "" {
			httpSrv.TLSConfig = &tls.Config{
				MinVersion: tls.VersionTLS12,
				GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
					cert, err := tls.LoadX509KeyPair(certFile, keyFile)
					if err != nil {
						return nil, err
					}
					return &cert, nil
				},
			}
			errCh <- httpSrv.ListenAndServeTLS("", "")
			return
		}
		errCh <- httpSrv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutdownCtx)
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func resolveSeedTenants(csv string, count int) []string {
	seen := make(map[string]bool)
	var out []string
	add := func(id string) {
		id = strings.TrimSpace(id)
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for item := range strings.SplitSeq(csv, ",") {
		add(item)
	}
	for i := 1; i <= count; i++ {
		add(fmt.Sprintf("tenant-%02d", i))
	}
	return out
}

func loadTenantTokensDir(dir string) (map[string]string, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	tokens := make(map[string]string)
	for _, entry := range entries {
		if !entry.Type().IsRegular() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		tokenFile, err := httputil.SecureSubpath(dir, entry.Name())
		if err != nil {
			continue
		}
		raw, err := os.ReadFile(tokenFile)
		if err != nil {
			return nil, err
		}
		if tok := strings.TrimSpace(string(raw)); tok != "" {
			tokens[entry.Name()] = tok
		}
	}
	return tokens, nil
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
