package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-demos/flux-substrate/internal/dispatcher"
	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/manifests/crds"
)

type dispatcherCLIFlags struct {
	dataDir           *string
	watchNamespace    *string
	tenantCount       *int
	seedFleet         *bool
	gitBaseURL        *string
	controlAddr       *string
	controlTokenFile  *string
	actorImage        *string
	templateName      *string
	routerURL         *string
	routerCAFile      *string
	podCertBundleFile *string
	podCertFile       *string
	podKeyFile        *string
	presenterToken    *string
}

func registerDispatcherFlags(fs *flag.FlagSet) dispatcherCLIFlags {
	defaultTenants := dispatcher.DefaultFleetTenantCount
	if raw := strings.TrimSpace(os.Getenv("TENANT_COUNT")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			defaultTenants = n
		}
	}
	defaultSeedFleet := true
	if raw := strings.TrimSpace(os.Getenv("FLUX_SUBSTRATE_SEED_FLEET")); raw != "" {
		if b, err := strconv.ParseBool(raw); err == nil {
			defaultSeedFleet = b
		}
	}
	return dispatcherCLIFlags{
		dataDir:           fs.String("data-dir", os.Getenv("FLUX_SUBSTRATE_DATA_DIR"), "Local runtime directory for repositories and DurableDir snapshots"),
		watchNamespace:    fs.String("watch-namespace", os.Getenv("WATCH_NAMESPACE"), "Optional namespace filter for Flux GitRepository and Kustomization CRs"),
		tenantCount:       fs.Int("tenant-count", defaultTenants, "Number of Tenant Environments seeded on startup"),
		seedFleet:         fs.Bool("seed-fleet", defaultSeedFleet, "Seed baseline 24-tenant Git repositories, Secrets, RBAC, and Flux CRs on startup"),
		gitBaseURL:        fs.String("git-base-url", envOrDefault("FLUX_SUBSTRATE_GIT_BASE_URL", "http://git-server.flux-substrate.svc.cluster.local"), "In-cluster Smart HTTP Git server base URL written into seeded Flux GitRepository CRs"),
		controlAddr:       fs.String("control-addr", os.Getenv("ATE_CONTROL_ADDR"), "Agent Substrate Control gRPC address (e.g. dns:///api.ate-system.svc:443)"),
		controlTokenFile:  fs.String("control-token-file", os.Getenv("ATE_CONTROL_TOKEN_FILE"), "Projected ServiceAccount token path for Agent Substrate Control gRPC"),
		actorImage:        fs.String("actor-image", os.Getenv("FLUX_SUBSTRATE_ACTOR_IMAGE"), "Digest-pinned Workflow Actor container image for ActorTemplate"),
		templateName:      fs.String("template-name", os.Getenv("ATE_TEMPLATE_NAME"), "Agent Substrate ActorTemplate name (default: flux-reconciler-v1)"),
		routerURL:         fs.String("router-url", os.Getenv("ATENET_ROUTER_URL"), "Agent Substrate atenet-router base URL for --mode=dispatcher"),
		routerCAFile:      fs.String("router-ca-file", os.Getenv("ATENET_ROUTER_CA_FILE"), "CA bundle PEM path for atenet-router and Control gRPC TLS"),
		podCertBundleFile: fs.String("pod-cert-bundle-file", os.Getenv("ATE_POD_CERT_BUNDLE_FILE"), "Pod identity mTLS single-file PEM bundle (cert + key) path"),
		podCertFile:       fs.String("pod-cert-file", os.Getenv("ATE_POD_CERT_FILE"), "Pod identity mTLS client certificate PEM path"),
		podKeyFile:        fs.String("pod-key-file", os.Getenv("ATE_POD_KEY_FILE"), "Pod identity mTLS client key PEM path"),
		presenterToken:    fs.String("presenter-token", os.Getenv("PRESENTER_TOKEN"), "Optional PRESENTER_TOKEN gating mutating Mission-Control POST endpoints"),
	}
}

func buildDispatcherModeHandler(
	ctx context.Context,
	mode string,
	addr string,
	atespace string,
	kubeconfigPath string,
	kubeBridgeURL string,
	opts dispatcherCLIFlags,
) (http.Handler, func(), error) {
	if strings.TrimSpace(atespace) == "" {
		atespace = "flux-substrate"
	}

	switch mode {
	case "local":
		var restCfg *rest.Config
		if kcPath := strings.TrimSpace(kubeconfigPath); kcPath != "" {
			loaded, err := clientcmd.BuildConfigFromFlags("", kcPath)
			if err != nil {
				return nil, nil, fmt.Errorf("load kubeconfig %s: %w", kcPath, err)
			}
			restCfg = loaded
		}
		stack, err := dispatcher.NewLocalStack(ctx, dispatcher.LocalStackConfig{
			BaseDir:        *opts.dataDir,
			Atespace:       atespace,
			TenantCount:    *opts.tenantCount,
			RestConfig:     restCfg,
			WebhookSecret:  os.Getenv("WEBHOOK_SECRET"),
			PresenterToken: *opts.presenterToken,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("initialize --mode=local fleet: %w", err)
		}
		slog.Info("starting standalone local 24-tenant fleet",
			slog.String("addr", addr),
			slog.Int("tenants", *opts.tenantCount))
		return stack.Handler(), stack.Close, nil

	case "dispatcher":
		restCfg, err := loadClusterOrKubeconfig(kubeconfigPath)
		if err != nil {
			return nil, nil, err
		}
		scheme, err := crds.NewScheme()
		if err != nil {
			return nil, nil, err
		}
		kc, err := client.New(restCfg, client.Options{Scheme: scheme})
		if err != nil {
			return nil, nil, fmt.Errorf("initialize Kubernetes client: %w", err)
		}

		var tempDispatcherDir string
		dataRoot := strings.TrimSpace(*opts.dataDir)
		if dataRoot == "" {
			tmp, tmpErr := os.MkdirTemp("", "flux-substrate-dispatcher-*")
			if tmpErr != nil {
				return nil, nil, tmpErr
			}
			dataRoot = tmp
			tempDispatcherDir = tmp
		}

		gs, err := gitserver.New(gitserver.Config{
			ReposDir:     filepath.Join(dataRoot, "repos"),
			DefaultToken: os.Getenv("GIT_SERVER_TOKEN"),
			AdminToken:   os.Getenv("GIT_ADMIN_TOKEN"),
		})
		if err != nil {
			if tempDispatcherDir != "" {
				_ = os.RemoveAll(tempDispatcherDir)
			}
			return nil, nil, fmt.Errorf("initialize embedded Smart HTTP git-server: %w", err)
		}

		if opts.seedFleet != nil && *opts.seedFleet {
			ctrlNS := strings.TrimSpace(*opts.watchNamespace)
			if ctrlNS == "" {
				ctrlNS = atespace
			}
			if seedErr := dispatcher.SeedFleet(ctx, dispatcher.SeedFleetOptions{
				KubeClient:       kc,
				GitServer:        gs,
				GitBaseURL:       *opts.gitBaseURL,
				ControlNamespace: ctrlNS,
				TenantCount:      *opts.tenantCount,
			}); seedErr != nil {
				if tempDispatcherDir != "" {
					_ = os.RemoveAll(tempDispatcherDir)
				}
				return nil, nil, fmt.Errorf("seed %d-tenant cluster fleet: %w", *opts.tenantCount, seedErr)
			}
			slog.Info("seeded cluster 24-tenant GitOps fleet",
				slog.Int("tenants", *opts.tenantCount),
				slog.String("git_base_url", *opts.gitBaseURL))
		}

		var backend dispatcher.ActorBackend
		var backendClose func()
		if strings.TrimSpace(*opts.routerURL) != "" && strings.TrimSpace(*opts.controlAddr) != "" {
			sb, sbErr := dispatcher.NewSubstrateActorBackend(dispatcher.SubstrateBackendConfig{
				Atespace:          atespace,
				TemplateName:      *opts.templateName,
				ActorImage:        *opts.actorImage,
				KubeBridgeURL:     kubeBridgeURL,
				ControlAddr:       *opts.controlAddr,
				ControlTokenFile:  *opts.controlTokenFile,
				RouterURL:         *opts.routerURL,
				RouterCAFile:      *opts.routerCAFile,
				PodCertBundleFile: *opts.podCertBundleFile,
				PodCertFile:       *opts.podCertFile,
				PodKeyFile:        *opts.podKeyFile,
			})
			if sbErr != nil {
				if tempDispatcherDir != "" {
					_ = os.RemoveAll(tempDispatcherDir)
				}
				return nil, nil, fmt.Errorf("initialize Agent Substrate Control ActorBackend: %w", sbErr)
			}
			backend = sb
			backendClose = func() { _ = sb.Close() }
		} else if strings.TrimSpace(*opts.routerURL) != "" {
			rb, rbErr := dispatcher.NewHTTPRouterActorBackend(dispatcher.HTTPRouterBackendConfig{
				RouterURL:    *opts.routerURL,
				RouterCAFile: *opts.routerCAFile,
				PodCertFile:  *opts.podCertFile,
				PodKeyFile:   *opts.podKeyFile,
			})
			if rbErr != nil {
				if tempDispatcherDir != "" {
					_ = os.RemoveAll(tempDispatcherDir)
				}
				return nil, nil, fmt.Errorf("initialize router ActorBackend: %w", rbErr)
			}
			backend = rb
		} else {
			lb, lbErr := dispatcher.NewLocalActorBackend(dispatcher.LocalActorBackendConfig{
				Atespace:       atespace,
				BaseDurableDir: filepath.Join(dataRoot, "actors"),
				RestConfig:     restCfg,
				KubeBridgeURL:  kubeBridgeURL,
			})
			if lbErr != nil {
				if tempDispatcherDir != "" {
					_ = os.RemoveAll(tempDispatcherDir)
				}
				return nil, nil, fmt.Errorf("initialize local fallback ActorBackend: %w", lbErr)
			}
			backend = lb
		}

		disp, err := dispatcher.New(dispatcher.Config{
			Mode:           "dispatcher",
			Atespace:       atespace,
			WatchNamespace: *opts.watchNamespace,
			KubeClient:     kc,
			Backend:        backend,
			GitServer:      gs,
			KubeBridgeURL:  kubeBridgeURL,
			WebhookSecret:  os.Getenv("WEBHOOK_SECRET"),
			PresenterToken: *opts.presenterToken,
		})
		if err != nil {
			if backendClose != nil {
				backendClose()
			}
			if tempDispatcherDir != "" {
				_ = os.RemoveAll(tempDispatcherDir)
			}
			return nil, nil, fmt.Errorf("initialize Dispatcher: %w", err)
		}
		if err := disp.Start(ctx); err != nil {
			if backendClose != nil {
				backendClose()
			}
			if tempDispatcherDir != "" {
				_ = os.RemoveAll(tempDispatcherDir)
			}
			return nil, nil, fmt.Errorf("start Dispatcher: %w", err)
		}
		slog.Info("starting Dispatcher HTTP server",
			slog.String("addr", addr),
			slog.String("atespace", atespace))
		closeFn := func() {
			disp.Close()
			if backendClose != nil {
				backendClose()
			}
			if tempDispatcherDir != "" {
				_ = os.RemoveAll(tempDispatcherDir)
			}
		}

		gitHandler := gs.Handler()
		dispHandler := disp.Handler()
		combinedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/git/") || strings.HasPrefix(r.URL.Path, "/admin/repos/") {
				gitHandler.ServeHTTP(w, r)
				return
			}
			dispHandler.ServeHTTP(w, r)
		})
		return combinedHandler, closeFn, nil

	default:
		return nil, nil, fmt.Errorf("unsupported dispatcher mode %q", mode)
	}
}

func loadClusterOrKubeconfig(kubeconfigPath string) (*rest.Config, error) {
	var (
		cfg *rest.Config
		err error
	)
	if p := strings.TrimSpace(kubeconfigPath); p != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", p)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %s: %w", p, err)
		}
	} else {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load in-cluster Kubernetes config: %w", err)
		}
	}
	cfg.ContentType = "application/json"
	cfg.AcceptContentTypes = "application/json"
	if cfg.QPS == 0 {
		cfg.QPS = 200
	}
	if cfg.Burst == 0 {
		cfg.Burst = 400
	}
	return cfg, nil
}
