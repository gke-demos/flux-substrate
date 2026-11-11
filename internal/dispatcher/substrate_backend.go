package dispatcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/gke-demos/flux-substrate/internal/actor"
	ateapi "github.com/gke-demos/flux-substrate/internal/ateapi/v1"
)

const (
	// DefaultActorTemplateName is the shared ActorTemplate provisioned for all 24 Tenant Environments.
	DefaultActorTemplateName = "flux-reconciler-v1"
	// DefaultWorkerRoleLabelKey is the WorkerPool selector key pinning Flux reconciler actors.
	DefaultWorkerRoleLabelKey = "flux-substrate.io/role"
	// DefaultWorkerRoleLabelValue is the WorkerPool selector value for Flux reconciler actors.
	DefaultWorkerRoleLabelValue = "flux-reconciler"

	defaultActorCPUMillicores = 300
	defaultActorMemoryBytes   = 256 * 1024 * 1024 // 256 MiB

	defaultRouterTrustBundlePath = "/run/router-client/trust-bundle.pem"
	defaultRouterCredBundlePath  = "/run/router-client/credential-bundle.pem"
	defaultAPITokenPath          = "/run/router-client/ate-api-token"
	roundRobinGRPCServiceConfig  = `{"loadBalancingConfig": [{"round_robin":{}}]}`
)

// SubstrateBackendConfig configures the Agent Substrate v1.0.0 ControlClient gRPC
// and atenet-router HTTP(S) ActorBackend.
type SubstrateBackendConfig struct {
	Atespace          string
	TemplateName      string
	ActorImage        string
	KubeBridgeURL     string
	ControlAddr       string
	ControlServerName string
	ControlTokenFile  string
	ControlClient     ateapi.ControlClient
	ControlInsecure   bool
	RouterURL         string
	RouterServerName  string
	RouterCAFile      string
	RouterCAPEM       []byte
	PodCertBundleFile string
	PodCertFile       string
	PodKeyFile        string
	TurnTimeout       time.Duration
	CPUMillicores     int64
	MemoryBytes       int64
	WorkerMatchLabels map[string]string
}

// SubstrateActorBackend implements ActorBackend against a live Agent Substrate v1.0.0
// Control gRPC service (ate-api-server) and atenet-router.
type SubstrateActorBackend struct {
	cfg           SubstrateBackendConfig
	atespace      string
	templateName  string
	actorImage    string
	kubeBridgeURL string
	routerURL     string
	controlClient ateapi.ControlClient
	grpcConn      *grpc.ClientConn
	httpClient    *http.Client

	mu              sync.Mutex
	templateEnsured map[string]bool
}

// NewSubstrateActorBackend constructs an ActorBackend backed by Agent Substrate v1.0.0
// ControlClient gRPC and atenet-router HTTP(S) forwarding.
func NewSubstrateActorBackend(cfg SubstrateBackendConfig) (*SubstrateActorBackend, error) {
	routerURL := strings.TrimRight(strings.TrimSpace(cfg.RouterURL), "/")
	if routerURL == "" {
		return nil, errors.New("RouterURL is required for SubstrateActorBackend")
	}
	if cfg.ControlClient == nil && strings.TrimSpace(cfg.ControlAddr) == "" {
		return nil, errors.New("ControlClient or ControlAddr is required for SubstrateActorBackend")
	}

	atespace := strings.TrimSpace(cfg.Atespace)
	if atespace == "" {
		atespace = defaultAtespace
	}
	templateName := strings.TrimSpace(cfg.TemplateName)
	if templateName == "" {
		templateName = DefaultActorTemplateName
	}
	actorImage := strings.TrimSpace(cfg.ActorImage)
	if actorImage == "" {
		actorImage = "ko.local/flux-substrate@sha256:0000000000000000000000000000000000000000000000000000000000000000"
	}
	if cfg.CPUMillicores <= 0 {
		cfg.CPUMillicores = defaultActorCPUMillicores
	}
	if cfg.MemoryBytes <= 0 {
		cfg.MemoryBytes = defaultActorMemoryBytes
	}

	if strings.TrimSpace(cfg.RouterCAFile) == "" && len(cfg.RouterCAPEM) == 0 && fileExists(defaultRouterTrustBundlePath) {
		cfg.RouterCAFile = defaultRouterTrustBundlePath
	}
	if strings.TrimSpace(cfg.PodCertBundleFile) == "" && strings.TrimSpace(cfg.PodCertFile) == "" && fileExists(defaultRouterCredBundlePath) {
		cfg.PodCertBundleFile = defaultRouterCredBundlePath
	}
	if strings.TrimSpace(cfg.ControlTokenFile) == "" && !cfg.ControlInsecure && fileExists(defaultAPITokenPath) {
		cfg.ControlTokenFile = defaultAPITokenPath
	}

	routerTLS, err := buildDynamicMTLSConfig(
		cfg.RouterCAPEM,
		cfg.RouterCAFile,
		cfg.PodCertBundleFile,
		cfg.PodCertFile,
		cfg.PodKeyFile,
		resolveRouterServerName(routerURL, cfg.RouterServerName),
	)
	if err != nil {
		return nil, err
	}

	var grpcConn *grpc.ClientConn
	controlClient := cfg.ControlClient
	if controlClient == nil {
		var dialOpts []grpc.DialOption
		if cfg.ControlInsecure {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		} else {
			controlTLS, tlsErr := buildDynamicMTLSConfig(
				cfg.RouterCAPEM,
				cfg.RouterCAFile,
				"",
				"",
				"",
				resolveControlServerName(cfg.ControlAddr, cfg.ControlServerName),
			)
			if tlsErr != nil {
				return nil, tlsErr
			}
			dialOpts = append(dialOpts,
				grpc.WithTransportCredentials(credentials.NewTLS(controlTLS)),
				grpc.WithDefaultServiceConfig(roundRobinGRPCServiceConfig),
			)
			if tokFile := strings.TrimSpace(cfg.ControlTokenFile); tokFile != "" {
				dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(fileBearerTokenCreds(tokFile)))
			}
		}
		conn, err := grpc.NewClient(strings.TrimSpace(cfg.ControlAddr), dialOpts...)
		if err != nil {
			return nil, fmt.Errorf("dial Agent Substrate Control gRPC %s: %w", cfg.ControlAddr, err)
		}
		grpcConn = conn
		controlClient = ateapi.NewControlClient(conn)
	}

	timeout := cfg.TurnTimeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}

	isHTTPSRouter := strings.HasPrefix(strings.ToLower(routerURL), "https://")
	return &SubstrateActorBackend{
		cfg:           cfg,
		atespace:      atespace,
		templateName:  templateName,
		actorImage:    actorImage,
		kubeBridgeURL: strings.TrimSpace(cfg.KubeBridgeURL),
		routerURL:     routerURL,
		controlClient: controlClient,
		grpcConn:      grpcConn,
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				TLSClientConfig:     routerTLS,
				ForceAttemptHTTP2:   isHTTPSRouter,
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		templateEnsured: make(map[string]bool),
	}, nil
}

// Close closes the underlying gRPC connection if owned by this backend.
func (b *SubstrateActorBackend) Close() error {
	if b.grpcConn != nil {
		return b.grpcConn.Close()
	}
	return nil
}

// EnsureTemplate ensures the Atespace and shared SNAPSHOT_FIDELITY_VOLUMES ActorTemplate
// exist in Agent Substrate before provisioning tenant Workflow Actors.
func (b *SubstrateActorBackend) EnsureTemplate(ctx context.Context, atespace string) error {
	if strings.TrimSpace(atespace) == "" {
		atespace = b.atespace
	}

	b.mu.Lock()
	if b.templateEnsured[atespace] {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	_, err := b.controlClient.GetAtespace(ctx, &ateapi.GetAtespaceRequest{
		Atespace: &ateapi.ObjectRef{Name: atespace},
	})
	if status.Code(err) == codes.NotFound {
		_, err = b.controlClient.CreateAtespace(ctx, &ateapi.CreateAtespaceRequest{
			Atespace: &ateapi.Atespace{
				Metadata: &ateapi.ResourceMetadata{Name: atespace},
			},
		})
	}
	if err != nil && status.Code(err) != codes.AlreadyExists && status.Code(err) != codes.PermissionDenied {
		return fmt.Errorf("ensure Atespace %s: %w", atespace, err)
	}

	desiredTemplate := b.buildDesiredActorTemplate(atespace)
	_, err = b.controlClient.GetActorTemplate(ctx, &ateapi.GetActorTemplateRequest{
		ActorTemplate: &ateapi.ObjectRef{
			Atespace: atespace,
			Name:     b.templateName,
		},
	})
	if status.Code(err) == codes.NotFound {
		_, err = b.controlClient.CreateActorTemplate(ctx, &ateapi.CreateActorTemplateRequest{
			ActorTemplate: desiredTemplate,
		})
	}
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("ensure ActorTemplate %s/%s: %w", atespace, b.templateName, err)
	}

	b.mu.Lock()
	b.templateEnsured[atespace] = true
	b.mu.Unlock()
	return nil
}

func (b *SubstrateActorBackend) buildDesiredActorTemplate(atespace string) *ateapi.ActorTemplate {
	matchLabels := map[string]string{
		DefaultWorkerRoleLabelKey: DefaultWorkerRoleLabelValue,
	}
	for k, v := range b.cfg.WorkerMatchLabels {
		matchLabels[k] = v
	}

	args := []string{
		"--mode=actor",
		"--addr=:80",
		"--durable-dir=/var/lib/flux",
	}
	if b.kubeBridgeURL != "" {
		args = append(args, "--kube-bridge-url="+b.kubeBridgeURL)
	}

	return &ateapi.ActorTemplate{
		Metadata: &ateapi.ResourceMetadata{
			Atespace: atespace,
			Name:     b.templateName,
		},
		WorkerSelector: &ateapi.Selector{
			MatchLabels: matchLabels,
		},
		SnapshotConfig: &ateapi.SnapshotConfig{
			PreferredFidelity: ateapi.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			GoldenSnapshotConfig: &ateapi.GoldenSnapshotConfig{
				Mode: ateapi.GoldenSnapshotMode_GOLDEN_SNAPSHOT_MODE_DISABLED,
			},
		},
		Resources: &ateapi.ResourceRequirements{
			Limits: &ateapi.Resources{
				CpuMillicores: b.cfg.CPUMillicores,
				MemoryBytes:   b.cfg.MemoryBytes,
			},
		},
		Volumes: []*ateapi.Volume{
			{
				Name:       "flux-cache",
				DurableDir: &ateapi.DurableDirVolumeSource{},
			},
			{
				Name: "ate-system-info",
				SystemInfo: &ateapi.SystemInfoVolumeSource{
					DataSources: []*ateapi.SystemInfoDataSource{
						{
							ActorMetadata: &ateapi.ActorMetadataDataSource{
								Items: []*ateapi.ActorMetadataItem{
									{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "actor-name"},
									{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
									{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "actor-uid"},
								},
							},
						},
						{
							TrustBundle: &ateapi.TrustBundleDataSource{
								Names: []string{"egress-mitm.ate.dev", "system-roots.ate.dev"},
								Path:  "trust-bundle.pem",
							},
						},
					},
				},
			},
		},
		Containers: []*ateapi.Container{
			{
				Name:  "reconciler",
				Image: b.actorImage,
				Args:  args,
				Env: []*ateapi.EnvVar{
					{Name: "SSL_CERT_FILE", Value: "/run/ate/trust-bundle.pem"},
					{Name: "GIT_SSL_CAINFO", Value: "/run/ate/trust-bundle.pem"},
				},
				WakeupProbe: &ateapi.WakeupProbe{
					HttpGet: &ateapi.HttpGetAction{
						Path: "/readyz",
						Port: 80,
					},
					TimeoutSeconds: 30,
				},
				VolumeMounts: []*ateapi.VolumeMount{
					{Name: "flux-cache", MountPath: "/var/lib/flux"},
					{Name: "ate-system-info", MountPath: "/run/ate"},
				},
			},
		},
	}
}

// EnsureActor ensures the tenant's Workflow Actor exists in Agent Substrate.
func (b *SubstrateActorBackend) EnsureActor(ctx context.Context, spec ActorSpec) error {
	atespace := strings.TrimSpace(spec.Atespace)
	if atespace == "" {
		atespace = b.atespace
	}
	if err := b.EnsureTemplate(ctx, atespace); err != nil {
		return err
	}

	ref := &ateapi.ObjectRef{
		Atespace: atespace,
		Name:     spec.ActorName,
	}
	_, err := b.controlClient.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return fmt.Errorf("GetActor %s/%s: %w", atespace, spec.ActorName, err)
	}

	_, err = b.controlClient.CreateActor(ctx, &ateapi.CreateActorRequest{
		Actor: &ateapi.Actor{
			Metadata: &ateapi.ResourceMetadata{
				Atespace: atespace,
				Name:     spec.ActorName,
			},
			ActorTemplate: &ateapi.ObjectRef{
				Atespace: atespace,
				Name:     b.templateName,
			},
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		return fmt.Errorf("CreateActor %s/%s: %w", atespace, spec.ActorName, err)
	}
	return nil
}

// EnsureEgressPolicy creates or updates the singleton "default" ActorEgressPolicy for a Workflow Actor,
// configuring replaceHeaders with actorJwt for the Identity Bridge and ate-secret:// for Git HTTP(S).
func (b *SubstrateActorBackend) EnsureEgressPolicy(ctx context.Context, policy EgressPolicySpec) error {
	atespace := strings.TrimSpace(policy.Atespace)
	if atespace == "" {
		atespace = b.atespace
	}
	actorRef := &ateapi.ObjectRef{
		Atespace: atespace,
		Name:     policy.ActorName,
	}
	rules := buildAteEgressRules(policy)

	existing, err := b.controlClient.GetActorEgressPolicy(ctx, &ateapi.GetActorEgressPolicyRequest{
		Actor: actorRef,
		Name:  "default",
	})
	if status.Code(err) == codes.NotFound {
		_, createErr := b.controlClient.CreateActorEgressPolicy(ctx, &ateapi.CreateActorEgressPolicyRequest{
			Actor: actorRef,
			EgressPolicy: &ateapi.EgressPolicy{
				Metadata: &ateapi.ResourceMetadata{
					Atespace: atespace,
					Name:     "default",
				},
				Rules: rules,
			},
		})
		if createErr != nil && status.Code(createErr) != codes.AlreadyExists {
			return fmt.Errorf("CreateActorEgressPolicy %s/%s: %w", atespace, policy.ActorName, createErr)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("GetActorEgressPolicy %s/%s: %w", atespace, policy.ActorName, err)
	}

	meta := existing.GetMetadata()
	if meta == nil {
		meta = &ateapi.ResourceMetadata{
			Atespace: atespace,
			Name:     "default",
		}
	}
	_, err = b.controlClient.UpdateActorEgressPolicy(ctx, &ateapi.UpdateActorEgressPolicyRequest{
		Actor: actorRef,
		EgressPolicy: &ateapi.EgressPolicy{
			Metadata: meta,
			Rules:    rules,
		},
	})
	if err != nil {
		return fmt.Errorf("UpdateActorEgressPolicy %s/%s: %w", atespace, policy.ActorName, err)
	}
	return nil
}

func buildAteEgressRules(policy EgressPolicySpec) []*ateapi.EgressRule {
	var rules []*ateapi.EgressRule

	gitScheme := strings.ToLower(strings.TrimSpace(policy.GitScheme))
	if gitScheme == "" {
		gitScheme = "https"
	}
	gitHost, _ := splitHostAndDefaultPort(policy.GitHost, gitScheme)
	if gitHost != "" {
		var gitEffects *ateapi.HttpRuleEffects
		if credURI := strings.TrimSpace(policy.GitCredentialURI); credURI != "" {
			gitEffects = &ateapi.HttpRuleEffects{
				ReplaceHeaders: []*ateapi.CredentialHeader{
					{
						Header:        "Authorization",
						Prefix:        "Bearer ",
						CredentialUri: credURI,
					},
				},
			}
		}
		if gitScheme == "http" {
			httpHosts := []string{gitHost}
			const sharedDemoRepoHost = "git-oci.demo-repo.svc.cluster.local"
			if gitHost != sharedDemoRepoHost {
				httpHosts = append(httpHosts, sharedDemoRepoHost)
			}
			rules = append(rules, &ateapi.EgressRule{
				Http: &ateapi.HttpRule{
					Hostnames: httpHosts,
					Ports:     &ateapi.Ports{Numbers: []int32{80}},
					Effects:   gitEffects,
				},
			})
		} else {
			rules = append(rules, &ateapi.EgressRule{
				Https: &ateapi.HttpsRule{
					Hostnames: []string{gitHost},
					Ports:     &ateapi.Ports{Numbers: []int32{443}},
					Effects:   gitEffects,
				},
			})
		}
	}

	aud := strings.TrimSpace(policy.KubeBridgeAudience)
	bridgeScheme := "https"
	if strings.HasPrefix(strings.ToLower(aud), "http://") {
		bridgeScheme = "http"
	}
	bridgeHost, _ := splitHostAndDefaultPort(policy.KubeBridgeHost, bridgeScheme)
	if bridgeHost != "" && !(bridgeHost == gitHost && bridgeScheme == gitScheme) {
		if aud == "" {
			aud = bridgeScheme + "://" + bridgeHost
		}
		expSecs := policy.ActorJWTLifetimeSecs
		if expSecs < 300 {
			expSecs = 600
		}
		if expSecs > 3600 {
			expSecs = 3600
		}
		bridgeEffects := &ateapi.HttpRuleEffects{
			ReplaceHeaders: []*ateapi.CredentialHeader{
				{
					Header: "Authorization",
					Prefix: "Bearer ",
					ActorJwt: &ateapi.ActorJwtSource{
						Audiences:         []string{aud},
						ExpirationSeconds: expSecs,
					},
				},
			},
		}
		if bridgeScheme == "http" {
			rules = append(rules, &ateapi.EgressRule{
				Http: &ateapi.HttpRule{
					Hostnames: []string{bridgeHost},
					Ports:     &ateapi.Ports{Numbers: []int32{80}},
					Effects:   bridgeEffects,
				},
			})
		} else {
			rules = append(rules, &ateapi.EgressRule{
				Https: &ateapi.HttpsRule{
					Hostnames: []string{bridgeHost},
					Ports:     &ateapi.Ports{Numbers: []int32{443}},
					Effects:   bridgeEffects,
				},
			})
		}
	}

	return rules
}

func splitHostAndDefaultPort(rawHost, scheme string) (string, int32) {
	rawHost = strings.TrimSpace(rawHost)
	if rawHost == "" {
		return "", 0
	}
	defaultPort := int32(443)
	if strings.EqualFold(scheme, "http") {
		defaultPort = 80
	}
	if h, pStr, err := net.SplitHostPort(rawHost); err == nil {
		if p, convErr := strconv.Atoi(pStr); convErr == nil && p > 0 && p <= 65535 {
			return strings.ToLower(h), int32(p)
		}
		return strings.ToLower(h), defaultPort
	}
	return strings.ToLower(rawHost), defaultPort
}

// InvokeReconcile wakes a SUSPENDED actor via ResumeActor, sends POST /reconcile through
// atenet-router with ate-target-actor: <atespace>/<actor>, and converts HTTP 507 or
// ACTOR_STATE_CRASHED into an *ActorCrashError.
func (b *SubstrateActorBackend) InvokeReconcile(ctx context.Context, inv TurnInvocation) (actor.ReconcileResult, int, error) {
	atespace := strings.TrimSpace(inv.Atespace)
	if atespace == "" {
		atespace = b.atespace
	}

	var resumeErr error
	for attempt := 1; attempt <= 8; attempt++ {
		_, resumeErr = b.controlClient.ResumeActor(ctx, &ateapi.ResumeActorRequest{
			Actor: &ateapi.ObjectRef{
				Atespace: atespace,
				Name:     inv.ActorName,
			},
		})
		if resumeErr == nil || status.Code(resumeErr) == codes.Unimplemented {
			break
		}
		if (status.Code(resumeErr) == codes.Aborted || status.Code(resumeErr) == codes.Unavailable) && attempt < 8 && ctx.Err() == nil {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		break
	}
	if resumeErr != nil && status.Code(resumeErr) != codes.Unimplemented {
		if crashErr := b.checkActorCrashed(ctx, atespace, inv.ActorName, resumeErr.Error()); crashErr != nil {
			return actor.ReconcileResult{
				TenantID:    inv.Request.TenantID,
				FailedStage: "render",
				Error:       crashErr.Error(),
			}, http.StatusInsufficientStorage, crashErr
		}
		return actor.ReconcileResult{}, http.StatusBadGateway, fmt.Errorf("ResumeActor %s/%s: %w", atespace, inv.ActorName, resumeErr)
	}

	body, err := json.Marshal(inv.Request)
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, err
	}

	const maxRouteAttempts = 15
	for attempt := 1; attempt <= maxRouteAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.routerURL+"/reconcile", bytes.NewReader(body))
		if err != nil {
			return actor.ReconcileResult{}, http.StatusInternalServerError, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("ate-target-actor", fmt.Sprintf("%s/%s", atespace, inv.ActorName))

		resp, err := b.httpClient.Do(req)
		if err != nil {
			if crashErr := b.checkActorCrashed(ctx, atespace, inv.ActorName, err.Error()); crashErr != nil {
				return actor.ReconcileResult{
					TenantID:    inv.Request.TenantID,
					FailedStage: "render",
					Error:       crashErr.Error(),
				}, http.StatusInsufficientStorage, crashErr
			}
			if attempt < maxRouteAttempts && ctx.Err() == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
			return actor.ReconcileResult{}, http.StatusBadGateway, err
		}

		var result actor.ReconcileResult
		decodeErr := json.NewDecoder(resp.Body).Decode(&result)
		_ = resp.Body.Close()

		if resp.StatusCode == http.StatusInsufficientStorage {
			reason := result.Error
			if reason == "" {
				reason = "sandbox memory limit exceeded (HTTP 507)"
			}
			crashErr := &ActorCrashError{
				ActorName: inv.ActorName,
				Reason:    reason,
			}
			if result.FailedStage == "" {
				result.FailedStage = "render"
			}
			if result.Error == "" {
				result.Error = crashErr.Error()
			}
			return result, resp.StatusCode, crashErr
		}

		if resp.StatusCode >= http.StatusInternalServerError {
			if crashErr := b.checkActorCrashed(ctx, atespace, inv.ActorName, result.Error); crashErr != nil {
				if result.FailedStage == "" {
					result.FailedStage = "render"
				}
				if result.Error == "" {
					result.Error = crashErr.Error()
				}
				return result, http.StatusInsufficientStorage, crashErr
			}
			// If atenet-router returned a non-JSON 502/503/504 while Envoy xDS routes propagate
			// right after ResumeActor, retry briefly.
			if (resp.StatusCode == http.StatusBadGateway ||
				resp.StatusCode == http.StatusServiceUnavailable ||
				resp.StatusCode == http.StatusGatewayTimeout) &&
				result.FailedStage == "" && attempt < maxRouteAttempts && ctx.Err() == nil {
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}

		if decodeErr != nil {
			return actor.ReconcileResult{}, resp.StatusCode, fmt.Errorf("decode router ReconcileResult: %w", decodeErr)
		}
		return result, resp.StatusCode, nil
	}

	return actor.ReconcileResult{}, http.StatusBadGateway, errors.New("exhausted atenet-router retry attempts")
}

func (b *SubstrateActorBackend) checkActorCrashed(ctx context.Context, atespace, actorName, fallbackReason string) *ActorCrashError {
	act, err := b.controlClient.GetActor(ctx, &ateapi.GetActorRequest{
		Actor: &ateapi.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	})
	if err != nil || act.GetStatus().GetState() != ateapi.ActorState_ACTOR_STATE_CRASHED {
		return nil
	}
	reason := strings.TrimSpace(act.GetStatus().GetCrash().GetMessage())
	if reason == "" {
		reason = strings.TrimSpace(fallbackReason)
	}
	return &ActorCrashError{
		ActorName: actorName,
		Reason:    reason,
	}
}

// SuspendActor checkpoints a Workflow Actor after its Reconcile Turn completes using DURABILITY_MODE_ASYNCHRONOUS.
func (b *SubstrateActorBackend) SuspendActor(ctx context.Context, atespace, actorName string) error {
	if strings.TrimSpace(atespace) == "" {
		atespace = b.atespace
	}
	ref := &ateapi.ObjectRef{
		Atespace: atespace,
		Name:     actorName,
	}
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		_, err = b.controlClient.SuspendActor(ctx, &ateapi.SuspendActorRequest{
			Actor: ref,
			Options: &ateapi.SuspendOptions{
				Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS,
			},
		})
		if err == nil {
			return nil
		}
		if status.Code(err) == codes.FailedPrecondition {
			if act, getErr := b.controlClient.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref}); getErr == nil &&
				act.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				return nil
			}
		}
		if status.Code(err) == codes.Aborted && attempt < 5 && ctx.Err() == nil {
			time.Sleep(150 * time.Millisecond)
			continue
		}
		break
	}
	if err != nil {
		return fmt.Errorf("SuspendActor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

// RevertActor rolls a crashed or dirty Workflow Actor back to its last durable volume snapshot.
func (b *SubstrateActorBackend) RevertActor(ctx context.Context, atespace, actorName string) error {
	if strings.TrimSpace(atespace) == "" {
		atespace = b.atespace
	}
	ref := &ateapi.ObjectRef{
		Atespace: atespace,
		Name:     actorName,
	}
	for attempt := 1; attempt <= 5; attempt++ {
		_, err := b.controlClient.RevertActor(ctx, &ateapi.RevertActorRequest{
			Actor: ref,
		})
		if err == nil {
			return nil
		}
		if status.Code(err) == codes.FailedPrecondition {
			if act, getErr := b.controlClient.GetActor(ctx, &ateapi.GetActorRequest{Actor: ref}); getErr == nil &&
				act.GetStatus().GetState() == ateapi.ActorState_ACTOR_STATE_SUSPENDED {
				return nil
			}
		}
		if status.Code(err) == codes.Aborted && attempt < 5 && ctx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return fmt.Errorf("RevertActor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

type fileBearerTokenCreds string

func (c fileBearerTokenCreds) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	raw, err := os.ReadFile(string(c))
	if err != nil {
		return nil, fmt.Errorf("read ateapi token file %q: %w", string(c), err)
	}
	tok := strings.TrimSpace(string(raw))
	if tok == "" {
		return nil, fmt.Errorf("ateapi token file %q is empty", string(c))
	}
	return map[string]string{"authorization": "Bearer " + tok}, nil
}

func (c fileBearerTokenCreds) RequireTransportSecurity() bool {
	return true
}

func resolveControlServerName(controlAddr, explicit string) string {
	if s := strings.TrimSpace(explicit); s != "" {
		return s
	}
	addr := strings.TrimSpace(controlAddr)
	addr = strings.TrimPrefix(addr, "dns:///")
	addr = strings.TrimPrefix(addr, "dns://")
	if h, _, err := net.SplitHostPort(addr); err == nil && h != "" {
		return h
	}
	return addr
}

func resolveRouterServerName(routerURL, explicit string) string {
	if s := strings.TrimSpace(explicit); s != "" {
		return s
	}
	if u, err := url.Parse(routerURL); err == nil && u.Hostname() != "" {
		return u.Hostname()
	}
	return ""
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func buildDynamicMTLSConfig(caPEM []byte, caFile, bundleFile, certFile, keyFile, serverName string) (*tls.Config, error) {
	resolvedCA := append([]byte(nil), caPEM...)
	if len(resolvedCA) == 0 && strings.TrimSpace(caFile) != "" {
		raw, err := os.ReadFile(strings.TrimSpace(caFile))
		if err != nil {
			return nil, fmt.Errorf("read RouterCAFile %s: %w", caFile, err)
		}
		resolvedCA = raw
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: strings.TrimSpace(serverName),
	}
	if len(resolvedCA) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(resolvedCA) {
			return nil, errors.New("failed to parse CA PEM")
		}
		tlsCfg.RootCAs = pool
	}

	bFile := strings.TrimSpace(bundleFile)
	cFile := strings.TrimSpace(certFile)
	kFile := strings.TrimSpace(keyFile)
	switch {
	case bFile != "":
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			raw, err := os.ReadFile(bFile)
			if err != nil {
				return nil, err
			}
			pair, err := tls.X509KeyPair(raw, raw)
			if err != nil {
				return nil, err
			}
			return &pair, nil
		}
	case cFile != "" && kFile != "":
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			pair, err := tls.LoadX509KeyPair(cFile, kFile)
			if err != nil {
				return nil, err
			}
			return &pair, nil
		}
	}
	return tlsCfg, nil
}
