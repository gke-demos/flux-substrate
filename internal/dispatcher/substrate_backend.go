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
	DefaultWorkerRoleLabelKey = "pool.ate.dev/role"
	// DefaultWorkerRoleLabelValue is the WorkerPool selector value for Flux reconciler actors.
	DefaultWorkerRoleLabelValue = "flux-reconciler"

	defaultActorCPUMillicores = 500
	defaultActorMemoryBytes   = 512 * 1024 * 1024 // 512 MiB
)

// SubstrateBackendConfig configures the Agent Substrate v1.0.0 ControlClient gRPC
// and atenet-router HTTP(S) ActorBackend.
type SubstrateBackendConfig struct {
	Atespace          string
	TemplateName      string
	ActorImage        string
	KubeBridgeURL     string
	ControlAddr       string
	ControlClient     ateapi.ControlClient
	ControlInsecure   bool
	RouterURL         string
	RouterCAFile      string
	RouterCAPEM       []byte
	PodCertFile       string
	PodKeyFile        string
	TurnTimeout       time.Duration
	CPUMillicores     int64
	MemoryBytes       int64
	WorkerMatchLabels map[string]string
}

// SubstrateActorBackend implements ActorBackend against a live Agent Substrate v1.0.0
// Control gRPC service (ate-system-server) and atenet-router.
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

	tlsCfg, err := buildDynamicMTLSConfig(cfg.RouterCAPEM, cfg.RouterCAFile, cfg.PodCertFile, cfg.PodKeyFile)
	if err != nil {
		return nil, err
	}

	var grpcConn *grpc.ClientConn
	controlClient := cfg.ControlClient
	if controlClient == nil {
		var dialOpt grpc.DialOption
		if cfg.ControlInsecure {
			dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
		} else {
			dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg.Clone()))
		}
		conn, err := grpc.NewClient(strings.TrimSpace(cfg.ControlAddr), dialOpt)
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
				TLSClientConfig: tlsCfg,
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
	if err != nil && status.Code(err) != codes.AlreadyExists {
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
		"--addr=:8080",
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
									{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_NAME, Path: "name"},
									{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_ATESPACE, Path: "atespace"},
									{Field: ateapi.ActorMetadataField_ACTOR_METADATA_FIELD_UID, Path: "uid"},
								},
							},
						},
						{
							TrustBundle: &ateapi.TrustBundleDataSource{
								Names: []string{"egress-mitm.ate.dev", "system-roots.ate.dev"},
								Path:  "ca-bundle.pem",
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
					{Name: "SSL_CERT_FILE", Value: "/var/run/ate/system-info/ca-bundle.pem"},
					{Name: "GIT_SSL_CAINFO", Value: "/var/run/ate/system-info/ca-bundle.pem"},
				},
				WakeupProbe: &ateapi.WakeupProbe{
					HttpGet: &ateapi.HttpGetAction{
						Path: "/readyz",
						Port: 8080,
					},
					TimeoutSeconds: 30,
				},
				VolumeMounts: []*ateapi.VolumeMount{
					{Name: "flux-cache", MountPath: "/var/lib/flux"},
					{Name: "ate-system-info", MountPath: "/var/run/ate/system-info"},
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
// configuring replaceHeaders with actorJwt for the Identity Bridge and ate-secret:// for Git HTTPS.
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

	gitHost, gitPort := splitHostAndDefaultPort(policy.GitHost, policy.GitScheme)
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
		ports := &ateapi.Ports{Numbers: []int32{gitPort}}
		if strings.EqualFold(policy.GitScheme, "http") {
			rules = append(rules, &ateapi.EgressRule{
				Http: &ateapi.HttpRule{
					Hostnames: []string{gitHost},
					Ports:     ports,
					Effects:   gitEffects,
				},
			})
		} else {
			rules = append(rules, &ateapi.EgressRule{
				Https: &ateapi.HttpsRule{
					Hostnames: []string{gitHost},
					Ports:     ports,
					Effects:   gitEffects,
				},
			})
		}
	}

	bridgeHost, bridgePort := splitHostAndDefaultPort(policy.KubeBridgeHost, "https")
	if bridgeHost != "" && !(bridgeHost == gitHost && bridgePort == gitPort) {
		aud := strings.TrimSpace(policy.KubeBridgeAudience)
		if aud == "" {
			aud = "https://" + bridgeHost
		}
		expSecs := policy.ActorJWTLifetimeSecs
		if expSecs < 300 {
			expSecs = 600
		}
		if expSecs > 3600 {
			expSecs = 3600
		}
		rules = append(rules, &ateapi.EgressRule{
			Https: &ateapi.HttpsRule{
				Hostnames: []string{bridgeHost},
				Ports:     &ateapi.Ports{Numbers: []int32{bridgePort}},
				Effects: &ateapi.HttpRuleEffects{
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
				},
			},
		})
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

// InvokeReconcile sends POST /reconcile through atenet-router with ate-target-actor: <atespace>/<actor>
// and converts HTTP 507 or ACTOR_STATE_CRASHED into an *ActorCrashError.
func (b *SubstrateActorBackend) InvokeReconcile(ctx context.Context, inv TurnInvocation) (actor.ReconcileResult, int, error) {
	atespace := strings.TrimSpace(inv.Atespace)
	if atespace == "" {
		atespace = b.atespace
	}

	body, err := json.Marshal(inv.Request)
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, err
	}
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
		return actor.ReconcileResult{}, http.StatusBadGateway, err
	}
	defer resp.Body.Close()

	var result actor.ReconcileResult
	decodeErr := json.NewDecoder(resp.Body).Decode(&result)

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
	}

	if decodeErr != nil {
		return actor.ReconcileResult{}, resp.StatusCode, fmt.Errorf("decode router ReconcileResult: %w", decodeErr)
	}
	return result, resp.StatusCode, nil
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
	_, err := b.controlClient.SuspendActor(ctx, &ateapi.SuspendActorRequest{
		Actor: &ateapi.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
		Options: &ateapi.SuspendOptions{
			Mode: ateapi.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS,
		},
	})
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
	_, err := b.controlClient.RevertActor(ctx, &ateapi.RevertActorRequest{
		Actor: &ateapi.ObjectRef{
			Atespace: atespace,
			Name:     actorName,
		},
	})
	if err != nil {
		return fmt.Errorf("RevertActor %s/%s: %w", atespace, actorName, err)
	}
	return nil
}

func buildDynamicMTLSConfig(caPEM []byte, caFile, certFile, keyFile string) (*tls.Config, error) {
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
	}
	if len(resolvedCA) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(resolvedCA) {
			return nil, errors.New("failed to parse CA PEM")
		}
		tlsCfg.RootCAs = pool
	}
	cFile := strings.TrimSpace(certFile)
	kFile := strings.TrimSpace(keyFile)
	if cFile != "" && kFile != "" {
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
