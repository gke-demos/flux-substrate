package dispatcher_test

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/gke-demos/flux-substrate/internal/actor"
	"github.com/gke-demos/flux-substrate/internal/dispatcher"
	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
	"github.com/gke-demos/flux-substrate/manifests/crds"
)

var (
	sharedRestCfg    *rest.Config
	sharedKubeClient client.Client
	nsCounter        atomic.Int32
)

func TestMain(m *testing.M) {
	ensureKubebuilderAssetsEnv()

	parsedCRDs, err := crds.ParseCRDs()
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse embedded Flux CRDs: %v\n", err)
		os.Exit(1)
	}
	scheme, err := crds.NewScheme()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build scheme: %v\n", err)
		os.Exit(1)
	}

	testEnv := &envtest.Environment{
		CRDs: parsedCRDs,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start envtest: %v\n", err)
		os.Exit(1)
	}
	sharedRestCfg = cfg

	kc, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		_ = testEnv.Stop()
		fmt.Fprintf(os.Stderr, "create kube client: %v\n", err)
		os.Exit(1)
	}
	sharedKubeClient = kc

	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

func ensureKubebuilderAssetsEnv() {
	if os.Getenv("KUBEBUILDER_ASSETS") != "" {
		return
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	k8sDir := filepath.Join(home, ".local", "share", "kubebuilder-envtest", "k8s")
	entries, err := os.ReadDir(k8sDir)
	if err != nil || len(entries) == 0 {
		return
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return
	}
	slices.Sort(names)
	_ = os.Setenv("KUBEBUILDER_ASSETS", filepath.Join(k8sDir, names[len(names)-1]))
}

func createTestNamespace(t *testing.T, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("%s-%03d", prefix, nsCounter.Add(1))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := sharedKubeClient.Create(context.Background(), ns); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	return name
}

func newGitServerTLS(t *testing.T, tenantTokens map[string]string, defaultToken string) (*gitserver.Server, string, []byte) {
	t.Helper()
	adminToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	gs, err := gitserver.New(gitserver.Config{
		ReposDir:     t.TempDir(),
		DefaultToken: defaultToken,
		AdminToken:   adminToken,
		TenantTokens: tenantTokens,
	})
	if err != nil {
		t.Fatalf("gitserver.New: %v", err)
	}

	egressSim := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		if r2.Header.Get("Authorization") == "Bearer actor-placeholder" {
			injected := defaultToken
			if injected == "" {
				for _, tok := range tenantTokens {
					injected = tok
					break
				}
			}
			r2.Header.Set("Authorization", "Bearer "+injected)
		}
		gs.Handler().ServeHTTP(w, r2)
	})

	tlsSrv := httptest.NewTLSServer(egressSim)
	t.Cleanup(tlsSrv.Close)

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsSrv.Certificate().Raw,
	})
	return gs, tlsSrv.URL, certPEM
}

type recordingBackend struct {
	inner dispatcher.ActorBackend

	mu       sync.Mutex
	calls    map[string][]string
	policies map[string][]dispatcher.EgressPolicySpec
}

func newRecordingBackend(inner dispatcher.ActorBackend) *recordingBackend {
	return &recordingBackend{
		inner:    inner,
		calls:    make(map[string][]string),
		policies: make(map[string][]dispatcher.EgressPolicySpec),
	}
}

func (r *recordingBackend) record(actorName, op string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls[actorName] = append(r.calls[actorName], op)
}

func (r *recordingBackend) callsFor(actorName string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls[actorName])
}

func (r *recordingBackend) policiesFor(actorName string) []dispatcher.EgressPolicySpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.policies[actorName])
}

func (r *recordingBackend) EnsureActor(ctx context.Context, spec dispatcher.ActorSpec) error {
	r.record(spec.ActorName, "EnsureActor")
	return r.inner.EnsureActor(ctx, spec)
}

func (r *recordingBackend) EnsureEgressPolicy(ctx context.Context, policy dispatcher.EgressPolicySpec) error {
	r.mu.Lock()
	r.calls[policy.ActorName] = append(r.calls[policy.ActorName], "EnsureEgressPolicy")
	r.policies[policy.ActorName] = append(r.policies[policy.ActorName], policy)
	r.mu.Unlock()
	return r.inner.EnsureEgressPolicy(ctx, policy)
}

func (r *recordingBackend) InvokeReconcile(ctx context.Context, inv dispatcher.TurnInvocation) (actor.ReconcileResult, int, error) {
	r.record(inv.ActorName, "InvokeReconcile")
	return r.inner.InvokeReconcile(ctx, inv)
}

func (r *recordingBackend) SuspendActor(ctx context.Context, atespace, actorName string) error {
	r.record(actorName, "SuspendActor")
	return r.inner.SuspendActor(ctx, atespace, actorName)
}

func (r *recordingBackend) RevertActor(ctx context.Context, atespace, actorName string) error {
	r.record(actorName, "RevertActor")
	return r.inner.RevertActor(ctx, atespace, actorName)
}

func waitUntil(t *testing.T, timeout time.Duration, notify <-chan struct{}, check func() bool, desc string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if check() {
		return
	}
	ticker := time.NewTicker(15 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", desc)
		case <-notify:
			if check() {
				return
			}
		case <-ticker.C:
			if check() {
				return
			}
		}
	}
}

func findCondition(conds []metav1.Condition, condType string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

func TestDispatcherReconcilesManagedFluxCRsEnsureEgressAndSuspendsActor(t *testing.T) {
	managedNS := createTestNamespace(t, "s1-managed")
	unmanagedNS := createTestNamespace(t, "s1-unmanaged")

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	gs, gitBaseURL, trustPEM := newGitServerTLS(t, nil, gitToken)

	seedManaged, err := gs.SeedDefaultTenant(managedNS, managedNS)
	if err != nil {
		t.Fatalf("SeedDefaultTenant managed: %v", err)
	}
	if _, err := gs.SeedDefaultTenant(unmanagedNS, unmanagedNS); err != nil {
		t.Fatalf("SeedDefaultTenant unmanaged: %v", err)
	}

	// Managed Flux GitRepository + Kustomization (labeled reconciler.ate.dev/managed-by=flux-substrate).
	managedLabels := map[string]string{
		dispatcher.ManagedByLabelKey: dispatcher.ManagedByLabelValue,
		dispatcher.TenantLabelKey:    managedNS,
	}
	managedGR := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      managedNS,
			Namespace: managedNS,
			Labels:    managedLabels,
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       fmt.Sprintf("%s/git/%s.git", gitBaseURL, managedNS),
			Interval:  metav1.Duration{Duration: 10 * time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
			SecretRef: &fluxmeta.LocalObjectReference{Name: "git-creds"},
		},
	}
	if err := sharedKubeClient.Create(context.Background(), managedGR); err != nil {
		t.Fatalf("create managed GitRepository: %v", err)
	}
	managedKS := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Name:      managedNS,
			Namespace: managedNS,
			Labels:    managedLabels,
		},
		Spec: kustomizev1.KustomizationSpec{
			Interval: metav1.Duration{Duration: 10 * time.Minute},
			Path:     "./",
			Prune:    true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: sourcev1.GitRepositoryKind,
				Name: managedNS,
			},
			TargetNamespace: managedNS,
		},
	}
	if err := sharedKubeClient.Create(context.Background(), managedKS); err != nil {
		t.Fatalf("create managed Kustomization: %v", err)
	}

	// Unmanaged Flux CRs (no reconciler.ate.dev/managed-by label) MUST be ignored.
	unmanagedGR := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unmanagedNS,
			Namespace: unmanagedNS,
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       fmt.Sprintf("%s/git/%s.git", gitBaseURL, unmanagedNS),
			Interval:  metav1.Duration{Duration: 10 * time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
		},
	}
	if err := sharedKubeClient.Create(context.Background(), unmanagedGR); err != nil {
		t.Fatalf("create unmanaged GitRepository: %v", err)
	}
	unmanagedKS := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Name:      unmanagedNS,
			Namespace: unmanagedNS,
		},
		Spec: kustomizev1.KustomizationSpec{
			Interval: metav1.Duration{Duration: 10 * time.Minute},
			Path:     "./",
			Prune:    true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: sourcev1.GitRepositoryKind,
				Name: unmanagedNS,
			},
			TargetNamespace: unmanagedNS,
		},
	}
	if err := sharedKubeClient.Create(context.Background(), unmanagedKS); err != nil {
		t.Fatalf("create unmanaged Kustomization: %v", err)
	}

	localBackend, err := dispatcher.NewLocalActorBackend(dispatcher.LocalActorBackendConfig{
		Atespace:       "flux-substrate",
		BaseDurableDir: t.TempDir(),
		RestConfig:     sharedRestCfg,
		TrustBundlePEM: trustPEM,
	})
	if err != nil {
		t.Fatalf("NewLocalActorBackend: %v", err)
	}
	recBackend := newRecordingBackend(localBackend)

	disp, err := dispatcher.New(dispatcher.Config{
		Mode:           "dispatcher",
		Atespace:       "flux-substrate",
		WatchNamespace: "",
		KubeClient:     sharedKubeClient,
		Backend:        recBackend,
		GitServer:      gs,
		KubeBridgeURL:  "https://kube-bridge.flux-substrate.svc",
		CRSyncInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("dispatcher.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		disp.Close()
	})
	if err := disp.Start(ctx); err != nil {
		t.Fatalf("disp.Start: %v", err)
	}

	ts := httptest.NewServer(disp.Handler())
	t.Cleanup(ts.Close)

	// Verify mandatory security headers and GET/POST method restriction.
	stateResp, err := ts.Client().Get(ts.URL + "/api/state")
	if err != nil {
		t.Fatalf("GET /api/state: %v", err)
	}
	stateResp.Body.Close()
	if stateResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/state status = %d, want 200", stateResp.StatusCode)
	}
	if got := stateResp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", got)
	}
	if got := stateResp.Header.Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("X-Frame-Options = %q, want DENY", got)
	}
	for _, m := range []string{http.MethodPut, http.MethodDelete, http.MethodTrace} {
		req, _ := http.NewRequest(m, ts.URL+"/api/state", nil)
		mResp, reqErr := ts.Client().Do(req)
		if reqErr != nil {
			t.Fatalf("%s /api/state: %v", m, reqErr)
		}
		mResp.Body.Close()
		if mResp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s /api/state status = %d, want 405", m, mResp.StatusCode)
		}
	}

	// Wait for managedNS initial reconcile + suspend to finish.
	waitUntil(t, 15*time.Second, disp.TurnCompletedNotify(), func() bool {
		calls := recBackend.callsFor(managedNS)
		return len(calls) >= 4 && calls[len(calls)-1] == "SuspendActor"
	}, "managed tenant initial Reconcile Turn and SuspendActor")

	wantCalls := []string{"EnsureActor", "EnsureEgressPolicy", "InvokeReconcile", "SuspendActor"}
	if gotCalls := recBackend.callsFor(managedNS); !slices.Equal(gotCalls, wantCalls) {
		t.Fatalf("ActorBackend call sequence = %v, want %v", gotCalls, wantCalls)
	}
	if unmanagedCalls := recBackend.callsFor(unmanagedNS); len(unmanagedCalls) != 0 {
		t.Fatalf("expected unmanaged CR to be ignored, got calls %v", unmanagedCalls)
	}

	parsedGitURL, _ := url.Parse(gitBaseURL)
	policies := recBackend.policiesFor(managedNS)
	if len(policies) != 1 {
		t.Fatalf("recorded EgressPolicies = %d, want 1", len(policies))
	}
	if policies[0].GitScheme != "https" || policies[0].GitHost != parsedGitURL.Host {
		t.Errorf("EgressPolicy Git target = %s://%s, want https://%s", policies[0].GitScheme, policies[0].GitHost, parsedGitURL.Host)
	}
	wantCredURI := fmt.Sprintf("ate-secret://k8s.io/default/%s/git-creds/token", managedNS)
	if policies[0].GitCredentialURI != wantCredURI {
		t.Errorf("GitCredentialURI = %q, want %q", policies[0].GitCredentialURI, wantCredURI)
	}
	if policies[0].KubeBridgeAudience != "https://kube-bridge.flux-substrate.svc" || policies[0].ActorJWTLifetimeSecs != 600 {
		t.Errorf("unexpected KubeBridge actorJwt policy: %+v", policies[0])
	}

	// Verify Flux GitRepository status on real kube-apiserver.
	var liveGR sourcev1.GitRepository
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: managedNS, Name: managedNS}, &liveGR); err != nil {
		t.Fatalf("get GitRepository: %v", err)
	}
	wantRev := "main@sha1:" + seedManaged.SHA
	if liveGR.Status.Artifact == nil || liveGR.Status.Artifact.Revision != wantRev {
		t.Fatalf("GitRepository.Status.Artifact = %+v, want revision %q", liveGR.Status.Artifact, wantRev)
	}
	grReady := findCondition(liveGR.Status.Conditions, fluxmeta.ReadyCondition)
	if grReady == nil || grReady.Status != metav1.ConditionTrue {
		t.Errorf("GitRepository Ready condition = %+v, want True", grReady)
	}
	if grReconciling := findCondition(liveGR.Status.Conditions, fluxmeta.ReconcilingCondition); grReconciling != nil {
		t.Errorf("expected Reconciling condition to be removed on completion, got %+v", grReconciling)
	}

	// Verify Flux Kustomization status on real kube-apiserver.
	var liveKS kustomizev1.Kustomization
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: managedNS, Name: managedNS}, &liveKS); err != nil {
		t.Fatalf("get Kustomization: %v", err)
	}
	if liveKS.Status.LastAppliedRevision != wantRev {
		t.Errorf("Kustomization.Status.LastAppliedRevision = %q, want %q", liveKS.Status.LastAppliedRevision, wantRev)
	}
	if liveKS.Status.Inventory == nil || len(liveKS.Status.Inventory.Entries) != 3 {
		t.Errorf("Kustomization.Status.Inventory = %+v, want 3 entries", liveKS.Status.Inventory)
	}
	ksReady := findCondition(liveKS.Status.Conditions, fluxmeta.ReadyCondition)
	if ksReady == nil || ksReady.Status != metav1.ConditionTrue {
		t.Errorf("Kustomization Ready condition = %+v, want True", ksReady)
	}
	if ksReconciling := findCondition(liveKS.Status.Conditions, fluxmeta.ReconcilingCondition); ksReconciling != nil {
		t.Errorf("expected Kustomization Reconciling condition removed on completion, got %+v", ksReconciling)
	}

	// Verify unmanaged namespace had zero workload objects applied.
	var unmanagedCM corev1.ConfigMap
	err = sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: unmanagedNS, Name: "app-config"}, &unmanagedCM)
	if !apierrors.IsNotFound(err) {
		t.Errorf("expected unmanaged ConfigMap NotFound, got err=%v", err)
	}

	// Verify GET /api/tenants/{id} reports SUSPENDED actor state and Kustomization YAML.
	tResp, err := ts.Client().Get(ts.URL + "/api/tenants/" + managedNS)
	if err != nil {
		t.Fatalf("GET /api/tenants/%s: %v", managedNS, err)
	}
	defer tResp.Body.Close()
	if tResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/tenants/%s status = %d, want 200", managedNS, tResp.StatusCode)
	}
	var detail dispatcher.TenantDetail
	if err := json.NewDecoder(tResp.Body).Decode(&detail); err != nil {
		t.Fatalf("decode TenantDetail: %v", err)
	}
	if detail.ActorState != dispatcher.ActorStateSuspended {
		t.Errorf("TenantDetail.ActorState = %q, want %q", detail.ActorState, dispatcher.ActorStateSuspended)
	}
	if !detail.Ready || detail.SnapshotCount < 1 || detail.KustomizationYAML == "" {
		t.Errorf("unexpected TenantDetail: ready=%v snapshots=%d yamlLen=%d", detail.Ready, detail.SnapshotCount, len(detail.KustomizationYAML))
	}
}

func TestDispatcherWebhookBurstCoalescingAndEgressHostUpdate(t *testing.T) {
	tenantNS := createTestNamespace(t, "s2-burst")

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	gs, gitBaseURL, trustPEM := newGitServerTLS(t, nil, gitToken)
	if _, err := gs.SeedDefaultTenant(tenantNS, tenantNS); err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	labels := map[string]string{
		dispatcher.ManagedByLabelKey: dispatcher.ManagedByLabelValue,
		dispatcher.TenantLabelKey:    tenantNS,
	}
	repoURL := fmt.Sprintf("%s/git/%s.git", gitBaseURL, tenantNS)
	gr := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{Name: tenantNS, Namespace: tenantNS, Labels: labels},
		Spec: sourcev1.GitRepositorySpec{
			URL:       repoURL,
			Interval:  metav1.Duration{Duration: 10 * time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
			SecretRef: &fluxmeta.LocalObjectReference{Name: "git-creds"},
		},
	}
	if err := sharedKubeClient.Create(context.Background(), gr); err != nil {
		t.Fatalf("create GitRepository: %v", err)
	}
	ks := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{Name: tenantNS, Namespace: tenantNS, Labels: labels},
		Spec: kustomizev1.KustomizationSpec{
			Interval:        metav1.Duration{Duration: 10 * time.Minute},
			Path:            "./",
			Prune:           true,
			SourceRef:       kustomizev1.CrossNamespaceSourceReference{Kind: sourcev1.GitRepositoryKind, Name: tenantNS},
			TargetNamespace: tenantNS,
		},
	}
	if err := sharedKubeClient.Create(context.Background(), ks); err != nil {
		t.Fatalf("create Kustomization: %v", err)
	}

	var (
		activeInActor atomic.Int32
		maxInActor    atomic.Int32
		turnSeq       atomic.Int32
		turn2Entered  = make(chan struct{})
		releaseTurn2  = make(chan struct{})
	)

	localBackend, err := dispatcher.NewLocalActorBackend(dispatcher.LocalActorBackendConfig{
		Atespace:       "flux-substrate",
		BaseDurableDir: t.TempDir(),
		RestConfig:     sharedRestCfg,
		TrustBundlePEM: trustPEM,
		TurnHook: func(ctx context.Context, inv dispatcher.TurnInvocation) error {
			if inv.ActorName != tenantNS {
				return nil
			}
			cur := activeInActor.Add(1)
			for {
				prev := maxInActor.Load()
				if cur <= prev || maxInActor.CompareAndSwap(prev, cur) {
					break
				}
			}
			defer activeInActor.Add(-1)

			if turnSeq.Add(1) == 2 {
				close(turn2Entered)
				select {
				case <-releaseTurn2:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewLocalActorBackend: %v", err)
	}
	recBackend := newRecordingBackend(localBackend)

	disp, err := dispatcher.New(dispatcher.Config{
		Mode:           "dispatcher",
		Atespace:       "flux-substrate",
		WatchNamespace: tenantNS,
		KubeClient:     sharedKubeClient,
		Backend:        recBackend,
		GitServer:      gs,
		KubeBridgeURL:  "https://kube-bridge.flux-substrate.svc",
		CRSyncInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("dispatcher.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		disp.Close()
	})
	if err := disp.Start(ctx); err != nil {
		t.Fatalf("disp.Start: %v", err)
	}

	ts := httptest.NewServer(disp.Handler())
	t.Cleanup(ts.Close)

	// Wait for initial Turn 1 to finish and suspend.
	waitUntil(t, 15*time.Second, disp.TurnCompletedNotify(), func() bool {
		detail, ok := disp.SnapshotTenant(tenantNS)
		return ok && detail.Ready && detail.ActorState == dispatcher.ActorStateSuspended && detail.TurnCount == 1
	}, "initial baseline turn")

	// Kick off Turn 2 via path-targeted Git webhook; Turn 2 pauses inside TurnHook.
	resp, err := ts.Client().Post(ts.URL+"/api/webhooks/git/"+tenantNS, "application/json", nil)
	if err != nil {
		t.Fatalf("POST webhook turn 2: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/webhooks/git/%s status = %d, want 200/202", tenantNS, resp.StatusCode)
	}

	select {
	case <-turn2Entered:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Turn 2 to enter InvokeReconcile")
	}

	// While Turn 2 is active (inFlight == 1), commit 5 updates and fire 5 concurrent webhooks
	// (mixing /api/webhooks/git/{id} and GitHub-style /api/webhooks/git payloads).
	var latestRev gitserver.Revision
	for step := 2; step <= 6; step++ {
		rev, commitErr := gs.Commit(tenantNS, gitserver.CommitChange{
			Upsert: map[string]string{
				"configmap.yaml": fmt.Sprintf("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: app-config\n  namespace: %s\ndata:\n  tenant: %q\n  release: \"v%d\"\n", tenantNS, tenantNS, step),
			},
			Message: fmt.Sprintf("Release v%d", step),
		})
		if commitErr != nil {
			t.Fatalf("Commit v%d: %v", step, commitErr)
		}
		latestRev = rev
	}

	var wg sync.WaitGroup
	for i := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var wResp *http.Response
			var wErr error
			if idx%2 == 0 {
				wResp, wErr = ts.Client().Post(ts.URL+"/api/webhooks/git/"+tenantNS, "application/json", nil)
			} else {
				body := fmt.Sprintf(`{"repository":{"clone_url":%q},"after":%q}`, repoURL, latestRev.SHA)
				wResp, wErr = ts.Client().Post(ts.URL+"/api/webhooks/git", "application/json", strings.NewReader(body))
			}
			if wErr != nil {
				t.Errorf("burst webhook %d error: %v", idx, wErr)
				return
			}
			wResp.Body.Close()
			if wResp.StatusCode != http.StatusOK && wResp.StatusCode != http.StatusAccepted {
				t.Errorf("burst webhook %d status = %d", idx, wResp.StatusCode)
			}
		}(i)
	}
	wg.Wait()

	// Verify SuspendActor was NOT called while Turn 2 is still in flight.
	callsDuringTurn2 := recBackend.callsFor(tenantNS)
	suspendCountDuringTurn2 := 0
	for _, c := range callsDuringTurn2 {
		if c == "SuspendActor" {
			suspendCountDuringTurn2++
		}
	}
	if suspendCountDuringTurn2 != 1 {
		t.Fatalf("SuspendActor count while Turn 2 in flight = %d, want 1 (only Turn 1)", suspendCountDuringTurn2)
	}

	// Release Turn 2; Dispatcher should execute ONE coalesced follow-up turn (Turn 3) and then suspend.
	close(releaseTurn2)

	wantFinalRev := "main@sha1:" + latestRev.SHA
	waitUntil(t, 15*time.Second, disp.TurnCompletedNotify(), func() bool {
		detail, ok := disp.SnapshotTenant(tenantNS)
		return ok && detail.LastAppliedRevision == wantFinalRev && detail.ActorState == dispatcher.ActorStateSuspended
	}, "coalesced follow-up turn and post-burst suspend")

	if gotMax := maxInActor.Load(); gotMax != 1 {
		t.Fatalf("max concurrent turns on same Workflow Actor = %d, want 1", gotMax)
	}

	detail, _ := disp.SnapshotTenant(tenantNS)
	if detail.TurnCount != 3 {
		t.Errorf("TurnCount = %d, want 3 (Turn 1 baseline + Turn 2 active + 1 coalesced Turn 3)", detail.TurnCount)
	}
	if detail.SnapshotCount != 2 {
		t.Errorf("SnapshotCount = %d, want 2 (SuspendActor fired only when inFlight == 0)", detail.SnapshotCount)
	}
	if !detail.CacheHit {
		t.Errorf("expected warm DurableDir CacheHit=true on Turn 3")
	}

	var cm corev1.ConfigMap
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "app-config"}, &cm); err != nil {
		t.Fatalf("Get ConfigMap: %v", err)
	}
	if cm.Data["release"] != "v6" {
		t.Errorf("ConfigMap data[release] = %q, want v6", cm.Data["release"])
	}

	// Now update GitRepository.spec.url to a new TLS Git host and verify EnsureEgressPolicy updates before wake!
	gs2, gitBaseURL2, trustPEM2 := newGitServerTLS(t, nil, gitToken)
	_ = trustPEM2
	mirrorSeed, err := gs2.SeedDefaultTenant(tenantNS, tenantNS)
	if err != nil {
		t.Fatalf("gs2.SeedDefaultTenant: %v", err)
	}
	// Note: append gs2's cert into a new backend or check EnsureEgressPolicy call with new host.
	var liveGR sourcev1.GitRepository
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: tenantNS}, &liveGR); err != nil {
		t.Fatalf("Get GitRepository: %v", err)
	}
	liveGR.Spec.URL = fmt.Sprintf("%s/git/%s.git", gitBaseURL2, tenantNS)
	if err := sharedKubeClient.Update(context.Background(), &liveGR); err != nil {
		t.Fatalf("Update GitRepository URL: %v", err)
	}
	_ = mirrorSeed
	if err := disp.SyncCRs(ctx); err != nil {
		t.Fatalf("SyncCRs after URL host change: %v", err)
	}

	parsedGitURL2, _ := url.Parse(gitBaseURL2)
	waitUntil(t, 15*time.Second, disp.TurnCompletedNotify(), func() bool {
		pols := recBackend.policiesFor(tenantNS)
		return len(pols) >= 2 && pols[len(pols)-1].GitHost == parsedGitURL2.Host
	}, "EnsureEgressPolicy reconciled for new GitRepository host")
}

func TestDispatcherRetryIntervalRecoversFailedKustomization(t *testing.T) {
	tenantNS := createTestNamespace(t, "s2-retry")

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	gs, gitBaseURL, trustPEM := newGitServerTLS(t, nil, gitToken)

	// Seed an intentionally broken Kustomize overlay (referencing a missing manifest).
	if _, err := gs.SeedTenant(tenantNS, map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - missing.yaml\n",
	}, "Seed broken overlay"); err != nil {
		t.Fatalf("SeedTenant broken: %v", err)
	}

	labels := map[string]string{
		dispatcher.ManagedByLabelKey: dispatcher.ManagedByLabelValue,
		dispatcher.TenantLabelKey:    tenantNS,
	}
	gr := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{Name: tenantNS, Namespace: tenantNS, Labels: labels},
		Spec: sourcev1.GitRepositorySpec{
			URL:       fmt.Sprintf("%s/git/%s.git", gitBaseURL, tenantNS),
			Interval:  metav1.Duration{Duration: 10 * time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Branch: "main"},
		},
	}
	if err := sharedKubeClient.Create(context.Background(), gr); err != nil {
		t.Fatalf("create GitRepository: %v", err)
	}
	retryDur := metav1.Duration{Duration: 80 * time.Millisecond}
	ks := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{Name: tenantNS, Namespace: tenantNS, Labels: labels},
		Spec: kustomizev1.KustomizationSpec{
			Interval:        metav1.Duration{Duration: 10 * time.Minute},
			RetryInterval:   &retryDur,
			Path:            "./",
			Prune:           true,
			SourceRef:       kustomizev1.CrossNamespaceSourceReference{Kind: sourcev1.GitRepositoryKind, Name: tenantNS},
			TargetNamespace: tenantNS,
		},
	}
	if err := sharedKubeClient.Create(context.Background(), ks); err != nil {
		t.Fatalf("create Kustomization: %v", err)
	}

	localBackend, err := dispatcher.NewLocalActorBackend(dispatcher.LocalActorBackendConfig{
		Atespace:       "flux-substrate",
		BaseDurableDir: t.TempDir(),
		RestConfig:     sharedRestCfg,
		TrustBundlePEM: trustPEM,
	})
	if err != nil {
		t.Fatalf("NewLocalActorBackend: %v", err)
	}

	disp, err := dispatcher.New(dispatcher.Config{
		Mode:           "dispatcher",
		Atespace:       "flux-substrate",
		WatchNamespace: tenantNS,
		KubeClient:     sharedKubeClient,
		Backend:        localBackend,
		GitServer:      gs,
		CRSyncInterval: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("dispatcher.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		disp.Close()
	})
	if err := disp.Start(ctx); err != nil {
		t.Fatalf("disp.Start: %v", err)
	}

	// Wait for Turn 1 failure (Ready=False, Reason=BuildFailed).
	waitUntil(t, 15*time.Second, disp.TurnCompletedNotify(), func() bool {
		detail, ok := disp.SnapshotTenant(tenantNS)
		return ok && detail.TurnCount >= 1 && !detail.Ready && detail.FailedStage == "render"
	}, "initial failed turn")

	// Commit valid manifests directly in git-server WITHOUT firing any webhook;
	// spec.retryInterval (80ms) must automatically wake the actor and recover Ready=True!
	fixedRev, err := gs.SeedDefaultTenant(tenantNS, tenantNS)
	if err != nil {
		t.Fatalf("SeedDefaultTenant fix: %v", err)
	}
	wantRev := "main@sha1:" + fixedRev.SHA

	waitUntil(t, 15*time.Second, disp.TurnCompletedNotify(), func() bool {
		detail, ok := disp.SnapshotTenant(tenantNS)
		return ok && detail.Ready && detail.LastAppliedRevision == wantRev && detail.ActorState == dispatcher.ActorStateSuspended
	}, "automatic retryInterval recovery")
}

func TestDispatcher24TenantCommitWaveStrictFIFOMultiplexingAndSingleCommit(t *testing.T) {
	ctrlNS := createTestNamespace(t, "s3-ctrl")

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	gs, gitBaseURL, trustPEM := newGitServerTLS(t, nil, gitToken)

	if err := dispatcher.SeedFleet(context.Background(), dispatcher.SeedFleetOptions{
		KubeClient:       sharedKubeClient,
		GitServer:        gs,
		GitBaseURL:       gitBaseURL,
		ControlNamespace: ctrlNS,
		TenantCount:      24,
	}); err != nil {
		t.Fatalf("SeedFleet(24): %v", err)
	}

	var (
		gateMu          sync.Mutex
		waveGateEnabled bool
		enteredInWave   int
		firstNineFull   = make(chan struct{})
		slotReleaseCh   = make(chan struct{})
		waveStartOrder  []string
	)

	localBackend, err := dispatcher.NewLocalActorBackend(dispatcher.LocalActorBackendConfig{
		Atespace:       "flux-substrate",
		BaseDurableDir: t.TempDir(),
		RestConfig:     sharedRestCfg,
		TrustBundlePEM: trustPEM,
		TurnHook: func(ctx context.Context, inv dispatcher.TurnInvocation) error {
			gateMu.Lock()
			enabled := waveGateEnabled
			if enabled {
				waveStartOrder = append(waveStartOrder, inv.ActorName)
				enteredInWave++
				if enteredInWave == 9 {
					close(firstNineFull)
				}
			}
			gateMu.Unlock()

			if !enabled {
				return nil
			}
			select {
			case <-slotReleaseCh:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatalf("NewLocalActorBackend: %v", err)
	}

	disp, err := dispatcher.New(dispatcher.Config{
		Mode:           "local",
		Atespace:       "flux-substrate",
		WatchNamespace: ctrlNS,
		KubeClient:     sharedKubeClient,
		Backend:        localBackend,
		GitServer:      gs,
		KubeBridgeURL:  "https://kube-bridge.flux-substrate.svc",
		CRSyncInterval: 10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("dispatcher.New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		disp.Close()
	})
	if err := disp.Start(ctx); err != nil {
		t.Fatalf("disp.Start: %v", err)
	}

	ts := httptest.NewServer(disp.Handler())
	t.Cleanup(ts.Close)

	// Wait for all 24 tenants to complete initial cold baseline turns and suspend.
	waitUntil(t, 30*time.Second, disp.TurnCompletedNotify(), func() bool {
		st := disp.SnapshotState()
		return st.Summary.TotalTenants == 24 &&
			st.Summary.ReadyTenants == 24 &&
			st.Summary.SuspendedActors == 24 &&
			st.Summary.ActiveSlots == 0
	}, "24-tenant baseline reconciliation")

	// Enable deterministic wave slot gate and fire POST /api/wave across all 24 tenants.
	gateMu.Lock()
	waveGateEnabled = true
	gateMu.Unlock()

	waveResp, err := ts.Client().Post(ts.URL+"/api/wave", "application/json", strings.NewReader(`{"message":"24-tenant Commit Wave v2"}`))
	if err != nil {
		t.Fatalf("POST /api/wave: %v", err)
	}
	defer waveResp.Body.Close()
	if waveResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/wave status = %d, want 200", waveResp.StatusCode)
	}

	select {
	case <-firstNineFull:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for first 9 WorkerPool slots to fill")
	}

	// Inspect live GET /api/state while 9 actors are active and 15 are queued in FIFO order.
	midResp, err := ts.Client().Get(ts.URL + "/api/state")
	if err != nil {
		t.Fatalf("GET /api/state during wave: %v", err)
	}
	var midState dispatcher.FleetStateResponse
	if err := json.NewDecoder(midResp.Body).Decode(&midState); err != nil {
		midResp.Body.Close()
		t.Fatalf("decode FleetStateResponse: %v", err)
	}
	midResp.Body.Close()

	if midState.Summary.ActiveSlots != 9 || midState.Summary.ActiveActors != 9 {
		t.Fatalf("mid-wave ActiveSlots=%d ActiveActors=%d, want 9 and 9", midState.Summary.ActiveSlots, midState.Summary.ActiveActors)
	}
	if midState.Summary.QueuedTurns != 15 {
		t.Fatalf("mid-wave QueuedTurns = %d, want 15", midState.Summary.QueuedTurns)
	}
	if len(midState.WorkerPool.Workers) != 3 {
		t.Fatalf("WorkerPool.Workers len = %d, want 3", len(midState.WorkerPool.Workers))
	}
	for _, w := range midState.WorkerPool.Workers {
		if w.Capacity != 3 || w.Active != 3 {
			t.Errorf("worker %s Capacity=%d Active=%d, want 3 and 3", w.WorkerID, w.Capacity, w.Active)
		}
	}
	wantQueue := make([]string, 15)
	for i := range 15 {
		wantQueue[i] = fmt.Sprintf("tenant-%02d", i+10)
	}
	if !slices.Equal(midState.WorkerPool.Queue, wantQueue) {
		t.Fatalf("mid-wave FIFO queue = %v, want %v", midState.WorkerPool.Queue, wantQueue)
	}

	// Release slots one at a time for the first 15 completions so each freed slot
	// deterministically wakes the next FIFO waiter (tenant-10..tenant-24) in exact order!
	for i := range 15 {
		wantCount := 9 + i + 1
		slotReleaseCh <- struct{}{}
		waitUntil(t, 15*time.Second, nil, func() bool {
			gateMu.Lock()
			defer gateMu.Unlock()
			return len(waveStartOrder) >= wantCount
		}, fmt.Sprintf("FIFO waiter #%d slot acquisition", i+1))
	}
	// Release the remaining 9 active turns.
	close(slotReleaseCh)

	waitUntil(t, 30*time.Second, disp.TurnCompletedNotify(), func() bool {
		st := disp.SnapshotState()
		return st.Summary.TotalTurns >= 48 &&
			st.Summary.ReadyTenants == 24 &&
			st.Summary.SuspendedActors == 24 &&
			st.Summary.ActiveSlots == 0
	}, "all 24 tenants completing Commit Wave and suspending")

	if gotPeak := localBackend.PeakActiveSlots(); gotPeak > 9 {
		t.Fatalf("LocalActorBackend PeakActiveSlots = %d, want <= 9", gotPeak)
	}

	gateMu.Lock()
	gotTailOrder := slices.Clone(waveStartOrder[9:])
	gateMu.Unlock()
	if !slices.Equal(gotTailOrder, wantQueue) {
		t.Fatalf("FIFO slot acquisition order for queued tenants = %v, want %v", gotTailOrder, wantQueue)
	}

	finalState := disp.SnapshotState()
	for _, tsItem := range finalState.Tenants {
		if !tsItem.CacheHit {
			t.Errorf("tenant %s CacheHit = false on wave turn, want true", tsItem.TenantID)
		}
		if tsItem.ActorState != dispatcher.ActorStateSuspended {
			t.Errorf("tenant %s ActorState = %q, want SUSPENDED", tsItem.TenantID, tsItem.ActorState)
		}
	}

	// Exercise POST /api/tenants/tenant-07/commit single-tenant commit trigger.
	commitReq := `{"release":"v3-canary","message":"Single commit for tenant-07","wait":true}`
	cResp, err := ts.Client().Post(ts.URL+"/api/tenants/tenant-07/commit", "application/json", strings.NewReader(commitReq))
	if err != nil {
		t.Fatalf("POST /api/tenants/tenant-07/commit: %v", err)
	}
	defer cResp.Body.Close()
	if cResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/tenants/tenant-07/commit status = %d, want 200", cResp.StatusCode)
	}

	t07Resp, err := ts.Client().Get(ts.URL + "/api/tenants/tenant-07")
	if err != nil {
		t.Fatalf("GET /api/tenants/tenant-07: %v", err)
	}
	defer t07Resp.Body.Close()
	var t07 dispatcher.TenantDetail
	if err := json.NewDecoder(t07Resp.Body).Decode(&t07); err != nil {
		t.Fatalf("decode tenant-07 detail: %v", err)
	}
	if !strings.Contains(t07.LatestDiff, "v3-canary") {
		t.Errorf("expected tenant-07 LatestDiff to contain v3-canary, got %q", t07.LatestDiff)
	}
	if t07.LastCommitMessage != "Single commit for tenant-07" {
		t.Errorf("LastCommitMessage = %q, want %q", t07.LastCommitMessage, "Single commit for tenant-07")
	}

	var cm07 corev1.ConfigMap
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: "tenant-07", Name: "app-config"}, &cm07); err != nil {
		t.Fatalf("Get tenant-07 ConfigMap: %v", err)
	}
	if cm07.Data["release"] != "v3-canary" {
		t.Errorf("tenant-07 ConfigMap release = %q, want v3-canary", cm07.Data["release"])
	}

	var cm08 corev1.ConfigMap
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: "tenant-08", Name: "app-config"}, &cm08); err != nil {
		t.Fatalf("Get tenant-08 ConfigMap: %v", err)
	}
	if cm08.Data["release"] == "v3-canary" {
		t.Errorf("neighbor tenant-08 should not have tenant-07's release bump")
	}
}

func TestLocalStackStandalone24TenantsWithoutExternalKubeconfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stack, err := dispatcher.NewLocalStack(ctx, dispatcher.LocalStackConfig{
		BaseDir:     t.TempDir(),
		TenantCount: 24,
	})
	if err != nil {
		cancel()
		t.Fatalf("NewLocalStack: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		stack.Close()
	})

	ts := httptest.NewServer(stack.Handler())
	t.Cleanup(ts.Close)

	waitUntil(t, 30*time.Second, stack.Dispatcher().TurnCompletedNotify(), func() bool {
		st := stack.Dispatcher().SnapshotState()
		return st.Summary.TotalTenants == 24 &&
			st.Summary.ReadyTenants == 24 &&
			st.Summary.SuspendedActors == 24 &&
			st.Summary.ActiveSlots == 0
	}, "standalone --mode=local 24-tenant baseline reconcile")

	// Trigger 24-tenant Commit Wave and wait for completion.
	waveResp, err := ts.Client().Post(ts.URL+"/api/wave?wait=true", "application/json", strings.NewReader(`{"release":"local-wave-2","message":"Local wave v2"}`))
	if err != nil {
		t.Fatalf("POST /api/wave?wait=true: %v", err)
	}
	waveResp.Body.Close()
	if waveResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/wave status = %d, want 200", waveResp.StatusCode)
	}

	st := stack.Dispatcher().SnapshotState()
	if st.Summary.PeakActiveSlots > 9 {
		t.Fatalf("PeakActiveSlots = %d, want <= 9", st.Summary.PeakActiveSlots)
	}
	if st.Summary.ReadyTenants != 24 || st.Summary.SuspendedActors != 24 {
		t.Fatalf("after wave ReadyTenants=%d SuspendedActors=%d, want 24 and 24", st.Summary.ReadyTenants, st.Summary.SuspendedActors)
	}

	// Verify SSA inventory pruning works against standalone LocalKubeServer.
	prunePayload := `{
		"message": "Prune Service on tenant-03",
		"upsert": {
			"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - configmap.yaml\n  - deployment.yaml\n"
		},
		"delete": ["service.yaml"],
		"wait": true
	}`
	pResp, err := ts.Client().Post(ts.URL+"/api/tenants/tenant-03/commit", "application/json", strings.NewReader(prunePayload))
	if err != nil {
		t.Fatalf("POST prune commit: %v", err)
	}
	pResp.Body.Close()
	if pResp.StatusCode != http.StatusOK {
		t.Fatalf("POST prune commit status = %d, want 200", pResp.StatusCode)
	}

	detail03, ok := stack.Dispatcher().SnapshotTenant("tenant-03")
	if !ok {
		t.Fatal("tenant-03 not found")
	}
	if !detail03.CacheHit {
		t.Errorf("expected CacheHit=true on tenant-03")
	}
	if len(detail03.PrunedObjects) != 1 {
		t.Errorf("PrunedObjects = %v, want 1 pruned Service", detail03.PrunedObjects)
	}
	if len(detail03.Inventory) != 2 {
		t.Errorf("Inventory len = %d, want 2", len(detail03.Inventory))
	}
}

func TestHTTPRouterActorBackendRoutesWithTargetActorHeader(t *testing.T) {
	tenantNS := createTestNamespace(t, "s4-router")

	gitToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	gs, gitBaseURL, trustPEM := newGitServerTLS(t, nil, gitToken)
	seedRev, err := gs.SeedDefaultTenant(tenantNS, tenantNS)
	if err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	actorSrv, err := actor.NewServer(actor.Config{
		ActorName:      tenantNS,
		Atespace:       "flux-substrate",
		DurableDir:     t.TempDir(),
		TrustBundlePEM: trustPEM,
		RestConfig:     sharedRestCfg,
	})
	if err != nil {
		t.Fatalf("actor.NewServer: %v", err)
	}

	var gotTargetHeader atomic.Value
	routerSim := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTargetHeader.Store(r.Header.Get("ate-target-actor"))
		actorSrv.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(routerSim.Close)

	routerCAPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: routerSim.Certificate().Raw,
	})

	routerBackend, err := dispatcher.NewHTTPRouterActorBackend(dispatcher.HTTPRouterBackendConfig{
		RouterURL:   routerSim.URL,
		RouterCAPEM: routerCAPEM,
	})
	if err != nil {
		t.Fatalf("NewHTTPRouterActorBackend: %v", err)
	}

	ctx := context.Background()
	if err := routerBackend.EnsureActor(ctx, dispatcher.ActorSpec{Atespace: "flux-substrate", ActorName: tenantNS, TenantID: tenantNS}); err != nil {
		t.Fatalf("EnsureActor: %v", err)
	}
	if err := routerBackend.EnsureEgressPolicy(ctx, dispatcher.EgressPolicySpec{
		Atespace:  "flux-substrate",
		ActorName: tenantNS,
		GitScheme: "https",
		GitHost:   "127.0.0.1",
	}); err != nil {
		t.Fatalf("EnsureEgressPolicy: %v", err)
	}

	res, code, err := routerBackend.InvokeReconcile(ctx, dispatcher.TurnInvocation{
		Atespace:  "flux-substrate",
		ActorName: tenantNS,
		WorkerID:  "W1",
		SlotID:    "W1-1",
		Request: actor.ReconcileRequest{
			TenantID:        tenantNS,
			RepoURL:         fmt.Sprintf("%s/git/%s.git", gitBaseURL, tenantNS),
			Branch:          "main",
			Path:            "./",
			TargetNamespace: tenantNS,
		},
	})
	if err != nil {
		t.Fatalf("InvokeReconcile: %v", err)
	}
	if code != http.StatusOK || res.CommitSHA != seedRev.SHA {
		t.Fatalf("router InvokeReconcile code=%d sha=%q, want 200 and %q", code, res.CommitSHA, seedRev.SHA)
	}
	if got, _ := gotTargetHeader.Load().(string); got != "flux-substrate/"+tenantNS {
		t.Errorf("ate-target-actor header = %q, want %q", got, "flux-substrate/"+tenantNS)
	}
}
