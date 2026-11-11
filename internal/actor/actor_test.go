package actor_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/gke-demos/flux-substrate/internal/actor"
	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
	"github.com/gke-demos/flux-substrate/internal/oidcbridge"
)

var (
	sharedTestEnv    *envtest.Environment
	sharedRestCfg    *rest.Config
	sharedKubeClient client.Client
	nsSeq            atomic.Int32
)

func TestMain(m *testing.M) {
	ensureKubebuilderAssetsEnv()

	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start envtest control plane: %v\n", err)
		os.Exit(1)
	}
	sharedTestEnv = testEnv
	sharedRestCfg = cfg

	kc, err := client.New(cfg, client.Options{})
	if err != nil {
		_ = testEnv.Stop()
		fmt.Fprintf(os.Stderr, "failed to create kube client: %v\n", err)
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

// newGitFixtureWithEgressInjector boots a real git-server behind a 127.0.0.1 TLS server
// that simulates atenet-egress replaceHeaders: only requests carrying the actor's
// "Bearer actor-placeholder" header have the real tenant secret injected.
func newGitFixtureWithEgressInjector(t *testing.T, tenantID string) (*gitserver.Server, string, []byte) {
	t.Helper()

	tenantToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	adminToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken admin: %v", err)
	}

	gs, err := gitserver.New(gitserver.Config{
		ReposDir:   t.TempDir(),
		AdminToken: adminToken,
		TenantTokens: map[string]string{
			tenantID: tenantToken,
		},
	})
	if err != nil {
		t.Fatalf("gitserver.New: %v", err)
	}

	egressLeg := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r2 := r.Clone(r.Context())
		if r2.Header.Get("Authorization") == "Bearer actor-placeholder" {
			r2.Header.Set("Authorization", "Bearer "+tenantToken)
		}
		gs.Handler().ServeHTTP(w, r2)
	})

	tlsSrv := httptest.NewTLSServer(egressLeg)
	t.Cleanup(tlsSrv.Close)

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsSrv.Certificate().Raw,
	})

	repoURL := fmt.Sprintf("%s/git/%s.git", tlsSrv.URL, tenantID)
	return gs, repoURL, certPEM
}

func newTenantNamespace(t *testing.T, prefix string) string {
	t.Helper()
	name := fmt.Sprintf("%s-%02d", prefix, nsSeq.Add(1))
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := sharedKubeClient.Create(context.Background(), ns); err != nil {
		t.Fatalf("create namespace %s: %v", name, err)
	}
	return name
}

func TestWorkflowActorReadyzAndSecurityHeaders(t *testing.T) {
	durableDir := t.TempDir()

	srv, err := actor.NewServer(actor.Config{
		ActorName:  "tenant-01",
		Atespace:   "flux-substrate",
		DurableDir: durableDir,
		RestConfig: sharedRestCfg,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	resp, err := ts.Client().Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /readyz status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	for header, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Permissions-Policy":     "camera=(), microphone=(), geolocation=()",
	} {
		if got := resp.Header.Get(header); got != want {
			t.Errorf("header %s = %q, want %q", header, got, want)
		}
	}
	if csp := resp.Header.Get("Content-Security-Policy"); csp == "" {
		t.Errorf("expected non-empty Content-Security-Policy header")
	}

	var body struct {
		Status    string `json:"status"`
		ActorName string `json:"actorName"`
		Atespace  string `json:"atespace"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /readyz JSON: %v", err)
	}
	if body.Status != "ready" {
		t.Errorf("status = %q, want %q", body.Status, "ready")
	}
	if body.ActorName != "tenant-01" {
		t.Errorf("actorName = %q, want %q", body.ActorName, "tenant-01")
	}
	if body.Atespace != "flux-substrate" {
		t.Errorf("atespace = %q, want %q", body.Atespace, "flux-substrate")
	}

	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodTrace} {
		req, err := http.NewRequest(method, ts.URL+"/readyz", nil)
		if err != nil {
			t.Fatalf("NewRequest(%s): %v", method, err)
		}
		mResp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatalf("%s /readyz: %v", method, err)
		}
		mResp.Body.Close()
		if mResp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s /readyz status = %d, want %d", method, mResp.StatusCode, http.StatusMethodNotAllowed)
		}
	}
}

func TestWorkflowActorColdCloneRenderAndApply(t *testing.T) {
	tenantNS := newTenantNamespace(t, "tenant-cold")

	gs, repoURL, trustPEM := newGitFixtureWithEgressInjector(t, tenantNS)
	seeded, err := gs.SeedDefaultTenant(tenantNS, tenantNS)
	if err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	durableDir := t.TempDir()
	actorSrv, err := actor.NewServer(actor.Config{
		ActorName:      tenantNS,
		Atespace:       "flux-substrate",
		DurableDir:     durableDir,
		TrustBundlePEM: trustPEM,
		RestConfig:     sharedRestCfg,
	})
	if err != nil {
		t.Fatalf("actor.NewServer: %v", err)
	}

	actorTS := httptest.NewServer(actorSrv.Handler())
	t.Cleanup(actorTS.Close)

	reqBody, _ := json.Marshal(actor.ReconcileRequest{
		TenantID:        tenantNS,
		RepoURL:         repoURL,
		Branch:          "main",
		Path:            "./",
		TargetNamespace: tenantNS,
	})
	resp, err := actorTS.Client().Post(actorTS.URL+"/reconcile", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /reconcile: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /reconcile status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var result actor.ReconcileResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode ReconcileResult: %v", err)
	}

	if result.CacheHit {
		t.Errorf("Turn 1 CacheHit = true, want false on empty DurableDir")
	}
	if result.CommitSHA != seeded.SHA {
		t.Errorf("CommitSHA = %q, want %q", result.CommitSHA, seeded.SHA)
	}
	wantRevision := "main@sha1:" + seeded.SHA
	if result.Revision != wantRevision {
		t.Errorf("Revision = %q, want %q", result.Revision, wantRevision)
	}
	if !result.Ready {
		t.Errorf("Ready = false, want true (statusMessage=%q, error=%q)", result.StatusMessage, result.Error)
	}
	if len(result.Inventory) != 3 {
		t.Fatalf("Inventory len = %d, want 3 (%+v)", len(result.Inventory), result.Inventory)
	}
	if result.FetchMs < 0 || result.RenderMs < 0 || result.ApplyMs < 0 || result.Timings.TotalMs < 0 {
		t.Errorf("invalid negative phase timings: %+v", result.Timings)
	}

	var cm corev1.ConfigMap
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "app-config"}, &cm); err != nil {
		t.Fatalf("expected ConfigMap app-config in Kubernetes: %v", err)
	}
	if got := cm.Data["release"]; got != "v1" {
		t.Errorf("ConfigMap data[release] = %q, want %q", got, "v1")
	}

	var svc corev1.Service
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "tenant-workload"}, &svc); err != nil {
		t.Fatalf("expected Service tenant-workload in Kubernetes: %v", err)
	}

	var deploy appsv1.Deployment
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "tenant-workload"}, &deploy); err != nil {
		t.Fatalf("expected Deployment tenant-workload in Kubernetes: %v", err)
	}
}

func TestWorkflowActorWarmIncrementalFetchUpdatesResources(t *testing.T) {
	tenantNS := newTenantNamespace(t, "tenant-warm")

	gs, repoURL, trustPEM := newGitFixtureWithEgressInjector(t, tenantNS)
	rev1, err := gs.SeedDefaultTenant(tenantNS, tenantNS)
	if err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	durableDir := t.TempDir()

	runTurn := func() actor.ReconcileResult {
		t.Helper()
		// Construct a fresh actor server against the persisted DurableDir to exercise
		// SNAPSHOT_FIDELITY_VOLUMES wakeup behavior across turns.
		srv, err := actor.NewServer(actor.Config{
			ActorName:      tenantNS,
			Atespace:       "flux-substrate",
			DurableDir:     durableDir,
			TrustBundlePEM: trustPEM,
			RestConfig:     sharedRestCfg,
		})
		if err != nil {
			t.Fatalf("actor.NewServer: %v", err)
		}
		ts := httptest.NewServer(srv.Handler())
		defer ts.Close()

		payload, _ := json.Marshal(actor.ReconcileRequest{
			TenantID:        tenantNS,
			RepoURL:         repoURL,
			Branch:          "main",
			Path:            "./",
			TargetNamespace: tenantNS,
		})
		resp, err := ts.Client().Post(ts.URL+"/reconcile", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("POST /reconcile: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST /reconcile status = %d, want 200", resp.StatusCode)
		}
		var out actor.ReconcileResult
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode ReconcileResult: %v", err)
		}
		return out
	}

	turn1 := runTurn()
	if turn1.CacheHit {
		t.Fatalf("Turn 1 CacheHit = true, want false")
	}
	if turn1.CommitSHA != rev1.SHA {
		t.Fatalf("Turn 1 CommitSHA = %q, want %q", turn1.CommitSHA, rev1.SHA)
	}
	initialPackBytes := gs.Telemetry(tenantNS).LastPackBytes

	rev2, err := gs.Commit(tenantNS, gitserver.CommitChange{
		Upsert: map[string]string{
			"configmap.yaml": fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: %s
data:
  tenant: %q
  release: "v2"
  channel: "canary"
`, tenantNS, tenantNS),
		},
		Message: "Promote release to v2",
	})
	if err != nil {
		t.Fatalf("gs.Commit: %v", err)
	}

	turn2 := runTurn()
	if !turn2.CacheHit {
		t.Fatalf("Turn 2 CacheHit = false, want true against warm DurableDir")
	}
	if turn2.CommitSHA != rev2.SHA {
		t.Fatalf("Turn 2 CommitSHA = %q, want %q", turn2.CommitSHA, rev2.SHA)
	}
	warmPackBytes := gs.Telemetry(tenantNS).LastPackBytes
	if warmPackBytes >= initialPackBytes {
		t.Errorf("expected warm incremental delta pack (%d B) to be smaller than initial clone pack (%d B)", warmPackBytes, initialPackBytes)
	}

	var cm corev1.ConfigMap
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "app-config"}, &cm); err != nil {
		t.Fatalf("Get ConfigMap: %v", err)
	}
	if got := cm.Data["release"]; got != "v2" {
		t.Errorf("ConfigMap data[release] = %q, want %q", got, "v2")
	}
	if got := cm.Data["channel"]; got != "canary" {
		t.Errorf("ConfigMap data[channel] = %q, want %q", got, "canary")
	}
}

func TestWorkflowActorPrunesRemovedManifests(t *testing.T) {
	tenantNS := newTenantNamespace(t, "tenant-prune")

	gs, repoURL, trustPEM := newGitFixtureWithEgressInjector(t, tenantNS)
	if _, err := gs.SeedDefaultTenant(tenantNS, tenantNS); err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	durableDir := t.TempDir()
	srv, err := actor.NewServer(actor.Config{
		ActorName:      tenantNS,
		Atespace:       "flux-substrate",
		DurableDir:     durableDir,
		TrustBundlePEM: trustPEM,
		RestConfig:     sharedRestCfg,
	})
	if err != nil {
		t.Fatalf("actor.NewServer: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	doReconcile := func() actor.ReconcileResult {
		t.Helper()
		payload, _ := json.Marshal(actor.ReconcileRequest{
			TenantID:        tenantNS,
			RepoURL:         repoURL,
			Branch:          "main",
			Path:            "./",
			TargetNamespace: tenantNS,
		})
		resp, err := ts.Client().Post(ts.URL+"/reconcile", "application/json", bytes.NewReader(payload))
		if err != nil {
			t.Fatalf("POST /reconcile: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST /reconcile status = %d, want 200", resp.StatusCode)
		}
		var out actor.ReconcileResult
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatalf("decode ReconcileResult: %v", err)
		}
		return out
	}

	turn1 := doReconcile()
	if len(turn1.Inventory) != 3 {
		t.Fatalf("Turn 1 Inventory len = %d, want 3", len(turn1.Inventory))
	}

	var svc corev1.Service
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "tenant-workload"}, &svc); err != nil {
		t.Fatalf("expected Service before prune commit: %v", err)
	}

	rev2, err := gs.Commit(tenantNS, gitserver.CommitChange{
		Upsert: map[string]string{
			"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
labels:
  - pairs:
      app.kubernetes.io/managed-by: flux-substrate
      reconciler.ate.dev/tenant: ` + tenantNS + `
resources:
  - configmap.yaml
  - deployment.yaml
`,
		},
		Delete:  []string{"service.yaml"},
		Message: "Remove Service manifest",
	})
	if err != nil {
		t.Fatalf("gs.Commit: %v", err)
	}

	turn2 := doReconcile()
	if !turn2.CacheHit {
		t.Errorf("Turn 2 CacheHit = false, want true")
	}
	if turn2.CommitSHA != rev2.SHA {
		t.Errorf("Turn 2 CommitSHA = %q, want %q", turn2.CommitSHA, rev2.SHA)
	}
	if len(turn2.PrunedObjects) != 1 {
		t.Fatalf("Turn 2 PrunedObjects = %v, want 1 pruned Service", turn2.PrunedObjects)
	}
	if len(turn2.Inventory) != 2 {
		t.Fatalf("Turn 2 Inventory len = %d, want 2 (%+v)", len(turn2.Inventory), turn2.Inventory)
	}

	err = sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "tenant-workload"}, &svc)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected Service tenant-workload to be NotFound after prune, got err=%v", err)
	}

	var cm corev1.ConfigMap
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "app-config"}, &cm); err != nil {
		t.Fatalf("expected ConfigMap app-config to remain after prune: %v", err)
	}
	var deploy appsv1.Deployment
	if err := sharedKubeClient.Get(context.Background(), client.ObjectKey{Namespace: tenantNS, Name: "tenant-workload"}, &deploy); err != nil {
		t.Fatalf("expected Deployment tenant-workload to remain after prune: %v", err)
	}
}

func TestWorkflowActorSerializesConcurrentReconcileTurns(t *testing.T) {
	tenantNS := newTenantNamespace(t, "tenant-serial")

	tenantToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	adminToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken admin: %v", err)
	}

	gs, err := gitserver.New(gitserver.Config{
		ReposDir:     t.TempDir(),
		AdminToken:   adminToken,
		TenantTokens: map[string]string{tenantNS: tenantToken},
	})
	if err != nil {
		t.Fatalf("gitserver.New: %v", err)
	}
	if _, err := gs.SeedDefaultTenant(tenantNS, tenantNS); err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	var activeGitOps atomic.Int32
	var maxConcurrentGitOps atomic.Int32

	instrumentedGit := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := activeGitOps.Add(1)
		for {
			prev := maxConcurrentGitOps.Load()
			if cur <= prev || maxConcurrentGitOps.CompareAndSwap(prev, cur) {
				break
			}
		}
		defer activeGitOps.Add(-1)

		// Hold the Git request briefly so concurrent POST /reconcile calls overlap.
		time.Sleep(40 * time.Millisecond)

		r2 := r.Clone(r.Context())
		if r2.Header.Get("Authorization") == "Bearer actor-placeholder" {
			r2.Header.Set("Authorization", "Bearer "+tenantToken)
		}
		gs.Handler().ServeHTTP(w, r2)
	})

	tlsSrv := httptest.NewTLSServer(instrumentedGit)
	t.Cleanup(tlsSrv.Close)

	trustPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: tlsSrv.Certificate().Raw,
	})
	repoURL := fmt.Sprintf("%s/git/%s.git", tlsSrv.URL, tenantNS)

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
	actorTS := httptest.NewServer(actorSrv.Handler())
	t.Cleanup(actorTS.Close)

	const callers = 4
	results := make([]actor.ReconcileResult, callers)
	statuses := make([]int, callers)
	errs := make([]error, callers)

	var wg sync.WaitGroup
	startGate := make(chan struct{})
	for i := range callers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-startGate
			payload, _ := json.Marshal(actor.ReconcileRequest{
				TenantID:        tenantNS,
				RepoURL:         repoURL,
				Branch:          "main",
				Path:            "./",
				TargetNamespace: tenantNS,
			})
			resp, reqErr := actorTS.Client().Post(actorTS.URL+"/reconcile", "application/json", bytes.NewReader(payload))
			if reqErr != nil {
				errs[idx] = reqErr
				return
			}
			defer resp.Body.Close()
			statuses[idx] = resp.StatusCode
			errs[idx] = json.NewDecoder(resp.Body).Decode(&results[idx])
		}(i)
	}
	close(startGate)
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d error: %v", i, errs[i])
		}
		if statuses[i] != http.StatusOK {
			t.Fatalf("caller %d HTTP status = %d, want 200 (error=%q)", i, statuses[i], results[i].Error)
		}
		if !results[i].Ready {
			t.Fatalf("caller %d Ready = false, want true", i)
		}
	}

	if gotMax := maxConcurrentGitOps.Load(); gotMax != 1 {
		t.Fatalf("max concurrent Git operations from actor = %d, want 1 (serialized turns)", gotMax)
	}
}

func TestWorkflowActorReconcilesThroughIdentityBridgeAndEnforcesNamespaceRBAC(t *testing.T) {
	ctx := context.Background()
	tenant01 := newTenantNamespace(t, "tenant-rbac-a")
	tenant02 := newTenantNamespace(t, "tenant-rbac-b")

	// Provision a least-privilege Identity Bridge identity in envtest that can ONLY
	// impersonate users (zero direct permissions on namespaces or workloads).
	bridgeUserName := "system:serviceaccount:flux-substrate:kube-oidc-bridge-" + tenant01
	bridgeUser, err := sharedTestEnv.AddUser(envtest.User{
		Name:   bridgeUserName,
		Groups: []string{"system:serviceaccounts", "system:serviceaccounts:flux-substrate", "system:authenticated"},
	}, nil)
	if err != nil {
		t.Fatalf("envtest.AddUser: %v", err)
	}

	impersonatorRoleName := "oidc-bridge-impersonator-" + tenant01
	if err := sharedKubeClient.Create(ctx, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: impersonatorRoleName},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"users"},
				Verbs:     []string{"impersonate"},
			},
		},
	}); err != nil {
		t.Fatalf("create impersonator ClusterRole: %v", err)
	}
	if err := sharedKubeClient.Create(ctx, &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: impersonatorRoleName},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     impersonatorRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:     rbacv1.UserKind,
				APIGroup: rbacv1.GroupName,
				Name:     bridgeUserName,
			},
		},
	}); err != nil {
		t.Fatalf("create impersonator ClusterRoleBinding: %v", err)
	}

	// Bind per-namespace RBAC strictly to each tenant's impersonated actor identity.
	bindTenantActorRBAC(t, ctx, tenant01, "ate:actor:flux-substrate:"+tenant01)
	bindTenantActorRBAC(t, ctx, tenant02, "ate:actor:flux-substrate:"+tenant02)

	// Start TLS OIDC IdP publishing an ES256 JWK (ate-idp-server wire contract).
	ecPriv, idpServer, idpCAPEM := startActorTestIDP(t, "idp-key-1")

	// Start TLS Identity Bridge on 127.0.0.1 with audience bound to its own HTTPS URL.
	var bridgeHandler atomic.Value
	bridgeTLS := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bridgeHandler.Load().(http.Handler).ServeHTTP(w, r)
	}))
	t.Cleanup(bridgeTLS.Close)

	bridge, err := oidcbridge.NewServer(oidcbridge.Config{
		IssuerURL:          idpServer.URL,
		Audiences:          []string{bridgeTLS.URL},
		IssuerCAPEM:        idpCAPEM,
		UpstreamRestConfig: bridgeUser.Config(),
	})
	if err != nil {
		t.Fatalf("oidcbridge.NewServer: %v", err)
	}
	bridgeHandler.Store(bridge.Handler())

	bridgeCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: bridgeTLS.Certificate().Raw,
	})

	gs, repoURL, gitCertPEM := newGitFixtureWithEgressInjector(t, tenant01)
	seeded, err := gs.SeedDefaultTenant(tenant01, tenant01)
	if err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}

	combinedTrustPEM := slices.Concat(gitCertPEM, bridgeCertPEM)

	sysInfoDir := t.TempDir()
	mustWriteActorFile(t, filepath.Join(sysInfoDir, "actor-name"), tenant01)
	mustWriteActorFile(t, filepath.Join(sysInfoDir, "atespace"), "flux-substrate")
	mustWriteActorFile(t, filepath.Join(sysInfoDir, "actor-uid"), "uid-"+tenant01)
	if err := os.WriteFile(filepath.Join(sysInfoDir, "trust-bundle.pem"), combinedTrustPEM, 0o600); err != nil {
		t.Fatalf("write trust-bundle.pem: %v", err)
	}

	var sawPlaceholder atomic.Int64
	var subjectPrefix atomic.Value
	subjectPrefix.Store("actor")

	// Simulate atenet-egress replaceHeaders: verify actor sends only "Bearer actor-placeholder"
	// and replace it on the wire with a freshly signed actorJwt.
	egressReplaceHeaders := func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			r2 := req.Clone(req.Context())
			if r2.Header.Get("Authorization") == "Bearer actor-placeholder" {
				sawPlaceholder.Add(1)
				prefix := subjectPrefix.Load().(string)
				jwt := mintTestActorJWT(t, ecPriv, "idp-key-1", idpServer.URL, bridgeTLS.URL, prefix+"/flux-substrate/"+tenant01, tenant01)
				r2.Header.Set("Authorization", "Bearer "+jwt)
			}
			return rt.RoundTrip(r2)
		})
	}

	durableDir := t.TempDir()

	// Verify explicit non-existent TrustBundleFile fails closed instead of ignoring CA configuration.
	if _, badErr := actor.NewServer(actor.Config{
		DurableDir:      durableDir,
		TrustBundleFile: filepath.Join(t.TempDir(), "missing-bundle.pem"),
		KubeBridgeURL:   bridgeTLS.URL,
	}); badErr == nil {
		t.Fatalf("expected actor.NewServer to fail on unreadable explicit TrustBundleFile")
	}

	// Turn 1 server discovers projected trust-bundle.pem directly from SystemInfoDir (CAFile).
	actorSrvFileCA, err := actor.NewServer(actor.Config{
		DurableDir:        durableDir,
		SystemInfoDir:     sysInfoDir,
		KubeBridgeURL:     bridgeTLS.URL,
		KubeWrapTransport: egressReplaceHeaders,
	})
	if err != nil {
		t.Fatalf("actor.NewServer (SystemInfoDir trust-bundle.pem): %v", err)
	}
	actorTSFileCA := httptest.NewServer(actorSrvFileCA.Handler())
	t.Cleanup(actorTSFileCA.Close)

	// Turn 2/3 server uses in-memory TrustBundlePEM (CAData) against the same warm DurableDir.
	actorSrv, err := actor.NewServer(actor.Config{
		DurableDir:        durableDir,
		SystemInfoDir:     sysInfoDir,
		TrustBundlePEM:    combinedTrustPEM,
		KubeBridgeURL:     bridgeTLS.URL,
		KubeWrapTransport: egressReplaceHeaders,
	})
	if err != nil {
		t.Fatalf("actor.NewServer: %v", err)
	}
	actorTS := httptest.NewServer(actorSrv.Handler())
	t.Cleanup(actorTS.Close)

	postReconcileTo := func(ts *httptest.Server) (int, actor.ReconcileResult) {
		t.Helper()
		body, _ := json.Marshal(actor.ReconcileRequest{
			TenantID:        tenant01,
			RepoURL:         repoURL,
			Branch:          "main",
			Path:            "./",
			TargetNamespace: tenant01,
		})
		resp, reqErr := ts.Client().Post(ts.URL+"/reconcile", "application/json", bytes.NewReader(body))
		if reqErr != nil {
			t.Fatalf("POST /reconcile: %v", reqErr)
		}
		defer resp.Body.Close()
		var out actor.ReconcileResult
		if decErr := json.NewDecoder(resp.Body).Decode(&out); decErr != nil {
			t.Fatalf("decode ReconcileResult: %v", decErr)
		}
		return resp.StatusCode, out
	}
	postReconcile := func() (int, actor.ReconcileResult) {
		return postReconcileTo(actorTS)
	}

	// Turn 1: tenant-01 reconciles its own namespace cleanly via Identity Bridge using SystemInfoDir trust-bundle.pem.
	status1, turn1 := postReconcileTo(actorTSFileCA)
	if status1 != http.StatusOK {
		t.Fatalf("Turn 1 status = %d, want 200 (stage=%q err=%q)", status1, turn1.FailedStage, turn1.Error)
	}
	if !turn1.Ready || turn1.CommitSHA != seeded.SHA || len(turn1.Inventory) != 3 {
		t.Fatalf("Turn 1 unexpected result: %+v", turn1)
	}
	if sawPlaceholder.Load() == 0 {
		t.Fatalf("expected actor client-go transport to send Bearer actor-placeholder header")
	}

	var cm corev1.ConfigMap
	if err := sharedKubeClient.Get(ctx, client.ObjectKey{Namespace: tenant01, Name: "app-config"}, &cm); err != nil {
		t.Fatalf("expected ConfigMap app-config in %s: %v", tenant01, err)
	}

	// Turn 2 (Cross-Namespace RBAC Poison Pill): commit a manifest targeting tenant-02.
	if _, err := gs.Commit(tenant01, gitserver.CommitChange{
		Upsert: map[string]string{
			"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - configmap.yaml
  - deployment.yaml
  - service.yaml
  - escape.yaml
`,
			"escape.yaml": fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: cross-tenant-escape
  namespace: %s
data:
  breach: "attempted"
`, tenant02),
		},
		Message: "Poison pill: cross-namespace RBAC escape into " + tenant02,
	}); err != nil {
		t.Fatalf("gs.Commit cross-namespace poison pill: %v", err)
	}

	status2, turn2 := postReconcile()
	if status2 != http.StatusForbidden {
		t.Fatalf("Turn 2 cross-namespace status = %d, want 403 Forbidden (stage=%q err=%q)", status2, turn2.FailedStage, turn2.Error)
	}
	if !turn2.CacheHit {
		t.Errorf("Turn 2 CacheHit = false, want true")
	}
	if turn2.Ready {
		t.Errorf("Turn 2 Ready = true, want false on RBAC denial")
	}
	if turn2.FailedStage != "apply" {
		t.Errorf("Turn 2 FailedStage = %q, want %q", turn2.FailedStage, "apply")
	}

	var escaped corev1.ConfigMap
	err = sharedKubeClient.Get(ctx, client.ObjectKey{Namespace: tenant02, Name: "cross-tenant-escape"}, &escaped)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected cross-tenant-escape ConfigMap to NOT exist in %s, got err=%v", tenant02, err)
	}

	// Turn 3: verify worker-for-actor token is rejected with 403 Forbidden end-to-end.
	subjectPrefix.Store("worker-for-actor")
	status3, turn3 := postReconcile()
	if status3 != http.StatusForbidden {
		t.Fatalf("Turn 3 worker-for-actor status = %d, want 403 Forbidden (err=%q)", status3, turn3.Error)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func bindTenantActorRBAC(t *testing.T, ctx context.Context, namespace, actorPrincipal string) {
	t.Helper()
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tenant-reconciler",
			Namespace: namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"", "apps"},
				Resources: []string{"configmaps", "services", "deployments", "replicasets", "pods"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		},
	}
	if err := sharedKubeClient.Create(ctx, role); err != nil {
		t.Fatalf("create Role in %s: %v", namespace, err)
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tenant-reconciler-binding",
			Namespace: namespace,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     role.Name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:     rbacv1.UserKind,
				APIGroup: rbacv1.GroupName,
				Name:     actorPrincipal,
			},
		},
	}
	if err := sharedKubeClient.Create(ctx, rb); err != nil {
		t.Fatalf("create RoleBinding in %s: %v", namespace, err)
	}
}

func startActorTestIDP(t *testing.T, kid string) (*ecdsa.PrivateKey, *httptest.Server, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	uncompressed, err := priv.PublicKey.Bytes()
	if err != nil {
		t.Fatalf("PublicKey.Bytes: %v", err)
	}

	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("GET /.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   srv.URL,
			"jwks_uri": srv.URL + "/openid/v1/jwks",
		})
	})
	mux.HandleFunc("GET /openid/v1/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"keys": []map[string]any{
				{
					"kty": "EC",
					"kid": kid,
					"use": "sig",
					"alg": "ES256",
					"crv": "P-256",
					"x":   base64.RawURLEncoding.EncodeToString(uncompressed[1:33]),
					"y":   base64.RawURLEncoding.EncodeToString(uncompressed[33:65]),
				},
			},
		})
	})

	srv = httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	caPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: srv.Certificate().Raw,
	})
	return priv, srv, caPEM
}

func mintTestActorJWT(t *testing.T, priv *ecdsa.PrivateKey, kid, issuer, audience, sub, actorName string) string {
	t.Helper()
	now := time.Now()
	hdrJSON, _ := json.Marshal(map[string]any{
		"typ": "JWT",
		"alg": "ES256",
		"kid": kid,
	})
	claims := map[string]any{
		"iss": issuer,
		"sub": sub,
		"aud": []string{audience},
		"iat": now.Unix(),
		"nbf": now.Add(-5 * time.Minute).Unix(),
		"exp": now.Add(10 * time.Minute).Unix(),
	}
	if slices.Equal([]byte(sub[:5]), []byte("actor")) {
		claims["actor.ate.dev"] = map[string]any{
			"atespace":  "flux-substrate",
			"actorName": actorName,
			"actorUID":  "uid-" + actorName,
		}
	}
	payJSON, _ := json.Marshal(claims)
	signingInput := base64.RawURLEncoding.EncodeToString(hdrJSON) + "." + base64.RawURLEncoding.EncodeToString(payJSON)
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

func mustWriteActorFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content+"\n"), 0o600); err != nil {
		t.Fatalf("WriteFile %s: %v", path, err)
	}
}
