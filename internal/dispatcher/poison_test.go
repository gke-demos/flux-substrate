package dispatcher_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-demos/flux-substrate/internal/dispatcher"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

func TestDispatcherPoisonOOMContainmentRevertActorRecoveryAndFleetReset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stack, err := dispatcher.NewLocalStack(ctx, dispatcher.LocalStackConfig{
		BaseDir:     t.TempDir(),
		TenantCount: 6,
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

	waitUntil(t, 25*time.Second, stack.Dispatcher().TurnCompletedNotify(), func() bool {
		st := stack.Dispatcher().SnapshotState()
		return st.Summary.TotalTenants == 6 &&
			st.Summary.ReadyTenants == 6 &&
			st.Summary.SuspendedActors == 6 &&
			st.Summary.ActiveSlots == 0
	}, "6-tenant baseline reconciliation")

	// 1. Trigger Memory-Bomb Kustomize OOM Poison Pill on tenant-01.
	oomResp, err := ts.Client().Post(ts.URL+"/api/tenants/tenant-01/poison/oom?wait=true", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /api/tenants/tenant-01/poison/oom: %v", err)
	}
	defer oomResp.Body.Close()
	if oomResp.StatusCode != http.StatusOK {
		t.Fatalf("POST poison/oom status = %d, want 200", oomResp.StatusCode)
	}

	detail01, ok := stack.Dispatcher().SnapshotTenant("tenant-01")
	if !ok {
		t.Fatal("tenant-01 not found")
	}
	if detail01.ActorState != dispatcher.ActorStateCrashed {
		t.Fatalf("tenant-01 ActorState = %q, want %q", detail01.ActorState, dispatcher.ActorStateCrashed)
	}
	if detail01.Ready || !detail01.Stalled || detail01.FailedStage != "oom" {
		t.Fatalf("tenant-01 after OOM: ready=%v stalled=%v failedStage=%q err=%q",
			detail01.Ready, detail01.Stalled, detail01.FailedStage, detail01.LastError)
	}

	var ks01 kustomizev1.Kustomization
	if err := stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "flux-substrate", Name: "tenant-01"}, &ks01); err != nil {
		t.Fatalf("get tenant-01 Kustomization: %v", err)
	}
	stalledCond := findCondition(ks01.Status.Conditions, fluxmeta.StalledCondition)
	if stalledCond == nil || stalledCond.Status != metav1.ConditionTrue || stalledCond.Reason != "ActorCrashed" {
		t.Errorf("Kustomization Stalled condition = %+v, want True/ActorCrashed", stalledCond)
	}
	readyCond := findCondition(ks01.Status.Conditions, fluxmeta.ReadyCondition)
	if readyCond == nil || readyCond.Status != metav1.ConditionFalse || readyCond.Reason != "ActorCrashed" {
		t.Errorf("Kustomization Ready condition = %+v, want False/ActorCrashed", readyCond)
	}

	// 2. Verify blast-radius containment: fire a Commit Wave while tenant-01 is CRASHED
	// and verify sibling tenants tenant-02..tenant-06 reconcile normally on the shared WorkerPool.
	waveResp, err := ts.Client().Post(ts.URL+"/api/wave?wait=true", "application/json", strings.NewReader(`{"release":"v2-during-crash"}`))
	if err != nil {
		t.Fatalf("POST /api/wave during crash: %v", err)
	}
	waveResp.Body.Close()
	if waveResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/wave status = %d, want 200", waveResp.StatusCode)
	}

	stAfterWave := stack.Dispatcher().SnapshotState()
	if stAfterWave.Summary.CrashedActors != 1 || stAfterWave.Summary.ReadyTenants != 5 {
		t.Fatalf("summary during crash wave: CrashedActors=%d ReadyTenants=%d, want 1 and 5",
			stAfterWave.Summary.CrashedActors, stAfterWave.Summary.ReadyTenants)
	}

	var cm02 corev1.ConfigMap
	if err := stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "tenant-02", Name: "app-config"}, &cm02); err != nil {
		t.Fatalf("get tenant-02 ConfigMap: %v", err)
	}
	if cm02.Data["release"] != "v2-during-crash" {
		t.Errorf("tenant-02 ConfigMap release = %q, want v2-during-crash", cm02.Data["release"])
	}

	// 3. Recover tenant-01 via POST /api/tenants/tenant-01/recover (RevertActor + baseline restore).
	recResp, err := ts.Client().Post(ts.URL+"/api/tenants/tenant-01/recover?wait=true", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /api/tenants/tenant-01/recover: %v", err)
	}
	defer recResp.Body.Close()
	if recResp.StatusCode != http.StatusOK {
		t.Fatalf("POST recover status = %d, want 200", recResp.StatusCode)
	}

	recovered01, _ := stack.Dispatcher().SnapshotTenant("tenant-01")
	if recovered01.ActorState != dispatcher.ActorStateSuspended || !recovered01.Ready || recovered01.Stalled {
		t.Fatalf("tenant-01 after RevertActor recover: state=%q ready=%v stalled=%v err=%q",
			recovered01.ActorState, recovered01.Ready, recovered01.Stalled, recovered01.LastError)
	}

	if err := stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "flux-substrate", Name: "tenant-01"}, &ks01); err != nil {
		t.Fatalf("get tenant-01 Kustomization post-recover: %v", err)
	}
	if stalledAfter := findCondition(ks01.Status.Conditions, fluxmeta.StalledCondition); stalledAfter != nil {
		t.Errorf("expected StalledCondition removed after RevertActor recovery, got %+v", stalledAfter)
	}

	// 4. Crash tenant-03 and verify POST /api/reset restores the entire fleet in one click.
	oom03, err := ts.Client().Post(ts.URL+"/api/tenants/tenant-03/poison/oom?wait=true", "application/json", nil)
	if err != nil {
		t.Fatalf("POST tenant-03 poison/oom: %v", err)
	}
	oom03.Body.Close()

	resetResp, err := ts.Client().Post(ts.URL+"/api/reset?wait=true", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /api/reset: %v", err)
	}
	defer resetResp.Body.Close()
	if resetResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/reset status = %d, want 200", resetResp.StatusCode)
	}

	finalState := stack.Dispatcher().SnapshotState()
	if finalState.Summary.ReadyTenants != 6 || finalState.Summary.CrashedActors != 0 || finalState.Summary.SuspendedActors != 6 {
		t.Fatalf("after fleet reset: Ready=%d Crashed=%d Suspended=%d, want 6, 0, 6",
			finalState.Summary.ReadyTenants, finalState.Summary.CrashedActors, finalState.Summary.SuspendedActors)
	}
}

func TestDispatcherPoisonCrossNamespaceRBACAndUnauthorizedEgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stack, err := dispatcher.NewLocalStack(ctx, dispatcher.LocalStackConfig{
		BaseDir:     t.TempDir(),
		TenantCount: 3,
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

	waitUntil(t, 20*time.Second, stack.Dispatcher().TurnCompletedNotify(), func() bool {
		st := stack.Dispatcher().SnapshotState()
		return st.Summary.TotalTenants == 3 && st.Summary.ReadyTenants == 3 && st.Summary.ActiveSlots == 0
	}, "3-tenant baseline reconciliation")

	// 1. Cross-Namespace RBAC Escape Poison Pill: tenant-01 attempts to apply into tenant-02.
	rbacResp, err := ts.Client().Post(
		ts.URL+"/api/tenants/tenant-01/poison/rbac?wait=true",
		"application/json",
		strings.NewReader(`{"targetNamespace":"tenant-02"}`),
	)
	if err != nil {
		t.Fatalf("POST poison/rbac: %v", err)
	}
	defer rbacResp.Body.Close()
	if rbacResp.StatusCode != http.StatusOK {
		t.Fatalf("POST poison/rbac status = %d, want 200", rbacResp.StatusCode)
	}

	detailRBAC, _ := stack.Dispatcher().SnapshotTenant("tenant-01")
	if detailRBAC.Ready {
		t.Fatalf("expected tenant-01 Ready=false after cross-namespace RBAC denial")
	}
	if detailRBAC.FailedStage != "apply" {
		t.Errorf("FailedStage = %q, want %q (err=%q)", detailRBAC.FailedStage, "apply", detailRBAC.LastError)
	}
	if !strings.Contains(strings.ToLower(detailRBAC.LastError), "forbidden") {
		t.Errorf("expected 403 forbidden in LastError, got %q", detailRBAC.LastError)
	}

	// Verify neighbor namespace tenant-02 was untouched.
	var escapedCM corev1.ConfigMap
	err = stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "tenant-02", Name: "cross-tenant-escape"}, &escapedCM)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("expected cross-tenant-escape ConfigMap NotFound in tenant-02, got err=%v", err)
	}

	var ks01 kustomizev1.Kustomization
	if err := stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "flux-substrate", Name: "tenant-01"}, &ks01); err != nil {
		t.Fatalf("get tenant-01 Kustomization: %v", err)
	}
	readyCond := findCondition(ks01.Status.Conditions, fluxmeta.ReadyCondition)
	if readyCond == nil || readyCond.Status != metav1.ConditionFalse || readyCond.Reason != "AccessDenied" {
		t.Errorf("Kustomization Ready condition after RBAC denial = %+v, want False/AccessDenied", readyCond)
	}

	// 2. Unauthorized Remote-Base Egress Poison Pill: tenant-02 references an unapproved HTTPS base.
	var spyHits atomic.Int64
	spySrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		spyHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(spySrv.Close)

	egressBody := fmt.Sprintf(`{"remoteBaseUrl":%q}`, spySrv.URL+"/unapproved-base.yaml")
	egressResp, err := ts.Client().Post(
		ts.URL+"/api/tenants/tenant-02/poison/egress?wait=true",
		"application/json",
		strings.NewReader(egressBody),
	)
	if err != nil {
		t.Fatalf("POST poison/egress: %v", err)
	}
	defer egressResp.Body.Close()
	if egressResp.StatusCode != http.StatusOK {
		t.Fatalf("POST poison/egress status = %d, want 200", egressResp.StatusCode)
	}

	if spyHits.Load() != 0 {
		t.Fatalf("unapproved remote-base server received %d requests, want 0", spyHits.Load())
	}

	detailEgress, _ := stack.Dispatcher().SnapshotTenant("tenant-02")
	if detailEgress.Ready {
		t.Fatalf("expected tenant-02 Ready=false after unauthorized egress")
	}
	if detailEgress.FailedStage != "egress" {
		t.Errorf("FailedStage = %q, want %q (err=%q)", detailEgress.FailedStage, "egress", detailEgress.LastError)
	}

	var ks02 kustomizev1.Kustomization
	if err := stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "flux-substrate", Name: "tenant-02"}, &ks02); err != nil {
		t.Fatalf("get tenant-02 Kustomization: %v", err)
	}
	egressCond := findCondition(ks02.Status.Conditions, fluxmeta.ReadyCondition)
	if egressCond == nil || egressCond.Status != metav1.ConditionFalse || egressCond.Reason != "AccessDenied" {
		t.Errorf("Kustomization Ready condition after egress denial = %+v, want False/AccessDenied", egressCond)
	}
}

func TestDispatcherRegisterExternalRepoSynthesizesEgressPolicyAndReconciles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	stack, err := dispatcher.NewLocalStack(ctx, dispatcher.LocalStackConfig{
		BaseDir:     t.TempDir(),
		TenantCount: 2,
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

	waitUntil(t, 20*time.Second, stack.Dispatcher().TurnCompletedNotify(), func() bool {
		st := stack.Dispatcher().SnapshotState()
		return st.Summary.TotalTenants == 2 && st.Summary.ReadyTenants == 2 && st.Summary.ActiveSlots == 0
	}, "2-tenant baseline reconciliation")

	// 1. Validate rejection of non-HTTPS URLs, embedded credentials, path traversal, and invalid credentialUri.
	invalidPayloads := []string{
		`{"tenantId":"tenant-01","repoUrl":"http://github.com/example/gitops.git"}`,
		`{"tenantId":"tenant-01","repoUrl":"ssh://git@github.com/example/gitops.git"}`,
		`{"tenantId":"tenant-01","repoUrl":"https://user:secret@github.com/example/gitops.git"}`,
		`{"tenantId":"tenant-01","repoUrl":"https://github.com/example/gitops.git","path":"../escape"}`,
		`{"tenantId":"tenant-01","repoUrl":"https://github.com/example/gitops.git","credentialUri":"https://not-ate-secret"}`,
	}
	for _, bad := range invalidPayloads {
		resp, reqErr := ts.Client().Post(ts.URL+"/api/repos/external", "application/json", strings.NewReader(bad))
		if reqErr != nil {
			t.Fatalf("POST invalid external repo: %v", reqErr)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("payload %s returned status %d, want 400 Bad Request", bad, resp.StatusCode)
		}
	}

	// 2. Boot a separate TLS Git server simulating an external HTTPS Git provider (e.g. github.com)
	// and trust its CA certificate in the local backend.
	extToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	extGS, extBaseURL, extCertPEM := newGitServerTLS(t, nil, extToken)
	stack.LocalBackend().AddTrustBundlePEM(extCertPEM)

	extSeed, err := extGS.SeedTenant("ext-gitops", map[string]string{
		"kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1beta1\nkind: Kustomization\nresources:\n  - configmap.yaml\n",
		"configmap.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: tenant-01
data:
  tenant: "tenant-01"
  release: "v9-external-github"
  source: "external-https"
`,
	}, "Seed external GitHub-style GitOps repository")
	if err != nil {
		t.Fatalf("extGS.SeedTenant: %v", err)
	}

	extRepoURL := extBaseURL + "/git/ext-gitops.git"
	parsedExtURL, _ := url.Parse(extRepoURL)
	wantCredURI := "ate-secret://k8s.io/default/flux-substrate/git-external-t01/token"
	secretMarker := "ghp_sensitive_token_must_never_leak_998877"

	regPayload := fmt.Sprintf(`{
		"tenantId": "tenant-01",
		"repoUrl": %q,
		"branch": "main",
		"path": "./",
		"secretName": "git-external-t01",
		"credentialUri": %q,
		"token": %q,
		"wait": true
	}`, extRepoURL, wantCredURI, secretMarker)

	regResp, err := ts.Client().Post(ts.URL+"/api/repos/external", "application/json", strings.NewReader(regPayload))
	if err != nil {
		t.Fatalf("POST /api/repos/external: %v", err)
	}
	rawRespBytes, _ := io.ReadAll(regResp.Body)
	regResp.Body.Close()
	if regResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/repos/external status = %d, want 200 (body=%s)", regResp.StatusCode, string(rawRespBytes))
	}
	if strings.Contains(string(rawRespBytes), secretMarker) {
		t.Fatalf("POST /api/repos/external response leaked raw Git token")
	}

	detail01, ok := stack.Dispatcher().SnapshotTenant("tenant-01")
	if !ok {
		t.Fatal("tenant-01 not found")
	}
	if !detail01.Ready || detail01.CommitSHA != extSeed.SHA {
		t.Fatalf("tenant-01 after external repo sync: ready=%v sha=%q, want true and %q (err=%q)",
			detail01.Ready, detail01.CommitSHA, extSeed.SHA, detail01.LastError)
	}
	if detail01.EgressPolicy == nil {
		t.Fatal("expected EgressPolicy on tenant-01")
	}
	if detail01.EgressPolicy.GitHost != parsedExtURL.Host {
		t.Errorf("EgressPolicy.GitHost = %q, want %q", detail01.EgressPolicy.GitHost, parsedExtURL.Host)
	}
	if detail01.EgressPolicy.GitCredentialURI != wantCredURI {
		t.Errorf("EgressPolicy.GitCredentialURI = %q, want %q", detail01.EgressPolicy.GitCredentialURI, wantCredURI)
	}

	var cm01 corev1.ConfigMap
	if err := stack.KubeClient().Get(ctx, client.ObjectKey{Namespace: "tenant-01", Name: "app-config"}, &cm01); err != nil {
		t.Fatalf("get tenant-01 ConfigMap: %v", err)
	}
	if cm01.Data["release"] != "v9-external-github" {
		t.Errorf("tenant-01 ConfigMap release = %q, want v9-external-github", cm01.Data["release"])
	}
}

func TestDispatcherPresenterTokenSessionCSRFAndRateLimit(t *testing.T) {
	const presenterSecret = "presenter-stage-key-2026"

	ctx, cancel := context.WithCancel(context.Background())
	stack, err := dispatcher.NewLocalStack(ctx, dispatcher.LocalStackConfig{
		BaseDir:        t.TempDir(),
		TenantCount:    2,
		PresenterToken: presenterSecret,
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

	waitUntil(t, 20*time.Second, stack.Dispatcher().TurnCompletedNotify(), func() bool {
		st := stack.Dispatcher().SnapshotState()
		return st.Summary.TotalTenants == 2 && st.Summary.ReadyTenants == 2 && st.Summary.ActiveSlots == 0
	}, "2-tenant baseline reconciliation")

	// 1. Read-only GET routes remain open without presenter token.
	for _, getPath := range []string{"/readyz", "/api/state", "/api/tenants/tenant-01", "/api/auth/session"} {
		resp, reqErr := ts.Client().Get(ts.URL + getPath)
		if reqErr != nil {
			t.Fatalf("GET %s: %v", getPath, reqErr)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", getPath, resp.StatusCode)
		}
	}

	// 2. All mutating POST routes reject unauthenticated callers with 401 Unauthorized.
	protectedRoutes := []string{
		"/api/tenants/tenant-01/commit",
		"/api/wave",
		"/api/tenants/tenant-01/poison/oom",
		"/api/tenants/tenant-01/poison/rbac",
		"/api/tenants/tenant-01/poison/egress",
		"/api/tenants/tenant-01/recover",
		"/api/reset",
		"/api/repos/external",
		"/api/webhooks/git/tenant-01",
	}
	for _, postPath := range protectedRoutes {
		resp, reqErr := ts.Client().Post(ts.URL+postPath, "application/json", nil)
		if reqErr != nil {
			t.Fatalf("unauthenticated POST %s: %v", postPath, reqErr)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("unauthenticated POST %s status = %d, want 401 Unauthorized", postPath, resp.StatusCode)
		}
	}

	// 3. Invalid X-Presenter-Key is rejected with 401; valid X-Presenter-Key is accepted.
	badKeyReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/tenants/tenant-01/commit", strings.NewReader(`{"release":"v2"}`))
	badKeyReq.Header.Set("X-Presenter-Key", "wrong-key")
	badKeyResp, err := ts.Client().Do(badKeyReq)
	if err != nil {
		t.Fatalf("POST with wrong X-Presenter-Key: %v", err)
	}
	badKeyResp.Body.Close()
	if badKeyResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong X-Presenter-Key status = %d, want 401", badKeyResp.StatusCode)
	}

	goodKeyReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/tenants/tenant-01/commit?wait=true", strings.NewReader(`{"release":"v2-key"}`))
	goodKeyReq.Header.Set("X-Presenter-Key", presenterSecret)
	goodKeyResp, err := ts.Client().Do(goodKeyReq)
	if err != nil {
		t.Fatalf("POST with valid X-Presenter-Key: %v", err)
	}
	goodKeyResp.Body.Close()
	if goodKeyResp.StatusCode != http.StatusOK {
		t.Fatalf("valid X-Presenter-Key status = %d, want 200", goodKeyResp.StatusCode)
	}

	// 4. Browser session login via POST /api/auth/login sets HttpOnly, SameSite=Strict cookie + CSRF token.
	loginReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/auth/login", strings.NewReader(fmt.Sprintf(`{"token":%q}`, presenterSecret)))
	loginReq.Header.Set("X-Forwarded-Proto", "https")
	loginResp, err := ts.Client().Do(loginReq)
	if err != nil {
		t.Fatalf("POST /api/auth/login: %v", err)
	}
	var loginData struct {
		AuthRequired  bool   `json:"authRequired"`
		Authenticated bool   `json:"authenticated"`
		CSRFToken     string `json:"csrfToken"`
	}
	if err := json.NewDecoder(loginResp.Body).Decode(&loginData); err != nil {
		loginResp.Body.Close()
		t.Fatalf("decode login response: %v", err)
	}
	cookies := loginResp.Cookies()
	loginResp.Body.Close()

	if loginResp.StatusCode != http.StatusOK || !loginData.Authenticated || loginData.CSRFToken == "" {
		t.Fatalf("unexpected login result: status=%d data=%+v", loginResp.StatusCode, loginData)
	}

	var sessionCookie *http.Cookie
	for _, c := range cookies {
		if c.Name == "__Host-fs_session" {
			sessionCookie = c
			break
		}
	}
	if sessionCookie == nil {
		t.Fatalf("expected __Host-fs_session cookie on HTTPS login, got cookies=%+v", cookies)
	}
	if !sessionCookie.HttpOnly || !sessionCookie.Secure || sessionCookie.SameSite != http.SameSiteStrictMode {
		t.Errorf("session cookie security attributes: HttpOnly=%v Secure=%v SameSite=%v",
			sessionCookie.HttpOnly, sessionCookie.Secure, sessionCookie.SameSite)
	}

	// 5. Session cookie without X-CSRF-Token is rejected with 403 Forbidden.
	noCSRFReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/tenants/tenant-01/commit", strings.NewReader(`{"release":"v3"}`))
	noCSRFReq.AddCookie(sessionCookie)
	noCSRFResp, err := ts.Client().Do(noCSRFReq)
	if err != nil {
		t.Fatalf("POST without CSRF token: %v", err)
	}
	noCSRFResp.Body.Close()
	if noCSRFResp.StatusCode != http.StatusForbidden {
		t.Fatalf("session cookie without X-CSRF-Token status = %d, want 403 Forbidden", noCSRFResp.StatusCode)
	}

	// 6. Session cookie with matching X-CSRF-Token succeeds.
	withCSRFReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/tenants/tenant-01/commit?wait=true", strings.NewReader(`{"release":"v3-csrf"}`))
	withCSRFReq.AddCookie(sessionCookie)
	withCSRFReq.Header.Set("X-CSRF-Token", loginData.CSRFToken)
	withCSRFResp, err := ts.Client().Do(withCSRFReq)
	if err != nil {
		t.Fatalf("POST with CSRF token: %v", err)
	}
	withCSRFResp.Body.Close()
	if withCSRFResp.StatusCode != http.StatusOK {
		t.Fatalf("session cookie with matching X-CSRF-Token status = %d, want 200", withCSRFResp.StatusCode)
	}

	// 7. Logout invalidates the session on the server immediately.
	logoutReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/auth/logout", nil)
	logoutReq.AddCookie(sessionCookie)
	logoutReq.Header.Set("X-CSRF-Token", loginData.CSRFToken)
	logoutResp, err := ts.Client().Do(logoutReq)
	if err != nil {
		t.Fatalf("POST /api/auth/logout: %v", err)
	}
	logoutResp.Body.Close()
	if logoutResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/auth/logout status = %d, want 200", logoutResp.StatusCode)
	}

	replayReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/tenants/tenant-01/commit", strings.NewReader(`{"release":"v4"}`))
	replayReq.AddCookie(sessionCookie)
	replayReq.Header.Set("X-CSRF-Token", loginData.CSRFToken)
	replayResp, err := ts.Client().Do(replayReq)
	if err != nil {
		t.Fatalf("POST with logged-out session cookie: %v", err)
	}
	replayResp.Body.Close()
	if replayResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("logged-out session cookie replay status = %d, want 401 Unauthorized", replayResp.StatusCode)
	}

	// 8. Brute-force rate limiting on POST /api/auth/login locks out after 10 failures.
	for attempt := 1; attempt <= 10; attempt++ {
		failResp, failErr := ts.Client().Post(ts.URL+"/api/auth/login", "application/json", strings.NewReader(`{"token":"bad"}`))
		if failErr != nil {
			t.Fatalf("failed login attempt %d: %v", attempt, failErr)
		}
		failResp.Body.Close()
		if failResp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("attempt %d status = %d, want 401", attempt, failResp.StatusCode)
		}
	}
	rateResp, err := ts.Client().Post(ts.URL+"/api/auth/login", "application/json", strings.NewReader(`{"token":"bad"}`))
	if err != nil {
		t.Fatalf("rate-limited login request: %v", err)
	}
	rateResp.Body.Close()
	if rateResp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("11th failed login status = %d, want 429 Too Many Requests", rateResp.StatusCode)
	}
	if gotRetry := rateResp.Header.Get("Retry-After"); gotRetry != "60" {
		t.Errorf("Retry-After header = %q, want 60", gotRetry)
	}
}
