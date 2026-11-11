package dispatcher

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

// PoisonPillRequest is the optional JSON body for POST /api/tenants/{id}/poison/{oom,rbac,egress}.
type PoisonPillRequest struct {
	TargetNamespace string `json:"targetNamespace,omitempty"`
	RemoteBaseURL   string `json:"remoteBaseUrl,omitempty"`
	Message         string `json:"message,omitempty"`
	Wait            bool   `json:"wait,omitempty"`
}

// RecoverRequest is the optional JSON body for POST /api/tenants/{id}/recover and POST /api/reset.
type RecoverRequest struct {
	Message string `json:"message,omitempty"`
	Wait    bool   `json:"wait,omitempty"`
}

// ExternalRepoRequest is the JSON payload for POST /api/repos/external.
type ExternalRepoRequest struct {
	TenantID        string `json:"tenantId"`
	RepoURL         string `json:"repoUrl"`
	Branch          string `json:"branch,omitempty"`
	Path            string `json:"path,omitempty"`
	TargetNamespace string `json:"targetNamespace,omitempty"`
	SecretName      string `json:"secretName,omitempty"`
	CredentialURI   string `json:"credentialUri,omitempty"`
	Token           string `json:"token,omitempty"`
	Wait            bool   `json:"wait,omitempty"`
}

func (d *Dispatcher) handlePoisonOOM(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := d.validateTenantActionRequest(w, r)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req PoisonPillRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid poison OOM JSON payload"})
		return
	}

	d.schedMu.Lock()
	ts, exists := d.tenants[tenantID]
	var targetNS string
	var prevTurns int64
	if exists {
		targetNS = ts.targetNamespace
		prevTurns = ts.turnCount
	}
	d.schedMu.Unlock()
	if !exists {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}

	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		msg = fmt.Sprintf("Poison pill: Kustomize memory-bomb amplification overlay on %s", tenantID)
	}

	rev, err := d.gitServer.Commit(tenantID, gitserver.CommitChange{
		ReplaceTree: true,
		Upsert:      gitserver.MemoryBombManifests(tenantID, targetNS),
		Message:     msg,
	})
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to commit memory-bomb overlay"})
		return
	}

	d.schedMu.Lock()
	ts.lastCommitMessage = rev.Message
	ts.latestDiff = rev.SummaryDiff
	d.schedMu.Unlock()

	queued := d.Enqueue(tenantID, TriggerPoisonOOM)
	if shouldWaitRequest(r, req.Wait) && queued {
		if err := d.waitForTenantsTurnProgress(r.Context(), map[string]int64{tenantID: prevTurns}); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for OOM poison turn"})
			return
		}
	}

	detail, _ := d.SnapshotTenant(tenantID)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantId":    tenantID,
		"scenario":    "oom",
		"revision":    rev,
		"queued":      queued,
		"actorState":  detail.ActorState,
		"ready":       detail.Ready,
		"stalled":     detail.Stalled,
		"failedStage": detail.FailedStage,
		"lastError":   detail.LastError,
	})
}

func (d *Dispatcher) handlePoisonRBAC(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := d.validateTenantActionRequest(w, r)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req PoisonPillRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid poison RBAC JSON payload"})
		return
	}

	d.schedMu.Lock()
	ts, exists := d.tenants[tenantID]
	var ownNS string
	var prevTurns int64
	if exists {
		ownNS = ts.targetNamespace
		prevTurns = ts.turnCount
	}
	forbiddenNS := strings.TrimSpace(req.TargetNamespace)
	if forbiddenNS == "" || forbiddenNS == ownNS {
		for otherID, otherTS := range d.tenants {
			if otherID != tenantID && otherTS.targetNamespace != "" && otherTS.targetNamespace != ownNS {
				forbiddenNS = otherTS.targetNamespace
				break
			}
		}
	}
	d.schedMu.Unlock()
	if !exists {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}
	if forbiddenNS == "" || forbiddenNS == ownNS {
		if ownNS == "tenant-01" {
			forbiddenNS = "tenant-02"
		} else {
			forbiddenNS = "tenant-01"
		}
	}

	_ = ensureNamespace(r.Context(), d.kubeClient, forbiddenNS)

	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		msg = fmt.Sprintf("Poison pill: cross-namespace RBAC escape into %s", forbiddenNS)
	}

	rev, err := d.gitServer.Commit(tenantID, gitserver.CommitChange{
		ReplaceTree: true,
		Upsert:      gitserver.CrossNamespaceManifests(tenantID, ownNS, forbiddenNS),
		Message:     msg,
	})
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to commit cross-namespace RBAC overlay"})
		return
	}

	d.schedMu.Lock()
	ts.lastCommitMessage = rev.Message
	ts.latestDiff = rev.SummaryDiff
	d.schedMu.Unlock()

	queued := d.Enqueue(tenantID, TriggerPoisonRBAC)
	if shouldWaitRequest(r, req.Wait) && queued {
		if err := d.waitForTenantsTurnProgress(r.Context(), map[string]int64{tenantID: prevTurns}); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for RBAC poison turn"})
			return
		}
	}

	detail, _ := d.SnapshotTenant(tenantID)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantId":           tenantID,
		"scenario":           "rbac",
		"forbiddenNamespace": forbiddenNS,
		"revision":           rev,
		"queued":             queued,
		"actorState":         detail.ActorState,
		"ready":              detail.Ready,
		"failedStage":        detail.FailedStage,
		"lastError":          detail.LastError,
	})
}

func (d *Dispatcher) handlePoisonEgress(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := d.validateTenantActionRequest(w, r)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req PoisonPillRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid poison egress JSON payload"})
		return
	}

	d.schedMu.Lock()
	ts, exists := d.tenants[tenantID]
	var targetNS string
	var prevTurns int64
	if exists {
		targetNS = ts.targetNamespace
		prevTurns = ts.turnCount
	}
	d.schedMu.Unlock()
	if !exists {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}

	remoteBaseURL := strings.TrimSpace(req.RemoteBaseURL)
	if remoteBaseURL == "" {
		remoteBaseURL = gitserver.DefaultBlockedRemoteBaseURL
	}
	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		msg = fmt.Sprintf("Poison pill: unauthorized remote-base egress (%s)", remoteBaseURL)
	}

	rev, err := d.gitServer.Commit(tenantID, gitserver.CommitChange{
		ReplaceTree: true,
		Upsert:      gitserver.UnauthorizedEgressManifests(tenantID, targetNS, remoteBaseURL),
		Message:     msg,
	})
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to commit unauthorized egress overlay"})
		return
	}

	d.schedMu.Lock()
	ts.lastCommitMessage = rev.Message
	ts.latestDiff = rev.SummaryDiff
	d.schedMu.Unlock()

	queued := d.Enqueue(tenantID, TriggerPoisonEgress)
	if shouldWaitRequest(r, req.Wait) && queued {
		if err := d.waitForTenantsTurnProgress(r.Context(), map[string]int64{tenantID: prevTurns}); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for egress poison turn"})
			return
		}
	}

	detail, _ := d.SnapshotTenant(tenantID)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantId":      tenantID,
		"scenario":      "egress",
		"remoteBaseUrl": remoteBaseURL,
		"revision":      rev,
		"queued":        queued,
		"actorState":    detail.ActorState,
		"ready":         detail.Ready,
		"failedStage":   detail.FailedStage,
		"lastError":     detail.LastError,
	})
}

func (d *Dispatcher) handleRecoverTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("id"))
	if !validTenantIDRe.MatchString(tenantID) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant identifier"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req RecoverRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid recover JSON payload"})
		return
	}

	d.schedMu.Lock()
	ts, exists := d.tenants[tenantID]
	var (
		actorName   string
		targetNS    string
		baselineURL string
		grKey       client.ObjectKey
		prevTurns   int64
	)
	if exists {
		actorName = ts.actorName
		targetNS = ts.targetNamespace
		baselineURL = ts.baselineRepoURL
		grKey = ts.grKey
		prevTurns = ts.turnCount
	}
	d.schedMu.Unlock()
	if !exists {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}

	if err := d.backend.RevertActor(r.Context(), d.atespace, actorName); err != nil {
		httputil.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": fmt.Sprintf("RevertActor failed: %v", err)})
		return
	}

	if baselineURL != "" && grKey.Name != "" {
		_ = d.restoreGitRepositoryURL(r.Context(), grKey, baselineURL)
	}

	var rev gitserver.Revision
	if d.gitServer != nil {
		msg := strings.TrimSpace(req.Message)
		if msg == "" {
			msg = fmt.Sprintf("RevertActor recovery: restore baseline manifests for %s", tenantID)
		}
		committed, err := d.gitServer.Commit(tenantID, gitserver.CommitChange{
			ReplaceTree: true,
			Upsert:      gitserver.DefaultTenantManifests(tenantID, targetNS),
			Message:     msg,
		})
		if err != nil {
			seeded, seedErr := d.gitServer.SeedDefaultTenant(tenantID, targetNS)
			if seedErr != nil {
				httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to restore baseline Git manifests"})
				return
			}
			committed = seeded
		}
		rev = committed
	}

	d.schedMu.Lock()
	ts.actorState = ActorStateSuspended
	ts.stalled = false
	if baselineURL != "" {
		ts.repoURL = baselineURL
	}
	if rev.SHA != "" {
		ts.lastCommitMessage = rev.Message
		ts.latestDiff = rev.SummaryDiff
	}
	d.schedMu.Unlock()

	queued := d.Enqueue(tenantID, TriggerRecover)
	if shouldWaitRequest(r, req.Wait) && queued {
		if err := d.waitForTenantsTurnProgress(r.Context(), map[string]int64{tenantID: prevTurns}); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for RevertActor recovery turn"})
			return
		}
	}

	detail, _ := d.SnapshotTenant(tenantID)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantId":   tenantID,
		"actorState": detail.ActorState,
		"ready":      detail.Ready,
		"revision":   rev,
		"queued":     queued,
	})
}

func (d *Dispatcher) handleFleetReset(w http.ResponseWriter, r *http.Request) {
	if d.gitServer == nil {
		httputil.WriteJSON(w, http.StatusNotImplemented, map[string]string{"error": "in-process gitServer is not attached"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req RecoverRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid reset JSON payload"})
		return
	}

	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		msg = "Fleet reset to baseline manifests"
	}

	type resetTarget struct {
		tenantID    string
		actorName   string
		targetNS    string
		baselineURL string
		grKey       client.ObjectKey
		prevTurns   int64
	}

	d.schedMu.Lock()
	ids := make([]string, 0, len(d.tenants))
	for id := range d.tenants {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	targets := make([]resetTarget, 0, len(ids))
	for _, id := range ids {
		ts := d.tenants[id]
		targets = append(targets, resetTarget{
			tenantID:    id,
			actorName:   ts.actorName,
			targetNS:    ts.targetNamespace,
			baselineURL: ts.baselineRepoURL,
			grKey:       ts.grKey,
			prevTurns:   ts.turnCount,
		})
	}
	d.schedMu.Unlock()

	prevTurnMap := make(map[string]int64, len(targets))
	revisions := make(map[string]string, len(targets))
	enqueued := make([]string, 0, len(targets))

	for _, tgt := range targets {
		_ = d.backend.RevertActor(r.Context(), d.atespace, tgt.actorName)
		if tgt.baselineURL != "" && tgt.grKey.Name != "" {
			_ = d.restoreGitRepositoryURL(r.Context(), tgt.grKey, tgt.baselineURL)
		}

		rev, err := d.gitServer.Commit(tgt.tenantID, gitserver.CommitChange{
			ReplaceTree: true,
			Upsert:      gitserver.DefaultTenantManifests(tgt.tenantID, tgt.targetNS),
			Message:     msg,
		})
		if err != nil {
			rev, err = d.gitServer.SeedDefaultTenant(tgt.tenantID, tgt.targetNS)
			if err != nil {
				httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{
					"error": fmt.Sprintf("failed to reset repository for %s", tgt.tenantID),
				})
				return
			}
		}
		revisions[tgt.tenantID] = rev.SHA
		prevTurnMap[tgt.tenantID] = tgt.prevTurns

		d.schedMu.Lock()
		if ts, ok := d.tenants[tgt.tenantID]; ok {
			ts.actorState = ActorStateSuspended
			ts.stalled = false
			if tgt.baselineURL != "" {
				ts.repoURL = tgt.baselineURL
			}
			ts.lastCommitMessage = rev.Message
			ts.latestDiff = rev.SummaryDiff
		}
		d.schedMu.Unlock()

		if d.Enqueue(tgt.tenantID, TriggerReset) {
			enqueued = append(enqueued, tgt.tenantID)
		}
	}

	if shouldWaitRequest(r, req.Wait) && len(prevTurnMap) > 0 {
		if err := d.waitForTenantsTurnProgress(r.Context(), prevTurnMap); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for Fleet Reset"})
			return
		}
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantCount": len(enqueued),
		"tenants":     enqueued,
		"revisions":   revisions,
	})
}

func (d *Dispatcher) handleRegisterExternalRepo(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req ExternalRepoRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid external repository JSON payload"})
		return
	}

	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = "tenant-01"
	}
	if !validTenantIDRe.MatchString(tenantID) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenantId"})
		return
	}

	rawURL := strings.TrimSpace(req.RepoURL)
	parsedURL, err := url.Parse(rawURL)
	if err != nil || parsedURL.Host == "" || !strings.EqualFold(parsedURL.Scheme, "https") {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error": "repoUrl must be a valid https:// Git repository URL",
		})
		return
	}
	if parsedURL.User != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error": "repoUrl must not contain embedded user credentials; use ate-secret:// credential injection",
		})
		return
	}

	branch := strings.TrimSpace(req.Branch)
	if branch == "" {
		branch = "main"
	}

	overlayPath := strings.TrimSpace(req.Path)
	if overlayPath == "" {
		overlayPath = "./"
	}
	cleanedPath := path.Clean(overlayPath)
	if strings.HasPrefix(overlayPath, "/") || cleanedPath == ".." || strings.HasPrefix(cleanedPath, "../") {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid Kustomization overlay path",
		})
		return
	}

	d.schedMu.Lock()
	existingTS, exists := d.tenants[tenantID]
	var ctrlNS, targetNS string
	var prevTurns int64
	if exists {
		ctrlNS = existingTS.grKey.Namespace
		targetNS = existingTS.targetNamespace
		prevTurns = existingTS.turnCount
	}
	d.schedMu.Unlock()

	if ctrlNS == "" {
		ctrlNS = strings.TrimSpace(d.cfg.WatchNamespace)
		if ctrlNS == "" {
			ctrlNS = d.atespace
		}
	}
	if reqTargetNS := strings.TrimSpace(req.TargetNamespace); reqTargetNS != "" {
		if !validTenantIDRe.MatchString(reqTargetNS) {
			httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid targetNamespace"})
			return
		}
		targetNS = reqTargetNS
	}
	if targetNS == "" {
		targetNS = tenantID
	}

	secretName := strings.TrimSpace(req.SecretName)
	if secretName == "" {
		secretName = "git-" + tenantID
	}
	if !validTenantIDRe.MatchString(secretName) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid secretName"})
		return
	}

	credURI := strings.TrimSpace(req.CredentialURI)
	if credURI != "" {
		if !isValidAteSecretURI(credURI) {
			httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{
				"error": "credentialUri must follow ate-secret://k8s.io/<cluster>/<namespace>/<secret>/<key>",
			})
			return
		}
	} else {
		credURI = fmt.Sprintf("ate-secret://k8s.io/default/%s/%s/token", ctrlNS, secretName)
	}

	if err := ensureNamespace(r.Context(), d.kubeClient, ctrlNS); err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to ensure control namespace"})
		return
	}
	if err := ensureNamespace(r.Context(), d.kubeClient, targetNS); err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to ensure target namespace"})
		return
	}

	if tok := strings.TrimSpace(req.Token); tok != "" {
		if err := upsertGitTokenSecret(r.Context(), d.kubeClient, ctrlNS, secretName, tok); err != nil {
			httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to store Git token Secret"})
			return
		}
	}

	labels := map[string]string{
		ManagedByLabelKey: ManagedByLabelValue,
		TenantLabelKey:    tenantID,
	}
	annotations := map[string]string{
		CredentialURIAnnotation: credURI,
	}

	gr := &sourcev1.GitRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:        tenantID,
			Namespace:   ctrlNS,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: sourcev1.GitRepositorySpec{
			URL:       rawURL,
			Interval:  metav1.Duration{Duration: 5 * time.Minute},
			Reference: &sourcev1.GitRepositoryRef{Branch: branch},
			SecretRef: &fluxmeta.LocalObjectReference{Name: secretName},
		},
	}
	if err := upsertGitRepositoryWithAnnotations(r.Context(), d.kubeClient, gr); err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to upsert Flux GitRepository"})
		return
	}

	retryDur := metav1.Duration{Duration: 30 * time.Second}
	ks := &kustomizev1.Kustomization{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tenantID,
			Namespace: ctrlNS,
			Labels:    labels,
		},
		Spec: kustomizev1.KustomizationSpec{
			Interval:      metav1.Duration{Duration: 5 * time.Minute},
			RetryInterval: &retryDur,
			Path:          overlayPath,
			Prune:         true,
			SourceRef: kustomizev1.CrossNamespaceSourceReference{
				Kind: sourcev1.GitRepositoryKind,
				Name: tenantID,
			},
			TargetNamespace: targetNS,
		},
	}
	if err := upsertKustomization(r.Context(), d.kubeClient, ks); err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to upsert Flux Kustomization"})
		return
	}

	if err := d.SyncCRs(r.Context()); err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to sync Flux CRs"})
		return
	}

	queued := d.Enqueue(tenantID, TriggerExternalRepo)
	if shouldWaitRequest(r, req.Wait) && queued {
		if err := d.waitForTenantsTurnProgress(r.Context(), map[string]int64{tenantID: prevTurns}); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for external repo Reconcile Turn"})
			return
		}
	}

	policy, _ := d.synthesizeEgressPolicy(gr, tenantID)
	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantId":        tenantID,
		"repoUrl":         rawURL,
		"branch":          branch,
		"path":            overlayPath,
		"targetNamespace": targetNS,
		"credentialUri":   credURI,
		"egressPolicy":    policy,
		"queued":          queued,
	})
}

func (d *Dispatcher) validateTenantActionRequest(w http.ResponseWriter, r *http.Request) (string, bool) {
	tenantID := strings.TrimSpace(r.PathValue("id"))
	if !validTenantIDRe.MatchString(tenantID) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant identifier"})
		return "", false
	}
	if d.gitServer == nil {
		httputil.WriteJSON(w, http.StatusNotImplemented, map[string]string{"error": "in-process gitServer is not attached"})
		return "", false
	}
	return tenantID, true
}

func (d *Dispatcher) waitForTenantsTurnProgress(ctx context.Context, prevTurns map[string]int64) error {
	allSettled := func() bool {
		d.schedMu.Lock()
		defer d.schedMu.Unlock()
		for tid, before := range prevTurns {
			ts, ok := d.tenants[tid]
			if !ok {
				continue
			}
			if ts.queued || ts.inFlight > 0 || ts.suspending {
				return false
			}
			if ts.turnCount <= before && ts.actorState != ActorStateCrashed {
				return false
			}
		}
		return true
	}

	for {
		notify := d.TurnCompletedNotify()
		if allSettled() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (d *Dispatcher) restoreGitRepositoryURL(ctx context.Context, grKey client.ObjectKey, repoURL string) error {
	var gr sourcev1.GitRepository
	if err := d.kubeClient.Get(ctx, grKey, &gr); err != nil {
		return err
	}
	if gr.Spec.URL == repoURL && (gr.Annotations == nil || gr.Annotations[CredentialURIAnnotation] == "") {
		return nil
	}
	gr.Spec.URL = repoURL
	if gr.Annotations != nil {
		delete(gr.Annotations, CredentialURIAnnotation)
	}
	return d.kubeClient.Update(ctx, &gr)
}

func isValidAteSecretURI(uri string) bool {
	const prefix = "ate-secret://k8s.io/"
	if !strings.HasPrefix(uri, prefix) {
		return false
	}
	rest := strings.TrimPrefix(uri, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		if strings.TrimSpace(p) == "" || strings.Contains(p, "..") {
			return false
		}
	}
	return true
}

func upsertGitTokenSecret(ctx context.Context, kc client.Client, namespace, name, token string) error {
	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedByLabelKey: ManagedByLabelValue,
			},
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{
			"token": []byte(token),
		},
	}
	var existing corev1.Secret
	err := kc.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &existing)
	if apierrors.IsNotFound(err) {
		return kc.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Data = desired.Data
	return kc.Update(ctx, &existing)
}

func upsertGitRepositoryWithAnnotations(ctx context.Context, kc client.Client, desired *sourcev1.GitRepository) error {
	var existing sourcev1.GitRepository
	err := kc.Get(ctx, client.ObjectKeyFromObject(desired), &existing)
	if apierrors.IsNotFound(err) {
		return kc.Create(ctx, desired)
	}
	if err != nil {
		return err
	}
	existing.Labels = desired.Labels
	existing.Annotations = desired.Annotations
	existing.Spec = desired.Spec
	return kc.Update(ctx, &existing)
}
