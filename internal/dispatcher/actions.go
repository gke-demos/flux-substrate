package dispatcher

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

// SingleCommitRequest is the optional JSON body for POST /api/tenants/{id}/commit.
type SingleCommitRequest struct {
	Message string            `json:"message,omitempty"`
	Release string            `json:"release,omitempty"`
	Upsert  map[string]string `json:"upsert,omitempty"`
	Delete  []string          `json:"delete,omitempty"`
	Wait    bool              `json:"wait,omitempty"`
}

// WaveRequest is the optional JSON body for POST /api/wave.
type WaveRequest struct {
	Message string `json:"message,omitempty"`
	Release string `json:"release,omitempty"`
	Wait    bool   `json:"wait,omitempty"`
}

type gitWebhookPayload struct {
	TenantID string `json:"tenantId,omitempty"`
	RepoURL  string `json:"repoUrl,omitempty"`
	Revision string `json:"revision,omitempty"`
	After    string `json:"after,omitempty"`
	Ref      string `json:"ref,omitempty"`
	Wait     bool   `json:"wait,omitempty"`
	Commits  []struct {
		ID      string `json:"id,omitempty"`
		Message string `json:"message,omitempty"`
	} `json:"commits,omitempty"`
	Repository struct {
		Name     string `json:"name,omitempty"`
		FullName string `json:"full_name,omitempty"`
		CloneURL string `json:"clone_url,omitempty"`
		HTMLURL  string `json:"html_url,omitempty"`
		URL      string `json:"url,omitempty"`
	} `json:"repository,omitempty"`
}

const defaultClusterGitOCIRepoURL = "http://git-oci.demo-repo.svc.cluster.local/repo/fulfillment-api.git"

func (d *Dispatcher) registerActionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/tenants/{id}/commit", d.requirePresenterAuth(d.handleSingleTenantCommit))
	mux.HandleFunc("POST /api/wave", d.requirePresenterAuth(d.handleCommitWave))
	mux.HandleFunc("POST /api/push", d.requirePresenterAuth(d.handleRepoPush))
	mux.HandleFunc("POST /api/tenants/{id}/poison/oom", d.requirePresenterAuth(d.handlePoisonOOM))
	mux.HandleFunc("POST /api/tenants/{id}/poison/rbac", d.requirePresenterAuth(d.handlePoisonRBAC))
	mux.HandleFunc("POST /api/tenants/{id}/poison/egress", d.requirePresenterAuth(d.handlePoisonEgress))
	mux.HandleFunc("POST /api/tenants/{id}/poison/buggy-code", d.requirePresenterAuth(d.handlePoisonBuggyCode))
	mux.HandleFunc("POST /api/tenants/{id}/recover", d.requirePresenterAuth(d.handleRecoverTenant))
	mux.HandleFunc("POST /api/reset", d.requirePresenterAuth(d.handleFleetReset))
	mux.HandleFunc("POST /api/repos/external", d.requirePresenterAuth(d.handleRegisterExternalRepo))
	mux.HandleFunc("POST /api/webhook/gitea", d.handleGiteaWebhook)
	mux.HandleFunc("POST /webhook/gitea", d.handleGiteaWebhook)
	mux.HandleFunc("POST /api/webhooks/git", d.handleGitWebhook)
	mux.HandleFunc("POST /api/webhooks/git/{id}", d.handleGitWebhookByID)
	mux.HandleFunc("POST /webhooks/git", d.handleGitWebhook)
	mux.HandleFunc("POST /webhooks/git/{id}", d.handleGitWebhookByID)
}

func (d *Dispatcher) handleSingleTenantCommit(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("id"))
	if !validTenantIDRe.MatchString(tenantID) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant identifier"})
		return
	}
	if d.gitServer == nil {
		httputil.WriteJSON(w, http.StatusNotImplemented, map[string]string{"error": "in-process gitServer is not attached"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req SingleCommitRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid commit JSON payload"})
		return
	}

	d.schedMu.Lock()
	ts, exists := d.tenants[tenantID]
	var targetNS string
	var seq int64
	if exists {
		ts.commitSeq++
		seq = ts.commitSeq + 1
		targetNS = ts.targetNamespace
	}
	d.schedMu.Unlock()

	if !exists {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "tenant not found"})
		return
	}

	_ = d.restoreTenantBaselineSource(r.Context(), tenantID)

	change := gitserver.CommitChange{
		Upsert:  req.Upsert,
		Delete:  req.Delete,
		Message: strings.TrimSpace(req.Message),
	}
	if len(change.Upsert) == 0 && len(change.Delete) == 0 {
		rel := strings.TrimSpace(req.Release)
		if rel == "" {
			rel = fmt.Sprintf("v%d", seq)
		}
		if change.Message == "" {
			change.Message = fmt.Sprintf("Promote %s app-config to %s", tenantID, rel)
		}
		change.Upsert = map[string]string{
			"configmap.yaml": buildConfigMapManifest(targetNS, tenantID, rel, seq),
		}
	}

	rev, err := d.gitServer.Commit(tenantID, change)
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to commit tenant manifest update"})
		return
	}

	d.schedMu.Lock()
	ts.lastCommitMessage = rev.Message
	ts.latestDiff = rev.SummaryDiff
	d.schedMu.Unlock()

	queued := d.Enqueue(tenantID, TriggerCommit)
	if shouldWaitRequest(r, req.Wait) && queued {
		if err := d.waitForTenantsAtSHA(r.Context(), map[string]string{tenantID: rev.SHA}); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for Reconcile Turn"})
			return
		}
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"tenantId": tenantID,
		"revision": rev,
		"queued":   queued,
	})
}

func (d *Dispatcher) handleCommitWave(w http.ResponseWriter, r *http.Request) {
	if d.gitServer == nil {
		httputil.WriteJSON(w, http.StatusNotImplemented, map[string]string{"error": "in-process gitServer is not attached"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	var req WaveRequest
	if err := decodeOptionalJSON(r.Body, &req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid wave JSON payload"})
		return
	}

	waveNum := d.waveSeq.Add(1)
	rel := strings.TrimSpace(req.Release)
	if rel == "" {
		rel = fmt.Sprintf("wave-%d", waveNum)
	}
	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		msg = fmt.Sprintf("Commit Wave #%d (%s)", waveNum, rel)
	}

	type waveTarget struct {
		tenantID string
		targetNS string
		seq      int64
	}

	d.schedMu.Lock()
	ids := make([]string, 0, len(d.tenants))
	for id, ts := range d.tenants {
		if ts.actorState == ActorStateCrashed {
			continue
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	targets := make([]waveTarget, 0, len(ids))
	for _, id := range ids {
		ts := d.tenants[id]
		ts.commitSeq++
		targets = append(targets, waveTarget{
			tenantID: id,
			targetNS: ts.targetNamespace,
			seq:      ts.commitSeq + 1,
		})
	}
	d.schedMu.Unlock()

	// Phase 1: Commit new revisions across all tenant repositories.
	revisions := make(map[string]string, len(targets))
	diffs := make(map[string]gitserver.Revision, len(targets))
	for _, tgt := range targets {
		_ = d.restoreTenantBaselineSource(r.Context(), tgt.tenantID)
		rev, err := d.gitServer.Commit(tgt.tenantID, gitserver.CommitChange{
			Upsert: map[string]string{
				"configmap.yaml": buildConfigMapManifest(tgt.targetNS, tgt.tenantID, rel, tgt.seq),
			},
			Message: msg,
		})
		if err != nil {
			httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{
				"error": fmt.Sprintf("commit wave failed on %s", tgt.tenantID),
			})
			return
		}
		revisions[tgt.tenantID] = rev.SHA
		diffs[tgt.tenantID] = rev
	}

	d.schedMu.Lock()
	for tid, rev := range diffs {
		if ts, ok := d.tenants[tid]; ok {
			ts.lastCommitMessage = rev.Message
			ts.latestDiff = rev.SummaryDiff
		}
	}
	d.schedMu.Unlock()

	// Phase 2: Enqueue all tenants in deterministic sorted order into the 9-slot FIFO queue.
	enqueued := make([]string, 0, len(targets))
	for _, tgt := range targets {
		if d.Enqueue(tgt.tenantID, TriggerWave) {
			enqueued = append(enqueued, tgt.tenantID)
		}
	}

	if shouldWaitRequest(r, req.Wait) && len(revisions) > 0 {
		if err := d.waitForTenantsAtSHA(r.Context(), revisions); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for Commit Wave"})
			return
		}
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]any{
		"waveId":      waveNum,
		"release":     rel,
		"tenantCount": len(enqueued),
		"tenants":     enqueued,
		"revisions":   revisions,
	})
}

func (d *Dispatcher) waitForTenantsAtSHA(ctx context.Context, wantSHAs map[string]string) error {
	allSettled := func() bool {
		d.schedMu.Lock()
		defer d.schedMu.Unlock()
		for tid, sha := range wantSHAs {
			ts, ok := d.tenants[tid]
			if !ok {
				continue
			}
			if ts.queued || ts.inFlight > 0 || ts.suspending {
				return false
			}
			if ts.commitSHA != sha && ts.actorState != ActorStateCrashed {
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

func decodeOptionalJSON(r io.Reader, dst any) error {
	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

func shouldWaitRequest(r *http.Request, bodyWait bool) bool {
	if bodyWait {
		return true
	}
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("wait")))
	return q == "true" || q == "1"
}

func buildConfigMapManifest(namespace, tenantID, release string, seq int64) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: %s
data:
  app: "fulfillment-api"
  tenant: %q
  release: %q
  channel: "stable"
  sequence: "%d"
`, namespace, tenantID, release, seq)
}

// RepoPushRequest is the JSON body for POST /api/push, unifying fulfillment-api pushes across
// the 24-tenant Commit Wave and bad-push (buggy-code / oom) scenarios.
type RepoPushRequest struct {
	Scenario string `json:"scenario,omitempty"`
	TenantID string `json:"tenantId,omitempty"`
	Message  string `json:"message,omitempty"`
	Release  string `json:"release,omitempty"`
	Wait     bool   `json:"wait,omitempty"`
}

func (d *Dispatcher) handleRepoPush(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBodyBytes))
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid push request body"})
		return
	}
	var req RepoPushRequest
	if len(strings.TrimSpace(string(raw))) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid push JSON payload"})
			return
		}
	}

	tid := strings.TrimSpace(req.TenantID)
	if tid == "" {
		tid = "tenant-01"
	}
	r.SetPathValue("id", tid)
	r.Body = io.NopCloser(strings.NewReader(string(raw)))

	switch strings.ToLower(strings.TrimSpace(req.Scenario)) {
	case "buggy-code", "buggy":
		d.handlePoisonBuggyCode(w, r)
	case "oom":
		d.handlePoisonOOM(w, r)
	case "rbac":
		d.handlePoisonRBAC(w, r)
	case "egress":
		d.handlePoisonEgress(w, r)
	default:
		d.handleCommitWave(w, r)
	}
}

func (d *Dispatcher) handleGitWebhookByID(w http.ResponseWriter, r *http.Request) {
	tenantID := strings.TrimSpace(r.PathValue("id"))
	if !validTenantIDRe.MatchString(tenantID) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid tenant identifier"})
		return
	}

	if _, ok := d.readAndVerifyWebhookBody(w, r); !ok {
		return
	}

	if !d.Enqueue(tenantID, TriggerWebhook) {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "managed tenant not found or halted"})
		return
	}
	httputil.WriteJSON(w, http.StatusAccepted, map[string]any{
		"accepted": true,
		"tenants":  []string{tenantID},
	})
}

func (d *Dispatcher) handleGitWebhook(w http.ResponseWriter, r *http.Request) {
	rawBody, ok := d.readAndVerifyWebhookBody(w, r)
	if !ok {
		return
	}

	var payload gitWebhookPayload
	if len(rawBody) > 0 {
		if err := json.Unmarshal(rawBody, &payload); err != nil {
			httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid webhook JSON payload"})
			return
		}
	}

	targets := d.resolveWebhookTargets(payload)
	if len(targets) == 0 {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "no matching managed GitRepository found"})
		return
	}

	var enqueued []string
	for _, tid := range targets {
		if d.Enqueue(tid, TriggerWebhook) {
			enqueued = append(enqueued, tid)
		}
	}
	httputil.WriteJSON(w, http.StatusAccepted, map[string]any{
		"accepted": len(enqueued) > 0,
		"tenants":  enqueued,
	})
}

func (d *Dispatcher) handleGiteaWebhook(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	rawBody, err := io.ReadAll(r.Body)
	if err != nil && !errors.Is(err, io.EOF) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read gitea webhook body"})
		return
	}

	if secret := strings.TrimSpace(d.cfg.WebhookSecret); secret != "" {
		if !verifyWebhookSignatureOrToken(r, rawBody, secret) {
			httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid webhook signature or token"})
			return
		}
	}

	var payload gitWebhookPayload
	if len(strings.TrimSpace(string(rawBody))) > 0 {
		if err := json.Unmarshal(rawBody, &payload); err != nil {
			httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid gitea webhook JSON payload"})
			return
		}
	}

	targetRepoURL := strings.TrimSpace(payload.RepoURL)
	if targetRepoURL == "" && d.mode == "dispatcher" {
		targetRepoURL = defaultClusterGitOCIRepoURL
	}

	commitMsg := ""
	if len(payload.Commits) > 0 {
		commitMsg = strings.TrimSpace(payload.Commits[0].Message)
	}

	d.schedMu.Lock()
	var ids []string
	if tid := strings.TrimSpace(payload.TenantID); validTenantIDRe.MatchString(tid) {
		if _, ok := d.tenants[tid]; ok {
			ids = []string{tid}
		}
	}
	if len(ids) == 0 {
		for id, ts := range d.tenants {
			if ts.actorState == ActorStateCrashed {
				continue
			}
			ids = append(ids, id)
		}
		slices.Sort(ids)
	}
	prevTurns := make(map[string]int64, len(ids))
	ctrlNS := strings.TrimSpace(d.cfg.WatchNamespace)
	if ctrlNS == "" {
		ctrlNS = d.atespace
	}
	for _, id := range ids {
		if ts, ok := d.tenants[id]; ok {
			prevTurns[id] = ts.turnCount
			if ts.grKey.Namespace != "" {
				ctrlNS = ts.grKey.Namespace
			}
		}
	}
	d.schedMu.Unlock()

	if len(ids) == 0 {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "no managed tenants available"})
		return
	}

	if targetRepoURL != "" {
		_ = upsertGitTokenSecret(r.Context(), d.kubeClient, ctrlNS, "git-oci-auth", "substrate-multici-demo-token-2026")
		for _, id := range ids {
			overlayPath := "./"
			if strings.Contains(targetRepoURL, "fulfillment-api") {
				overlayPath = "./deploy/overlays/" + id
			}
			_ = d.pointTenantSourceToRepo(r.Context(), id, targetRepoURL, "git-oci-auth", overlayPath, commitMsg)
		}
	} else if commitMsg != "" {
		d.schedMu.Lock()
		for _, id := range ids {
			if ts, ok := d.tenants[id]; ok {
				ts.lastCommitMessage = commitMsg
			}
		}
		d.schedMu.Unlock()
	}

	var enqueued []string
	for _, id := range ids {
		if d.Enqueue(id, TriggerWebhook) {
			enqueued = append(enqueued, id)
		}
	}

	if shouldWaitRequest(r, payload.Wait) && len(enqueued) > 0 {
		if err := d.waitForTenantsTurnProgress(r.Context(), prevTurns); err != nil {
			httputil.WriteJSON(w, http.StatusGatewayTimeout, map[string]string{"error": "timed out waiting for Gitea webhook turns"})
			return
		}
	}

	httputil.WriteJSON(w, http.StatusAccepted, map[string]any{
		"accepted": len(enqueued) > 0,
		"after":    payload.After,
		"repoUrl":  targetRepoURL,
		"tenants":  enqueued,
	})
}

func verifyWebhookSignatureOrToken(r *http.Request, rawBody []byte, secret string) bool {
	if tok := strings.TrimSpace(r.Header.Get("X-Webhook-Token")); tok != "" {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(secret)) == 1 {
			return true
		}
	}
	if sigHeader := strings.TrimSpace(r.Header.Get("X-Hub-Signature-256")); strings.HasPrefix(sigHeader, "sha256=") {
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(rawBody)
		expectedHex := hex.EncodeToString(mac.Sum(nil))
		presentedHex := strings.TrimPrefix(sigHeader, "sha256=")
		if subtle.ConstantTimeCompare([]byte(presentedHex), []byte(expectedHex)) == 1 {
			return true
		}
	}
	return false
}

func (d *Dispatcher) readAndVerifyWebhookBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	defer r.Body.Close()

	rawBody, err := io.ReadAll(r.Body)
	if err != nil && !errors.Is(err, io.EOF) {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read webhook body"})
		return nil, false
	}

	secret := d.cfg.WebhookSecret
	if secret == "" {
		if d.auth.isEnabled() {
			if key, hasHeader := extractHeaderPresenterKey(r); hasHeader && d.auth.matchesToken(key) {
				return rawBody, true
			}
			httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "webhook or presenter authentication required"})
			return nil, false
		}
		return rawBody, true
	}

	if tok := strings.TrimSpace(r.Header.Get("X-Webhook-Token")); tok != "" {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(secret)) == 1 {
			return rawBody, true
		}
	}
	if sigHeader := strings.TrimSpace(r.Header.Get("X-Hub-Signature-256")); strings.HasPrefix(sigHeader, "sha256=") {
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(rawBody)
		expectedHex := hex.EncodeToString(mac.Sum(nil))
		presentedHex := strings.TrimPrefix(sigHeader, "sha256=")
		if subtle.ConstantTimeCompare([]byte(presentedHex), []byte(expectedHex)) == 1 {
			return rawBody, true
		}
	}

	httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid webhook signature or token"})
	return nil, false
}

func (d *Dispatcher) resolveWebhookTargets(p gitWebhookPayload) []string {
	if tid := strings.TrimSpace(p.TenantID); validTenantIDRe.MatchString(tid) {
		d.schedMu.Lock()
		_, ok := d.tenants[tid]
		d.schedMu.Unlock()
		if ok {
			return []string{tid}
		}
	}

	candidateURLs := make(map[string]bool)
	for _, raw := range []string{p.RepoURL, p.Repository.CloneURL, p.Repository.HTMLURL, p.Repository.URL} {
		if norm := normalizeGitRepoURL(raw); norm != "" {
			candidateURLs[norm] = true
		}
	}
	repoName := strings.TrimSuffix(strings.TrimSpace(p.Repository.Name), ".git")

	d.schedMu.Lock()
	defer d.schedMu.Unlock()

	var matched []string
	for id, ts := range d.tenants {
		if len(candidateURLs) > 0 && candidateURLs[normalizeGitRepoURL(ts.repoURL)] {
			matched = append(matched, id)
			continue
		}
		if repoName != "" && id == repoName {
			matched = append(matched, id)
		}
	}
	return matched
}

func normalizeGitRepoURL(raw string) string {
	s := strings.TrimSpace(strings.ToLower(raw))
	s = strings.TrimRight(s, "/")
	s = strings.TrimSuffix(s, ".git")
	return s
}
