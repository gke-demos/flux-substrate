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
	TenantID   string `json:"tenantId,omitempty"`
	RepoURL    string `json:"repoUrl,omitempty"`
	Revision   string `json:"revision,omitempty"`
	After      string `json:"after,omitempty"`
	Repository struct {
		Name     string `json:"name,omitempty"`
		CloneURL string `json:"clone_url,omitempty"`
		HTMLURL  string `json:"html_url,omitempty"`
		URL      string `json:"url,omitempty"`
	} `json:"repository,omitempty"`
}

func (d *Dispatcher) registerActionRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/tenants/{id}/commit", d.requirePresenterAuth(d.handleSingleTenantCommit))
	mux.HandleFunc("POST /api/wave", d.requirePresenterAuth(d.handleCommitWave))
	mux.HandleFunc("POST /api/tenants/{id}/poison/oom", d.requirePresenterAuth(d.handlePoisonOOM))
	mux.HandleFunc("POST /api/tenants/{id}/poison/rbac", d.requirePresenterAuth(d.handlePoisonRBAC))
	mux.HandleFunc("POST /api/tenants/{id}/poison/egress", d.requirePresenterAuth(d.handlePoisonEgress))
	mux.HandleFunc("POST /api/tenants/{id}/recover", d.requirePresenterAuth(d.handleRecoverTenant))
	mux.HandleFunc("POST /api/reset", d.requirePresenterAuth(d.handleFleetReset))
	mux.HandleFunc("POST /api/repos/external", d.requirePresenterAuth(d.handleRegisterExternalRepo))
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
  tenant: %q
  release: %q
  channel: "stable"
  sequence: "%d"
`, namespace, tenantID, release, seq)
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
