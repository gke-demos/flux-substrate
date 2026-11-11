package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"

	"github.com/gke-demos/flux-substrate/internal/actor"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

// LocalActorBackendConfig configures the capacity-bounded local ActorBackend simulator
// backed by real Workflow Actor servers and DurableDir checkouts on disk.
type LocalActorBackendConfig struct {
	Atespace           string
	BaseDurableDir     string
	RestConfig         *rest.Config
	RestConfigForActor func(actorName string) *rest.Config
	KubeBridgeURL      string
	KubeBearerToken    string
	GitBearerToken     string
	TrustBundlePEM     []byte
	MaxConcurrentSlots int
	MaxRenderBytes     int64
	TurnHook           func(ctx context.Context, inv TurnInvocation) error
}

type localActorRecord struct {
	mu            sync.Mutex
	activeTurns   atomic.Int32
	spec          ActorSpec
	policy        *EgressPolicySpec
	state         ActorState
	durableDir    string
	snapshotDir   string
	hasSnapshot   bool
	snapshotCount int64
}

// LocalActorBackend implements ActorBackend using real in-process Workflow Actor
// HTTP handlers backed by isolated DurableDir directories and a strict slot cap.
type LocalActorBackend struct {
	cfg          LocalActorBackendConfig
	durableRoot  string
	snapshotRoot string
	maxSlots     int32

	activeSlots atomic.Int32
	peakSlots   atomic.Int32

	mu             sync.Mutex
	actors         map[string]*localActorRecord
	trustBundlePEM []byte
}

// NewLocalActorBackend creates a capacity-bounded local ActorBackend (default 9 slots).
func NewLocalActorBackend(cfg LocalActorBackendConfig) (*LocalActorBackend, error) {
	baseDir := strings.TrimSpace(cfg.BaseDurableDir)
	if baseDir == "" {
		return nil, errors.New("BaseDurableDir is required for LocalActorBackend")
	}
	durableRoot := filepath.Join(baseDir, "durable")
	snapshotRoot := filepath.Join(baseDir, "snapshots")
	if err := os.MkdirAll(durableRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create durable root: %w", err)
	}
	if err := os.MkdirAll(snapshotRoot, 0o750); err != nil {
		return nil, fmt.Errorf("create snapshot root: %w", err)
	}

	maxSlots := cfg.MaxConcurrentSlots
	if maxSlots <= 0 {
		maxSlots = defaultWorkerCount * defaultSlotsPerWorker
	}
	if strings.TrimSpace(cfg.Atespace) == "" {
		cfg.Atespace = defaultAtespace
	}

	return &LocalActorBackend{
		cfg:            cfg,
		durableRoot:    durableRoot,
		snapshotRoot:   snapshotRoot,
		maxSlots:       int32(maxSlots),
		actors:         make(map[string]*localActorRecord),
		trustBundlePEM: append([]byte(nil), cfg.TrustBundlePEM...),
	}, nil
}

// AddTrustBundlePEM appends additional PEM certificates to the local actor trust bundle.
func (b *LocalActorBackend) AddTrustBundlePEM(pemBytes []byte) {
	if len(pemBytes) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.trustBundlePEM) > 0 && b.trustBundlePEM[len(b.trustBundlePEM)-1] != '\n' {
		b.trustBundlePEM = append(b.trustBundlePEM, '\n')
	}
	b.trustBundlePEM = append(b.trustBundlePEM, pemBytes...)
}

// PeakActiveSlots returns the maximum number of simultaneously running Reconcile Turns
// observed by this backend.
func (b *LocalActorBackend) PeakActiveSlots() int {
	return int(b.peakSlots.Load())
}

// EnsureActor provisions the actor record and its isolated DurableDir if not already created.
func (b *LocalActorBackend) EnsureActor(_ context.Context, spec ActorSpec) error {
	actorName := strings.TrimSpace(spec.ActorName)
	durableDir, err := httputil.SecureSubpath(b.durableRoot, actorName)
	if err != nil {
		return fmt.Errorf("invalid actor name %q: %w", actorName, err)
	}
	snapshotDir, err := httputil.SecureSubpath(b.snapshotRoot, actorName)
	if err != nil {
		return fmt.Errorf("invalid actor snapshot name %q: %w", actorName, err)
	}
	if err := os.MkdirAll(durableDir, 0o750); err != nil {
		return fmt.Errorf("mkdir actor DurableDir: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.actors[actorName]; !exists {
		b.actors[actorName] = &localActorRecord{
			spec:        spec,
			state:       ActorStateSuspended,
			durableDir:  durableDir,
			snapshotDir: snapshotDir,
		}
	}
	return nil
}

// EnsureEgressPolicy reconciles the actor's least-privilege EgressPolicy before wake.
func (b *LocalActorBackend) EnsureEgressPolicy(_ context.Context, policy EgressPolicySpec) error {
	actorName := strings.TrimSpace(policy.ActorName)
	b.mu.Lock()
	rec, exists := b.actors[actorName]
	b.mu.Unlock()
	if !exists {
		return fmt.Errorf("EnsureEgressPolicy called before EnsureActor for %q", actorName)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	copied := policy
	rec.policy = &copied
	return nil
}

// InvokeReconcile wakes the target Workflow Actor in a capacity-bounded slot and executes
// a real POST /reconcile turn against its persisted DurableDir.
func (b *LocalActorBackend) InvokeReconcile(ctx context.Context, inv TurnInvocation) (actor.ReconcileResult, int, error) {
	actorName := strings.TrimSpace(inv.ActorName)
	b.mu.Lock()
	rec, exists := b.actors[actorName]
	b.mu.Unlock()
	if !exists {
		return actor.ReconcileResult{}, http.StatusPreconditionFailed, fmt.Errorf("actor %q not ensured prior to wake", actorName)
	}

	cur := b.activeSlots.Add(1)
	defer b.activeSlots.Add(-1)
	for {
		prev := b.peakSlots.Load()
		if cur <= prev || b.peakSlots.CompareAndSwap(prev, cur) {
			break
		}
	}
	if cur > b.maxSlots {
		return actor.ReconcileResult{}, http.StatusServiceUnavailable, fmt.Errorf("WorkerPool slot capacity exceeded: %d > %d", cur, b.maxSlots)
	}

	actorInFlight := rec.activeTurns.Add(1)
	defer rec.activeTurns.Add(-1)
	if actorInFlight > 1 {
		return actor.ReconcileResult{}, http.StatusConflict, fmt.Errorf("duplicate concurrent Reconcile Turn on actor %q (%d active)", actorName, actorInFlight)
	}

	rec.mu.Lock()
	if rec.policy == nil {
		rec.mu.Unlock()
		return actor.ReconcileResult{}, http.StatusPreconditionFailed, fmt.Errorf("EgressPolicy not reconciled prior to waking %q", actorName)
	}
	if rec.state == ActorStateCrashed {
		rec.mu.Unlock()
		return actor.ReconcileResult{}, http.StatusServiceUnavailable, &ActorCrashError{
			ActorName: actorName,
			Reason:    "actor is in ACTOR_STATE_CRASHED; call RevertActor before resuming",
		}
	}
	rec.state = ActorStateRunning
	durableDir := rec.durableDir
	atespace := rec.spec.Atespace
	if atespace == "" {
		atespace = b.cfg.Atespace
	}
	var allowedHosts []string
	if h := strings.TrimSpace(rec.policy.GitHost); h != "" {
		allowedHosts = append(allowedHosts, h)
	}
	if h := strings.TrimSpace(rec.policy.KubeBridgeHost); h != "" {
		allowedHosts = append(allowedHosts, h)
	}
	rec.mu.Unlock()

	if b.cfg.TurnHook != nil {
		if err := b.cfg.TurnHook(ctx, inv); err != nil {
			var crashErr *ActorCrashError
			if errors.As(err, &crashErr) {
				rec.mu.Lock()
				rec.state = ActorStateCrashed
				rec.mu.Unlock()
			}
			return actor.ReconcileResult{}, http.StatusInternalServerError, err
		}
	}

	restCfg := b.cfg.RestConfig
	if b.cfg.RestConfigForActor != nil {
		if custom := b.cfg.RestConfigForActor(actorName); custom != nil {
			restCfg = custom
		}
	}

	b.mu.Lock()
	trustPEM := append([]byte(nil), b.trustBundlePEM...)
	b.mu.Unlock()

	allowedNS := strings.TrimSpace(inv.Request.TargetNamespace)
	if allowedNS == "" {
		allowedNS = actorName
	}
	impersonatedPrincipal := fmt.Sprintf("ate:actor:%s:%s", atespace, allowedNS)

	actorSrv, err := actor.NewServer(actor.Config{
		ActorName:          actorName,
		Atespace:           atespace,
		DurableDir:         durableDir,
		TrustBundlePEM:     trustPEM,
		GitBearerToken:     b.cfg.GitBearerToken,
		KubeBridgeURL:      b.cfg.KubeBridgeURL,
		KubeBearerToken:    b.cfg.KubeBearerToken,
		KubeWrapTransport:  namespaceIsolationTransport(impersonatedPrincipal, allowedNS),
		RestConfig:         restCfg,
		MaxRenderBytes:     b.cfg.MaxRenderBytes,
		AllowedEgressHosts: allowedHosts,
	})
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, fmt.Errorf("boot Workflow Actor %s: %w", actorName, err)
	}

	rawBody, err := json.Marshal(inv.Request)
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1/reconcile", bytes.NewReader(rawBody))
	if err != nil {
		return actor.ReconcileResult{}, http.StatusInternalServerError, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	recW := httptest.NewRecorder()
	actorSrv.Handler().ServeHTTP(recW, httpReq)

	resp := recW.Result()
	defer resp.Body.Close()

	var result actor.ReconcileResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return actor.ReconcileResult{}, resp.StatusCode, fmt.Errorf("decode actor ReconcileResult: %w", err)
	}
	if resp.StatusCode == http.StatusInsufficientStorage || result.FailedStage == "oom" {
		rec.mu.Lock()
		rec.state = ActorStateCrashed
		rec.mu.Unlock()
		reason := strings.TrimSpace(result.Error)
		if reason == "" {
			reason = "sandbox memory limit exceeded during Kustomize evaluation"
		}
		return result, resp.StatusCode, &ActorCrashError{
			ActorName: actorName,
			Reason:    reason,
		}
	}
	return result, resp.StatusCode, nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func namespaceIsolationTransport(impersonatedUser, allowedNamespace string) func(http.RoundTripper) http.RoundTripper {
	return func(next http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			cleanPath := strings.TrimRight(req.URL.Path, "/")
			if pr, ok := parseResourceRoute(cleanPath); ok {
				if !pr.def.Namespaced && req.Method != http.MethodGet {
					return forbiddenKubeResponse(req, pr.def.Kind, pr.name,
						fmt.Sprintf("%s %q is forbidden: User %q cannot mutate cluster-scoped resource %q",
							pr.def.Resource, pr.name, impersonatedUser, pr.def.Resource)), nil
				}
				if pr.def.Namespaced && pr.namespace != "" && pr.namespace != allowedNamespace {
					return forbiddenKubeResponse(req, pr.def.Kind, pr.name,
						fmt.Sprintf("%s %q is forbidden: User %q cannot access resource %q in namespace %q",
							pr.def.Resource, pr.name, impersonatedUser, pr.def.Resource, pr.namespace)), nil
				}
			}
			return next.RoundTrip(req)
		})
	}
}

func forbiddenKubeResponse(req *http.Request, kind, name, msg string) *http.Response {
	st := v1.Status{
		TypeMeta: v1.TypeMeta{Kind: "Status", APIVersion: "v1"},
		Status:   v1.StatusFailure,
		Message:  msg,
		Reason:   v1.StatusReasonForbidden,
		Code:     http.StatusForbidden,
	}
	if name != "" || kind != "" {
		st.Details = &v1.StatusDetails{Name: name, Kind: kind}
	}
	raw, _ := json.Marshal(st)
	hdr := make(http.Header)
	hdr.Set("Content-Type", "application/json")
	return &http.Response{
		Status:        "403 Forbidden",
		StatusCode:    http.StatusForbidden,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        hdr,
		Body:          io.NopCloser(bytes.NewReader(raw)),
		ContentLength: int64(len(raw)),
		Request:       req,
	}
}

// SuspendActor checkpoints the actor's clean DurableDir volume snapshot and marks it SUSPENDED.
func (b *LocalActorBackend) SuspendActor(_ context.Context, _, actorName string) error {
	actorName = strings.TrimSpace(actorName)
	b.mu.Lock()
	rec, exists := b.actors[actorName]
	b.mu.Unlock()
	if !exists {
		return fmt.Errorf("cannot suspend unknown actor %q", actorName)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.state == ActorStateCrashed {
		return nil
	}
	if err := syncDirectorySnapshot(rec.durableDir, rec.snapshotDir); err != nil {
		return fmt.Errorf("snapshot DurableDir for %s: %w", actorName, err)
	}
	rec.hasSnapshot = true
	rec.snapshotCount++
	rec.state = ActorStateSuspended
	return nil
}

// RevertActor restores the actor's DurableDir from its last clean volume snapshot and
// resets ACTOR_STATE_CRASHED back to SUSPENDED.
func (b *LocalActorBackend) RevertActor(_ context.Context, _, actorName string) error {
	actorName = strings.TrimSpace(actorName)
	b.mu.Lock()
	rec, exists := b.actors[actorName]
	b.mu.Unlock()
	if !exists {
		return fmt.Errorf("cannot revert unknown actor %q", actorName)
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.hasSnapshot {
		if err := syncDirectorySnapshot(rec.snapshotDir, rec.durableDir); err != nil {
			return fmt.Errorf("restore DurableDir snapshot for %s: %w", actorName, err)
		}
	} else {
		if err := os.RemoveAll(rec.durableDir); err != nil {
			return err
		}
		if err := os.MkdirAll(rec.durableDir, 0o750); err != nil {
			return err
		}
	}
	rec.state = ActorStateSuspended
	return nil
}

func syncDirectorySnapshot(srcDir, dstDir string) error {
	tmpDir := dstDir + ".tmp"
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	if err := os.MkdirAll(tmpDir, 0o750); err != nil {
		return err
	}
	err := filepath.WalkDir(srcDir, func(curPath string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(srcDir, curPath)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		targetPath := filepath.Join(tmpDir, rel)
		if d.IsDir() {
			return os.MkdirAll(targetPath, 0o750)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		return copyRegularFile(curPath, targetPath)
	})
	if err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}
	if err := os.RemoveAll(dstDir); err != nil {
		_ = os.RemoveAll(tmpDir)
		return err
	}
	return os.Rename(tmpDir, dstDir)
}

func copyRegularFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
