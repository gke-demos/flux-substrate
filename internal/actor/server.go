package actor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fluxcd/cli-utils/pkg/kstatus/polling"
	"github.com/fluxcd/cli-utils/pkg/kstatus/status"
	"github.com/fluxcd/cli-utils/pkg/object"
	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/git/gogit"
	"github.com/fluxcd/pkg/git/repository"
	"github.com/fluxcd/pkg/ssa"
	ssautils "github.com/fluxcd/pkg/ssa/utils"
	extgogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"

	"github.com/gke-demos/flux-substrate/internal/httputil"
)

const (
	defaultDurableDir        = "/var/lib/flux"
	defaultSystemInfoDir     = "/run/ate"
	defaultTrustBundleFile   = "/run/ate/trust-bundle.pem"
	defaultPlaceholderBearer = "actor-placeholder"
	maxReconcileBodyBytes    = 1 << 20 // 1 MiB reconcile request payload cap

	// DefaultMaxRenderBytes bounds rendered Kustomize YAML output inside a Workflow Actor
	// so runaway overlays deterministically trigger sandbox OOM failure without exhausting
	// shared host memory in --mode=local or unit tests.
	DefaultMaxRenderBytes int64 = 4 << 20 // 4 MiB
)

var (
	errRenderMemoryLimitExceeded = errors.New("sandbox memory limit exceeded")
	errEgressPolicyDenied        = errors.New("egress policy denied outbound fetch")
)

// Config configures a single-tenant Workflow Actor HTTP server.
type Config struct {
	ActorName          string
	Atespace           string
	ActorUID           string
	DurableDir         string
	SystemInfoDir      string
	TrustBundleFile    string
	TrustBundlePEM     []byte
	GitBearerToken     string
	KubeBridgeURL      string
	KubeBearerToken    string
	KubeWrapTransport  func(http.RoundTripper) http.RoundTripper
	KubeconfigPath     string
	RestConfig         *rest.Config
	MaxRenderBytes     int64
	AllowedEgressHosts []string
	ExitOnOOM          bool
}

// Server serves GET /readyz and POST /reconcile inside a Workflow Actor sandbox.
type Server struct {
	actorName          string
	atespace           string
	actorUID           string
	durableDir         string
	trustBundleFile    string
	trustBundlePEM     []byte
	gitBearerToken     string
	kubeBridgeURL      string
	kubeBearerToken    string
	kubeWrapTransport  func(http.RoundTripper) http.RoundTripper
	kubeconfigPath     string
	baseRestConfig     *rest.Config
	maxRenderBytes     int64
	allowedEgressHosts map[string]bool
	exitOnOOM          bool

	turnSem     chan struct{}
	turnCounter atomic.Int64
	clientMu    sync.Mutex
	kubeClient  client.Client

	handler http.Handler
}

// NewServer constructs a Workflow Actor server and ensures its DurableDir exists.
func NewServer(cfg Config) (*Server, error) {
	sysDir := cfg.SystemInfoDir
	if sysDir == "" {
		sysDir = defaultSystemInfoDir
	}

	actorName := strings.TrimSpace(cfg.ActorName)
	if actorName == "" {
		actorName = readTrimmedFile(filepath.Join(sysDir, "actor-name"), filepath.Join(sysDir, "actor-id"))
	}
	atespace := strings.TrimSpace(cfg.Atespace)
	if atespace == "" {
		atespace = readTrimmedFile(filepath.Join(sysDir, "atespace"))
	}
	actorUID := strings.TrimSpace(cfg.ActorUID)
	if actorUID == "" {
		actorUID = readTrimmedFile(filepath.Join(sysDir, "actor-uid"))
	}

	durableDir := strings.TrimSpace(cfg.DurableDir)
	if durableDir == "" {
		durableDir = defaultDurableDir
	}
	if err := os.MkdirAll(durableDir, 0o750); err != nil {
		return nil, fmt.Errorf("prepare DurableDir %s: %w", durableDir, err)
	}

	var resolvedTrustFile string
	trustPEM := slicesClone(cfg.TrustBundlePEM)
	if len(trustPEM) == 0 {
		explicitTrustPath := strings.TrimSpace(cfg.TrustBundleFile)
		if explicitTrustPath != "" && explicitTrustPath != defaultTrustBundleFile {
			b, err := os.ReadFile(explicitTrustPath)
			if err != nil {
				return nil, fmt.Errorf("read TrustBundleFile %s: %w", explicitTrustPath, err)
			}
			if len(bytes.TrimSpace(b)) == 0 {
				return nil, fmt.Errorf("TrustBundleFile %s is empty", explicitTrustPath)
			}
			trustPEM = b
			resolvedTrustFile = explicitTrustPath
		} else {
			for _, candidate := range []string{
				filepath.Join(sysDir, "trust-bundle.pem"),
				defaultTrustBundleFile,
			} {
				if b, err := os.ReadFile(candidate); err == nil && len(bytes.TrimSpace(b)) > 0 {
					trustPEM = b
					resolvedTrustFile = candidate
					break
				}
			}
		}
	}

	gitBearer := strings.TrimSpace(cfg.GitBearerToken)
	if gitBearer == "" {
		gitBearer = defaultPlaceholderBearer
	}
	kubeBearer := strings.TrimSpace(cfg.KubeBearerToken)
	if kubeBearer == "" {
		kubeBearer = defaultPlaceholderBearer
	}

	maxRenderBytes := cfg.MaxRenderBytes
	if maxRenderBytes == 0 {
		maxRenderBytes = DefaultMaxRenderBytes
	}

	var allowedHosts map[string]bool
	if len(cfg.AllowedEgressHosts) > 0 {
		allowedHosts = make(map[string]bool, len(cfg.AllowedEgressHosts)*2)
		for _, h := range cfg.AllowedEgressHosts {
			norm := normalizeEgressHost(h)
			if norm != "" {
				allowedHosts[norm] = true
			}
		}
	}

	s := &Server{
		actorName:          actorName,
		atespace:           atespace,
		actorUID:           actorUID,
		durableDir:         durableDir,
		trustBundleFile:    resolvedTrustFile,
		trustBundlePEM:     trustPEM,
		gitBearerToken:     gitBearer,
		kubeBridgeURL:      strings.TrimSpace(cfg.KubeBridgeURL),
		kubeBearerToken:    kubeBearer,
		kubeWrapTransport:  cfg.KubeWrapTransport,
		kubeconfigPath:     strings.TrimSpace(cfg.KubeconfigPath),
		baseRestConfig:     cfg.RestConfig,
		maxRenderBytes:     maxRenderBytes,
		allowedEgressHosts: allowedHosts,
		exitOnOOM:          cfg.ExitOnOOM,
		turnSem:            make(chan struct{}, 1),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("POST /reconcile", s.handleReconcile)
	s.handler = httputil.SecurityHeaders(mux)
	return s, nil
}

// Handler returns the HTTP handler for the Workflow Actor.
func (s *Server) Handler() http.Handler {
	return s.handler
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"status":    "ready",
		"actorName": s.actorName,
		"atespace":  s.atespace,
	})
}

func (s *Server) handleReconcile(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxReconcileBodyBytes)
	defer r.Body.Close()

	var req ReconcileRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, ReconcileResult{
			ActorName:   s.actorName,
			Atespace:    s.atespace,
			Error:       "invalid reconcile JSON payload",
			FailedStage: "request",
		})
		return
	}

	result, statusCode := s.executeTurn(r.Context(), req)
	httputil.WriteJSON(w, statusCode, result)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	if statusCode == http.StatusInsufficientStorage && s.exitOnOOM {
		go func() {
			time.Sleep(50 * time.Millisecond)
			os.Exit(137)
		}()
	}
}

func (s *Server) executeTurn(ctx context.Context, req ReconcileRequest) (ReconcileResult, int) {
	select {
	case s.turnSem <- struct{}{}:
		defer func() { <-s.turnSem }()
	case <-ctx.Done():
		return ReconcileResult{
			ActorName:   s.actorName,
			Atespace:    s.atespace,
			Error:       "reconcile canceled while waiting for active turn",
			FailedStage: "queue",
		}, http.StatusServiceUnavailable
	}

	turnStart := time.Now()
	turnNum := s.turnCounter.Add(1)

	tenantID := strings.TrimSpace(req.TenantID)
	if tenantID == "" {
		tenantID = s.actorName
	}
	branch := strings.TrimSpace(req.Branch)
	if branch == "" {
		branch = "main"
	}
	overlayRel := strings.TrimSpace(req.Path)
	if overlayRel == "" {
		overlayRel = "./"
	}

	res := ReconcileResult{
		ActorName:     s.actorName,
		Atespace:      s.atespace,
		TenantID:      tenantID,
		TurnNumber:    turnNum,
		Branch:        branch,
		PrunedObjects: []string{},
		Inventory:     []InventoryEntry{},
	}

	if strings.TrimSpace(req.RepoURL) == "" {
		res.Error = "repoUrl is required"
		res.FailedStage = "request"
		return res, http.StatusBadRequest
	}

	workflowKey := sanitizeWorkflowKey(req.WorkflowName, tenantID)
	checkoutDir, err := httputil.SecureSubpath(s.durableDir, workflowKey)
	if err != nil {
		res.Error = "invalid workflow identifier"
		res.FailedStage = "request"
		return res, http.StatusBadRequest
	}
	inventoryPath, err := httputil.SecureSubpath(s.durableDir, workflowKey+".inventory.json")
	if err != nil {
		res.Error = "invalid workflow inventory path"
		res.FailedStage = "request"
		return res, http.StatusBadRequest
	}

	// Stage 1: Fetch / Clone Git source into DurableDir.
	fetchStart := time.Now()
	commitSHA, cacheHit, err := s.checkoutSource(ctx, checkoutDir, req.RepoURL, branch)
	res.FetchMs = time.Since(fetchStart).Milliseconds()
	res.Timings.FetchMs = res.FetchMs
	res.CacheHit = cacheHit
	if err != nil {
		res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
		if errors.Is(err, errEgressPolicyDenied) {
			res.Error = fmt.Sprintf("egress policy denied git source sync: %v", err)
			res.FailedStage = "egress"
			return res, http.StatusForbidden
		}
		res.Error = fmt.Sprintf("git source sync failed: %v", err)
		res.FailedStage = "fetch"
		return res, http.StatusBadGateway
	}
	res.CommitSHA = commitSHA
	res.Revision = fmt.Sprintf("%s@sha1:%s", branch, commitSHA)

	// Stage 2: Render Kustomize overlay in-process with krusty.
	renderStart := time.Now()
	renderedObjs, err := s.renderKustomizeOverlay(checkoutDir, overlayRel)
	res.RenderMs = time.Since(renderStart).Milliseconds()
	res.Timings.RenderMs = res.RenderMs
	if err != nil {
		res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
		if errors.Is(err, errRenderMemoryLimitExceeded) {
			res.Error = fmt.Sprintf("kustomize build exceeded sandbox memory limit: %v", err)
			res.FailedStage = "oom"
			return res, http.StatusInsufficientStorage
		}
		if errors.Is(err, errEgressPolicyDenied) {
			res.Error = fmt.Sprintf("kustomize remote-base fetch denied by EgressPolicy: %v", err)
			res.FailedStage = "egress"
			return res, http.StatusForbidden
		}
		res.Error = fmt.Sprintf("kustomize build failed: %v", err)
		res.FailedStage = "render"
		return res, http.StatusUnprocessableEntity
	}

	// Stage 3: Apply via fluxcd/pkg/ssa & evaluate kstatus readiness.
	applyStart := time.Now()
	kubeClient, err := s.getOrCreateKubeClient()
	if err != nil {
		res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
		res.Error = fmt.Sprintf("kubernetes client initialization failed: %v", err)
		res.FailedStage = "apply"
		return res, kubeErrorStatusCode(err, http.StatusInternalServerError)
	}

	if err := defaultUnsetNamespaces(kubeClient.RESTMapper(), renderedObjs, strings.TrimSpace(req.TargetNamespace)); err != nil {
		res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
		res.Error = fmt.Sprintf("resolve target resource scope failed: %v", err)
		res.FailedStage = "apply"
		return res, kubeErrorStatusCode(err, http.StatusBadGateway)
	}

	poller := polling.NewStatusPoller(kubeClient, kubeClient.RESTMapper(), polling.Options{})
	manager := ssa.NewResourceManager(kubeClient, poller, ssa.Owner{
		Field: "flux-substrate",
		Group: "kustomize.toolkit.fluxcd.io",
	})

	changeSet, err := manager.ApplyAllStaged(ctx, renderedObjs, ssa.DefaultApplyOptions())
	if err != nil {
		res.ApplyMs = time.Since(applyStart).Milliseconds()
		res.Timings.ApplyMs = res.ApplyMs
		res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
		res.Error = fmt.Sprintf("server-side apply failed: %v", err)
		res.FailedStage = "apply"
		return res, kubeErrorStatusCode(err, http.StatusBadGateway)
	}

	actions := make(map[string]string, len(changeSet.Entries))
	for _, entry := range changeSet.Entries {
		actions[entry.Subject] = string(entry.Action)
	}
	res.AppliedActions = actions

	pruneEnabled := req.Prune == nil || *req.Prune
	if pruneEnabled {
		prevInv := req.PreviousInventory
		if prevInv == nil {
			prevInv = loadPersistedInventory(inventoryPath)
		}
		staleObjs := computeStaleObjects(prevInv, renderedObjs)
		if len(staleObjs) > 0 {
			delSet, err := manager.DeleteAll(ctx, staleObjs, ssa.DefaultDeleteOptions())
			if err != nil {
				res.ApplyMs = time.Since(applyStart).Milliseconds()
				res.Timings.ApplyMs = res.ApplyMs
				res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
				res.Error = fmt.Sprintf("prune stale objects failed: %v", err)
				res.FailedStage = "prune"
				return res, kubeErrorStatusCode(err, http.StatusBadGateway)
			}
			for _, entry := range delSet.Entries {
				if entry.Action == ssa.DeletedAction {
					res.PrunedObjects = append(res.PrunedObjects, entry.Subject)
				}
			}
		}
	}

	inventory, allHealthy, statusMsg, err := evaluateInventoryStatus(ctx, kubeClient, renderedObjs)
	res.ApplyMs = time.Since(applyStart).Milliseconds()
	res.Timings.ApplyMs = res.ApplyMs
	res.Timings.TotalMs = time.Since(turnStart).Milliseconds()
	if err != nil {
		res.Error = fmt.Sprintf("evaluate resource status failed: %v", err)
		res.FailedStage = "status"
		return res, kubeErrorStatusCode(err, http.StatusBadGateway)
	}

	if err := savePersistedInventory(inventoryPath, inventory); err != nil {
		res.Error = fmt.Sprintf("persist inventory failed: %v", err)
		res.FailedStage = "status"
		return res, http.StatusInternalServerError
	}

	res.Inventory = inventory
	res.Ready = allHealthy
	res.StatusMessage = statusMsg
	return res, http.StatusOK
}

func (s *Server) checkoutSource(ctx context.Context, checkoutDir, repoURL, branch string) (string, bool, error) {
	parsedURL, err := url.Parse(repoURL)
	if err != nil {
		return "", false, fmt.Errorf("invalid repoUrl: %w", err)
	}
	var transportType git.TransportType
	switch strings.ToLower(parsedURL.Scheme) {
	case "https":
		transportType = git.HTTPS
	case "http":
		transportType = git.HTTP
	default:
		return "", false, fmt.Errorf("unsupported git scheme %q (expected http or https)", parsedURL.Scheme)
	}
	if !s.isURLHostAllowed(parsedURL) {
		return "", false, fmt.Errorf("%w: host %q is not permitted by actor EgressPolicy (NXDOMAIN / 403)", errEgressPolicyDenied, parsedURL.Host)
	}

	if sha, warmOK, warmErr := s.tryWarmFetch(ctx, checkoutDir, repoURL, branch); warmOK || warmErr != nil {
		return sha, warmOK, warmErr
	}

	if err := os.RemoveAll(checkoutDir); err != nil {
		return "", false, fmt.Errorf("reset checkout dir: %w", err)
	}
	if err := os.MkdirAll(checkoutDir, 0o750); err != nil {
		return "", false, fmt.Errorf("mkdir checkout dir: %w", err)
	}

	clientOpts := []gogit.ClientOption{
		gogit.WithDiskStorage(),
		gogit.WithSingleBranch(true),
	}
	if transportType == git.HTTP {
		clientOpts = append(clientOpts, gogit.WithInsecureCredentialsOverHTTP())
	}

	fluxClient, err := gogit.NewClient(checkoutDir, &git.AuthOptions{
		Transport:   transportType,
		Host:        parsedURL.Host,
		BearerToken: s.gitBearerToken,
		CAFile:      s.trustBundlePEM,
	}, clientOpts...)
	if err != nil {
		return "", false, err
	}

	commit, err := fluxClient.Clone(ctx, repoURL, repository.CloneConfig{
		CheckoutStrategy: repository.CheckoutStrategy{Branch: branch},
		ShallowClone:     true,
	})
	if err != nil {
		return "", false, err
	}
	if commit == nil {
		return "", false, errors.New("remote repository is empty")
	}
	return commit.Hash.String(), false, nil
}

func (s *Server) tryWarmFetch(ctx context.Context, checkoutDir, repoURL, branch string) (string, bool, error) {
	dotGitInfo, err := os.Stat(filepath.Join(checkoutDir, extgogit.GitDirName))
	if err != nil || !dotGitInfo.IsDir() {
		return "", false, nil
	}

	repo, err := extgogit.PlainOpen(checkoutDir)
	if err != nil {
		return "", false, nil
	}
	origin, err := repo.Remote(git.DefaultRemote)
	if err != nil || len(origin.Config().URLs) == 0 || origin.Config().URLs[0] != repoURL {
		return "", false, nil
	}

	refSpec := config.RefSpec(fmt.Sprintf("+refs/heads/%s:refs/remotes/%s/%s", branch, git.DefaultRemote, branch))
	fetchOpts := &extgogit.FetchOptions{
		RemoteName: git.DefaultRemote,
		RefSpecs:   []config.RefSpec{refSpec},
		Depth:      1,
		Force:      true,
		Tags:       extgogit.NoTags,
		CABundle:   s.trustBundlePEM,
	}
	if s.gitBearerToken != "" {
		fetchOpts.Auth = &githttp.TokenAuth{Token: s.gitBearerToken}
	}

	if err := repo.FetchContext(ctx, fetchOpts); err != nil && !errors.Is(err, extgogit.NoErrAlreadyUpToDate) {
		return "", true, err
	}

	remoteRefName := plumbing.NewRemoteReferenceName(git.DefaultRemote, branch)
	targetHash, err := resolveWarmTargetHash(repo, remoteRefName, branch)
	if err != nil {
		return "", true, err
	}

	branchRefName := plumbing.NewBranchReferenceName(branch)
	if err := repo.Storer.SetReference(plumbing.NewHashReference(branchRefName, targetHash)); err != nil {
		return "", true, fmt.Errorf("update local branch ref: %w", err)
	}
	if err := repo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branchRefName)); err != nil {
		return "", true, fmt.Errorf("update symbolic HEAD: %w", err)
	}

	wt, err := repo.Worktree()
	if err != nil {
		return "", true, fmt.Errorf("open warm worktree: %w", err)
	}
	if err := wt.Reset(&extgogit.ResetOptions{
		Commit: targetHash,
		Mode:   extgogit.HardReset,
	}); err != nil {
		return "", true, fmt.Errorf("reset warm worktree: %w", err)
	}
	_ = wt.Clean(&extgogit.CleanOptions{Dir: true})

	return targetHash.String(), true, nil
}

func resolveWarmTargetHash(repo *extgogit.Repository, remoteRefName plumbing.ReferenceName, branch string) (plumbing.Hash, error) {
	if remRef, err := repo.Reference(remoteRefName, true); err == nil {
		return remRef.Hash(), nil
	}
	if localRef, err := repo.Reference(plumbing.NewBranchReferenceName(branch), true); err == nil {
		return localRef.Hash(), nil
	}
	head, err := repo.Head()
	if err != nil {
		return plumbing.ZeroHash, fmt.Errorf("resolve warm HEAD: %w", err)
	}
	return head.Hash(), nil
}

func (s *Server) renderKustomizeOverlay(checkoutDir, overlayRel string) ([]*unstructured.Unstructured, error) {
	targetDir, err := httputil.SecureRelativeDir(checkoutDir, overlayRel)
	if err != nil {
		return nil, err
	}

	if s.maxRenderBytes > 0 {
		if rawBytes, sizeErr := measureOverlayFileBytes(targetDir, s.maxRenderBytes*2); sizeErr != nil {
			return nil, sizeErr
		} else if rawBytes > s.maxRenderBytes*2 {
			return nil, fmt.Errorf("%w: overlay inputs (%d bytes) exceed budget (%d bytes)", errRenderMemoryLimitExceeded, rawBytes, s.maxRenderBytes)
		}
	}

	remoteRefs, err := s.validateOverlayRemoteReferences(targetDir)
	if err != nil {
		return nil, err
	}

	kustomizer := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	resMap, err := kustomizer.Run(filesys.MakeFsOnDisk(), targetDir)
	if err != nil {
		if len(remoteRefs) > 0 && looksLikeRemoteEgressFailure(err) {
			return nil, fmt.Errorf("%w: outbound remote-base fetch %q blocked (NXDOMAIN / 403): %v", errEgressPolicyDenied, remoteRefs[0], err)
		}
		return nil, err
	}

	yamlBytes, err := resMap.AsYaml()
	if err != nil {
		return nil, fmt.Errorf("marshal kustomize output: %w", err)
	}
	if s.maxRenderBytes > 0 && int64(len(yamlBytes)) > s.maxRenderBytes {
		return nil, fmt.Errorf("%w: rendered output (%d bytes) exceeds actor ceiling (%d bytes)", errRenderMemoryLimitExceeded, len(yamlBytes), s.maxRenderBytes)
	}
	if len(bytes.TrimSpace(yamlBytes)) == 0 {
		return nil, nil
	}

	objs, err := ssautils.ReadObjects(bytes.NewReader(yamlBytes))
	if err != nil {
		return nil, fmt.Errorf("decode rendered manifests: %w", err)
	}
	return objs, nil
}

func measureOverlayFileBytes(rootDir string, stopAfter int64) (int64, error) {
	var total int64
	errStop := errors.New("stop walk")
	err := filepath.WalkDir(rootDir, func(cur string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == extgogit.GitDirName {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		if stopAfter > 0 && total > stopAfter {
			return errStop
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStop) {
		return 0, err
	}
	return total, nil
}

func (s *Server) validateOverlayRemoteReferences(rootDir string) ([]string, error) {
	var remoteRefs []string
	err := filepath.WalkDir(rootDir, func(cur string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == extgogit.GitDirName {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || !isKustomizationFilename(d.Name()) {
			return nil
		}
		raw, err := os.ReadFile(cur)
		if err != nil {
			return err
		}
		refs := extractKustomizeReferences(raw)
		for _, ref := range refs {
			remoteURL, blockedProto, isRemote := classifyRemoteRef(ref)
			if !isRemote {
				continue
			}
			remoteRefs = append(remoteRefs, ref)
			if blockedProto {
				return fmt.Errorf("%w: non-HTTPS remote reference %q is blocked by sandbox EgressPolicy (403 Forbidden)", errEgressPolicyDenied, ref)
			}
			if !s.isURLHostAllowed(remoteURL) {
				return fmt.Errorf("%w: remote base %q targets unapproved host %q outside actor EgressPolicy (NXDOMAIN / 403 Forbidden)", errEgressPolicyDenied, ref, remoteURL.Host)
			}
		}
		return nil
	})
	return remoteRefs, err
}

func isKustomizationFilename(name string) bool {
	return name == "kustomization.yaml" || name == "kustomization.yml" || name == "Kustomization"
}

func extractKustomizeReferences(rawYAML []byte) []string {
	var doc map[string]any
	if err := yaml.Unmarshal(rawYAML, &doc); err != nil || doc == nil {
		return nil
	}
	var out []string
	addStr := func(v any) {
		if s, ok := v.(string); ok {
			if trimmed := strings.TrimSpace(s); trimmed != "" {
				out = append(out, trimmed)
			}
		}
	}
	for _, listKey := range []string{
		"resources", "bases", "components", "configurations",
		"crds", "generators", "transformers", "validators",
	} {
		items, _ := doc[listKey].([]any)
		for _, item := range items {
			addStr(item)
		}
	}
	for _, patchKey := range []string{"patches", "patchesStrategicMerge", "patchesJson6902"} {
		items, _ := doc[patchKey].([]any)
		for _, item := range items {
			addStr(item)
			if m, ok := item.(map[string]any); ok {
				addStr(m["path"])
			}
		}
	}
	if openapiMap, ok := doc["openapi"].(map[string]any); ok {
		addStr(openapiMap["path"])
	}
	if charts, ok := doc["helmCharts"].([]any); ok {
		for _, item := range charts {
			if m, ok := item.(map[string]any); ok {
				addStr(m["repo"])
			}
		}
	}
	for _, genKey := range []string{"configMapGenerator", "secretGenerator"} {
		items, _ := doc[genKey].([]any)
		for _, item := range items {
			if m, ok := item.(map[string]any); ok {
				addStr(m["env"])
				for _, subKey := range []string{"files", "envs"} {
					subItems, _ := m[subKey].([]any)
					for _, sub := range subItems {
						s, ok := sub.(string)
						if !ok {
							continue
						}
						if _, afterEq, hasEq := strings.Cut(s, "="); hasEq {
							addStr(afterEq)
						} else {
							addStr(s)
						}
					}
				}
			}
		}
	}
	return out
}

func classifyRemoteRef(ref string) (*url.URL, bool, bool) {
	lower := strings.ToLower(strings.TrimSpace(ref))
	switch {
	case strings.HasPrefix(lower, "ssh://"),
		strings.HasPrefix(lower, "git::"),
		strings.HasPrefix(lower, "git@"),
		strings.HasPrefix(lower, "git://"):
		return nil, true, true
	case strings.HasPrefix(lower, "github.com/"):
		u, err := url.Parse("https://" + strings.TrimSpace(ref))
		if err != nil || u.Host == "" {
			return nil, true, true
		}
		return u, false, true
	case strings.HasPrefix(lower, "http://"), strings.HasPrefix(lower, "https://"):
		u, err := url.Parse(strings.TrimSpace(ref))
		if err != nil || u.Host == "" {
			return nil, true, true
		}
		return u, false, true
	default:
		return nil, false, false
	}
}

func (s *Server) isURLHostAllowed(u *url.URL) bool {
	if len(s.allowedEgressHosts) == 0 {
		return true
	}
	if u == nil || u.Host == "" {
		return false
	}
	norm := normalizeEgressHost(u.Host)
	if s.allowedEgressHosts[norm] {
		return true
	}
	if strings.EqualFold(u.Scheme, "https") && u.Port() == "443" && s.allowedEgressHosts[strings.ToLower(u.Hostname())] {
		return true
	}
	if strings.EqualFold(u.Scheme, "http") && u.Port() == "80" && s.allowedEgressHosts[strings.ToLower(u.Hostname())] {
		return true
	}
	return false
}

func normalizeEgressHost(raw string) string {
	return strings.ToLower(strings.TrimSpace(raw))
}

func looksLikeRemoteEgressFailure(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, token := range []string{
		"no such host",
		"nxdomain",
		"status code 403",
		"forbidden",
		"http error",
		"dial tcp",
		"connection refused",
		"network is unreachable",
		"i/o timeout",
		"tls:",
		"x509:",
		"url is a git repository",
		"executable file not found",
	} {
		if strings.Contains(msg, token) {
			return true
		}
	}
	return false
}

func defaultUnsetNamespaces(mapper meta.RESTMapper, objs []*unstructured.Unstructured, targetNS string) error {
	if targetNS == "" {
		return nil
	}
	for _, obj := range objs {
		if obj.GetNamespace() != "" {
			continue
		}
		gvk := obj.GroupVersionKind()
		mapping, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return err
		}
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
			obj.SetNamespace(targetNS)
		}
	}
	return nil
}

func evaluateInventoryStatus(
	ctx context.Context,
	kubeClient client.Client,
	objs []*unstructured.Unstructured,
) ([]InventoryEntry, bool, string, error) {
	entries := make([]InventoryEntry, 0, len(objs))
	allHealthy := true
	var failedSummary []string

	for _, obj := range objs {
		live := &unstructured.Unstructured{}
		live.SetGroupVersionKind(obj.GroupVersionKind())
		if err := kubeClient.Get(ctx, client.ObjectKey{
			Namespace: obj.GetNamespace(),
			Name:      obj.GetName(),
		}, live); err != nil {
			return nil, false, "", err
		}

		res, err := status.Compute(live)
		if err != nil {
			return nil, false, "", err
		}

		meta := object.UnstructuredToObjMetadata(live)
		st := res.Status.String()
		if res.Status == status.FailedStatus || res.Status == status.TerminatingStatus {
			allHealthy = false
			failedSummary = append(failedSummary, fmt.Sprintf("%s/%s: %s", live.GetKind(), live.GetName(), res.Message))
		}

		entries = append(entries, InventoryEntry{
			ID:      meta.String(),
			Version: live.GroupVersionKind().Version,
			Status:  st,
			Message: res.Message,
		})
	}

	if !allHealthy {
		return entries, false, strings.Join(failedSummary, "; "), nil
	}
	return entries, true, fmt.Sprintf("Applied %d resources cleanly", len(entries)), nil
}

func computeStaleObjects(prev []InventoryEntry, currentObjs []*unstructured.Unstructured) []*unstructured.Unstructured {
	if len(prev) == 0 {
		return nil
	}
	currentIDs := make(map[string]bool, len(currentObjs))
	for _, obj := range currentObjs {
		currentIDs[object.UnstructuredToObjMetadata(obj).String()] = true
	}

	var stale []*unstructured.Unstructured
	seen := make(map[string]bool)
	for _, entry := range prev {
		if entry.ID == "" || currentIDs[entry.ID] || seen[entry.ID] {
			continue
		}
		metaObj, err := object.ParseObjMetadata(entry.ID)
		if err != nil {
			continue
		}
		ver := entry.Version
		if _, after, hasSlash := strings.Cut(ver, "/"); hasSlash {
			ver = after
		}
		if ver == "" {
			ver = "v1"
		}
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(metaObj.GroupKind.WithVersion(ver))
		u.SetNamespace(metaObj.Namespace)
		u.SetName(metaObj.Name)
		stale = append(stale, u)
		seen[entry.ID] = true
	}
	return stale
}

func loadPersistedInventory(inventoryPath string) []InventoryEntry {
	raw, err := os.ReadFile(inventoryPath)
	if err != nil {
		return nil
	}
	var entries []InventoryEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil
	}
	return entries
}

func savePersistedInventory(inventoryPath string, entries []InventoryEntry) error {
	raw, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	tmpPath := inventoryPath + ".tmp"
	if err := os.WriteFile(tmpPath, raw, 0o640); err != nil {
		return err
	}
	return os.Rename(tmpPath, inventoryPath)
}

func (s *Server) getOrCreateKubeClient() (client.Client, error) {
	s.clientMu.Lock()
	defer s.clientMu.Unlock()
	if s.kubeClient != nil {
		return s.kubeClient, nil
	}

	var cfg *rest.Config
	switch {
	case s.baseRestConfig != nil:
		cfg = rest.CopyConfig(s.baseRestConfig)
	case s.kubeBridgeURL != "":
		cfg = &rest.Config{
			Host:        s.kubeBridgeURL,
			BearerToken: s.kubeBearerToken,
		}
		switch {
		case s.trustBundleFile != "":
			cfg.TLSClientConfig = rest.TLSClientConfig{
				CAFile: s.trustBundleFile,
			}
		case len(s.trustBundlePEM) > 0:
			cfg.TLSClientConfig = rest.TLSClientConfig{
				CAData: s.trustBundlePEM,
			}
		}
	case s.kubeconfigPath != "":
		loaded, err := clientcmd.BuildConfigFromFlags("", s.kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("load kubeconfig %s: %w", s.kubeconfigPath, err)
		}
		cfg = loaded
	default:
		inCluster, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("no Kubernetes configuration configured: %w", err)
		}
		cfg = inCluster
	}
	if s.kubeWrapTransport != nil {
		cfg.WrapTransport = s.kubeWrapTransport
	}
	cfg.WarningHandlerWithContext = rest.NoWarnings{}

	kc, err := client.New(cfg, client.Options{})
	if err != nil {
		return nil, err
	}
	s.kubeClient = kc
	return kc, nil
}

func kubeErrorStatusCode(err error, fallback int) int {
	if apierrors.IsForbidden(err) || (err != nil && strings.Contains(err.Error(), "is forbidden:")) {
		return http.StatusForbidden
	}
	if apierrors.IsUnauthorized(err) {
		return http.StatusUnauthorized
	}
	return fallback
}

func sanitizeWorkflowKey(workflowName, tenantID string) string {
	raw := strings.ToLower(strings.TrimSpace(workflowName))
	if raw == "" {
		raw = strings.ToLower(strings.TrimSpace(tenantID))
	}
	if raw == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range raw {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	out := strings.Trim(b.String(), "-_.")
	if out == "" {
		return "default"
	}
	if len(out) > 60 {
		out = strings.TrimRight(out[:60], "-_.")
	}
	return out
}

func slicesClone(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

func readTrimmedFile(paths ...string) string {
	for _, p := range paths {
		if b, err := os.ReadFile(p); err == nil {
			if v := strings.TrimSpace(string(b)); v != "" {
				return v
			}
		}
	}
	return ""
}
