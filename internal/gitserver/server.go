package gitserver

import (
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/filemode"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/storage/filesystem"

	"github.com/gke-demos/flux-substrate/internal/httputil"
)

const (
	defaultBranchName = "main"
	maxGitBodyBytes   = 16 << 20 // 16 MiB upload-pack request cap
	maxAdminBodyBytes = 4 << 20  // 4 MiB admin commit payload cap
)

// Config configures the pure-Go Smart HTTP Git server.
type Config struct {
	ReposDir     string
	DefaultToken string
	AdminToken   string
	TenantTokens map[string]string
}

// Revision describes a committed Git revision on a tenant repository.
type Revision struct {
	TenantID    string    `json:"tenantId"`
	Branch      string    `json:"branch"`
	SHA         string    `json:"sha"`
	Message     string    `json:"message"`
	SummaryDiff string    `json:"summaryDiff,omitempty"`
	CommittedAt time.Time `json:"committedAt"`
}

// CommitChange describes file additions/updates and deletions to commit onto HEAD.
type CommitChange struct {
	Upsert      map[string]string `json:"upsert,omitempty"`
	Delete      []string          `json:"delete,omitempty"`
	ReplaceTree bool              `json:"replaceTree,omitempty"`
	Message     string            `json:"message"`
	AuthorName  string            `json:"authorName,omitempty"`
	AuthorEmail string            `json:"authorEmail,omitempty"`
}

// RepoTelemetry exposes Smart HTTP upload-pack transfer stats for observability and tests.
type RepoTelemetry struct {
	UploadPackRequests int64 `json:"uploadPackRequests"`
	LastPackBytes      int64 `json:"lastPackBytes"`
	TotalPackBytes     int64 `json:"totalPackBytes"`
}

type repoStats struct {
	uploadPackRequests atomic.Int64
	lastPackBytes      atomic.Int64
	totalPackBytes     atomic.Int64
}

// Server serves authenticated Smart HTTP Git repositories and programmatic commits.
type Server struct {
	reposDir     string
	defaultToken string
	adminToken   string

	mu           sync.RWMutex
	tenantTokens map[string]string
	repoLocks    map[string]*sync.Mutex
	stats        map[string]*repoStats

	handler http.Handler
}

// New initializes a pure-Go Smart HTTP Git server rooted at cfg.ReposDir.
func New(cfg Config) (*Server, error) {
	if strings.TrimSpace(cfg.ReposDir) == "" {
		return nil, errors.New("ReposDir is required")
	}
	if err := os.MkdirAll(cfg.ReposDir, 0o750); err != nil {
		return nil, fmt.Errorf("create ReposDir: %w", err)
	}

	defaultToken := cfg.DefaultToken
	adminToken := cfg.AdminToken
	if defaultToken == "" && len(cfg.TenantTokens) == 0 {
		generated, err := httputil.RandomToken(32)
		if err != nil {
			return nil, err
		}
		defaultToken = generated
		slog.Warn("GIT_SERVER_TOKEN unset; generated ephemeral instance-isolated Git auth token")
	}
	if adminToken == "" {
		generated, err := httputil.RandomToken(32)
		if err != nil {
			return nil, err
		}
		adminToken = generated
		slog.Warn("GIT_ADMIN_TOKEN unset; generated ephemeral instance-isolated admin token")
	}

	copiedTokens := make(map[string]string, len(cfg.TenantTokens))
	maps.Copy(copiedTokens, cfg.TenantTokens)

	s := &Server{
		reposDir:     cfg.ReposDir,
		defaultToken: defaultToken,
		adminToken:   adminToken,
		tenantTokens: copiedTokens,
		repoLocks:    make(map[string]*sync.Mutex),
		stats:        make(map[string]*repoStats),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /git/{repo}/info/refs", s.handleInfoRefs)
	mux.HandleFunc("POST /git/{repo}/git-upload-pack", s.handleUploadPack)
	mux.HandleFunc("POST /git/{repo}/git-receive-pack", s.handleReceivePackForbidden)
	mux.HandleFunc("POST /admin/repos/{tenant}/seed", s.handleAdminSeed)
	mux.HandleFunc("POST /admin/repos/{tenant}/commit", s.handleAdminCommit)
	mux.HandleFunc("GET /admin/repos/{tenant}/head", s.handleAdminHead)

	s.handler = httputil.SecurityHeaders(mux)
	return s, nil
}

// Handler returns the HTTP handler serving Smart HTTP Git and admin endpoints.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// SetTenantToken sets or rotates the expected HTTP token for a specific tenant repository.
func (s *Server) SetTenantToken(tenantID, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenantTokens[tenantID] = token
}

// Telemetry returns Smart HTTP upload-pack transfer stats for tenantID.
func (s *Server) Telemetry(tenantID string) RepoTelemetry {
	st := s.repoStat(tenantID)
	return RepoTelemetry{
		UploadPackRequests: st.uploadPackRequests.Load(),
		LastPackBytes:      st.lastPackBytes.Load(),
		TotalPackBytes:     st.totalPackBytes.Load(),
	}
}

// SeedDefaultTenant initializes (or resets) tenantID's bare repository with valid baseline Kustomize manifests.
func (s *Server) SeedDefaultTenant(tenantID, targetNamespace string) (Revision, error) {
	return s.SeedTenant(tenantID, DefaultTenantManifests(tenantID, targetNamespace), fmt.Sprintf("Seed baseline manifests for %s", tenantID))
}

// SeedTenant initializes (or resets) a bare repository for tenantID with exact file contents on main.
func (s *Server) SeedTenant(tenantID string, files map[string]string, message string) (Revision, error) {
	repoPath, err := s.resolveBareRepoPath(tenantID)
	if err != nil {
		return Revision{}, err
	}
	if len(files) == 0 {
		return Revision{}, errors.New("seed files cannot be empty")
	}

	lock := s.repoMutex(tenantID)
	lock.Lock()
	defer lock.Unlock()

	if err := os.RemoveAll(repoPath); err != nil {
		return Revision{}, fmt.Errorf("clear existing repo: %w", err)
	}
	if err := os.MkdirAll(repoPath, 0o750); err != nil {
		return Revision{}, fmt.Errorf("mkdir bare repo: %w", err)
	}

	sto := newBareStorer(repoPath)
	if _, err := gogit.InitWithOptions(sto, nil, gogit.InitOptions{
		DefaultBranch: plumbing.NewBranchReferenceName(defaultBranchName),
	}); err != nil {
		return Revision{}, fmt.Errorf("init bare repo: %w", err)
	}

	return s.commitLocked(tenantID, sto, CommitChange{
		Upsert:  files,
		Message: message,
	}, true)
}

// Commit applies file additions/updates and deletions on top of refs/heads/main and advances HEAD.
func (s *Server) Commit(tenantID string, change CommitChange) (Revision, error) {
	repoPath, err := s.resolveBareRepoPath(tenantID)
	if err != nil {
		return Revision{}, err
	}

	lock := s.repoMutex(tenantID)
	lock.Lock()
	defer lock.Unlock()

	if _, err := os.Stat(repoPath); err != nil {
		return Revision{}, fmt.Errorf("repository %q not found: %w", tenantID, err)
	}
	sto := newBareStorer(repoPath)
	return s.commitLocked(tenantID, sto, change, false)
}

// HeadRevision returns the 40-character hex commit SHA currently at refs/heads/main.
func (s *Server) HeadRevision(tenantID string) (string, error) {
	repoPath, err := s.resolveBareRepoPath(tenantID)
	if err != nil {
		return "", err
	}

	lock := s.repoMutex(tenantID)
	lock.Lock()
	defer lock.Unlock()

	sto := newBareStorer(repoPath)
	ref, err := sto.Reference(plumbing.NewBranchReferenceName(defaultBranchName))
	if err != nil {
		return "", fmt.Errorf("read HEAD of %s: %w", tenantID, err)
	}
	return ref.Hash().String(), nil
}

func (s *Server) commitLocked(tenantID string, sto *filesystem.Storage, change CommitChange, replaceAll bool) (Revision, error) {
	branchRefName := plumbing.NewBranchReferenceName(defaultBranchName)

	fileContents := make(map[string][]byte)
	var parentHashes []plumbing.Hash
	var oldTree *object.Tree

	if !replaceAll {
		if ref, err := sto.Reference(branchRefName); err == nil {
			parentCommit, err := object.GetCommit(sto, ref.Hash())
			if err != nil {
				return Revision{}, fmt.Errorf("load parent commit: %w", err)
			}
			parentHashes = []plumbing.Hash{parentCommit.Hash}
			t, err := parentCommit.Tree()
			if err != nil {
				return Revision{}, fmt.Errorf("load parent tree: %w", err)
			}
			oldTree = t
			if !change.ReplaceTree {
				if err := t.Files().ForEach(func(f *object.File) error {
					content, err := f.Contents()
					if err != nil {
						return err
					}
					fileContents[f.Name] = []byte(content)
					return nil
				}); err != nil {
					return Revision{}, fmt.Errorf("read parent files: %w", err)
				}
			}
		}
	}

	for _, delPath := range change.Delete {
		cleanRel, err := sanitizeRepoFilePath(delPath)
		if err != nil {
			return Revision{}, err
		}
		delete(fileContents, cleanRel)
	}
	for rawPath, body := range change.Upsert {
		cleanRel, err := sanitizeRepoFilePath(rawPath)
		if err != nil {
			return Revision{}, err
		}
		fileContents[cleanRel] = []byte(body)
	}
	if len(fileContents) == 0 {
		return Revision{}, errors.New("cannot create empty tree commit")
	}

	rootHash, err := writeTreeHierarchy(sto, fileContents)
	if err != nil {
		return Revision{}, fmt.Errorf("encode git tree: %w", err)
	}
	newTree, err := object.GetTree(sto, rootHash)
	if err != nil {
		return Revision{}, fmt.Errorf("reload new tree: %w", err)
	}

	msg := strings.TrimSpace(change.Message)
	if msg == "" {
		msg = "Update tenant manifests"
	}
	authorName := strings.TrimSpace(change.AuthorName)
	if authorName == "" {
		authorName = "Flux Substrate"
	}
	authorEmail := strings.TrimSpace(change.AuthorEmail)
	if authorEmail == "" {
		authorEmail = "gitops@flux-substrate.internal"
	}
	now := time.Now().UTC()
	sig := object.Signature{
		Name:  authorName,
		Email: authorEmail,
		When:  now,
	}

	commitObj := &object.Commit{
		Author:       sig,
		Committer:    sig,
		Message:      msg,
		TreeHash:     rootHash,
		ParentHashes: parentHashes,
	}
	encCommit := sto.NewEncodedObject()
	if err := commitObj.Encode(encCommit); err != nil {
		return Revision{}, fmt.Errorf("encode commit: %w", err)
	}
	commitHash, err := sto.SetEncodedObject(encCommit)
	if err != nil {
		return Revision{}, fmt.Errorf("store commit: %w", err)
	}

	if err := sto.SetReference(plumbing.NewHashReference(branchRefName, commitHash)); err != nil {
		return Revision{}, fmt.Errorf("update branch ref: %w", err)
	}
	if err := sto.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, branchRefName)); err != nil {
		return Revision{}, fmt.Errorf("update symbolic HEAD: %w", err)
	}

	var diffSummary string
	if oldTree != nil {
		if patch, err := oldTree.Patch(newTree); err == nil {
			diffSummary = patch.String()
		}
	}

	return Revision{
		TenantID:    tenantID,
		Branch:      defaultBranchName,
		SHA:         commitHash.String(),
		Message:     msg,
		SummaryDiff: diffSummary,
		CommittedAt: now,
	}, nil
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleReceivePackForbidden(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusForbidden, map[string]string{
		"error": "git-receive-pack is disabled; repositories are read-only over Smart HTTP",
	})
}

func (s *Server) handleInfoRefs(w http.ResponseWriter, r *http.Request) {
	tenantID, repoPath, ok := s.authorizeGitRequest(w, r)
	if !ok {
		return
	}

	service := r.URL.Query().Get("service")
	switch service {
	case "git-upload-pack":
	case "git-receive-pack":
		s.handleReceivePackForbidden(w, r)
		return
	default:
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported git service"})
		return
	}

	lock := s.repoMutex(tenantID)
	lock.Lock()
	defer lock.Unlock()

	if _, err := os.Stat(repoPath); err != nil {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "repository not found"})
		return
	}

	sto := newBareStorer(repoPath)
	ar, err := buildAdvertisedRefs(sto)
	if err != nil {
		httputil.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to read repository refs"})
		return
	}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	enc := pktline.NewEncoder(w)
	if err := enc.Encodef("# service=git-upload-pack\n"); err != nil {
		return
	}
	if err := enc.Flush(); err != nil {
		return
	}
	_ = ar.Encode(w)
}

func (s *Server) handleUploadPack(w http.ResponseWriter, r *http.Request) {
	tenantID, repoPath, ok := s.authorizeGitRequest(w, r)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxGitBodyBytes)
	defer r.Body.Close()

	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read upload-pack request"})
		return
	}

	req, err := decodeUploadPackHTTPRequest(rawBody)
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid upload-pack request"})
		return
	}

	lock := s.repoMutex(tenantID)
	lock.Lock()
	defer lock.Unlock()

	if _, err := os.Stat(repoPath); err != nil {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "repository not found"})
		return
	}

	sto := newBareStorer(repoPath)
	packBytes, shallows, ackHash, err := buildUploadPackPayload(sto, req)
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "upload-pack negotiation failed"})
		return
	}

	st := s.repoStat(tenantID)
	st.uploadPackRequests.Add(1)
	st.lastPackBytes.Store(int64(len(packBytes)))
	st.totalPackBytes.Add(int64(len(packBytes)))

	resp := packp.NewUploadPackResponseWithPackfile(req, io.NopCloser(bytes.NewReader(packBytes)))
	resp.Shallows = shallows
	if !ackHash.IsZero() {
		resp.ACKs = []plumbing.Hash{ackHash}
	}

	w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	_ = resp.Encode(w)
}

func (s *Server) handleAdminSeed(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	tenantID := normalizeTenantID(r.PathValue("tenant"))
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminBodyBytes)
	defer r.Body.Close()

	var payload struct {
		TargetNamespace string            `json:"targetNamespace,omitempty"`
		Files           map[string]string `json:"files,omitempty"`
		Message         string            `json:"message,omitempty"`
	}
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
			httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid seed JSON body"})
			return
		}
	}

	var (
		rev Revision
		err error
	)
	if len(payload.Files) > 0 {
		rev, err = s.SeedTenant(tenantID, payload.Files, payload.Message)
	} else {
		rev, err = s.SeedDefaultTenant(tenantID, payload.TargetNamespace)
	}
	if err != nil {
		slog.Error("admin seed failed", slog.String("tenant", tenantID), slog.Any("err", err))
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to seed tenant repository"})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, rev)
}

func (s *Server) handleAdminCommit(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	tenantID := normalizeTenantID(r.PathValue("tenant"))
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminBodyBytes)
	defer r.Body.Close()

	var change CommitChange
	if err := json.NewDecoder(r.Body).Decode(&change); err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid commit JSON body"})
		return
	}

	rev, err := s.Commit(tenantID, change)
	if err != nil {
		slog.Error("admin commit failed", slog.String("tenant", tenantID), slog.Any("err", err))
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to commit repository change"})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, rev)
}

func (s *Server) handleAdminHead(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(w, r) {
		return
	}
	tenantID := normalizeTenantID(r.PathValue("tenant"))
	sha, err := s.HeadRevision(tenantID)
	if err != nil {
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{"error": "repository not found"})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"tenantId": tenantID,
		"branch":   defaultBranchName,
		"sha":      sha,
	})
}

func (s *Server) authorizeAdmin(w http.ResponseWriter, r *http.Request) bool {
	presented := extractPresentedSecret(r.Header.Get("Authorization"))
	if presented == "" || subtle.ConstantTimeCompare([]byte(presented), []byte(s.adminToken)) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="flux-substrate-git-admin"`)
		httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return false
	}
	return true
}

func (s *Server) authorizeGitRequest(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	rawRepo := r.PathValue("repo")
	tenantID := normalizeTenantID(rawRepo)
	repoPath, err := s.resolveBareRepoPath(tenantID)
	if err != nil {
		httputil.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid repository name"})
		return "", "", false
	}

	expected := s.expectedTokenFor(tenantID)
	presented := extractPresentedSecret(r.Header.Get("Authorization"))
	if expected == "" || presented == "" || subtle.ConstantTimeCompare([]byte(presented), []byte(expected)) != 1 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="flux-substrate-git"`)
		httputil.WriteJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return "", "", false
	}
	return tenantID, repoPath, true
}

func (s *Server) expectedTokenFor(tenantID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if tok, ok := s.tenantTokens[tenantID]; ok && tok != "" {
		return tok
	}
	return s.defaultToken
}

func (s *Server) resolveBareRepoPath(tenantID string) (string, error) {
	if tenantID == "" || strings.HasSuffix(tenantID, ".git") {
		return "", errors.New("invalid tenant identifier")
	}
	return httputil.SecureSubpath(s.reposDir, tenantID+".git")
}

func (s *Server) repoMutex(tenantID string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.repoLocks[tenantID]
	if !ok {
		m = &sync.Mutex{}
		s.repoLocks[tenantID] = m
	}
	return m
}

func (s *Server) repoStat(tenantID string) *repoStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.stats[tenantID]
	if !ok {
		st = &repoStats{}
		s.stats[tenantID] = st
	}
	return st
}

func normalizeTenantID(raw string) string {
	return strings.TrimSuffix(strings.TrimSpace(raw), ".git")
}

func extractPresentedSecret(authHeader string) string {
	scheme, rest, ok := strings.Cut(strings.TrimSpace(authHeader), " ")
	if !ok {
		return ""
	}
	rest = strings.TrimSpace(rest)
	switch {
	case strings.EqualFold(scheme, "Bearer"):
		return rest
	case strings.EqualFold(scheme, "Basic"):
		decoded, err := base64.StdEncoding.DecodeString(rest)
		if err != nil {
			return ""
		}
		_, password, hasColon := strings.Cut(string(decoded), ":")
		if hasColon {
			return password
		}
		return string(decoded)
	default:
		return ""
	}
}

func newBareStorer(repoPath string) *filesystem.Storage {
	return filesystem.NewStorage(osfs.New(repoPath, osfs.WithBoundOS()), cache.NewObjectLRUDefault())
}

func sanitizeRepoFilePath(raw string) (string, error) {
	if raw == "" || strings.ContainsRune(raw, '\x00') || strings.ContainsRune(raw, '\\') {
		return "", fmt.Errorf("invalid repository file path %q", raw)
	}
	if strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("repository file path must be relative: %q", raw)
	}
	cleaned := path.Clean(raw)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("repository file path traversal rejected: %q", raw)
	}
	for seg := range strings.SplitSeq(cleaned, "/") {
		if seg == "" || seg == "." || seg == ".." || seg == ".git" {
			return "", fmt.Errorf("disallowed path segment %q in %q", seg, raw)
		}
	}
	return cleaned, nil
}

type dirNode struct {
	files   map[string][]byte
	subdirs map[string]*dirNode
}

func newDirNode() *dirNode {
	return &dirNode{
		files:   make(map[string][]byte),
		subdirs: make(map[string]*dirNode),
	}
}

func writeTreeHierarchy(sto storer.EncodedObjectStorer, files map[string][]byte) (plumbing.Hash, error) {
	root := newDirNode()
	for relPath, content := range files {
		parts := strings.Split(relPath, "/")
		curr := root
		for _, dirPart := range parts[:len(parts)-1] {
			next, ok := curr.subdirs[dirPart]
			if !ok {
				next = newDirNode()
				curr.subdirs[dirPart] = next
			}
			curr = next
		}
		curr.files[parts[len(parts)-1]] = content
	}
	return encodeDirNode(sto, root)
}

func encodeDirNode(sto storer.EncodedObjectStorer, node *dirNode) (plumbing.Hash, error) {
	entries := make([]object.TreeEntry, 0, len(node.files)+len(node.subdirs))

	for name, content := range node.files {
		blobObj := sto.NewEncodedObject()
		blobObj.SetType(plumbing.BlobObject)
		blobObj.SetSize(int64(len(content)))
		w, err := blobObj.Writer()
		if err != nil {
			return plumbing.ZeroHash, err
		}
		if _, err := w.Write(content); err != nil {
			_ = w.Close()
			return plumbing.ZeroHash, err
		}
		if err := w.Close(); err != nil {
			return plumbing.ZeroHash, err
		}
		blobHash, err := sto.SetEncodedObject(blobObj)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		entries = append(entries, object.TreeEntry{
			Name: name,
			Mode: filemode.Regular,
			Hash: blobHash,
		})
	}

	for dirName, child := range node.subdirs {
		subHash, err := encodeDirNode(sto, child)
		if err != nil {
			return plumbing.ZeroHash, err
		}
		entries = append(entries, object.TreeEntry{
			Name: dirName,
			Mode: filemode.Dir,
			Hash: subHash,
		})
	}

	slices.SortFunc(entries, func(a, b object.TreeEntry) int {
		return strings.Compare(gitTreeSortKey(a), gitTreeSortKey(b))
	})

	tree := &object.Tree{Entries: entries}
	encTree := sto.NewEncodedObject()
	if err := tree.Encode(encTree); err != nil {
		return plumbing.ZeroHash, err
	}
	return sto.SetEncodedObject(encTree)
}

func gitTreeSortKey(e object.TreeEntry) string {
	if e.Mode == filemode.Dir {
		return e.Name + "/"
	}
	return e.Name
}

func setServerCapabilities(caps *capability.List) error {
	if err := caps.Set(capability.Agent, capability.DefaultAgent()); err != nil {
		return err
	}
	if err := caps.Set(capability.OFSDelta); err != nil {
		return err
	}
	if err := caps.Set(capability.Shallow); err != nil {
		return err
	}
	if err := caps.Set(capability.NoProgress); err != nil {
		return err
	}
	return nil
}

func buildAdvertisedRefs(sto *filesystem.Storage) (*packp.AdvRefs, error) {
	ar := packp.NewAdvRefs()
	if err := setServerCapabilities(ar.Capabilities); err != nil {
		return nil, err
	}

	iter, err := sto.IterReferences()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	err = iter.ForEach(func(ref *plumbing.Reference) error {
		if ref.Type() != plumbing.HashReference {
			return nil
		}
		ar.References[ref.Name().String()] = ref.Hash()
		return nil
	})
	if err != nil {
		return nil, err
	}

	if headRef, err := storer.ResolveReference(sto, plumbing.HEAD); err == nil && headRef != nil {
		h := headRef.Hash()
		ar.Head = &h
	}
	return ar, nil
}

// decodeUploadPackHTTPRequest decodes both the UploadRequest section (up to the
// first flush-pkt) and the trailing "have <sha>" negotiation section sent by
// Smart HTTP git-upload-pack clients.
func decodeUploadPackHTTPRequest(raw []byte) (*packp.UploadPackRequest, error) {
	req := packp.NewUploadPackRequest()
	if err := req.Decode(bytes.NewReader(raw)); err != nil {
		return nil, err
	}

	scn := pktline.NewScanner(bytes.NewReader(raw))
	seenUploadFlush := false
	for scn.Scan() {
		line := scn.Bytes()
		if !seenUploadFlush {
			if len(line) == 0 {
				seenUploadFlush = true
			}
			continue
		}
		trimmed := strings.TrimSpace(string(line))
		if trimmed == "" || trimmed == "done" {
			continue
		}
		if after, ok := strings.CutPrefix(trimmed, "have "); ok {
			hexStr := strings.TrimSpace(after)
			if len(hexStr) == 40 {
				req.Haves = append(req.Haves, plumbing.NewHash(hexStr))
			}
		}
	}
	if err := scn.Err(); err != nil {
		return nil, err
	}
	return req, nil
}

func buildUploadPackPayload(
	sto *filesystem.Storage,
	req *packp.UploadPackRequest,
) ([]byte, []plumbing.Hash, plumbing.Hash, error) {
	if err := req.Validate(); err != nil {
		return nil, nil, plumbing.ZeroHash, err
	}

	supported := capability.NewList()
	if err := setServerCapabilities(supported); err != nil {
		return nil, nil, plumbing.ZeroHash, err
	}
	for _, c := range req.Capabilities.All() {
		if !supported.Supports(c) {
			return nil, nil, plumbing.ZeroHash, fmt.Errorf("unsupported capability %s", c)
		}
	}

	clientShallowSet := make(map[plumbing.Hash]bool, len(req.Shallows))
	for _, sh := range req.Shallows {
		clientShallowSet[sh] = true
	}

	haveObjects := make(map[plumbing.Hash]bool)
	var firstACK plumbing.Hash
	visitedHaveCommits := make(map[plumbing.Hash]bool)

	for _, haveHash := range req.Haves {
		if sto.HasEncodedObject(haveHash) != nil {
			continue
		}
		if firstACK.IsZero() {
			firstACK = haveHash
		}
		collectHaveGraph(sto, haveHash, clientShallowSet, visitedHaveCommits, haveObjects)
	}

	maxDepth := 0
	if !req.Depth.IsZero() {
		dc, ok := req.Depth.(packp.DepthCommits)
		if !ok || int(dc) < 0 {
			return nil, nil, plumbing.ZeroHash, errors.New("unsupported depth specification")
		}
		maxDepth = int(dc)
	}

	type commitStep struct {
		hash  plumbing.Hash
		depth int
	}

	wantOrder := make([]plumbing.Hash, 0, 32)
	wantSeen := make(map[plumbing.Hash]bool)
	addWantObj := func(h plumbing.Hash) {
		if haveObjects[h] || wantSeen[h] {
			return
		}
		wantSeen[h] = true
		wantOrder = append(wantOrder, h)
	}

	var queue []commitStep
	queuedCommits := make(map[plumbing.Hash]int)
	var shallowBoundaries []plumbing.Hash
	shallowSeen := make(map[plumbing.Hash]bool)

	for _, wantHash := range req.Wants {
		targetHash := wantHash
		if tagObj, err := object.GetTag(sto, wantHash); err == nil {
			addWantObj(tagObj.Hash)
			targetHash = tagObj.Target
		}
		if _, err := object.GetCommit(sto, targetHash); err == nil {
			if prevDepth, exists := queuedCommits[targetHash]; !exists || 1 < prevDepth {
				queuedCommits[targetHash] = 1
				queue = append(queue, commitStep{hash: targetHash, depth: 1})
			}
		} else if sto.HasEncodedObject(targetHash) == nil {
			addWantObj(targetHash)
		} else {
			return nil, nil, plumbing.ZeroHash, fmt.Errorf("wanted object %s not found", wantHash)
		}
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		commitObj, err := object.GetCommit(sto, curr.hash)
		if err != nil {
			return nil, nil, plumbing.ZeroHash, err
		}
		addWantObj(commitObj.Hash)
		if err := collectTreeObjects(sto, commitObj.TreeHash, haveObjects, wantSeen, &wantOrder); err != nil {
			return nil, nil, plumbing.ZeroHash, err
		}

		if maxDepth > 0 && curr.depth >= maxDepth {
			if len(commitObj.ParentHashes) > 0 && !shallowSeen[commitObj.Hash] {
				shallowSeen[commitObj.Hash] = true
				shallowBoundaries = append(shallowBoundaries, commitObj.Hash)
			}
			continue
		}
		if maxDepth == 0 && clientShallowSet[commitObj.Hash] {
			continue
		}

		for _, parentHash := range commitObj.ParentHashes {
			if haveObjects[parentHash] {
				continue
			}
			nextDepth := curr.depth + 1
			if prevDepth, exists := queuedCommits[parentHash]; !exists || nextDepth < prevDepth {
				queuedCommits[parentHash] = nextDepth
				queue = append(queue, commitStep{hash: parentHash, depth: nextDepth})
			}
		}
	}

	var packBuf bytes.Buffer
	enc := packfile.NewEncoder(&packBuf, sto, false)
	if _, err := enc.Encode(wantOrder, 10); err != nil {
		return nil, nil, plumbing.ZeroHash, fmt.Errorf("encode packfile: %w", err)
	}
	return packBuf.Bytes(), shallowBoundaries, firstACK, nil
}

func collectHaveGraph(
	sto *filesystem.Storage,
	start plumbing.Hash,
	clientShallowSet map[plumbing.Hash]bool,
	visitedCommits map[plumbing.Hash]bool,
	haveObjects map[plumbing.Hash]bool,
) {
	stack := []plumbing.Hash{start}
	for len(stack) > 0 {
		h := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if visitedCommits[h] {
			continue
		}
		visitedCommits[h] = true
		haveObjects[h] = true

		c, err := object.GetCommit(sto, h)
		if err != nil {
			continue
		}
		markTreeObjectsHave(sto, c.TreeHash, haveObjects)
		if clientShallowSet[h] {
			continue
		}
		stack = append(stack, c.ParentHashes...)
	}
}

func markTreeObjectsHave(sto *filesystem.Storage, treeHash plumbing.Hash, haveObjects map[plumbing.Hash]bool) {
	if haveObjects[treeHash] {
		return
	}
	t, err := object.GetTree(sto, treeHash)
	if err != nil {
		return
	}
	haveObjects[treeHash] = true
	for _, entry := range t.Entries {
		if entry.Mode == filemode.Dir {
			markTreeObjectsHave(sto, entry.Hash, haveObjects)
		} else {
			haveObjects[entry.Hash] = true
		}
	}
}

func collectTreeObjects(
	sto *filesystem.Storage,
	treeHash plumbing.Hash,
	haveObjects map[plumbing.Hash]bool,
	wantSeen map[plumbing.Hash]bool,
	wantOrder *[]plumbing.Hash,
) error {
	if haveObjects[treeHash] || wantSeen[treeHash] {
		return nil
	}
	t, err := object.GetTree(sto, treeHash)
	if err != nil {
		return err
	}
	wantSeen[treeHash] = true
	*wantOrder = append(*wantOrder, treeHash)

	for _, entry := range t.Entries {
		if entry.Mode == filemode.Dir {
			if err := collectTreeObjects(sto, entry.Hash, haveObjects, wantSeen, wantOrder); err != nil {
				return err
			}
		} else if !haveObjects[entry.Hash] && !wantSeen[entry.Hash] {
			wantSeen[entry.Hash] = true
			*wantOrder = append(*wantOrder, entry.Hash)
		}
	}
	return nil
}
