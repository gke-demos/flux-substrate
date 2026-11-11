package gitserver_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fluxcd/pkg/git"
	"github.com/fluxcd/pkg/git/gogit"
	"github.com/fluxcd/pkg/git/repository"
	extgogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/gke-demos/flux-substrate/internal/gitserver"
	"github.com/gke-demos/flux-substrate/internal/httputil"
)

func TestGitServerAuthSeedCloneAndCommit(t *testing.T) {
	reposDir := t.TempDir()
	tenantToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken: %v", err)
	}
	adminToken, err := httputil.RandomToken(24)
	if err != nil {
		t.Fatalf("RandomToken admin: %v", err)
	}

	gs, err := gitserver.New(gitserver.Config{
		ReposDir:   reposDir,
		AdminToken: adminToken,
		TenantTokens: map[string]string{
			"tenant-01": tenantToken,
		},
	})
	if err != nil {
		t.Fatalf("gitserver.New: %v", err)
	}

	seeded, err := gs.SeedDefaultTenant("tenant-01", "tenant-01")
	if err != nil {
		t.Fatalf("SeedDefaultTenant: %v", err)
	}
	if len(seeded.SHA) != 40 {
		t.Fatalf("seeded SHA = %q, want 40-char hex", seeded.SHA)
	}

	ts := httptest.NewServer(gs.Handler())
	t.Cleanup(ts.Close)

	repoURL := ts.URL + "/git/tenant-01.git"

	// 1. Missing Authorization header -> 401 Unauthorized.
	unauthResp, err := ts.Client().Get(repoURL + "/info/refs?service=git-upload-pack")
	if err != nil {
		t.Fatalf("GET info/refs unauth: %v", err)
	}
	unauthResp.Body.Close()
	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated info/refs status = %d, want %d", unauthResp.StatusCode, http.StatusUnauthorized)
	}

	// 2. Wrong token (e.g. unreplaced placeholder) -> 401 Unauthorized.
	wrongReq, _ := http.NewRequest(http.MethodGet, repoURL+"/info/refs?service=git-upload-pack", nil)
	wrongReq.Header.Set("Authorization", "Bearer actor-placeholder")
	wrongResp, err := ts.Client().Do(wrongReq)
	if err != nil {
		t.Fatalf("GET info/refs wrong token: %v", err)
	}
	wrongResp.Body.Close()
	if wrongResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong-token info/refs status = %d, want %d", wrongResp.StatusCode, http.StatusUnauthorized)
	}

	// 3. Disallowed git-receive-pack -> 403 Forbidden.
	pushProbeReq, _ := http.NewRequest(http.MethodGet, repoURL+"/info/refs?service=git-receive-pack", nil)
	pushProbeReq.Header.Set("Authorization", "Bearer "+tenantToken)
	pushProbeResp, err := ts.Client().Do(pushProbeReq)
	if err != nil {
		t.Fatalf("GET git-receive-pack: %v", err)
	}
	pushProbeResp.Body.Close()
	if pushProbeResp.StatusCode != http.StatusForbidden {
		t.Fatalf("git-receive-pack status = %d, want %d", pushProbeResp.StatusCode, http.StatusForbidden)
	}

	// 4. Shallow clone via real fluxcd/pkg/git/gogit succeeds and builds valid Kustomize manifests.
	cloneDir := t.TempDir()
	fluxGitClient, err := gogit.NewClient(
		cloneDir,
		&git.AuthOptions{
			Transport:   git.HTTP,
			BearerToken: tenantToken,
		},
		gogit.WithDiskStorage(),
		gogit.WithSingleBranch(true),
		gogit.WithInsecureCredentialsOverHTTP(),
	)
	if err != nil {
		t.Fatalf("gogit.NewClient: %v", err)
	}

	commit, err := fluxGitClient.Clone(context.Background(), repoURL, repository.CloneConfig{
		CheckoutStrategy: repository.CheckoutStrategy{Branch: "main"},
		ShallowClone:     true,
	})
	if err != nil {
		t.Fatalf("fluxGitClient.Clone: %v", err)
	}
	if got := commit.Hash.String(); got != seeded.SHA {
		t.Fatalf("cloned commit = %q, want %q", got, seeded.SHA)
	}

	kustomizer := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	resMap, err := kustomizer.Run(filesys.MakeFsOnDisk(), cloneDir)
	if err != nil {
		t.Fatalf("krusty.Run on seeded repo: %v", err)
	}
	if resMap.Size() == 0 {
		t.Fatalf("expected non-empty rendered Kustomize resources")
	}

	// 5. Programmatic commit via HTTP admin endpoint advances HEAD and supports incremental fetch.
	commitPayload, _ := json.Marshal(gitserver.CommitChange{
		Upsert: map[string]string{
			"configmap.yaml": `apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: tenant-01
data:
  release: "v2"
`,
		},
		Message: "Bump release to v2",
	})
	adminReq, _ := http.NewRequest(http.MethodPost, ts.URL+"/admin/repos/tenant-01/commit", bytes.NewReader(commitPayload))
	adminReq.Header.Set("Authorization", "Bearer "+adminToken)
	adminReq.Header.Set("Content-Type", "application/json")
	adminResp, err := ts.Client().Do(adminReq)
	if err != nil {
		t.Fatalf("POST /admin/repos/tenant-01/commit: %v", err)
	}
	defer adminResp.Body.Close()
	if adminResp.StatusCode != http.StatusOK {
		t.Fatalf("admin commit status = %d, want %d", adminResp.StatusCode, http.StatusOK)
	}
	var nextRev gitserver.Revision
	if err := json.NewDecoder(adminResp.Body).Decode(&nextRev); err != nil {
		t.Fatalf("decode Revision: %v", err)
	}
	if nextRev.SHA == "" || nextRev.SHA == seeded.SHA {
		t.Fatalf("expected HEAD to advance from %q, got %q", seeded.SHA, nextRev.SHA)
	}

	headSHA, err := gs.HeadRevision("tenant-01")
	if err != nil {
		t.Fatalf("HeadRevision: %v", err)
	}
	if headSHA != nextRev.SHA {
		t.Fatalf("HeadRevision = %q, want %q", headSHA, nextRev.SHA)
	}

	// Verify existing shallow checkout can incrementally fetch the new commit.
	localRepo, err := extgogit.PlainOpen(cloneDir)
	if err != nil {
		t.Fatalf("PlainOpen: %v", err)
	}
	err = localRepo.FetchContext(context.Background(), &extgogit.FetchOptions{
		RemoteName: "origin",
		RefSpecs:   []config.RefSpec{"+refs/heads/main:refs/remotes/origin/main"},
		Depth:      1,
		Force:      true,
		Auth:       &githttp.TokenAuth{Token: tenantToken},
	})
	if err != nil {
		t.Fatalf("incremental FetchContext(Depth=1): %v", err)
	}
}
