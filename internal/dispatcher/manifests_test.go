package dispatcher_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
)

func TestPortableBaseManifestsBuildWithKustomize(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repoRoot: %v", err)
	}
	baseDir := filepath.Join(repoRoot, "manifests", "base")

	opts := krusty.MakeDefaultOptions()
	opts.LoadRestrictions = types.LoadRestrictionsNone
	k := krusty.MakeKustomizer(opts)

	resMap, err := k.Run(filesys.MakeFsOnDisk(), baseDir)
	if err != nil {
		t.Fatalf("kustomize build manifests/base: %v", err)
	}
	yamlBytes, err := resMap.AsYaml()
	if err != nil {
		t.Fatalf("serialize kustomize output: %v", err)
	}
	rendered := string(yamlBytes)

	mustContain := []string{
		"kind: CustomResourceDefinition",
		"name: gitrepositories.source.toolkit.fluxcd.io",
		"name: kustomizations.kustomize.toolkit.fluxcd.io",
		"kind: WorkerPool",
		"name: flux-reconciler-pool",
		"flux-substrate.io/role: flux-reconciler",
		"ko://github.com/gke-demos/flux-substrate/cmd/flux-substrate",
		"--mode=git-server",
		"--mode=oidc-bridge",
		"--mode=dispatcher",
	}
	for _, s := range mustContain {
		if !strings.Contains(rendered, s) {
			t.Errorf("rendered manifests/base missing expected snippet %q", s)
		}
	}

	// Ensure zero hardcoded personal cluster/project/node identifiers in manifests or hack scripts.
	forbiddenIdentifiers := []string{
		"mofilabs",
		"ate-cluster",
		"substrate-workflows",
		"gke_mofilabs",
		"fluid compute",
		"Vertex AI",
	}
	checkPaths := []string{
		filepath.Join(repoRoot, ".ko.yaml"),
		filepath.Join(repoRoot, "README.md"),
		filepath.Join(repoRoot, "hack", "deploy.sh"),
		filepath.Join(repoRoot, "hack", "verify-e2e.sh"),
	}
	for _, p := range checkPaths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		text := string(raw)
		for _, forbidden := range forbiddenIdentifiers {
			if strings.Contains(text, forbidden) {
				t.Errorf("%s contains forbidden hardcoded identifier %q", p, forbidden)
			}
		}
	}
	for _, forbidden := range forbiddenIdentifiers {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("rendered manifests/base contains forbidden hardcoded identifier %q", forbidden)
		}
	}
}
