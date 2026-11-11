// Package crds embeds the upstream Flux GitRepository and Kustomization CRDs
// for envtest control planes and cluster bootstrap helpers.
package crds

import (
	"embed"
	"fmt"

	kustomizev1 "github.com/fluxcd/kustomize-controller/api/v1"
	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// FS exposes the raw upstream CRD YAML manifests under manifests/crds/.
//
//go:embed *.yaml
var FS embed.FS

//go:embed source.toolkit.fluxcd.io_gitrepositories.yaml
var gitRepositoryCRDBytes []byte

//go:embed kustomize.toolkit.fluxcd.io_kustomizations.yaml
var kustomizationCRDBytes []byte

// RawManifests returns the raw YAML bytes for GitRepository and Kustomization CRDs.
func RawManifests() [][]byte {
	return [][]byte{
		append([]byte(nil), gitRepositoryCRDBytes...),
		append([]byte(nil), kustomizationCRDBytes...),
	}
}

// ParseCRDs decodes the embedded GitRepository and Kustomization CustomResourceDefinitions
// for direct installation in sigs.k8s.io/controller-runtime/pkg/envtest.
func ParseCRDs() ([]*apiextensionsv1.CustomResourceDefinition, error) {
	raws := []struct {
		name string
		data []byte
	}{
		{name: "source.toolkit.fluxcd.io_gitrepositories.yaml", data: gitRepositoryCRDBytes},
		{name: "kustomize.toolkit.fluxcd.io_kustomizations.yaml", data: kustomizationCRDBytes},
	}

	out := make([]*apiextensionsv1.CustomResourceDefinition, 0, len(raws))
	for _, item := range raws {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := yaml.Unmarshal(item.data, &crd); err != nil {
			return nil, fmt.Errorf("decode %s: %w", item.name, err)
		}
		out = append(out, &crd)
	}
	return out, nil
}

// AddToScheme registers core Kubernetes types and Flux GitRepository/Kustomization v1 types.
func AddToScheme(s *runtime.Scheme) error {
	builders := []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		rbacv1.AddToScheme,
		apiextensionsv1.AddToScheme,
		sourcev1.AddToScheme,
		kustomizev1.AddToScheme,
	}
	for _, add := range builders {
		if err := add(s); err != nil {
			return err
		}
	}
	return nil
}

// NewScheme builds a runtime.Scheme pre-populated with core Kubernetes and Flux v1 CRDs.
func NewScheme() (*runtime.Scheme, error) {
	s := runtime.NewScheme()
	if err := AddToScheme(s); err != nil {
		return nil, err
	}
	return s, nil
}
