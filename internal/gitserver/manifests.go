package gitserver

import (
	"fmt"
	"strings"
)

// DefaultTenantManifests returns the baseline fulfillment-api Go source and Kustomize overlay files
// seeded for a Tenant Environment (go.mod, main.go, pricing.go, pricing_test.go, ConfigMap, Service, Deployment, and kustomization.yaml).
func DefaultTenantManifests(tenantID, targetNamespace string) map[string]string {
	if targetNamespace == "" {
		targetNamespace = tenantID
	}
	return map[string]string{
		"go.mod": "module example.com/fulfillment-api\n\ngo 1.24\n",
		"main.go": `package main

import (
	"fmt"
	"net/http"
)

func main() {
	http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	_ = http.ListenAndServe(":8080", nil)
}
`,
		"pricing.go": CleanFulfillmentPricingGo,
		"pricing_test.go": `package main

import "testing"

func TestCalculateTotal(t *testing.T) {
	if got := CalculateTotal(10000, 1500); got != 8500 {
		t.Fatalf("CalculateTotal(10000, 1500) = %d, want 8500", got)
	}
	if got := CalculateTotal(1000, 100); got != 900 {
		t.Fatalf("CalculateTotal(1000, 100) = %d, want 900 (off-by-one cent in net total calculation)", got)
	}
}
`,
		"kustomization.yaml": `apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
labels:
  - pairs:
      app.kubernetes.io/managed-by: flux-substrate
      reconciler.ate.dev/tenant: ` + tenantID + `
resources:
  - configmap.yaml
  - service.yaml
  - deployment.yaml
`,
		"configmap.yaml": fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: %s
data:
  app: "fulfillment-api"
  tenant: %q
  release: "v1"
  channel: "stable"
`, targetNamespace, tenantID),
		"service.yaml": fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: tenant-workload
  namespace: %s
spec:
  selector:
    app.kubernetes.io/name: tenant-workload
    reconciler.ate.dev/tenant: %s
  ports:
    - name: http
      port: 80
      targetPort: 8080
`, targetNamespace, tenantID),
		"deployment.yaml": fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: tenant-workload
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: tenant-workload
      reconciler.ate.dev/tenant: %s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: tenant-workload
        reconciler.ate.dev/tenant: %s
    spec:
      containers:
        - name: pause
          image: registry.k8s.io/pause:3.10
          ports:
            - containerPort: 8080
`, targetNamespace, tenantID, tenantID),
	}
}

// CleanFulfillmentPricingGo is the baseline pricing.go source for fulfillment-api.
const CleanFulfillmentPricingGo = `package main

// CalculateTotal returns the net amount in cents after subtracting discountCents,
// floored at zero so promotional discounts never produce a negative balance.
func CalculateTotal(subtotalCents, discountCents int) int {
	if discountCents <= 0 {
		return subtotalCents
	}
	if discountCents >= subtotalCents {
		return 0
	}
	return subtotalCents - discountCents
}
`

// BuggyFulfillmentPricingGo contains the off-by-one cent discount regression in CalculateTotal.
const BuggyFulfillmentPricingGo = `package main

// CalculateTotal returns the net amount in cents after subtracting discountCents,
// floored at zero so promotional discounts never produce a negative balance.
func CalculateTotal(subtotalCents, discountCents int) int {
	if discountCents <= 0 {
		return subtotalCents
	}
	if discountCents >= subtotalCents {
		return 0
	}
	return subtotalCents - discountCents - 1
}
`

// BuggyCodeManifests returns a fulfillment-api commit with the off-by-one cent discount
// regression in pricing.go to demonstrate build/test failure isolation and RevertActor recovery.
func BuggyCodeManifests(tenantID, targetNamespace string) map[string]string {
	m := DefaultTenantManifests(tenantID, targetNamespace)
	m["pricing.go"] = BuggyFulfillmentPricingGo
	return m
}

// MemoryBombManifests builds a dense Kustomize configMapGenerator amplification overlay
// (~280 KiB in Git expanding to ~4.5 MiB of rendered YAML ASTs) engineered to trip the
// Workflow Actor sandbox memory ceiling without bloating Git pack transfers.
func MemoryBombManifests(tenantID, targetNamespace string) map[string]string {
	if targetNamespace == "" {
		targetNamespace = tenantID
	}

	line := fmt.Sprintf("tenant=%s-runaway-kustomize-amplification-block-0123456789abcdef\n", tenantID)
	repeats := (280 << 10) / len(line)
	if repeats < 1 {
		repeats = 4096
	}
	payload := strings.Repeat(line, repeats)

	var kb strings.Builder
	kb.WriteString("apiVersion: kustomize.config.k8s.io/v1beta1\n")
	kb.WriteString("kind: Kustomization\n")
	kb.WriteString("namespace: " + targetNamespace + "\n")
	kb.WriteString("configMapGenerator:\n")
	for i := range 16 {
		fmt.Fprintf(&kb, "  - name: runaway-shard-%03d\n    files:\n      - payload.txt\n", i)
	}

	return map[string]string{
		"kustomization.yaml": kb.String(),
		"payload.txt":        payload,
	}
}

// CrossNamespaceManifests returns a Kustomize overlay for tenantID that includes a
// ConfigMap explicitly targeting forbiddenNamespace to test per-actor namespace RBAC isolation.
func CrossNamespaceManifests(tenantID, ownNamespace, forbiddenNamespace string) map[string]string {
	if ownNamespace == "" {
		ownNamespace = tenantID
	}
	if forbiddenNamespace == "" {
		if ownNamespace == "tenant-01" {
			forbiddenNamespace = "tenant-02"
		} else {
			forbiddenNamespace = "tenant-01"
		}
	}
	base := DefaultTenantManifests(tenantID, ownNamespace)
	base["kustomization.yaml"] = fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
labels:
  - pairs:
      app.kubernetes.io/managed-by: flux-substrate
      reconciler.ate.dev/tenant: %s
resources:
  - configmap.yaml
  - service.yaml
  - deployment.yaml
  - escape.yaml
`, tenantID)
	base["escape.yaml"] = fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: cross-tenant-escape
  namespace: %s
data:
  originTenant: %q
  breach: "attempted"
`, forbiddenNamespace, tenantID)
	return base
}

// DefaultBlockedRemoteBaseURL is an RFC 2606 .invalid HTTPS remote-base URL outside
// every Workflow Actor's synthesized EgressPolicy (guaranteed NXDOMAIN / 403).
const DefaultBlockedRemoteBaseURL = "https://unapproved-base.egress-blocked.invalid/remote-base.yaml"

// UnauthorizedEgressManifests returns a Kustomize overlay referencing an unapproved
// external HTTPS remote base outside the Workflow Actor's EgressPolicy.
func UnauthorizedEgressManifests(tenantID, targetNamespace, remoteBaseURL string) map[string]string {
	if targetNamespace == "" {
		targetNamespace = tenantID
	}
	remote := strings.TrimSpace(remoteBaseURL)
	if remote == "" {
		remote = DefaultBlockedRemoteBaseURL
	}
	base := DefaultTenantManifests(tenantID, targetNamespace)
	base["kustomization.yaml"] = fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
labels:
  - pairs:
      app.kubernetes.io/managed-by: flux-substrate
      reconciler.ate.dev/tenant: %s
resources:
  - configmap.yaml
  - service.yaml
  - deployment.yaml
  - %s
`, tenantID, remote)
	return base
}
