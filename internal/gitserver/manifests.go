package gitserver

import "fmt"

// DefaultTenantManifests returns the baseline Kustomize overlay files seeded for a
// Tenant Environment (ConfigMap, Service, Deployment, and kustomization.yaml).
func DefaultTenantManifests(tenantID, targetNamespace string) map[string]string {
	if targetNamespace == "" {
		targetNamespace = tenantID
	}
	return map[string]string{
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
