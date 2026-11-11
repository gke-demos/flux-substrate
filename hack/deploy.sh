#!/usr/bin/env bash
set -euo pipefail

# Portable deployment script for FluxCD on Agent Substrate.
# Uses the active kubectl context by default (override via KUBE_CONTEXT).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

KUBE_CONTEXT="${KUBE_CONTEXT:-$(kubectl config current-context)}"
if [[ -z "${KO_DOCKER_REPO:-}" ]]; then
  GCP_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
  if [[ -n "${GCP_PROJECT}" && "${GCP_PROJECT}" != "(unset)" ]]; then
    KO_DOCKER_REPO="gcr.io/${GCP_PROJECT}/flux-substrate"
  else
    KO_DOCKER_REPO="ko.local"
  fi
fi
KO_PLATFORM="${KO_PLATFORM:-linux/amd64}"

echo "==> Deploying FluxCD on Agent Substrate to context: ${KUBE_CONTEXT} (KO_DOCKER_REPO=${KO_DOCKER_REPO})"

kubectl --context="${KUBE_CONTEXT}" apply -f manifests/crds/source.toolkit.fluxcd.io_gitrepositories.yaml
kubectl --context="${KUBE_CONTEXT}" apply -f manifests/crds/kustomize.toolkit.fluxcd.io_kustomizations.yaml
kubectl --context="${KUBE_CONTEXT}" apply -f manifests/base/namespace.yaml

# Resolve the cluster's digest-pinned ateworker-gvisor image if an existing WorkerPool is present.
if [[ -z "${SUBSTRATE_WORKER_IMAGE:-}" ]]; then
  SUBSTRATE_WORKER_IMAGE="$(kubectl --context="${KUBE_CONTEXT}" get workerpools -A -o jsonpath='{.items[0].spec.workerImage}' 2>/dev/null || true)"
fi
SUBSTRATE_WORKER_IMAGE="${SUBSTRATE_WORKER_IMAGE:-ghcr.io/agent-substrate/substrate/ateworker-gvisor:v1.0.0}"

python3 - "${SUBSTRATE_WORKER_IMAGE}" << 'EOF' | kubectl --context="${KUBE_CONTEXT}" apply -f -
import sys
worker_img = sys.argv[1]
with open("manifests/base/workerpool.yaml", "r", encoding="utf-8") as fh:
    raw = fh.read()
print(raw.replace("ghcr.io/agent-substrate/substrate/ateworker-gvisor:v1.0.0", worker_img))
EOF

# Authorize atespace flux-substrate to resolve Kubernetes Secrets in namespace flux-substrate and demo-repo
# via k8s-credential-provider if installed in ate-system, preserving all other atespaces.
if kubectl --context="${KUBE_CONTEXT}" -n ate-system get configmap k8s-credential-provider-namespace-policy >/dev/null 2>&1; then
  EXISTING_POLICY="$(kubectl --context="${KUBE_CONTEXT}" -n ate-system get configmap k8s-credential-provider-namespace-policy -o jsonpath='{.data.namespace-policy\.yaml}' 2>/dev/null || true)"
  if [[ "${EXISTING_POLICY}" != *"flux-substrate"* ]]; then
    echo "==> Appending atespace flux-substrate to k8s-credential-provider-namespace-policy..."
    UPDATED_POLICY="$(python3 - "${EXISTING_POLICY}" << 'EOF'
import sys
raw = sys.argv[1].rstrip()
if not raw:
    raw = "policies:"
entry = "\n- atespace: flux-substrate\n  allowedNamespaces:\n  - flux-substrate\n  - demo-repo\n"
print(raw + entry)
EOF
)"
    kubectl --context="${KUBE_CONTEXT}" -n ate-system create configmap k8s-credential-provider-namespace-policy \
      --from-literal=namespace-policy.yaml="${UPDATED_POLICY}" \
      --dry-run=client -o yaml | kubectl --context="${KUBE_CONTEXT}" apply -f -
    kubectl --context="${KUBE_CONTEXT}" -n ate-system rollout restart deployment/k8s-credential-provider
    kubectl --context="${KUBE_CONTEXT}" -n ate-system rollout status deployment/k8s-credential-provider --timeout=120s
  fi
fi

# Ensure shared git-oci-auth Secret exists in flux-substrate if not already created.
if ! kubectl --context="${KUBE_CONTEXT}" -n flux-substrate get secret git-oci-auth >/dev/null 2>&1; then
  DEMO_TOKEN="${DEMO_GIT_TOKEN:-substrate-multici-demo-token-2026}"
  kubectl --context="${KUBE_CONTEXT}" -n flux-substrate create secret generic git-oci-auth \
    --from-literal=username=substrate \
    --from-literal=password="${DEMO_TOKEN}" \
    --from-literal=token="${DEMO_TOKEN}"
fi

# Ensure Atespace flux-substrate and its AccessPolicy exist in Agent Substrate.
if command -v kubectl-ate >/dev/null 2>&1; then
  echo "==> Ensuring Agent Substrate Atespace flux-substrate and AccessPolicy..."
  kubectl ate --context="${KUBE_CONTEXT}" create atespace flux-substrate 2>/dev/null || true
  if ! kubectl ate --context="${KUBE_CONTEXT}" get access-policy -a flux-substrate >/dev/null 2>&1; then
    kubectl ate --context="${KUBE_CONTEXT}" create access-policy -a flux-substrate -f manifests/base/atespace-access-policy.yaml
  fi
fi

echo "==> Building and publishing digest-pinned flux-substrate image via ko (${KO_PLATFORM})..."
ACTOR_IMAGE="$(KO_DOCKER_REPO="${KO_DOCKER_REPO}" ko build --platform="${KO_PLATFORM}" ./cmd/flux-substrate)"
echo "==> Built image: ${ACTOR_IMAGE}"

if command -v kubectl-ate >/dev/null 2>&1; then
  EXISTING_TPL_IMG="$(kubectl ate --context="${KUBE_CONTEXT}" get actor-template flux-reconciler-v1 -a flux-substrate -o json 2>/dev/null | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("containers",[{}])[0].get("image",""))' 2>/dev/null || true)"
  if [[ -n "${EXISTING_TPL_IMG}" && "${EXISTING_TPL_IMG}" != "${ACTOR_IMAGE}" ]]; then
    echo "==> ActorTemplate image changed; rolling Workflow Actors in atespace flux-substrate to new image..."
    for i in $(seq -w 1 24); do
      kubectl ate --context="${KUBE_CONTEXT}" delete actor "tenant-${i}" -a flux-substrate 2>/dev/null || true
    done
    kubectl ate --context="${KUBE_CONTEXT}" delete actor-template flux-reconciler-v1 -a flux-substrate 2>/dev/null || true
    ROTATED_TEMPLATE="true"
  fi
fi

for manifest in manifests/base/git-server.yaml manifests/base/oidc-bridge.yaml manifests/base/dispatcher.yaml; do
  python3 - "${manifest}" "${ACTOR_IMAGE}" << 'EOF' | kubectl --context="${KUBE_CONTEXT}" apply -f -
import sys
manifest_path, image_ref = sys.argv[1], sys.argv[2]
with open(manifest_path, "r", encoding="utf-8") as fh:
    raw = fh.read()
print(raw.replace("ko://github.com/gke-demos/flux-substrate/cmd/flux-substrate", image_ref))
EOF
done

if [[ "${ROTATED_TEMPLATE:-false}" == "true" ]]; then
  kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout restart deployment/dispatcher
fi

echo "==> Waiting for WorkerPool, git-server, kube-bridge, and dispatcher rollouts in namespace flux-substrate..."
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout status deployment/git-server --timeout=180s
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout status deployment/kube-bridge --timeout=180s
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout status deployment/dispatcher --timeout=180s
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate wait --for=condition=Ready pod -l flux-substrate.io/role=flux-reconciler --timeout=180s

echo "==> Deployment complete. Port-forward Mission Control with:"
echo "    kubectl --context=\"${KUBE_CONTEXT}\" -n flux-substrate port-forward svc/dispatcher 8080:8080"
