#!/usr/bin/env bash
set -euo pipefail

# Portable deployment script for FluxCD on Agent Substrate.
# Uses the active kubectl context by default (override via KUBE_CONTEXT).
KUBE_CONTEXT="${KUBE_CONTEXT:-$(kubectl config current-context)}"
KO_DOCKER_REPO="${KO_DOCKER_REPO:-ko.local}"

echo "==> Deploying FluxCD on Agent Substrate to context: ${KUBE_CONTEXT} (KO_DOCKER_REPO=${KO_DOCKER_REPO})"

kubectl --context="${KUBE_CONTEXT}" apply -f manifests/crds/source.toolkit.fluxcd.io_gitrepositories.yaml
kubectl --context="${KUBE_CONTEXT}" apply -f manifests/crds/kustomize.toolkit.fluxcd.io_kustomizations.yaml
kubectl --context="${KUBE_CONTEXT}" apply -f manifests/base/namespace.yaml
kubectl --context="${KUBE_CONTEXT}" apply -f manifests/base/workerpool.yaml

KO_DOCKER_REPO="${KO_DOCKER_REPO}" ko resolve -f manifests/base/git-server.yaml \
  | kubectl --context="${KUBE_CONTEXT}" apply -f -
KO_DOCKER_REPO="${KO_DOCKER_REPO}" ko resolve -f manifests/base/oidc-bridge.yaml \
  | kubectl --context="${KUBE_CONTEXT}" apply -f -
KO_DOCKER_REPO="${KO_DOCKER_REPO}" ko resolve -f manifests/base/dispatcher.yaml \
  | kubectl --context="${KUBE_CONTEXT}" apply -f -

echo "==> Waiting for git-server, kube-bridge, and dispatcher rollouts in namespace flux-substrate..."
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout status deployment/git-server --timeout=120s
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout status deployment/kube-bridge --timeout=120s
kubectl --context="${KUBE_CONTEXT}" -n flux-substrate rollout status deployment/dispatcher --timeout=120s

echo "==> Deployment complete. Port-forward Mission Control with:"
echo "    kubectl --context=\"${KUBE_CONTEXT}\" -n flux-substrate port-forward svc/dispatcher 8080:8080"
