# Flux on Agent Substrate

Multi-tenant GitOps execution model where each isolated tenant environment reconciles inside its own turn-based Agent Substrate actor rather than a shared long-lived controller pod or ephemeral cold-start job.

## Language

**Workflow Actor**:
A single-tenant Agent Substrate actor that owns the end-to-end GitOps reconciliation of one Tenant Environment, waking for active turns and sleeping in object storage when idle.
_Avoid_: Controller pod, runner job, Flux pod, shared reconciler

**Reconcile Turn**:
One bounded activation of a Workflow Actor in which it fetches source changes, renders manifests, applies desired state, and returns to a suspended state.
_Avoid_: Watch loop, background daemon, continuous reconciliation

**Dispatcher**:
The cluster-resident coordinator that watches Flux custom resources, receives webhooks, tracks reconciliation schedules, and drives Workflow Actor wake and suspend transitions.
_Avoid_: Flux controller, actor scheduler, gateway controller

**Identity Bridge**:
The in-cluster Kubernetes API reverse proxy that verifies a Workflow Actor's cryptographic `actorJwt` against Substrate's OIDC discovery endpoint and forwards requests to `kube-apiserver` under that actor's impersonated Kubernetes user identity.
_Avoid_: Kubeconfig secret, token vault, admin proxy

**Tenant Environment**:
An isolated target namespace, Git repository, and Kubernetes RBAC boundary reconciled exclusively by its corresponding Workflow Actor.
_Avoid_: Workspace, account, shared cluster target

**Commit Wave**:
A simultaneous burst of Git commits and webhook notifications across all Tenant Environments used to exercise `WorkerPool` multiplexing under concurrency limits.
_Avoid_: Load test, synthetic benchmark

**Poison Pill**:
A commit pushed to a single tenant's repository designed to trigger sandbox memory exhaustion, cross-namespace RBAC denial, or disallowed network egress to verify blast-radius containment.
_Avoid_: Mock failure, chaos script, simulated error
