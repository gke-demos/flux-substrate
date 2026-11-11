# FluxCD on Agent Substrate (`flux-substrate`)

Multi-tenant FluxCD running on **Agent Substrate `v1.0.0`**, where each `Tenant Environment` (`tenant-01`..`tenant-24`) is reconciled by its own hardware-isolated `Workflow Actor` (`gVisor` / `microVM`) multiplexed across a 3-pod `WorkerPool` (`W1..W3`, `3 × 3 = 9` concurrent slots).

Instead of running always-on cluster-wide `source-controller` and `kustomize-controller` informers for every tenant, `flux-substrate` collapses the entire Flux GitOps pipeline into a **turn-based `Workflow Actor`** (`POST /reconcile`) backed by **`SNAPSHOT_FIDELITY_VOLUMES`** (`DurableDir` mounted at `/var/lib/flux`) and **zero in-sandbox secrets**.

---

## Core Architecture

1. **Turn-Based Flux Reconcile Engine ([ADR-0001](docs/adr/0001-collapse-flux-pipeline-into-turn-based-workflow-actor.md))**:
   - **Phase 1 — Incremental Git Fetch**: Uses `github.com/fluxcd/pkg/git/gogit` against a warm shallow `.git` checkout persisted in `DurableDir` (`/var/lib/flux/git`), fetching only delta packfiles on warm turns (`cacheHit: true`).
   - **Phase 2 — In-Process Kustomize Build**: Evaluates overlays using `sigs.k8s.io/kustomize/api/krusty` with `LoadRestrictionsRootOnly` and a strict 4 MiB render cap.
   - **Phase 3 — Server-Side Apply & Inventory Prune**: Applies manifests via `github.com/fluxcd/pkg/ssa` (`ResourceManager`, field manager `flux-substrate-actor`), computes `kstatus` readiness (`github.com/fluxcd/cli-utils/pkg/kstatus/status`), and prunes stale inventory objects.
   - **Real Flux CRDs**: Reconciles standard `source.toolkit.fluxcd.io/v1` `GitRepository` and `kustomize.toolkit.fluxcd.io/v1` `Kustomization` custom resources labeled `reconciler.ate.dev/managed-by=flux-substrate` with zero upstream Flux controller pods installed.

2. **Zero-Secret Sandbox Identity ([ADR-0002](docs/adr/0002-oidc-impersonation-bridge-for-actor-jwt-rbac.md))**:
   - **Kubernetes API Auth**: Outbound HTTPS requests from a `Workflow Actor` to the in-cluster `Identity Bridge` (`https://kube-bridge.flux-substrate.svc`) have an OIDC `actorJwt` (`sub: actor/<atespace>/<actor>`) injected outside the sandbox by `atenet-egress`. The `Identity Bridge` verifies the JWT against `ate-idp-server` JWKS, strips incoming `Authorization` and `Impersonate-*` headers, and reverse-proxies to `kubernetes.default.svc` as `Impersonate-User: ate:actor:<atespace>:<actor>`, bound strictly to a namespace-scoped `RoleBinding`.
   - **Git HTTPS Auth**: Outbound HTTPS requests to the built-in Smart HTTP `git-server` or external `https://github.com/<org>/<repo>.git` repositories have credentials injected by `atenet-egress` via `ate-secret://k8s.io/...` (`k8s-credential-provider`).

3. **Volume-Fidelity Snapshots & 1-Click `RevertActor` Recovery ([ADR-0003](docs/adr/0003-volume-fidelity-snapshots-for-gitops-actors.md))**:
   - Between turns, each tenant's `Workflow Actor` is suspended (`ControlClient.SuspendActor` with `SNAPSHOT_FIDELITY_VOLUMES`) so idle tenants consume **0 CPU and 0 RAM**.
   - If a faulty Kustomize overlay triggers a sandbox OOM (`ACTOR_STATE_CRASHED`), the blast radius is strictly contained to that single tenant's `Workflow Actor`. Calling `RevertActor` rolls `DurableDir` back to the last healthy turn snapshot and restores `Ready=True`.

---

## Quickstart

### 1. Standalone Local 24-Tenant Fleet (`--mode=local`)

Run the full 24-tenant Mission-Control dashboard, built-in Smart HTTP `git-server`, `Identity Bridge`, and 9-slot `WorkerPool` scheduler in a single process against any local Kubernetes API (or `envtest`):

```bash
go run ./cmd/flux-substrate --mode=local --addr=:8080 --kubeconfig="${HOME}/.kube/config"
```

Open:
- **Mission Control**: `http://localhost:8080/`
- **Interactive Architecture & Signal Simulator**: `http://localhost:8080/about`

### 2. Deploy to a `kind` or GKE Cluster with Agent Substrate `v1.0.0`

Deploy the Flux CRDs, 3-pod `WorkerPool`, `git-server`, `kube-bridge`, and `dispatcher` to your active `kubectl` context using `ko`:

```bash
./hack/deploy.sh
```

Port-forward the Mission-Control UI and run the end-to-end verification suite:

```bash
kubectl -n flux-substrate port-forward svc/dispatcher 8080:8080
./hack/verify-e2e.sh
```

---

## Live Stage Scenarios

From the Mission-Control UI (`/`) or HTTP API:

- **24-Tenant `Commit Wave`** (`POST /api/wave`): Pushes a concurrent release commit across all 24 tenant Git repositories and visualizes 9 active `WorkerPool` slots (`W1..W3`) draining the 15 queued turns in FIFO waves.
- **Poison Pill 1 — Memory-Bomb Kustomize OOM** (`POST /api/tenants/{id}/poison/oom`): Commits a high-amplification `configMapGenerator` overlay that trips the sandbox memory ceiling (`HTTP 507` -> `ACTOR_STATE_CRASHED`) while the other 23 tenants remain healthy.
- **Poison Pill 2 — Cross-Namespace RBAC Escape (`403`)** (`POST /api/tenants/{id}/poison/rbac`): Attempts to apply a resource into a sibling tenant's namespace; blocked by `Identity Bridge` `Impersonate-User` RBAC with `403 Forbidden`.
- **Poison Pill 3 — Unauthorized Remote-Base Egress (`403`)** (`POST /api/tenants/{id}/poison/egress`): References an unapproved external HTTPS Kustomize base; blocked by `LoadRestrictionsRootOnly` and per-actor `EgressPolicy`.
- **1-Click `RevertActor` Recovery** (`POST /api/tenants/{id}/recover`): Calls `ControlClient.RevertActor` to roll `DurableDir` back to the pre-crash volume snapshot, reverts the poison Git commit, and runs a warm recovery turn to restore `Ready=True`.
- **External GitHub `GitRepository`** (`POST /api/repos/external`): Dynamically points any tenant's Flux `GitRepository` CR at an external `https://github.com/<org>/<repo>.git` repository with `ate-secret://` egress header injection.

---

## Testing

Run the full race-enabled test suite (`envtest` + in-process gRPC `ControlServer` + real `go-git`, `krusty`, `fluxcd/pkg/ssa`, and `Identity Bridge` RBAC):

```bash
go test -race ./...
```
