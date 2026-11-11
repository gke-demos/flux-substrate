# AGENTS.md

Context for AI coding agents working in the `gke-demos/flux-substrate` repository — **Multi-Tenant FluxCD on Agent Substrate**, demonstrating turn-based `Workflow Actor`s as the unit of GitOps operations on Kubernetes and GKE.

## Project overview

`flux-substrate` runs end-to-end FluxCD reconciliation (`fluxcd/pkg/git`, `krusty` Kustomize, `fluxcd/pkg/ssa` Server-Side Apply, and `kstatus`) inside isolated **Agent Substrate `v1.0.0`** actors (`gVisor` / `microVM`), multiplexing **24 isolated `Tenant Environment`s** onto a **3-pod `WorkerPool`** (`9` concurrent slots):

1. **1 Workflow Actor = 1 Tenant Environment:** Each tenant's `GitRepository` + `Kustomization` pipeline runs inside its own single-tenant sandbox with a warm shallow `.git` checkout persisted in a `DurableDir` volume (`/var/lib/flux`) under `SNAPSHOT_FIDELITY_VOLUMES`, sleeping in object storage at zero compute cost between `Reconcile Turn`s.
2. **Zero-Secret Sandbox Credentials:** Actors hold zero Git tokens or kubeconfigs in memory, env vars, or snapshots. Outbound Git HTTPS traffic authenticates via `atenet-egress` `replaceHeaders` (`ate-secret://k8s.io/...`), and outbound Kubernetes API calls inject a short-lived `actorJwt` verified by the in-cluster `Identity Bridge` (`Impersonate-User: ate:actor:<atespace>:<actor>`) against per-namespace `RoleBinding`s.
3. **Blast-Radius & Multiplexing Mission Control (`/` and `/about`):** Interactive UI with SVG iconography and a zero-layout-shift Moon/Sun Projector Light Mode toggle driving Single-Tenant Commits, 24-Tenant `Commit Wave`s, and three `Poison Pill` proofs (Kustomize memory-bomb OOM + `RevertActor`, cross-namespace RBAC `403`, and unauthorized remote-base egress `403`), alongside an animated `/about` architecture topology view.

## Conventions

- **Cluster portability:** Never hardcode personal GKE cluster, project, or node names in scripts, manifests, Makefiles, or docs. Default scripts to `kubectl config current-context` and support both GKE and `kind`.
- **UI standards:** Use path-based routing (`/` and `/about`), SVG icons (zero emojis in UI), and a top-right icon-only segmented Projector Light Mode toggle (Moon/Sun) with pure CSS token overrides (zero layout shift). Keep `/about` synchronized whenever system behavior changes.

## Agent skills

### Issue tracker

Issues and specs for this repo live in GitHub Issues (`gke-demos/flux-substrate`) managed via the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the five canonical triage labels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout (`GLOSSARY.md` at repo root and ADRs in `docs/adr/`). See `docs/agents/domain.md`.
