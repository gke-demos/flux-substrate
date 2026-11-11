# Default Workflow Actors to SNAPSHOT_FIDELITY_VOLUMES with a DurableDir Git cache

Rendering Kustomize overlays allocates transient Go heap spikes for YAML ASTs while the only state a `Workflow Actor` needs across `Reconcile Turn`s is its shallow `.git` repository cache in `/var/lib/flux`. Using `SNAPSHOT_FIDELITY_VOLUMES` checkpoints only the `DurableDir` volume—avoiding tens of megabytes of transient heap uploads to object storage on every suspend, resetting pooled HTTP/2 sockets and clocks cleanly on every wakeup, and preserving the warm Git cache across `ActorTemplate` upgrades.
