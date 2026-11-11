(function () {
  "use strict";

  var COMPONENTS = {
    dispatcher: {
      title: "Dispatcher Coordinator & 9-Slot FIFO Multiplexer",
      adr: "ADR-0001 · ADR-0002",
      summary:
        "Watches Flux GitRepository and Kustomization CRDs labeled reconciler.ate.dev/managed-by=flux-substrate, receives Git push webhooks, synthesizes per-actor EgressPolicy rules, and multiplexes 24 Tenant Environments across 9 concurrent WorkerPool slots with strict FIFO fairness.",
      facts: [
        ["Managed CRDs", "source.toolkit.fluxcd.io/v1 + kustomize.toolkit.fluxcd.io/v1"],
        ["Multiplexing Ratio", "24 Workflow Actors -> 3 WorkerPool Pods (W1..W3, 9 concurrent slots)"],
        ["Trigger Coalescing", "inFlight > 0 coalesces burst webhooks into 1 follow-up turn before SuspendActor"],
        ["CRD Status Authority", "Sole writer of status.conditions, status.inventory, and lastAppliedRevision"]
      ]
    },
    router: {
      title: "atenet-router (Wake-on-Request Reverse Proxy)",
      adr: "ADR-0001",
      summary:
        "Routes POST /reconcile requests from the Dispatcher to the target Workflow Actor using the ate-target-actor header. Automatically wakes suspended actors on a free WorkerPool slot and verifies mTLS PodIdentity certificates.",
      facts: [
        ["Routing Header", "ate-target-actor: flux-gitops/tenant-NN"],
        ["Client Auth", "Dynamic mTLS reload from /var/run/secrets/podidentity.podcert.ate.dev/identity"],
        ["Readiness Gate", "Holds request until Workflow Actor WakeupProbe GET /readyz returns 200 OK"],
        ["Crash Propagation", "Returns HTTP 507 / ACTOR_STATE_CRASHED when a sandbox exceeds memory limits"]
      ]
    },
    actor: {
      title: "Workflow Actor (gVisor Sandbox Reconcile Engine)",
      adr: "ADR-0001",
      summary:
        "Collapses Flux source-controller, fulfillment-api build & test verification, and kustomize-controller into a single turn-based HTTP service (POST /reconcile) running inside an isolated gVisor sandbox with zero controller-runtime informers.",
      facts: [
        ["Phase 1: Fetch", "fluxcd/pkg/git/gogit incremental delta fetch against warm DurableDir .git"],
        ["Phase 2: Build & Test", "Warm /var/lib/flux/build-cache verification of fulfillment-api (pricing.go & pricing_test.go)"],
        ["Phase 3: Render & Apply", "kustomize/api/krusty + fluxcd/pkg/ssa ResourceManager Server-Side Apply + kstatus"],
        ["Cluster-Wide Informers", "0 (eliminates always-on Go informer caches across all 24 tenants)"]
      ]
    },
    durable: {
      title: "DurableDir Volume Snapshot (/var/lib/flux)",
      adr: "ADR-0003",
      summary:
        "Persists the tenant's warm shallow .git repository, fulfillment-api build cache, and last-applied inventory metadata across SuspendActor and ResumeActor cycles using SNAPSHOT_FIDELITY_VOLUMES.",
      facts: [
        ["Snapshot Fidelity", "SNAPSHOT_FIDELITY_VOLUMES (DurableDir mounted at /var/lib/flux)"],
        ["Warm Caches", "Persists shallow .git + /var/lib/flux/build-cache (cacheHit: true, buildCacheHit: true)"],
        ["Post-Turn Action", "Dispatcher calls ControlClient.SuspendActor as soon as inFlight reaches 0"],
        ["Crash Recovery", "ControlClient.RevertActor rolls DurableDir back to the last healthy turn snapshot in <1s"]
      ]
    },
    egress: {
      title: "atenet-egress (Zero-Secret Sandbox Egress Proxy)",
      adr: "ADR-0002",
      summary:
        "Intercepts all outbound traffic leaving the Workflow Actor sandbox, blocks unauthorized destinations (403 Forbidden), and injects short-lived actorJwt or ate-secret:// credentials outside the sandbox boundary.",
      facts: [
        ["In-Sandbox Secrets", "0 (no Kubernetes ServiceAccount tokens or Git PATs inside actor memory/disk)"],
        ["K8s API Header Injection", "Authorization: Bearer ${actorJwt} (aud: https://kube-bridge.flux-substrate.svc)"],
        ["Git HTTPS Header Injection", "Authorization: Basic ${ate-secret://k8s.io/...} via k8s-credential-provider"],
        ["Default Egress Posture", "Deny-all except the tenant's declared GitRepository host and Identity Bridge"]
      ]
    },
    git: {
      title: "Smart HTTP Git Server & External GitHub Sources",
      adr: "ADR-0001 · ADR-0002",
      summary:
        "Serves per-tenant Git repositories over standard Git Smart HTTP (info/refs and git-upload-pack) and supports live commits, 24-tenant Commit Waves, Poison Pill commits, and external GitHub HTTPS repositories.",
      facts: [
        ["Built-In Repositories", "/git/tenant-01.git .. /git/tenant-24.git (pure-Go go-git memory/disk backing)"],
        ["External HTTPS Support", "Any public or private https://github.com/<org>/<repo>.git via GitRepository CR"],
        ["Webhook Dispatch", "HMAC-SHA256 X-Hub-Signature-256 push event triggers immediate Dispatcher enqueue"],
        ["Poison Pill Commits", "Reversible Git commits for Kustomize OOM, Cross-Namespace RBAC, and Egress 403"]
      ]
    },
    bridge: {
      title: "Identity Bridge (kube-oidc-bridge -> kube-apiserver)",
      adr: "ADR-0002",
      summary:
        "Verifies the atenet-egress-injected actorJwt against ate-idp-server OIDC discovery and JWKS, strips spoofed headers, and reverse-proxies to kube-apiserver using tight per-tenant Kubernetes Impersonation.",
      facts: [
        ["JWT Verification", "RS256/ES256 JWKS verification + iss, aud, exp, and sub: actor/<atespace>/<actor>"],
        ["Privilege Spoof Protection", "Strips incoming Authorization and all Impersonate-* headers before proxying"],
        ["Impersonated Identity", "Impersonate-User: ate:actor:<atespace>:<actor>"],
        ["Tenant RBAC Boundary", "Namespace-scoped RoleBinding in tenant-NN blocks cross-namespace writes (403)"]
      ]
    }
  };

  var FLOWS = [
    {
      name: "Flow 1",
      steps: [
        {
          title: "Flow 1 · Step 1: Git Webhook or Flux CR Change Arrives at Dispatcher",
          adr: "ADR-0001",
          node: "dispatcher",
          edges: [],
          packet: { cx: 105, cy: 120, color: "#38bdf8" },
          desc:
            "A Git push webhook or Flux Kustomization spec update arrives for tenant-07. The Dispatcher checks the 9-slot WorkerPool matrix (W1..W3), claims a free slot (or queues in FIFO order during a 24-tenant Commit Wave), and patches Reconciling=True.",
          wire:
            "POST /api/webhooks/git (X-Flux-Tenant: tenant-07)\n-> Dispatcher claims WorkerPool slot W1-1 (activeSlots: 1/9, queueDepth: 0)"
        },
        {
          title: "Flow 1 · Step 2: Dispatcher Invokes Workflow Actor via atenet-router",
          adr: "ADR-0001",
          node: "router",
          edges: ["edge-dispatcher-router", "edge-router-actor"],
          packet: { cx: 385, cy: 120, color: "#38bdf8" },
          desc:
            "The Dispatcher sends POST /reconcile to atenet-router with header ate-target-actor: flux-gitops/tenant-07. atenet-router wakes the tenant-07 Workflow Actor on WorkerPool pod W1 from its DurableDir volume snapshot.",
          wire:
            "POST https://atenet-router.ate-system.svc/reconcile\nate-target-actor: flux-gitops/tenant-07\n{\"tenantId\":\"tenant-07\",\"repoUrl\":\"http://git-server/git/tenant-07.git\",\"targetNamespace\":\"tenant-07\"}"
        },
        {
          title: "Flow 1 · Step 3: Warm Incremental Git Fetch + In-Process Kustomize Build",
          adr: "ADR-0001 · ADR-0003",
          node: "actor",
          edges: ["edge-actor-durable", "edge-actor-egress", "edge-egress-git"],
          packet: { cx: 670, cy: 135, color: "#10b981" },
          desc:
            "Because /var/lib/flux/git already holds the warm shallow repository from the previous turn snapshot, fluxcd/pkg/git/gogit performs an incremental delta fetch (cacheHit: true) and krusty renders the Kustomize overlay in-process.",
          wire:
            "DurableDir /var/lib/flux/git -> cacheHit: true (incremental pack fetch: 4 ms)\nsigs.k8s.io/kustomize/api/krusty.MakeKustomizer(LoadRestrictionsRootOnly) -> 2 resources rendered (3 ms)"
        },
        {
          title: "Flow 1 · Step 4: Server-Side Apply + SuspendActor to 0 Watts",
          adr: "ADR-0001 · ADR-0003",
          node: "durable",
          edges: ["edge-actor-durable"],
          packet: { cx: 670, cy: 310, color: "#10b981" },
          desc:
            "fluxcd/pkg/ssa applies the manifests, prunes stale inventory objects, and returns the turn result. The Dispatcher patches Kustomization status.conditions[Ready]=True and calls SuspendActor so tenant-07 consumes 0 CPU/RAM until its next commit.",
          wire:
            "ControlClient.SuspendActor(atespace=\"flux-gitops\", name=\"tenant-07\", SNAPSHOT_FIDELITY_VOLUMES)\n-> Slot W1-1 released · Actor state: ACTOR_STATE_SUSPENDED (0 MiB idle RAM)"
        }
      ]
    },
    {
      name: "Flow 2",
      steps: [
        {
          title: "Flow 2 · Step 1: Workflow Actor Initiates Plain HTTPS Call to Identity Bridge",
          adr: "ADR-0002",
          node: "actor",
          edges: ["edge-actor-egress"],
          packet: { cx: 560, cy: 155, color: "#818cf8" },
          desc:
            "During Phase 3 (fluxcd/pkg/ssa Server-Side Apply), the Workflow Actor sends Kubernetes API requests to https://kube-bridge.flux-substrate.svc without holding any ServiceAccount token or secret inside the gVisor sandbox.",
          wire:
            "PATCH https://kube-bridge.flux-substrate.svc/apis/apps/v1/namespaces/tenant-07/deployments/whoami?fieldManager=flux-substrate-actor\n(No Authorization header inside sandbox)"
        },
        {
          title: "Flow 2 · Step 2: atenet-egress Injects Cryptographic actorJwt",
          adr: "ADR-0002",
          node: "egress",
          edges: ["edge-actor-egress", "edge-egress-bridge"],
          packet: { cx: 385, cy: 305, color: "#818cf8" },
          desc:
            "atenet-egress intercepts the outbound HTTPS connection, matches the actor's ActorEgressPolicy rule for kube-bridge.flux-substrate.svc, and injects a short-lived OIDC actorJwt signed by ate-idp-server.",
          wire:
            "replaceHeaders:\n  Authorization: \"Bearer <actorJwt>\"\nClaims: { \"iss\": \"https://ate-idp-server.ate-system.svc\", \"sub\": \"actor/flux-gitops/tenant-07\", \"aud\": \"https://kube-bridge.flux-substrate.svc\" }"
        },
        {
          title: "Flow 2 · Step 3: Identity Bridge Verifies JWKS & Enforces Per-Tenant RBAC",
          adr: "ADR-0002",
          node: "bridge",
          edges: ["edge-egress-bridge"],
          packet: { cx: 105, cy: 350, color: "#10b981" },
          desc:
            "The Identity Bridge verifies the JWT against ate-idp-server's JWKS endpoint, strips Authorization and any spoofed Impersonate-* headers, and forwards the request to kube-apiserver with Impersonate-User: ate:actor:flux-gitops:tenant-07.",
          wire:
            "Identity Bridge -> kube-apiserver:\n  Impersonate-User: ate:actor:flux-gitops:tenant-07\nRBAC RoleBinding in namespace tenant-07 -> ALLOWED (cross-namespace tenant-02 -> 403 Forbidden)"
        }
      ]
    },
    {
      name: "Flow 3",
      steps: [
        {
          title: "Flow 3 · Step 1: Dispatcher Synthesizes EgressPolicy from GitRepository CR",
          adr: "ADR-0002",
          node: "dispatcher",
          edges: ["edge-dispatcher-router"],
          packet: { cx: 105, cy: 120, color: "#38bdf8" },
          desc:
            "When a tenant's GitRepository points to an external HTTPS repository (e.g. https://github.com/org/repo.git) and references a Secret or secret.ate.dev/credential-uri annotation, the Dispatcher programs an ActorEgressPolicy for that exact host.",
          wire:
            "GitRepository spec.url: https://github.com/gke-demos/podinfo.git\nAnnotation: secret.ate.dev/credential-uri: ate-secret://k8s.io/default/flux-system/github-pat/token"
        },
        {
          title: "Flow 3 · Step 2: atenet-egress Resolves ate-secret:// Outside the Sandbox",
          adr: "ADR-0002",
          node: "egress",
          edges: ["edge-actor-egress", "edge-egress-git"],
          packet: { cx: 385, cy: 290, color: "#10b981" },
          desc:
            "When fluxcd/pkg/git/gogit connects to github.com over HTTPS, atenet-egress resolves the ate-secret:// URI via k8s-credential-provider and injects the Authorization header on the wire. The raw secret never enters actor memory or DurableDir snapshots.",
          wire:
            "GET https://github.com/gke-demos/podinfo.git/info/refs?service=git-upload-pack\natenet-egress replaceHeaders -> Authorization: Basic <resolved-outside-sandbox>"
        },
        {
          title: "Flow 3 · Step 3: Unauthorized Remote Kustomize Bases Blocked at Egress",
          adr: "ADR-0002",
          node: "git",
          edges: ["edge-egress-git"],
          packet: { cx: 105, cy: 248, color: "#f59e0b" },
          desc:
            "If a malicious Kustomize commit attempts to fetch a remote base from an unapproved host (such as https://evil.example.com), both LoadRestrictionsRootOnly and atenet-egress deny the connection with HTTP 403 Forbidden.",
          wire:
            "Poison Pill (egress-violation): resources: [\"https://evil.example.com/malicious-base?ref=main\"]\n-> Blocked by krusty LoadRestrictionsRootOnly & atenet-egress host allowlist (HTTP 403)"
        }
      ]
    },
    {
      name: "Flow 4",
      steps: [
        {
          title: "Flow 4 · Step 1: Memory-Bomb Kustomize Commit Pushed to Single Tenant",
          adr: "ADR-0001 · ADR-0003",
          node: "git",
          edges: ["edge-egress-git"],
          packet: { cx: 105, cy: 248, color: "#ef4444" },
          desc:
            "The presenter triggers the Memory-Bomb Kustomize OOM Poison Pill on a single tenant (e.g. tenant-01), committing a Kustomize configMapGenerator overlay that expands past the sandbox's memory ceiling.",
          wire:
            "POST /api/tenants/tenant-01/poison/oom {\"wait\":true}\n-> Commits 16x280KiB configMapGenerator expansion (> 4 MiB sandbox memory budget)"
        },
        {
          title: "Flow 4 · Step 2: Sandbox Crashes (ACTOR_STATE_CRASHED); 23 Sibling Tenants Unaffected",
          adr: "ADR-0001 · ADR-0003",
          node: "actor",
          edges: ["edge-router-actor"],
          packet: { cx: 670, cy: 115, color: "#ef4444" },
          desc:
            "Inside a traditional shared kustomize-controller Pod, an OOMKill crashes the controller for every tenant. On Agent Substrate, only tenant-01's Workflow Actor enters ACTOR_STATE_CRASHED (HTTP 507); tenants 02..24 continue reconciling normally.",
          wire:
            "WorkerPool slot W1-1 catches sandbox OOM -> returns HTTP 507 Insufficient Storage\ntenant-01 state: ACTOR_STATE_CRASHED · Kustomization Ready=False (Reason: ActorCrashedOOM)"
        },
        {
          title: "Flow 4 · Step 3: 1-Click RevertActor Rolls Back DurableDir Snapshot",
          adr: "ADR-0003",
          node: "durable",
          edges: ["edge-actor-durable"],
          packet: { cx: 670, cy: 310, color: "#10b981" },
          desc:
            "Clicking RevertActor calls ControlClient.RevertActor to roll tenant-01's DurableDir (/var/lib/flux) back to the last healthy volume snapshot, reverts the poison Git commit, and runs a recovery turn to restore Ready=True.",
          wire:
            "POST /api/tenants/tenant-01/recover {\"wait\":true}\n-> ControlClient.RevertActor(flux-gitops/tenant-01) + warm recovery turn -> Ready=True"
        }
      ]
    }
  ];

  var currentFlowIdx = 0;
  var currentStepIdx = 0;
  var autoPlayTimer = null;

  function refreshAuthSession() {
    fetch("/api/auth/session", { credentials: "same-origin" })
      .then(function (resp) {
        return resp.json();
      })
      .then(function (data) {
        var pillEl = document.getElementById("sso-session-pill");
        var signoutEl = document.getElementById("sso-signout-link");
        var emailEl = document.getElementById("sso-user-email");
        if (emailEl && data && data.subject) {
          emailEl.textContent = data.subject + " · Google SSO";
        }
        if (pillEl) {
          pillEl.classList.remove("is-hidden");
        }
        if (signoutEl) {
          signoutEl.classList.remove("is-hidden");
        }
      })
      .catch(function () {
        // Ignore transient network errors.
      });
  }

  function initTheme() {
    var darkBtn = document.getElementById("theme-dark-btn");
    var lightBtn = document.getElementById("theme-light-btn");

    function applyTheme(theme) {
      document.documentElement.setAttribute("data-theme", theme);
      if (darkBtn && lightBtn) {
        darkBtn.classList.toggle("active", theme === "dark");
        lightBtn.classList.toggle("active", theme === "light");
      }
    }

    if (darkBtn) {
      darkBtn.addEventListener("click", function () {
        applyTheme("dark");
      });
    }
    if (lightBtn) {
      lightBtn.addEventListener("click", function () {
        applyTheme("light");
      });
    }
  }

  function selectComponent(nodeKey) {
    var info = COMPONENTS[nodeKey] || COMPONENTS.dispatcher;
    var nodes = document.querySelectorAll("#topology-svg .topo-node");
    nodes.forEach(function (node) {
      node.classList.toggle("active", node.getAttribute("data-node") === nodeKey);
    });

    var titleEl = document.getElementById("component-title");
    var adrEl = document.getElementById("component-adr-badge");
    var summaryEl = document.getElementById("component-summary");
    var factsEl = document.getElementById("component-facts");

    if (titleEl) {
      titleEl.textContent = info.title;
    }
    if (adrEl) {
      adrEl.textContent = info.adr;
    }
    if (summaryEl) {
      summaryEl.textContent = info.summary;
    }
    if (factsEl) {
      var rows = info.facts.map(function (pair) {
        var row = document.createElement("div");
        row.className = "kv-row";
        var k = document.createElement("span");
        k.className = "kv-key";
        k.textContent = pair[0];
        var v = document.createElement("span");
        v.className = "kv-val";
        v.textContent = pair[1];
        row.appendChild(k);
        row.appendChild(v);
        return row;
      });
      factsEl.replaceChildren.apply(factsEl, rows);
    }
  }

  function renderFlowStep() {
    var flow = FLOWS[currentFlowIdx];
    var step = flow.steps[currentStepIdx];

    for (var i = 0; i < FLOWS.length; i++) {
      var btn = document.getElementById("flow-btn-" + i);
      if (btn) {
        btn.classList.toggle("btn-primary", i === currentFlowIdx);
      }
    }

    var indicator = document.getElementById("flow-step-indicator");
    var titleEl = document.getElementById("flow-step-title");
    var adrEl = document.getElementById("flow-step-adr");
    var descEl = document.getElementById("flow-step-desc");
    var wireEl = document.getElementById("flow-step-wire");

    if (indicator) {
      indicator.textContent =
        flow.name + " · Step " + (currentStepIdx + 1) + " / " + flow.steps.length;
    }
    if (titleEl) {
      titleEl.textContent = step.title;
    }
    if (adrEl) {
      adrEl.textContent = step.adr;
    }
    if (descEl) {
      descEl.textContent = step.desc;
    }
    if (wireEl) {
      wireEl.textContent = step.wire;
    }

    var allEdges = document.querySelectorAll("#topology-svg .topo-edge");
    allEdges.forEach(function (edge) {
      var active = step.edges.indexOf(edge.id) !== -1;
      edge.classList.toggle("active", active);
    });

    var packet = document.getElementById("signal-packet");
    if (packet && step.packet) {
      packet.setAttribute("cx", String(step.packet.cx));
      packet.setAttribute("cy", String(step.packet.cy));
      packet.setAttribute("fill", step.packet.color);
    }

    selectComponent(step.node);
  }

  function setFlow(flowIdx) {
    currentFlowIdx = flowIdx;
    currentStepIdx = 0;
    renderFlowStep();
  }

  function stepNext() {
    var flow = FLOWS[currentFlowIdx];
    currentStepIdx = (currentStepIdx + 1) % flow.steps.length;
    renderFlowStep();
  }

  function stepPrev() {
    var flow = FLOWS[currentFlowIdx];
    currentStepIdx = (currentStepIdx - 1 + flow.steps.length) % flow.steps.length;
    renderFlowStep();
  }

  function toggleAutoPlay() {
    var playBtn = document.getElementById("step-play-btn");
    if (autoPlayTimer) {
      window.clearInterval(autoPlayTimer);
      autoPlayTimer = null;
      if (playBtn) {
        playBtn.textContent = "Auto-Play";
      }
      return;
    }
    autoPlayTimer = window.setInterval(stepNext, 2800);
    if (playBtn) {
      playBtn.textContent = "Pause";
    }
  }

  function bindControls() {
    for (var i = 0; i < FLOWS.length; i++) {
      (function (idx) {
        var btn = document.getElementById("flow-btn-" + idx);
        if (btn) {
          btn.addEventListener("click", function () {
            setFlow(idx);
          });
        }
      })(i);
    }

    var prevBtn = document.getElementById("step-prev-btn");
    if (prevBtn) {
      prevBtn.addEventListener("click", stepPrev);
    }
    var nextBtn = document.getElementById("step-next-btn");
    if (nextBtn) {
      nextBtn.addEventListener("click", stepNext);
    }
    var playBtn = document.getElementById("step-play-btn");
    if (playBtn) {
      playBtn.addEventListener("click", toggleAutoPlay);
    }

    var nodes = document.querySelectorAll("#topology-svg .topo-node");
    nodes.forEach(function (node) {
      node.addEventListener("click", function () {
        var key = node.getAttribute("data-node");
        if (key) {
          selectComponent(key);
        }
      });
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    refreshAuthSession();
    initTheme();
    bindControls();
    renderFlowStep();
  });
})();
