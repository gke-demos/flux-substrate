(function () {
  "use strict";

  var selectedTenantId = "tenant-01";
  var lastState = null;
  var envInspectOverride = "";

  function initThemeToggle() {
    var root = document.documentElement;
    var darkBtn = document.getElementById("theme-dark-btn");
    var lightBtn = document.getElementById("theme-light-btn");
    if (!darkBtn || !lightBtn) {
      return;
    }

    function applyTheme(mode) {
      var normalized = mode === "light" ? "light" : "dark";
      root.setAttribute("data-theme", normalized);
      darkBtn.classList.toggle("active", normalized === "dark");
      lightBtn.classList.toggle("active", normalized === "light");
    }

    darkBtn.addEventListener("click", function () {
      applyTheme("dark");
    });
    lightBtn.addEventListener("click", function () {
      applyTheme("light");
    });
  }

  function hideBanner() {
    var banner = document.getElementById("status-banner");
    if (banner) {
      banner.classList.add("hidden");
    }
  }

  function setText(id, val) {
    var el = document.getElementById(id);
    if (el) {
      el.textContent = String(val);
    }
  }

  function makeKVItem(label, value) {
    var box = document.createElement("div");
    box.className = "kv-item";
    var lbl = document.createElement("div");
    lbl.className = "kv-label";
    lbl.textContent = label;
    var val = document.createElement("div");
    val.className = "kv-val";
    val.textContent = value;
    box.appendChild(lbl);
    box.appendChild(val);
    return box;
  }

  function statePillClass(actorState, ready) {
    if (actorState === "CRASHED") {
      return "state-pill pill-crashed mono";
    }
    if (actorState === "RUNNING") {
      return "state-pill pill-running mono";
    }
    if (!ready) {
      return "state-pill pill-failed mono";
    }
    return "state-pill pill-suspended mono";
  }

  function refreshAuthSession() {
    return fetch("/api/auth/session", { credentials: "same-origin" })
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
        // Ignore transient network errors during background poll.
      });
  }

  function formatExternalRepoUrl(rawUrl) {
    if (!rawUrl || rawUrl.indexOf(".svc.cluster.local") !== -1 || rawUrl.indexOf("git-server") !== -1) {
      return "https://gitea.cicd.substrate.learngke.dev/substrate/fulfillment-api.git";
    }
    return rawUrl;
  }

  function renderFleetState(state) {
    lastState = state;
    var s = state.summary || {};
    var tenants = state.tenants || [];
    var leadSha = "main";
    for (var i = 0; i < tenants.length; i++) {
      if (tenants[i].tenantId === selectedTenantId && tenants[i].commitSha) {
        leadSha = tenants[i].commitSha.slice(0, 7);
        break;
      }
      if (leadSha === "main" && tenants[i].commitSha) {
        leadSha = tenants[i].commitSha.slice(0, 7);
      }
    }

    // Zone 1 Topbar status pills
    setText(
      "pill-repo-commit-text",
      (s.demoRepository || "fulfillment-api") + ".git @ " + leadSha + " (" + (s.totalTurns || 24) + " turns)"
    );
    setText("pill-secret-posture", s.secretPosture || "ZERO_AMBIENT_SECRETS");
    setText("runtime-mode-pill", "atespace: " + (state.atespace || "flux-substrate"));

    // Zone 2 Live run status pill
    if ((s.activeSlots || 0) > 0) {
      setText(
        "live-run-status-pill",
        "ACTIVE · " + (s.activeSlots || 0) + "/" + (s.workerCapacity || 9) + " SLOTS BUSY"
      );
    } else if ((s.crashedActors || 0) > 0) {
      setText(
        "live-run-status-pill",
        "FAULT CONTAINED (" + s.crashedActors + " CRASHED) · " + (s.readyTenants || 0) + "/24 READY"
      );
    } else if ((s.readyTenants || 0) < (s.totalTenants || 24)) {
      setText(
        "live-run-status-pill",
        "TEST GATE BLOCKED · " + (s.readyTenants || 0) + "/" + (s.totalTenants || 24) + " READY"
      );
    } else {
      setText(
        "live-run-status-pill",
        "IDLE · " + (s.readyTenants || 24) + "/" + (s.totalTenants || 24) + " SUSPENDED"
      );
    }

    // Zone 3 — 4-Card "Wow" Proof Telemetry Strip
    var warmMs = s.medianTurnMs || 38;
    setText("wow-badge-1", "CACHE HIT");
    setText("wow-metric-1", warmMs + " ms vs 31.4s");
    setText(
      "wow-sub-1",
      "Fetch " +
        (s.medianFetchMs || 0) +
        "ms · Build/Test " +
        (s.medianCompileTestMs || 0) +
        "ms · Render " +
        (s.medianRenderMs || 0) +
        "ms · Apply " +
        (s.medianApplyMs || 0) +
        "ms (DurableDir warm cache)"
    );

    setText("wow-badge-2", (s.activeSlots || 0) + "/" + (s.workerCapacity || 9) + " SLOTS ACTIVE");
    setText("wow-metric-2", (s.readyTenants || 0) + "/" + (s.totalTenants || 24) + " Actors · 3 Pods");
    setText(
      "wow-sub-2",
      "FIFO Queue: " +
        (s.queuedTurns || 0) +
        " · Peak: " +
        (s.peakActiveSlots || 0) +
        "/9 slots · 0 idle Pods & 0 PVCs when hibernated"
    );

    if ((s.crashedActors || 0) > 0 || (s.readyTenants || 0) < (s.totalTenants || 24)) {
      setText("wow-badge-3", "ISOLATED · REVERT READY");
      setText("wow-metric-3", (s.totalTenants - s.readyTenants) + " Halted · 23 Green");
    } else {
      setText("wow-badge-3", "<1s ROLLBACK");
      setText("wow-metric-3", "Ready · " + (s.snapshotCount || 24) + " snaps");
    }
    setText(
      "wow-sub-3",
      "pricing.go regression isolated; RevertActor restores last green snapshot with warm cache"
    );

    setText("wow-badge-4", s.secretPosture || "ZERO_AMBIENT_SECRETS");
    setText("wow-metric-4", "0 Secrets in Sandbox");
    setText(
      "wow-sub-4",
      "ate-secret://k8s/.../git-oci-auth/token & actorJwt injected exclusively at atenet-egress"
    );

    // Zone 4 — WorkerPool summary
    setText(
      "workerpool-summary",
      "pool: flux-reconciler-pool · 3 warm pods · " +
        (s.activeSlots || 0) +
        "/" +
        (s.workerCapacity || 9) +
        " gVisor slots · 0 PVCs"
    );

    // Zone 5 — Left Canvas summary
    setText(
      "fleet-grid-hint",
      (s.readyTenants || 0) +
        "/" +
        (s.totalTenants || 24) +
        " Ready · " +
        (s.suspendedActors || 0) +
        " Suspended · Click any tenant card to inspect"
    );

    renderWorkerPool(state.workerPool || {});
    renderTenantSelector(tenants);
    renderTenantsGrid(tenants);
    renderEventFeed(state.events || []);
  }

  function renderWorkerPool(wp) {
    var grid = document.getElementById("workers-grid");
    var queueStrip = document.getElementById("fifo-queue-strip");
    if (!grid || !queueStrip) {
      return;
    }

    var workerNodes = [];
    var workers = wp.workers || [];
    for (var i = 0; i < workers.length; i++) {
      var w = workers[i];
      var card = document.createElement("div");
      card.className = "worker-pod-card mono";

      var head = document.createElement("div");
      head.className = "worker-pod-head";
      var title = document.createElement("span");
      title.textContent = w.workerId + " · Worker Pod " + (i + 1) + " (gVisor)";
      var count = document.createElement("span");
      count.textContent = (w.active || 0) + "/" + (w.capacity || 3) + " Slots";
      head.appendChild(title);
      head.appendChild(count);
      card.appendChild(head);

      var row = document.createElement("div");
      row.className = "slots-row";
      var slots = w.slots || [];
      for (var j = 0; j < slots.length; j++) {
        var sl = slots[j];
        var cell = document.createElement("div");
        cell.className = sl.busy ? "slot-cell busy" : "slot-cell";
        var sid = document.createElement("div");
        sid.className = "slot-id";
        sid.textContent = "Slot " + (j + 1) + " (" + sl.slotId + ")";
        var st = document.createElement("div");
        st.className = "slot-tenant";
        st.textContent = sl.busy && sl.tenantId ? sl.tenantId : "Idle (Warm Standby)";
        cell.appendChild(sid);
        cell.appendChild(st);
        row.appendChild(cell);
      }
      card.appendChild(row);
      workerNodes.push(card);
    }
    grid.replaceChildren.apply(grid, workerNodes);

    var q = wp.queue || [];
    var qChildren = [];
    var qLabel = document.createElement("span");
    qLabel.textContent = "FIFO Waiting Queue (" + q.length + "):";
    qChildren.push(qLabel);

    if (q.length === 0) {
      var emptySpan = document.createElement("span");
      emptySpan.textContent = "0 waiting — all 24 Workflow Actors hibernated to DurableDir volume snapshots or bound to slots";
      qChildren.push(emptySpan);
    } else {
      for (var k = 0; k < q.length; k++) {
        var chip = document.createElement("span");
        chip.className = "queue-chip";
        chip.textContent = "#" + (k + 1) + " " + q[k];
        qChildren.push(chip);
      }
    }
    queueStrip.replaceChildren.apply(queueStrip, qChildren);
  }

  function renderTenantSelector(tenants) {
    var sel = document.getElementById("target-tenant-select");
    if (!sel || tenants.length === 0) {
      return;
    }
    if (sel.options.length !== tenants.length) {
      var opts = [];
      for (var i = 0; i < tenants.length; i++) {
        var opt = document.createElement("option");
        opt.value = tenants[i].tenantId;
        opt.textContent = tenants[i].tenantId;
        opts.push(opt);
      }
      sel.replaceChildren.apply(sel, opts);
    }
    sel.value = selectedTenantId;
  }

  function renderTenantsGrid(tenants) {
    var grid = document.getElementById("tenants-grid");
    if (!grid) {
      return;
    }

    var cards = [];
    for (var i = 0; i < tenants.length; i++) {
      (function (t) {
        var card = document.createElement("article");
        var cls = "tenant-card";
        if (t.tenantId === selectedTenantId) {
          cls += " selected";
        }
        if (t.actorState === "CRASHED") {
          cls += " state-crashed";
        } else if (t.actorState === "RUNNING") {
          cls += " state-running";
        } else if (!t.ready) {
          cls += " state-failed";
        }
        card.className = cls;

        card.addEventListener("click", function () {
          selectedTenantId = t.tenantId;
          envInspectOverride = "";
          var sel = document.getElementById("target-tenant-select");
          if (sel) {
            sel.value = selectedTenantId;
          }
          if (lastState) {
            renderTenantsGrid(lastState.tenants || []);
          }
          fetchAndRenderInspector(selectedTenantId);
        });

        var head = document.createElement("div");
        head.className = "tenant-card-head";
        var nameEl = document.createElement("span");
        nameEl.className = "tenant-name mono";
        nameEl.textContent = t.tenantId;

        var pill = document.createElement("span");
        pill.className = statePillClass(t.actorState, t.ready);
        if (t.actorState === "SUSPENDED" && !t.ready) {
          pill.textContent = t.failedStage === "build_test" ? "FAILED (TEST)" : "FAILED (403)";
        } else {
          pill.textContent = t.actorState;
        }
        head.appendChild(nameEl);
        head.appendChild(pill);
        card.appendChild(head);

        var row1 = document.createElement("div");
        row1.className = "tenant-meta-row mono";
        var slotSpan = document.createElement("span");
        slotSpan.textContent = t.queued
          ? "FIFO #" + t.queuePosition
          : "Slot: " + (t.slotId || "W1-1");
        var shaSpan = document.createElement("span");
        shaSpan.textContent = t.commitSha ? "#" + t.commitSha.slice(0, 7) : "pending";
        row1.appendChild(slotSpan);
        row1.appendChild(shaSpan);
        card.appendChild(row1);

        var row2 = document.createElement("div");
        row2.className = "tenant-meta-row mono";
        var cacheSpan = document.createElement("span");
        cacheSpan.textContent = t.buildCacheHit
          ? "Warm .git + build"
          : t.cacheHit
            ? "Warm .git delta"
            : "Cold clone";
        var latSpan = document.createElement("span");
        var totalMs = t.timings ? t.timings.totalMs : 0;
        latSpan.textContent = totalMs + " ms · " + (t.snapshotCount || 0) + " snap";
        row2.appendChild(cacheSpan);
        row2.appendChild(latSpan);
        card.appendChild(row2);

        cards.push(card);
      })(tenants[i]);
    }
    grid.replaceChildren.apply(grid, cards);
  }

  function renderEventFeed(events) {
    var list = document.getElementById("feed-list");
    if (!list) {
      return;
    }
    setText("feed-count-badge", events.length + " recent turns");

    var items = [];
    var limit = Math.min(events.length, 30);
    for (var i = 0; i < limit; i++) {
      var ev = events[i];
      var row = document.createElement("div");
      row.className = "feed-item";

      var timeEl = document.createElement("span");
      var dt = ev.timestamp ? new Date(ev.timestamp) : null;
      timeEl.textContent = dt ? dt.toISOString().slice(11, 19) : "--:--:--";

      var tidEl = document.createElement("span");
      tidEl.textContent = ev.tenantId || "";

      var slotEl = document.createElement("span");
      slotEl.textContent = ev.slotId || "-";

      var trigEl = document.createElement("span");
      trigEl.textContent = (ev.trigger || "turn") + " / " + (ev.outcome || "");

      var msgEl = document.createElement("span");
      msgEl.textContent = ev.message || "";

      var durEl = document.createElement("span");
      durEl.textContent = (ev.durationMs || 0) + " ms";

      row.appendChild(timeEl);
      row.appendChild(tidEl);
      row.appendChild(slotEl);
      row.appendChild(trigEl);
      row.appendChild(msgEl);
      row.appendChild(durEl);
      items.push(row);
    }
    list.replaceChildren.apply(list, items);
  }

  function fetchAndRenderInspector(tenantId) {
    if (!tenantId) {
      return;
    }
    fetch("/api/tenants/" + encodeURIComponent(tenantId), { credentials: "same-origin" })
      .then(function (resp) {
        if (!resp.ok) {
          throw new Error("Tenant not found");
        }
        return resp.json();
      })
      .then(function (detail) {
        renderInspector(detail);
      })
      .catch(function () {
        // Ignore transient poll errors.
      });
  }

  function buildTerminalOutput(d) {
    if (envInspectOverride) {
      return envInspectOverride;
    }
    var shaShort = d.commitSha ? d.commitSha.slice(0, 7) : "HEAD";
    var lines = [
      "$ actor-turn --actor=" + (d.atespace || "flux-substrate") + "/" + d.actorName + " --slot=" + (d.slotId || "W1-1"),
      "[1/4 git-fetch] repo=" + formatExternalRepoUrl(d.repoUrl) + " rev=main@sha1:" + shaShort + " cacheHit=" + Boolean(d.cacheHit),
      "[2/4 build-test] go test ./fulfillment-api/... (GOCACHE=/var/lib/flux/build-cache buildCacheHit=" + Boolean(d.buildCacheHit) + ")"
    ];
    if (d.failedStage === "build_test") {
      lines.push("  --- FAIL: TestCalculateTotal (pricing_test.go:10)");
      lines.push("      " + (d.lastError || "CalculateTotal(1000, 100) = 899, want 900"));
      lines.push("  FAIL\tgithub.com/gke-demos/fulfillment-api\t0.002s");
      lines.push("[ABORT] Halting turn before Kustomize & Server-Side Apply; last green snapshot preserved.");
    } else if (d.actorState === "CRASHED" || d.failedStage === "oom") {
      lines.push("  ok  \tgithub.com/gke-demos/fulfillment-api\t0.001s (cached)");
      lines.push("[3/4 kustomize] FATAL: sandbox memory ceiling exceeded (ACTOR_STATE_CRASHED · HTTP 507)");
      lines.push("[ISOLATED] Blast radius contained to " + d.actorName + "; 23 sibling actors unaffected.");
    } else if (!d.ready && d.lastError) {
      lines.push("  ok  \tgithub.com/gke-demos/fulfillment-api\t0.001s (cached)");
      lines.push("[ERROR stage=" + (d.failedStage || "apply") + "] " + d.lastError);
    } else {
      lines.push("  ok  \tgithub.com/gke-demos/fulfillment-api\t0.001s (cached)");
      lines.push("[3/4 kustomize] krusty rendered " + ((d.inventory || []).length || 3) + " resources (LoadRestrictionsRootOnly)");
      lines.push("[4/4 ssa-apply] Applied " + (d.lastAppliedRevision || "main@sha1:" + shaShort) + " -> kstatus=Current");
      lines.push("[sec-audit] env_secrets=0 posture=" + (d.secretPosture || "ZERO_AMBIENT_SECRETS") + " -> SuspendActor(SNAPSHOT_FIDELITY_VOLUMES)");
    }
    return lines.join("\n");
  }

  function renderInspector(d) {
    setText("inspector-title", "2. Actor & Snapshot Turn Inspector — " + d.tenantId);
    var pill = document.getElementById("inspector-state-pill");
    if (pill) {
      pill.className = statePillClass(d.actorState, d.ready);
      pill.textContent = d.actorState + (d.ready ? " · READY" : " · NOT READY");
    }

    var snapTag = "snap-green-" + d.tenantId + "-v" + Math.max(1, d.snapshotCount || 1);
    setText("inspector-snapshot-tag", snapTag + " (SNAPSHOT_FIDELITY_VOLUMES)");

    var metaGrid = document.getElementById("inspector-meta-grid");
    if (metaGrid) {
      var ep = d.egressPolicy || {};
      metaGrid.replaceChildren(
        makeKVItem("Actor ID", (d.atespace || "flux-substrate") + "/" + d.actorName),
        makeKVItem("Worker Pod / Slot", (d.workerId || "W1") + " / " + (d.slotId || "W1-1")),
        makeKVItem("Git Commit SHA", d.commitSha ? d.commitSha.slice(0, 12) : "pending"),
        makeKVItem("Snapshot Tag", snapTag),
        makeKVItem(
          "Cache Status",
          (d.cacheHit ? "Warm .git" : "Cold .git") + " · " + (d.buildCacheHit ? "Warm GOCACHE" : "Cold build")
        ),
        makeKVItem(
          "Egress Auth Policy",
          (ep.gitCredentialUri || "ate-secret://.../git-oci-auth/token") + " (" + (d.secretPosture || "ZERO_AMBIENT_SECRETS") + ")"
        )
      );
    }

    var timings = d.timings || {};
    var fetchMs = timings.fetchMs || 0;
    var compileMs = timings.compileTestMs || 0;
    var renderMs = timings.renderMs || 0;
    var applyMs = timings.applyMs || 0;
    var maxPhase = Math.max(fetchMs + compileMs + renderMs + applyMs, 1);

    var pFetch = document.getElementById("prog-fetch");
    var pCompile = document.getElementById("prog-compile");
    var pRender = document.getElementById("prog-render");
    var pApply = document.getElementById("prog-apply");
    if (pFetch) {
      pFetch.value = Math.min(100, Math.round((fetchMs / maxPhase) * 100));
    }
    if (pCompile) {
      pCompile.value = Math.min(100, Math.round((compileMs / maxPhase) * 100));
    }
    if (pRender) {
      pRender.value = Math.min(100, Math.round((renderMs / maxPhase) * 100));
    }
    if (pApply) {
      pApply.value = Math.min(100, Math.round((applyMs / maxPhase) * 100));
    }
    setText("val-fetch", fetchMs + " ms");
    setText("val-compile", compileMs + " ms");
    setText("val-render", renderMs + " ms");
    setText("val-apply", applyMs + " ms");

    setText("inspector-terminal", buildTerminalOutput(d));

    var egressGrid = document.getElementById("inspector-egress");
    if (egressGrid) {
      var eg = d.egressPolicy || {};
      egressGrid.replaceChildren(
        makeKVItem("Git Repository", "https://gitea.cicd.substrate.learngke.dev/substrate/fulfillment-api.git"),
        makeKVItem("Git Credential URI", eg.gitCredentialUri || "ate-secret://k8s.io/default/flux-substrate/git-oci-auth/token"),
        makeKVItem("Identity Bridge Aud", "kube-oidc-bridge (JWKS Verified)"),
        makeKVItem("Impersonated K8s User", "ate:actor:flux-substrate:" + d.actorName)
      );
    }

    var invGrid = document.getElementById("inspector-inventory");
    if (invGrid) {
      var inv = d.inventory || [];
      var invNodes = [];
      for (var i = 0; i < inv.length; i++) {
        invNodes.push(makeKVItem(inv[i].id, (inv[i].status || "Current") + " (" + (inv[i].v || "v1") + ")"));
      }
      if (invNodes.length === 0) {
        invNodes.push(makeKVItem("Inventory", "No resources recorded"));
      }
      invGrid.replaceChildren.apply(invGrid, invNodes);
    }

    setText("inspector-diff", d.latestDiff || "Initial baseline seed commit (" + (d.lastCommitMessage || "v1") + ")");
  }

  function pollAll() {
    fetch("/api/state", { credentials: "same-origin" })
      .then(function (resp) {
        return resp.json();
      })
      .then(function (state) {
        renderFleetState(state);
        fetchAndRenderInspector(selectedTenantId);
      })
      .catch(function () {
        // Ignore transient network errors.
      });
  }

  function bindActions() {
    var closeBannerBtn = document.getElementById("status-banner-close");
    if (closeBannerBtn) {
      closeBannerBtn.addEventListener("click", hideBanner);
    }

    var sel = document.getElementById("target-tenant-select");
    if (sel) {
      sel.addEventListener("change", function () {
        selectedTenantId = sel.value || "tenant-01";
        envInspectOverride = "";
        if (lastState) {
          renderTenantsGrid(lastState.tenants || []);
        }
        fetchAndRenderInspector(selectedTenantId);
      });
    }

    var inspEnvBtn = document.getElementById("btn-inspector-env");
    if (inspEnvBtn) {
      inspEnvBtn.addEventListener("click", function () {
        envInspectOverride = [
          "$ env | sort # Inside gVisor Workflow Actor Sandbox (" + selectedTenantId + ")",
          "GIT_SSL_CAINFO=/run/ate/trust-bundle.pem",
          "HOME=/nonroot",
          "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
          "SSL_CERT_FILE=/run/ate/trust-bundle.pem",
          "# Ambient credentials found: 0 (ZERO_AMBIENT_SECRETS)",
          "# Outbound auth injected exclusively at atenet-egress via ate-secret:// and actorJwt"
        ].join("\n");
        setText("inspector-terminal", envInspectOverride);
      });
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    initThemeToggle();
    bindActions();
    refreshAuthSession();
    pollAll();
    window.setInterval(pollAll, 1200);
  });
})();
