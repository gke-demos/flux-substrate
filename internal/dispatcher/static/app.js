(function () {
  "use strict";

  var selectedTenantId = "tenant-01";
  var csrfToken = "";
  var authRequired = false;
  var authenticated = true;
  var lastState = null;

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

  function showBanner(msg) {
    var banner = document.getElementById("status-banner");
    var text = document.getElementById("status-banner-text");
    if (!banner || !text) {
      return;
    }
    text.textContent = msg;
    banner.classList.remove("hidden");
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
        authRequired = Boolean(data.authRequired);
        authenticated = Boolean(data.authenticated);
        csrfToken = data.csrfToken || "";
        updateAuthUI();
      })
      .catch(function () {
        // Ignore transient network errors during background poll.
      });
  }

  function updateAuthUI() {
    var btn = document.getElementById("btn-auth-modal");
    var lbl = document.getElementById("auth-btn-label");
    if (!btn || !lbl) {
      return;
    }
    if (!authRequired) {
      btn.classList.add("hidden");
      return;
    }
    btn.classList.remove("hidden");
    if (authenticated) {
      lbl.textContent = "Presenter Unlocked";
      btn.className = "btn btn-ok";
    } else {
      lbl.textContent = "Unlock Stage Actions";
      btn.className = "btn btn-warn";
    }
  }

  function apiPost(url, payload) {
    var headers = { "Content-Type": "application/json" };
    if (csrfToken) {
      headers["X-CSRF-Token"] = csrfToken;
    }
    return fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: headers,
      body: payload ? JSON.stringify(payload) : "{}"
    }).then(function (resp) {
      return resp.json().then(function (data) {
        if (resp.status === 401 || resp.status === 403) {
          var authModal = document.getElementById("auth-modal");
          if (authModal) {
            authModal.classList.remove("hidden");
          }
          throw new Error(data.error || "Presenter authentication required");
        }
        if (!resp.ok) {
          throw new Error(data.error || "Request failed with HTTP " + resp.status);
        }
        return data;
      });
    });
  }

  function renderFleetState(state) {
    lastState = state;
    var s = state.summary || {};
    setText("runtime-mode-pill", "mode: " + (state.mode || "local") + " · atespace: " + (state.atespace || "flux-substrate"));
    setText("kpi-ready-tenants", (s.readyTenants || 0) + " / " + (s.totalTenants || 0) + " Ready");
    setText("kpi-actors-running", "RUNNING: " + (s.activeActors || 0));
    setText("kpi-actors-suspended", "SUSPENDED: " + (s.suspendedActors || 0));
    setText("kpi-actors-crashed", "CRASHED: " + (s.crashedActors || 0));

    setText("kpi-active-slots", (s.activeSlots || 0) + " / " + (s.workerCapacity || 9) + " Active Slots");
    setText("kpi-queued-turns", "FIFO Queue: " + (s.queuedTurns || 0));
    setText("kpi-peak-slots", "Peak Concurrency: " + (s.peakActiveSlots || 0) + " / " + (s.workerCapacity || 9));

    setText("kpi-median-turn", (s.medianTurnMs || 0) + " ms");
    setText(
      "kpi-phase-breakdown",
      "Fetch " + (s.medianFetchMs || 0) + "ms · Render " + (s.medianRenderMs || 0) + "ms · Apply " + (s.medianApplyMs || 0) + "ms"
    );

    setText("kpi-snapshots", (s.snapshotCount || 0) + " Snapshots");
    setText("kpi-total-turns", "Total Turns: " + (s.totalTurns || 0));

    setText("workerpool-summary", (s.activeSlots || 0) + " / " + (s.workerCapacity || 9) + " slots active");
    renderWorkerPool(state.workerPool || {});
    renderTenantSelector(state.tenants || []);
    renderTenantsGrid(state.tenants || []);
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
      title.textContent = "Worker Pod " + w.workerId + " (gVisor)";
      var count = document.createElement("span");
      count.textContent = (w.active || 0) + " / " + (w.capacity || 3) + " busy";
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
        sid.textContent = sl.slotId;
        var st = document.createElement("div");
        st.className = "slot-tenant";
        st.textContent = sl.busy && sl.tenantId ? sl.tenantId : "IDLE";
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
      emptySpan.textContent = "0 waiting — all Workflow Actors suspended or assigned to slots";
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
        pill.textContent = t.actorState === "SUSPENDED" && !t.ready ? "FAILED (403)" : t.actorState;
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
        cacheSpan.textContent = t.cacheHit ? "Warm .git delta" : "Cold clone";
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

  function renderInspector(d) {
    setText("inspector-title", "Tenant Inspector: " + d.tenantId);
    var pill = document.getElementById("inspector-state-pill");
    if (pill) {
      pill.className = statePillClass(d.actorState, d.ready);
      pill.textContent = d.actorState + (d.ready ? " · READY" : " · NOT READY");
    }

    var metaGrid = document.getElementById("inspector-meta-grid");
    if (metaGrid) {
      metaGrid.replaceChildren(
        makeKVItem("Workflow Actor", (d.atespace || "flux-substrate") + "/" + d.actorName),
        makeKVItem("Target Namespace", d.targetNamespace || d.tenantId),
        makeKVItem("WorkerPool Slot", (d.workerId || "W1") + " / " + (d.slotId || "W1-1")),
        makeKVItem("Warm DurableDir Cache", d.cacheHit ? "HIT (incremental fetch)" : "MISS (shallow clone)"),
        makeKVItem("Turns / Coalesced", (d.turnCount || 0) + " turns / " + (d.coalescedTriggers || 0) + " coalesced"),
        makeKVItem("Volume Snapshots", String(d.snapshotCount || 0)),
        makeKVItem("Applied Revision", d.lastAppliedRevision || "none"),
        makeKVItem("Status / Error", d.lastError || d.statusMessage || "Healthy")
      );
    }

    var timings = d.timings || {};
    var fetchMs = timings.fetchMs || 0;
    var renderMs = timings.renderMs || 0;
    var applyMs = timings.applyMs || 0;
    var maxPhase = Math.max(fetchMs + renderMs + applyMs, 1);

    var pFetch = document.getElementById("prog-fetch");
    var pRender = document.getElementById("prog-render");
    var pApply = document.getElementById("prog-apply");
    if (pFetch) {
      pFetch.value = Math.min(100, Math.round((fetchMs / maxPhase) * 100));
    }
    if (pRender) {
      pRender.value = Math.min(100, Math.round((renderMs / maxPhase) * 100));
    }
    if (pApply) {
      pApply.value = Math.min(100, Math.round((applyMs / maxPhase) * 100));
    }
    setText("val-fetch", fetchMs + " ms");
    setText("val-render", renderMs + " ms");
    setText("val-apply", applyMs + " ms");

    var egressGrid = document.getElementById("inspector-egress");
    if (egressGrid) {
      var ep = d.egressPolicy || {};
      egressGrid.replaceChildren(
        makeKVItem("Allowed Git Host", (ep.gitScheme || "https") + "://" + (ep.gitHost || "git-server")),
        makeKVItem("Git Credential URI", ep.gitCredentialUri || "ate-secret://k8s.io/default/flux-substrate/git-" + d.tenantId + "/token"),
        makeKVItem("Identity Bridge Aud", ep.kubeBridgeAudience || "https://kube-bridge.flux-substrate.svc"),
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
    setText("inspector-yaml", d.kustomizationYaml || "");
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
        if (lastState) {
          renderTenantsGrid(lastState.tenants || []);
        }
        fetchAndRenderInspector(selectedTenantId);
      });
    }

    var waveBtn = document.getElementById("btn-wave");
    if (waveBtn) {
      waveBtn.addEventListener("click", function () {
        showBanner("Dispatching 24-tenant Commit Wave across 9 WorkerPool slots (W1..W3)...");
        apiPost("/api/wave", { message: "Stage Commit Wave across 24 Tenant Environments" })
          .then(function (res) {
            showBanner("Commit Wave #" + res.waveId + " enqueued across " + res.tenantCount + " Workflow Actors.");
            pollAll();
          })
          .catch(function (err) {
            showBanner("Commit Wave error: " + err.message);
          });
      });
    }

    var singleBtn = document.getElementById("btn-single-commit");
    if (singleBtn) {
      singleBtn.addEventListener("click", function () {
        showBanner("Committing release promotion to " + selectedTenantId + "...");
        apiPost("/api/tenants/" + encodeURIComponent(selectedTenantId) + "/commit", {})
          .then(function (res) {
            showBanner("Committed revision " + (res.revision ? res.revision.sha.slice(0, 7) : "") + " on " + selectedTenantId + ".");
            pollAll();
          })
          .catch(function (err) {
            showBanner("Commit error: " + err.message);
          });
      });
    }

    var oomBtn = document.getElementById("btn-poison-oom");
    if (oomBtn) {
      oomBtn.addEventListener("click", function () {
        showBanner("Injecting Memory-Bomb Kustomize OOM Poison Pill into " + selectedTenantId + "...");
        apiPost("/api/tenants/" + encodeURIComponent(selectedTenantId) + "/poison/oom", { wait: true })
          .then(function (res) {
            showBanner(
              selectedTenantId + " transitioned to " + res.actorState + " (sandbox OOM contained; sibling tenants unaffected)."
            );
            pollAll();
          })
          .catch(function (err) {
            showBanner("OOM poison pill error: " + err.message);
          });
      });
    }

    var rbacBtn = document.getElementById("btn-poison-rbac");
    if (rbacBtn) {
      rbacBtn.addEventListener("click", function () {
        showBanner("Injecting Cross-Namespace RBAC Poison Pill into " + selectedTenantId + "...");
        apiPost("/api/tenants/" + encodeURIComponent(selectedTenantId) + "/poison/rbac", { wait: true })
          .then(function (res) {
            showBanner(
              "Identity Bridge rejected cross-namespace escape into " + res.forbiddenNamespace + " with 403 Forbidden."
            );
            pollAll();
          })
          .catch(function (err) {
            showBanner("RBAC poison pill error: " + err.message);
          });
      });
    }

    var egressBtn = document.getElementById("btn-poison-egress");
    if (egressBtn) {
      egressBtn.addEventListener("click", function () {
        showBanner("Injecting Unauthorized Remote-Base Egress Poison Pill into " + selectedTenantId + "...");
        apiPost("/api/tenants/" + encodeURIComponent(selectedTenantId) + "/poison/egress", { wait: true })
          .then(function () {
            showBanner("Per-actor EgressPolicy blocked unapproved remote Kustomize base with NXDOMAIN / 403 Forbidden.");
            pollAll();
          })
          .catch(function (err) {
            showBanner("Egress poison pill error: " + err.message);
          });
      });
    }

    var recoverBtn = document.getElementById("btn-recover");
    if (recoverBtn) {
      recoverBtn.addEventListener("click", function () {
        showBanner("Invoking RevertActor snapshot rollback and baseline recovery on " + selectedTenantId + "...");
        apiPost("/api/tenants/" + encodeURIComponent(selectedTenantId) + "/recover", { wait: true })
          .then(function () {
            showBanner(selectedTenantId + " DurableDir rolled back via RevertActor and reconciled to Ready=True.");
            pollAll();
          })
          .catch(function (err) {
            showBanner("RevertActor recovery error: " + err.message);
          });
      });
    }

    var resetBtn = document.getElementById("btn-reset");
    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        showBanner("Resetting all 24 Tenant Environments and rolling back crashed actors...");
        apiPost("/api/reset", {})
          .then(function (res) {
            showBanner("Fleet Reset triggered across " + res.tenantCount + " Tenant Environments.");
            pollAll();
          })
          .catch(function (err) {
            showBanner("Fleet Reset error: " + err.message);
          });
      });
    }

    var extModal = document.getElementById("external-modal");
    var openExtBtn = document.getElementById("btn-external-modal");
    var closeExtBtn = document.getElementById("btn-close-external-modal");
    var extForm = document.getElementById("external-repo-form");
    if (openExtBtn && extModal) {
      openExtBtn.addEventListener("click", function () {
        var tidInput = document.getElementById("ext-tenant-id");
        if (tidInput) {
          tidInput.value = selectedTenantId;
        }
        extModal.classList.remove("hidden");
      });
    }
    if (closeExtBtn && extModal) {
      closeExtBtn.addEventListener("click", function () {
        extModal.classList.add("hidden");
      });
    }
    if (extForm && extModal) {
      extForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        var tid = document.getElementById("ext-tenant-id").value;
        var repoUrl = document.getElementById("ext-repo-url").value;
        var branch = document.getElementById("ext-branch").value;
        var pathVal = document.getElementById("ext-path").value;
        var credUri = document.getElementById("ext-cred-uri").value;

        apiPost("/api/repos/external", {
          tenantId: tid,
          repoUrl: repoUrl,
          branch: branch,
          path: pathVal,
          credentialUri: credUri
        })
          .then(function (res) {
            extModal.classList.add("hidden");
            selectedTenantId = res.tenantId || tid;
            showBanner(
              "Synthesized EgressPolicy for " + res.repoUrl + " on " + selectedTenantId + " and enqueued Reconcile Turn."
            );
            pollAll();
          })
          .catch(function (err) {
            showBanner("External repo registration failed: " + err.message);
          });
      });
    }

    var authModal = document.getElementById("auth-modal");
    var openAuthBtn = document.getElementById("btn-auth-modal");
    var closeAuthBtn = document.getElementById("btn-close-auth-modal");
    var authForm = document.getElementById("auth-login-form");
    var logoutBtn = document.getElementById("btn-auth-logout");
    if (openAuthBtn && authModal) {
      openAuthBtn.addEventListener("click", function () {
        authModal.classList.remove("hidden");
      });
    }
    if (closeAuthBtn && authModal) {
      closeAuthBtn.addEventListener("click", function () {
        authModal.classList.add("hidden");
      });
    }
    if (authForm && authModal) {
      authForm.addEventListener("submit", function (ev) {
        ev.preventDefault();
        var tokInput = document.getElementById("auth-token-input");
        var tokenVal = tokInput ? tokInput.value : "";
        fetch("/api/auth/login", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token: tokenVal })
        })
          .then(function (resp) {
            return resp.json().then(function (data) {
              if (!resp.ok) {
                throw new Error(data.error || "Invalid presenter token");
              }
              return data;
            });
          })
          .then(function (data) {
            authenticated = Boolean(data.authenticated);
            csrfToken = data.csrfToken || "";
            if (tokInput) {
              tokInput.value = "";
            }
            authModal.classList.add("hidden");
            updateAuthUI();
            showBanner("Presenter session unlocked (HttpOnly SameSite=Strict cookie + CSRF token active).");
          })
          .catch(function (err) {
            showBanner("Presenter login failed: " + err.message);
          });
      });
    }
    if (logoutBtn && authModal) {
      logoutBtn.addEventListener("click", function () {
        apiPost("/api/auth/logout", {})
          .then(function () {
            authenticated = false;
            csrfToken = "";
            authModal.classList.add("hidden");
            updateAuthUI();
            showBanner("Presenter session logged out.");
          })
          .catch(function (err) {
            showBanner("Logout error: " + err.message);
          });
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
