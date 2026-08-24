/* Admin AI-models page: provider registry CRUD + slot assignments.
   The panel's data attributes carry the endpoint URLs; the CSRF token comes
   from the <meta name="csrf-token"> tag injected by the dashboard shell.
   Keys are write-only: password inputs never carry a saved value, and this
   script never writes a key anywhere except into a save/probe request body.
   Provider rows and model rows are rebuilt with DOM APIs + textContent,
   never innerHTML with interpolated names. */
(function () {
  "use strict";

  var panel = document.getElementById("modelsPanel");
  if (!panel) return;

  var probeUrl = panel.dataset.probeUrl;
  var saveUrl = panel.dataset.saveUrl;
  var providerSaveUrl = panel.dataset.providerSaveUrl;
  var providerDeleteUrl = panel.dataset.providerDeleteUrl;
  var meta = document.querySelector('meta[name="csrf-token"]');
  var csrf = meta ? meta.getAttribute("content") : "";

  var providers = [];      // last-known provider list from the server
  var editingProviderId = null;

  var ROLE_LABELS = { text: "Text", multimodal: "Multimodal", embedding: "Embedding" };
  var STATE_LABELS = { online: "Online", offline: "Offline", error: "Error", checking: "Checking…" };

  function statusPill(state, title) {
    var wrap = document.createElement("span");
    wrap.className = "models-status models-status--" + (state || "checking");
    if (title) wrap.title = title;
    var dot = document.createElement("span");
    dot.className = "models-status__dot";
    dot.setAttribute("aria-hidden", "true");
    var label = document.createElement("span");
    label.textContent = STATE_LABELS[state] || state;
    wrap.appendChild(dot);
    wrap.appendChild(label);
    return wrap;
  }

  function setProviderState(pid, state, detail) {
    var cell = panel.querySelector('td[data-status-provider="' + pid + '"]');
    if (!cell) return;
    cell.textContent = "";
    cell.appendChild(statusPill(state, detail || ""));
  }

  function probeAllProviders() {
    providers.forEach(function (p) {
      post(probeUrl, { slot: "provider", provider_id: p.id }).then(function (res) {
        setProviderState(p.id, res.data.state || (res.data.ok ? "online" : "error"),
                         res.data.error || "");
      }).catch(function () {
        setProviderState(p.id, "error", "probe request failed");
      });
    });
  }
  var SLOT_ROLES = {
    textFlash: ["text", "multimodal"],
    textThink: ["text", "multimodal"],
    embedModel: ["embedding"],
    visionModel: ["multimodal"]
  };

  function id(name) { return document.getElementById(name); }
  function value(name) { return id(name) ? id(name).value.trim() : ""; }

  function post(url, body) {
    return fetch(url, {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json", "X-CSRFToken": csrf },
      body: JSON.stringify(body)
    }).then(function (res) {
      return res.json().catch(function () { return {}; }).then(function (data) {
        return { status: res.status, data: data };
      });
    });
  }

  function setResult(box, text, ok) {
    box.textContent = text;
    box.classList.remove("model-card__result--ok", "model-card__result--err");
    box.classList.add(ok ? "model-card__result--ok" : "model-card__result--err");
  }

  function showBanner(result) {
    var banner = id("modelsBanner");
    var file = panel.dataset.envFile || ".env";
    var parts = ["Applied — saved to " + file + "."];
    if (result.restarted) parts.push("Web workers are recycling.");
    else parts.push("Running in this process; restart the dev server to apply everywhere.");
    if (result.satellite_notice) parts.push("Publishing & attachment workers pick this up after their next restart.");
    banner.textContent = parts.join(" ");
    banner.classList.remove("d-none", "alert-danger");
    banner.classList.add("alert-success");
  }

  function showError(message) {
    var banner = id("modelsBanner");
    banner.textContent = message || "Save failed.";
    banner.classList.remove("d-none", "alert-success");
    banner.classList.add("alert-danger");
  }

  function syncMtimes(data) {
    if (data.snap && data.snap.env_mtime) panel.dataset.envMtime = String(data.snap.env_mtime);
    if (data.json_mtime) panel.dataset.jsonMtime = String(data.json_mtime);
    else if (data.snap && data.snap.json_mtime) panel.dataset.jsonMtime = String(data.snap.json_mtime);
  }

  /* ── Providers table ─────────────────────────────────────────────────── */
  function renderProviders(list) {
    providers = list || [];
    var body = id("providersBody");
    body.textContent = "";
    if (!providers.length) {
      var row = body.insertRow();
      var cell = row.insertCell(0);
      cell.colSpan = 3;
      cell.className = "text-muted";
      cell.textContent = "No providers configured — add one to begin.";
      return;
    }
    providers.forEach(function (p) {
      var tr = body.insertRow();
      tr.dataset.providerId = p.id;

      var nameCell = tr.insertCell(0);
      var nameSpan = document.createElement("span");
      nameSpan.className = "td-title";
      nameSpan.textContent = p.name;
      nameCell.appendChild(nameSpan);

      var keyCell = tr.insertCell(1);
      keyCell.setAttribute("data-status-provider", p.id);
      keyCell.appendChild(statusPill("checking"));

      var actionsCell = tr.insertCell(2);
      actionsCell.className = "td-actions";
      var editBtn = document.createElement("button");
      editBtn.type = "button";
      editBtn.className = "kp-btn kp-btn--ghost kp-btn--sm";
      editBtn.textContent = "Edit";
      editBtn.setAttribute("data-edit-provider", p.id);
      editBtn.addEventListener("click", function () { openProviderModal(p.id); });
      var delBtn = document.createElement("button");
      delBtn.type = "button";
      delBtn.className = "kp-btn kp-btn--quiet kp-btn--sm";
      delBtn.textContent = "Delete";
      delBtn.setAttribute("data-delete-provider", p.id);
      delBtn.addEventListener("click", function () { deleteProvider(p); });
      actionsCell.appendChild(editBtn);
      actionsCell.appendChild(delBtn);
    });
    probeAllProviders();
  }

  function providerById(pid) {
    return providers.find(function (p) { return p.id === pid; }) || null;
  }

  /* ── Slot pickers: provider select + model select per role ───────────── */
  function fillProviderSelect(selectEl) {
    if (!selectEl) return;
    var previous = selectEl.dataset.selected || selectEl.value;
    selectEl.textContent = "";
    if (!providers.length) {
      var none = document.createElement("option");
      none.value = "";
      none.disabled = true;
      none.selected = true;
      none.textContent = "No providers yet — add one first";
      selectEl.appendChild(none);
      return;
    }
    providers.forEach(function (p) {
      var opt = document.createElement("option");
      opt.value = p.id;
      opt.textContent = p.name;
      if (p.id === previous) opt.selected = true;
      selectEl.appendChild(opt);
    });
  }

  function fillModelSelect(selectEl, providerId, allowedRoles) {
    if (!selectEl) return;
    var previous = selectEl.dataset.selected || selectEl.value;
    selectEl.textContent = "";
    var provider = providerById(providerId);
    var eligible = provider
      ? (provider.models || []).filter(function (m) { return allowedRoles.indexOf(m.role) >= 0; })
      : [];
    if (!eligible.length) {
      var none = document.createElement("option");
      none.value = "";
      none.disabled = true;
      none.selected = true;
      none.textContent = "No matching model on this provider";
      selectEl.appendChild(none);
      return;
    }
    eligible.forEach(function (m) {
      var opt = document.createElement("option");
      opt.value = m.id;
      opt.textContent = m.id;
      if (m.id === previous) opt.selected = true;
      selectEl.appendChild(opt);
    });
    delete selectEl.dataset.selected;
  }

  function visionModelProviderId() {
    return visionMode() === "dedicated" ? value("visionProviderSel") : value("textProviderSel");
  }

  function refreshModelPickers() {
    fillProviderSelect(id("textProviderSel"));
    fillProviderSelect(id("embedProviderSel"));
    fillProviderSelect(id("visionProviderSel"));
    fillModelSelect(id("textFlash"), value("textProviderSel"), SLOT_ROLES.textFlash);
    fillModelSelect(id("textThink"), value("textProviderSel"), SLOT_ROLES.textThink);
    fillModelSelect(id("embedModel"), value("embedProviderSel"), SLOT_ROLES.embedModel);
    fillModelSelect(id("visionModel"), visionModelProviderId(), SLOT_ROLES.visionModel);
    var warn = id("embedReindexWarn");
    if (warn) warn.classList.toggle("d-none", value("embedModel") === (id("embedModel").dataset.initial || ""));
  }

  /* ── Snapshot refresh after any save ─────────────────────────────────── */
  function refreshFromSnapshot(snap) {
    if (!snap) return;
    var caps = {
      capAsk: "ask",
      capSemantic: "semantic_search",
      capVision: "vision_first",
      capWeb: "web_access"
    };
    Object.keys(caps).forEach(function (elId) {
      var el = id(elId);
      var feature = snap.features && snap.features[caps[elId]];
      if (!el || !feature) return;
      var dot = el.querySelector(".cap-dot");
      if (dot) dot.classList.toggle("cap-dot--on", !!feature.on);
      var model = el.querySelector(".cap-model");
      if (model) model.textContent = feature.model || (feature.on ? "on" : "—");
    });
    if (snap.slots) {
      if (snap.slots.text) id("textKeyStatus").textContent = snap.slots.text.key_set ? "Configured" : "Not set";
      if (snap.slots.vision) {
        id("visionKeyStatus").textContent = snap.slots.vision.mode === "text"
          ? "uses the Text key"
          : (snap.slots.vision.key_set ? "Configured" : "Not set");
      }
      if (snap.slots.search) id("searchKeyStatus").textContent = snap.slots.search.key_set ? "Configured" : "Not set";
    }
    if (snap.providers) {
      renderProviders(snap.providers);
    }
    if (snap.assignments) {
      // Keep the pickers in step with the saved assignments.
      var sel = id("textProviderSel");
      if (sel) sel.dataset.selected = snap.assignments.text.provider_id || "";
      var flash = id("textFlash");
      if (flash) flash.dataset.selected = snap.assignments.text.flash || "";
      var think = id("textThink");
      if (think) think.dataset.selected = snap.assignments.text.think || "";
      var embedSel = id("embedProviderSel");
      if (embedSel) embedSel.dataset.selected = snap.assignments.embed.provider_id || "";
      var embedModel = id("embedModel");
      if (embedModel) {
        embedModel.dataset.selected = snap.assignments.embed.model || "";
        embedModel.dataset.initial = snap.assignments.embed.model || "";
      }
      var visionSel = id("visionProviderSel");
      if (visionSel) visionSel.dataset.selected = snap.assignments.vision.provider_id || "";
      var visionModel = id("visionModel");
      if (visionModel) visionModel.dataset.selected = snap.assignments.vision.model || "";
    }
    refreshModelPickers();
  }

  /* ── Modal placement ───────────────────────────────────────────────────
     The dashboard shell is position:fixed, which creates a stacking context:
     a Bootstrap modal left inside it paints BELOW its own body-appended
     backdrop, showing a grey page with an invisible dialog. Move the modal
     to <body> so both layer at the root, and clear orphaned copies left by
     earlier partial swaps. */
  function adoptModalIntoBody() {
    document.querySelectorAll("#providerModal").forEach(function (el) {
      if (!el.closest("#dashboardMain")) {
        if (window.bootstrap && window.bootstrap.Modal) {
          var inst = window.bootstrap.Modal.getInstance(el);
          if (inst) inst.dispose();
        }
        el.remove();
      }
    });
    document.querySelectorAll(".modal-backdrop").forEach(function (el) { el.remove(); });
    document.body.classList.remove("modal-open");
    var modalEl = id("providerModal");
    if (modalEl && modalEl.parentElement !== document.body) {
      document.body.appendChild(modalEl);
    }
  }
  adoptModalIntoBody();

  function modalInstance() {
    return window.bootstrap && window.bootstrap.Modal
      ? window.bootstrap.Modal.getOrCreateInstance(id("providerModal"))
      : null;
  }

  function modelRow(modelId, role) {
    var row = document.createElement("div");
    row.className = "input-group mb-2 provider-model-row";
    var input = document.createElement("input");
    input.type = "text";
    input.className = "form-control mono";
    input.setAttribute("data-model-id", "");
    input.placeholder = "model-id";
    input.value = modelId || "";
    input.autocomplete = "off";
    var select = document.createElement("select");
    select.className = "form-select provider-model-row__role";
    select.setAttribute("data-model-role", "");
    select.style.maxWidth = "150px";
    ["text", "multimodal", "embedding"].forEach(function (r) {
      var opt = document.createElement("option");
      opt.value = r;
      opt.textContent = ROLE_LABELS[r];
      if (r === (role || "text")) opt.selected = true;
      select.appendChild(opt);
    });
    var remove = document.createElement("button");
    remove.type = "button";
    remove.className = "btn btn-outline-secondary";
    remove.textContent = "×";
    remove.title = "Remove";
    remove.addEventListener("click", function () { row.remove(); });
    row.appendChild(input);
    row.appendChild(select);
    row.appendChild(remove);
    return row;
  }

  function renderModelRows(models) {
    var wrap = id("providerModelsBody");
    wrap.textContent = "";
    (models || []).forEach(function (m) { wrap.appendChild(modelRow(m.id, m.role)); });
  }

  function openProviderModal(providerId) {
    editingProviderId = providerId || null;
    var provider = providerById(providerId);
    id("providerName").value = provider ? provider.name : "";
    id("providerBaseUrl").value = provider ? provider.base_url : "";
    id("providerApiKey").value = "";
    id("providerApiKey").placeholder = provider && provider.key_set
      ? "Configured — enter a new key to replace it" : "Enter an API key";
    renderModelRows(provider ? provider.models : []);
    setResult(id("providerResult"), "", true);
    var inst = modalInstance();
    if (inst) inst.show();
  }

  id("providerAddBtn").addEventListener("click", function () { openProviderModal(null); });

  // Delegation scoped to the panel (not document) so the listener dies with
  // the panel on a partial swap instead of stacking stale handlers.
  panel.addEventListener("click", function (e) {
    var editBtn = e.target.closest("[data-edit-provider]");
    if (editBtn) { openProviderModal(editBtn.dataset.editProvider); return; }
    var delBtn = e.target.closest("[data-delete-provider]");
    if (delBtn) {
      var p = providerById(delBtn.dataset.deleteProvider);
      if (p) deleteProvider(p);
    }
  });

  function deleteProvider(provider) {
    if (!window.confirm("Delete provider “" + provider.name + "” and its saved key?")) return;
    post(providerDeleteUrl, {
      id: provider.id,
      env_mtime: panel.dataset.envMtime || "",
      json_mtime: panel.dataset.jsonMtime || ""
    }).then(function (res) {
      if (res.status === 200 && res.data.ok) {
        syncMtimes(res.data);
        refreshFromSnapshot(res.data.snap);
      } else {
        showError(res.data.error || "Delete failed.");
      }
    });
  }

  function providerModalPayload() {
    var models = [];
    document.querySelectorAll("#providerModelsBody .provider-model-row").forEach(function (row) {
      var modelId = row.querySelector('[data-model-id]').value.trim();
      if (!modelId) return;
      models.push({
        id: modelId,
        role: row.querySelector('[data-model-role]').value
      });
    });
    return {
      id: editingProviderId || "",
      name: value("providerName"),
      base_url: value("providerBaseUrl"),
      api_key: value("providerApiKey"),
      models: models
    };
  }

  id("providerModelAddBtn").addEventListener("click", function () {
    id("providerModelsBody").appendChild(modelRow("", "text"));
  });

  id("providerFetchBtn").addEventListener("click", function () {
    setResult(id("providerResult"), "Fetching models…", true);
    post(probeUrl, {
      slot: "provider",
      provider_id: editingProviderId || "",
      base_url: value("providerBaseUrl"),
      api_key: value("providerApiKey")
    }).then(function (res) {
      if (res.data.ok) {
        // Replace the rows with the endpoint's list; fetched ids arrive as
        // text models — the admin adjusts roles afterwards.
        renderModelRows((res.data.models || []).map(function (m) { return { id: m, role: "text" }; }));
        setResult(id("providerResult"), res.data.models.length + " models loaded.", true);
      } else setResult(id("providerResult"), res.data.error || "Fetch failed.", false);
    });
  });

  id("providerTestBtn").addEventListener("click", function () {
    setResult(id("providerResult"), "Testing…", true);
    post(probeUrl, {
      slot: "provider",
      provider_id: editingProviderId || "",
      base_url: value("providerBaseUrl"),
      api_key: value("providerApiKey")
    }).then(function (res) {
      if (res.data.ok) setResult(id("providerResult"), "OK — endpoint reachable.", true);
      else setResult(id("providerResult"), res.data.error || "Test failed.", false);
    });
  });

  id("providerSaveBtn").addEventListener("click", function () {
    if (!value("providerName")) {
      setResult(id("providerResult"), "Enter a provider name.", false);
      return;
    }
    setResult(id("providerResult"), "Saving…", true);
    var payload = providerModalPayload();
    payload.env_mtime = panel.dataset.envMtime || "";
    payload.json_mtime = panel.dataset.jsonMtime || "";
    post(providerSaveUrl, payload).then(function (res) {
      if (res.status === 200 && res.data.ok) {
        setResult(id("providerResult"), "Saved.", true);
        syncMtimes(res.data);
        refreshFromSnapshot(res.data.snap);
        var inst = modalInstance();
        if (inst) inst.hide();
      } else {
        setResult(id("providerResult"), res.data.error || "Save failed.", false);
      }
    });
  });

  /* ── Slot assignments ────────────────────────────────────────────────── */
  ["textProviderSel", "embedProviderSel", "visionProviderSel"].forEach(function (selId) {
    var el = id(selId);
    if (el) el.addEventListener("change", refreshModelPickers);
  });

  id("textThinkFollow").addEventListener("change", function () {
    id("textThink").disabled = this.checked;
  });

  function visionMode() { return id("visionMode").value; }

  id("visionMode").addEventListener("change", function () {
    var mode = visionMode();
    id("visionModelGroup").classList.toggle("d-none", mode === "disabled");
    id("visionDedicatedGroup").classList.toggle("d-none", mode !== "dedicated");
    refreshModelPickers();
  });

  id("embedModel").addEventListener("change", function () {
    var warn = id("embedReindexWarn");
    if (warn) warn.classList.toggle("d-none", this.value === (this.dataset.initial || ""));
  });

  function mtimes() {
    return {
      env_mtime: panel.dataset.envMtime || "",
      json_mtime: panel.dataset.jsonMtime || ""
    };
  }

  function textPayload() {
    var follow = id("textThinkFollow").checked;
    return {
      provider_id: value("textProviderSel"),
      flash: value("textFlash"),
      think: follow ? "" : value("textThink")
    };
  }

  function embedPayload() {
    return {
      provider_id: value("embedProviderSel"),
      model: value("embedModel")
    };
  }

  function visionPayload() {
    var mode = visionMode();
    return {
      mode: mode,
      provider_id: mode === "dedicated" ? value("visionProviderSel") : "",
      model: mode === "disabled" ? "" : value("visionModel")
    };
  }

  function saveSlot(slot, payload, resultEl) {
    payload.slot = slot;
    Object.assign(payload, mtimes());
    setResult(resultEl, "Saving…", true);
    post(saveUrl, payload).then(function (res) {
      if (res.status === 200 && res.data.ok) {
        setResult(resultEl, "Saved.", true);
        showBanner(res.data);
        syncMtimes(res.data);
        refreshFromSnapshot(res.data.snap);
      } else {
        setResult(resultEl, res.data.error || "Save failed.", false);
        if (res.status === 409) showError(res.data.error);
      }
    }).catch(function (err) {
      setResult(resultEl, "Save failed: " + (err && err.message || err), false);
    });
  }

  id("textSaveBtn").addEventListener("click", function () { saveSlot("text", textPayload(), id("textResult")); });
  id("embedSaveBtn").addEventListener("click", function () { saveSlot("embed", embedPayload(), id("embedResult")); });
  id("visionSaveBtn").addEventListener("click", function () { saveSlot("vision", visionPayload(), id("visionResult")); });

  id("searchTestBtn").addEventListener("click", function () {
    setResult(id("searchResult"), "Testing…", true);
    post(probeUrl, { slot: "search", api_key: value("searchApiKey") }).then(function (res) {
      if (res.data.ok) setResult(id("searchResult"), "OK.", true);
      else setResult(id("searchResult"), res.data.error || "Test failed.", false);
    });
  });
  id("searchSaveBtn").addEventListener("click", function () {
    saveSlot("search", { api_key: value("searchApiKey") }, id("searchResult"));
  });

  /* ── Boot: seed pickers from the server-rendered snapshot ────────────── */
  var dataEl = document.getElementById("modelsProviderData");
  if (dataEl) {
    try { renderProviders(JSON.parse(dataEl.textContent)); } catch (e) { /* stale markup */ }
  }
  refreshModelPickers();
})();
