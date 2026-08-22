/* Admin AI-models page: fetch models, test, and save per provider card.
   The panel's data attributes carry the endpoint URLs; the CSRF token comes
   from the <meta name="csrf-token"> tag injected by the dashboard shell.
   Keys are write-only: password inputs never carry a saved value, and this
   script never writes a key anywhere except into a save/probe request body. */
(function () {
  "use strict";

  var panel = document.getElementById("modelsPanel");
  if (!panel) return;

  var probeUrl = panel.dataset.probeUrl;
  var saveUrl = panel.dataset.saveUrl;
  var meta = document.querySelector('meta[name="csrf-token"]');
  var csrf = meta ? meta.getAttribute("content") : "";

  function id(name) { return document.getElementById(name); }
  function value(name) { var el = id(name); return el ? el.value.trim() : ""; }

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

  /* ── Capability strip / statuses refresh after a save ─────────────────── */
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
    var statuses = {
      textKeyStatus: snap.slots && snap.slots.text,
      embedKeyStatus: snap.slots && snap.slots.embed,
      visionKeyStatus: snap.slots && snap.slots.vision,
      searchKeyStatus: snap.slots && snap.slots.search
    };
    Object.keys(statuses).forEach(function (elId) {
      var slot = statuses[elId];
      var el = id(elId);
      if (!el || !slot) return;
      if (elId === "embedKeyStatus" && slot.uses_text_key) el.textContent = "uses the Text key";
      else if (elId === "visionKeyStatus" && slot.uses_text_key) el.textContent = "uses the Text key";
      else el.textContent = slot.key_set ? "Configured" : "Not set";
    });
    if (snap.slots) {
      if (snap.slots.text && id("textProvider")) id("textProvider").textContent = snap.slots.text.provider;
      if (snap.slots.embed && id("embedProvider")) id("embedProvider").textContent = snap.slots.embed.provider;
      if (snap.slots.vision && id("visionProvider")) id("visionProvider").textContent = snap.slots.vision.provider;
    }
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

  function save(slot, payload, resultEl) {
    payload.slot = slot;
    payload.env_mtime = panel.dataset.envMtime || "";
    setResult(resultEl, "Saving…", true);
    post(saveUrl, payload).then(function (res) {
      if (res.status === 200 && res.data.ok) {
        setResult(resultEl, "Saved.", true);
        showBanner(res.data);
        if (res.data.snap && res.data.snap.env_mtime) {
          panel.dataset.envMtime = String(res.data.snap.env_mtime);
        }
        refreshFromSnapshot(res.data.snap);
      } else {
        setResult(resultEl, res.data.error || "Save failed.", false);
        if (res.status === 409) showError(res.data.error);
      }
    }).catch(function (err) {
      setResult(resultEl, "Save failed: " + (err && err.message || err), false);
    });
  }

  /* ── Text ─────────────────────────────────────────────────────────────── */
  function textPayload() {
    var follow = id("textThinkFollow").checked;
    return {
      base_url: value("textBaseUrl"),
      api_key: value("textApiKey"),
      flash: value("textFlash"),
      think: follow ? "" : value("textThink"),
      model: value("textFlash")
    };
  }

  id("textThinkFollow").addEventListener("change", function () {
    id("textThink").disabled = this.checked;
  });

  id("textFetchBtn").addEventListener("click", function () {
    setResult(id("textResult"), "Fetching models…", true);
    post(probeUrl, Object.assign({ slot: "text" }, textPayload())).then(function (res) {
      if (res.data.ok) {
        fillDatalist("textModels", res.data.models);
        setResult(id("textResult"), res.data.models.length + " models loaded.", true);
      } else setResult(id("textResult"), res.data.error || "Fetch failed.", false);
    });
  });

  id("textTestBtn").addEventListener("click", function () {
    setResult(id("textResult"), "Testing…", true);
    post(probeUrl, Object.assign({ slot: "text" }, textPayload())).then(function (res) {
      if (res.data.ok) {
        var note = res.data.model_listed === false ? "OK — but the endpoint does not list that model id." : "OK.";
        setResult(id("textResult"), note, true);
      } else setResult(id("textResult"), res.data.error || "Test failed.", false);
    });
  });

  id("textSaveBtn").addEventListener("click", function () {
    save("text", textPayload(), id("textResult"));
  });

  /* ── Embedding ────────────────────────────────────────────────────────── */
  function embedPayload() {
    return {
      base_url: value("embedBaseUrl"),
      api_key: value("embedApiKey"),
      model: value("embedModel")
    };
  }

  id("embedFetchBtn").addEventListener("click", function () {
    setResult(id("embedResult"), "Fetching models…", true);
    post(probeUrl, Object.assign({ slot: "embed" }, embedPayload())).then(function (res) {
      if (res.data.ok) {
        fillDatalist("embedModels", res.data.models);
        setResult(id("embedResult"), res.data.models.length + " models loaded.", true);
      } else setResult(id("embedResult"), res.data.error || "Fetch failed.", false);
    });
  });

  id("embedTestBtn").addEventListener("click", function () {
    setResult(id("embedResult"), "Testing…", true);
    post(probeUrl, Object.assign({ slot: "embed" }, embedPayload())).then(function (res) {
      if (res.data.ok) {
        setResult(id("embedResult"), "OK — " + res.data.dimension + "-d vectors"
          + (res.data.dimension_ok ? "" : " (column is " + res.data.expected + "-d!)"),
          res.data.dimension_ok);
      } else setResult(id("embedResult"), res.data.error || "Test failed.", false);
    });
  });

  id("embedModel").addEventListener("input", function () {
    var warn = id("embedReindexWarn");
    if (warn) warn.classList.toggle("d-none", this.value.trim() === (this.dataset.initial || "").trim());
  });

  id("embedSaveBtn").addEventListener("click", function () {
    save("embed", embedPayload(), id("embedResult"));
  });

  /* ── Vision ───────────────────────────────────────────────────────────── */
  function visionMode() { return id("visionMode").value; }

  function visionPayload() {
    var mode = visionMode();
    return {
      mode: mode,
      model: mode === "disabled" ? "" : value("visionModel"),
      base_url: mode === "dedicated" ? value("visionBaseUrl") : "",
      api_key: mode === "dedicated" ? value("visionApiKey") : ""
    };
  }

  // "Same as the Text provider" asks the TEXT endpoint the form shows.

  id("visionMode").addEventListener("change", function () {
    var mode = visionMode();
    id("visionModelGroup").classList.toggle("d-none", mode === "disabled");
    id("visionDedicatedGroup").classList.toggle("d-none", mode !== "dedicated");
    id("visionFetchBtn").disabled = mode === "disabled";
    id("visionTestBtn").disabled = mode === "disabled";
  });

  id("visionFetchBtn").addEventListener("click", function () {
    setResult(id("visionResult"), "Fetching models…", true);
    var body;
    if (visionMode() === "dedicated") {
      body = Object.assign({ slot: "vision" }, { base_url: value("visionBaseUrl"), api_key: value("visionApiKey") });
    } else {
      body = Object.assign({ slot: "text" }, textPayload());
    }
    post(probeUrl, body).then(function (res) {
      if (res.data.ok) {
        fillDatalist("visionModels", res.data.models);
        setResult(id("visionResult"), res.data.models.length + " models loaded.", true);
      } else setResult(id("visionResult"), res.data.error || "Fetch failed.", false);
    });
  });

  id("visionTestBtn").addEventListener("click", function () {
    setResult(id("visionResult"), "Testing…", true);
    var body;
    if (visionMode() === "dedicated") {
      body = { slot: "vision", base_url: value("visionBaseUrl"), api_key: value("visionApiKey"), model: value("visionModel") };
    } else {
      body = Object.assign({ slot: "text" }, textPayload());
    }
    post(probeUrl, body).then(function (res) {
      if (res.data.ok) {
        var note = res.data.model_listed === false ? "OK — but the endpoint does not list that model id." : "OK.";
        setResult(id("visionResult"), note, true);
      } else setResult(id("visionResult"), res.data.error || "Test failed.", false);
    });
  });

  id("visionSaveBtn").addEventListener("click", function () {
    save("vision", visionPayload(), id("visionResult"));
  });

  /* ── Web search ───────────────────────────────────────────────────────── */
  id("searchTestBtn").addEventListener("click", function () {
    setResult(id("searchResult"), "Testing…", true);
    post(probeUrl, { slot: "search", api_key: value("searchApiKey") }).then(function (res) {
      if (res.data.ok) setResult(id("searchResult"), "OK.", true);
      else setResult(id("searchResult"), res.data.error || "Test failed.", false);
    });
  });

  id("searchSaveBtn").addEventListener("click", function () {
    save("search", { api_key: value("searchApiKey") }, id("searchResult"));
  });

  function fillDatalist(listId, models) {
    var list = id(listId);
    if (!list) return;
    list.innerHTML = "";
    (models || []).forEach(function (modelId) {
      var opt = document.createElement("option");
      opt.value = modelId;
      list.appendChild(opt);
    });
  }
})();
