/* Admin Version page: update trigger + status polling.
   The panel's data attributes carry the endpoint URLs; the CSRF token comes
   from the <meta name="csrf-token"> tag injected by the dashboard shell. */
(function () {
  "use strict";

  var panel = document.getElementById("versionPanel");
  if (!panel) return;

  var updateUrl = panel.dataset.updateUrl;
  var statusUrl = panel.dataset.statusUrl;
  var btn = document.getElementById("versionUpdateBtn");
  var checkBtn = document.getElementById("versionCheckBtn");
  var progress = document.getElementById("versionProgress");
  var phaseEl = document.getElementById("versionPhase");
  var logEl = document.getElementById("versionLog");
  var spinner = document.getElementById("versionSpinner");
  var meta = document.querySelector('meta[name="csrf-token"]');
  var csrf = meta ? meta.getAttribute("content") : "";

  var POLL_MS = 2000;
  var timer = null;
  var targetSha = null;

  if (checkBtn) {
    checkBtn.addEventListener("click", function () {
      location.reload();
    });
  }

  if (!btn) return;

  btn.addEventListener("click", function () {
    btn.disabled = true;
    setPhase("Starting…");
    fetch(updateUrl, {
      method: "POST",
      headers: { "X-CSRFToken": csrf, Accept: "application/json" },
    })
      .then(function (res) {
        return res.json().catch(function () { return {}; }).then(function (data) {
          return { ok: res.ok, data: data };
        });
      })
      .then(function (out) {
        if (!out.ok) {
          setPhase("");
          appendLog((out.data && out.data.error) || "Update request failed.");
          showProgress();
          stopSpinner();
          btn.disabled = false;
          return;
        }
        showProgress();
        schedulePoll(0);
      })
      .catch(function () {
        setPhase("");
        appendLog("Update request failed.");
        showProgress();
        stopSpinner();
        btn.disabled = false;
      });
  });

  function schedulePoll(delay) {
    if (timer) clearTimeout(timer);
    timer = setTimeout(pollOnce, delay);
  }

  function pollOnce() {
    fetch(statusUrl, { headers: { Accept: "application/json" } })
      .then(function (res) {
        if (!res.ok) throw new Error("status " + res.status);
        return res.json();
      })
      .then(onStatus)
      .catch(function () {
        // The server is restarting; keep waiting for it to come back.
        setPhase("Restarting — waiting for the app to come back…");
        schedulePoll(POLL_MS);
      });
  }

  function onStatus(status) {
    if (status.target_sha) targetSha = status.target_sha;
    if (status.phase === "failed") {
      setPhase("Update failed");
      if (status.error) appendLog(status.error);
      renderLog(status.log);
      stopSpinner();
      btn.disabled = false;
      return;
    }
    var done = status.phase === "complete" ||
      (!status.running && status.head_sha && targetSha && status.head_sha === targetSha);
    if (done) {
      renderLog(status.log);
      setPhase("Update complete — " + (status.head_sha || "").slice(0, 7));
      stopSpinner();
      setTimeout(function () { location.reload(); }, 1500);
      return;
    }
    if (status.phase === "updating") setPhase("Applying commits…");
    else if (status.phase === "installing-dependencies") setPhase("Installing dependencies…");
    else if (status.phase === "restarting") setPhase("Restarting the app…");
    else if (status.phase && status.phase !== "idle") setPhase(status.phase);
    renderLog(status.log);
    schedulePoll(POLL_MS);
  }

  function showProgress() {
    if (progress) progress.classList.remove("d-none");
  }

  function setPhase(text) {
    if (phaseEl) phaseEl.textContent = text;
  }

  function stopSpinner() {
    if (spinner) spinner.classList.add("d-none");
  }

  function renderLog(lines) {
    if (logEl && Array.isArray(lines)) logEl.textContent = lines.join("\n");
  }

  function appendLog(line) {
    if (!logEl) return;
    logEl.textContent += (logEl.textContent ? "\n" : "") + line;
  }
})();
