(function () {
  "use strict";
  const form = document.getElementById("workerSearchForm");
  if (!form) return;
  form.addEventListener("submit", async function (event) {
    event.preventDefault();
    const button = form.querySelector("button");
    const result = document.getElementById("workerSearchResult");
    button.disabled = true;
    try {
      const response = await fetch(form.action, {
        method: "POST",
        headers: {"Content-Type": "application/json", "X-CSRFToken": form.elements.csrf_token.value},
        body: JSON.stringify({slot: "search", api_key: form.elements.api_key.value, env_mtime: form.elements.env_mtime.value}),
      });
      const data = await response.json();
      if (!response.ok) throw new Error();
      form.elements.api_key.value = "";
      form.elements.env_mtime.value = data.snap.env_mtime;
      result.textContent = form.dataset.saved;
    } catch {
      result.textContent = form.dataset.failed;
    } finally {
      button.disabled = false;
    }
  });
}());
