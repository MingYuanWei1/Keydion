// static/js/ask.js — Ask the Library chat client (vanilla JS, no build step)
(function () {
  "use strict";
  var bootEl = document.getElementById("ask-boot");
  if (!bootEl) return;
  var BOOT = JSON.parse(bootEl.textContent);
  var I18N = BOOT.i18n;

  var thread = document.getElementById("kd-thread");
  var empty = document.getElementById("kd-empty");
  var input = document.getElementById("kd-composer-input");
  var sendBtn = document.getElementById("kd-send");
  var agent = document.getElementById("kd-agent");
  var mode = "flash";
  var busy = false;

  // --- Flash / Thinking toggle ---
  agent.addEventListener("click", function (e) {
    var btn = e.target.closest(".kd-agent__opt");
    if (!btn) return;
    mode = btn.getAttribute("data-mode");
    agent.querySelectorAll(".kd-agent__opt").forEach(function (b) {
      b.classList.toggle("is-active", b === btn);
    });
  });

  // --- suggested prompts ---
  document.querySelectorAll("[data-suggest]").forEach(function (card) {
    card.addEventListener("click", function () {
      input.value = card.getAttribute("data-suggest");
      send();
    });
  });

  // --- textarea autogrow + Enter to send ---
  input.addEventListener("input", function () {
    input.style.height = "auto";
    input.style.height = Math.min(input.scrollHeight, 200) + "px";
  });
  input.addEventListener("keydown", function (e) {
    if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
  });
  sendBtn.addEventListener("click", send);

  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }

  function addUser(text) {
    if (empty) empty.style.display = "none";
    var msg = el("div", "kd-msg kd-msg--user");
    msg.appendChild(el("div", "kd-bubble", text));
    thread.appendChild(msg);
    scroll();
  }

  function addAi() {
    var msg = el("div", "kd-msg kd-msg--ai");
    msg.appendChild(el("div", "kd-ai__avatar", "K"));
    var body = el("div", "kd-ai__body");
    var bubble = el("div", "kd-bubble");
    var typing = el("div", "kd-typing");
    typing.appendChild(el("span")); typing.appendChild(el("span")); typing.appendChild(el("span"));
    bubble.appendChild(typing);
    body.appendChild(bubble);
    msg.appendChild(body);
    thread.appendChild(msg);
    scroll();
    return { bubble: bubble, body: body, typing: typing, text: "" };
  }

  function renderSources(body, items) {
    if (!items || !items.length) return;
    var box = el("div", "kd-sources");
    box.appendChild(el("div", "kd-sources__label", I18N.sources));
    items.forEach(function (it) {
      var a = el("a", "kd-source");
      a.href = it.url;
      a.appendChild(el("span", "kd-source__n", "[" + it.n + "]"));
      a.appendChild(el("span", "kd-source__title", it.title + (it.authors ? " — " + it.authors : "")));
      box.appendChild(a);
    });
    body.appendChild(box);
  }

  function addActions(body, getText) {
    var bar = el("div", "kd-msg__actions");
    var copy = el("button", "kd-iconbtn", I18N.copy);
    copy.addEventListener("click", function () { navigator.clipboard.writeText(getText()); });
    var regen = el("button", "kd-iconbtn", I18N.regenerate);
    regen.addEventListener("click", function () { if (window.__lastQuestion) send(window.__lastQuestion); });
    bar.appendChild(copy); bar.appendChild(regen);
    body.appendChild(bar);
  }

  function scroll() { thread.scrollTop = thread.scrollHeight; }

  function send(forced) {
    if (busy) return;
    var q = (forced != null ? forced : input.value).trim();
    if (!q) return;
    window.__lastQuestion = q;
    busy = true; sendBtn.disabled = true;
    addUser(q);
    input.value = ""; input.style.height = "auto";
    var ai = addAi();

    fetch(BOOT.api_url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question: q, mode: mode })
    }).then(function (resp) {
      if (!resp.ok) {
        return resp.json().catch(function () { return { error: I18N.error }; })
          .then(function (j) { throw new Error(j.error || I18N.error); });
      }
      return readStream(resp, ai);
    }).catch(function (err) {
      ai.typing.remove();
      ai.bubble.textContent = err.message || I18N.error;
    }).finally(function () {
      busy = false; sendBtn.disabled = false;
    });
  }

  function readStream(resp, ai) {
    var reader = resp.body.getReader();
    var decoder = new TextDecoder();
    var buffer = "";
    function pump() {
      return reader.read().then(function (res) {
        if (res.done) return;
        buffer += decoder.decode(res.value, { stream: true });
        var frames = buffer.split("\n\n");
        buffer = frames.pop();
        frames.forEach(function (frame) {
          var line = frame.replace(/^data: /, "").trim();
          if (!line) return;
          var evt;
          try { evt = JSON.parse(line); } catch (e) { return; }
          handle(evt, ai);
        });
        return pump();
      });
    }
    return pump();
  }

  function handle(evt, ai) {
    if (evt.type === "token") {
      if (ai.typing && ai.typing.parentNode) ai.typing.remove();
      ai.text += evt.text;
      ai.bubble.textContent = ai.text;
      scroll();
    } else if (evt.type === "citations") {
      renderSources(ai.body, evt.items);
      scroll();
    } else if (evt.type === "done") {
      addActions(ai.body, function () { return ai.text; });
      scroll();
    } else if (evt.type === "error") {
      if (ai.typing && ai.typing.parentNode) ai.typing.remove();
      ai.bubble.textContent = evt.message || I18N.error;
    }
  }
})();
