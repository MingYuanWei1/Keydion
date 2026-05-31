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

    ensureConversation().then(function (cid) {
      return fetch(BOOT.api_url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q, mode: mode, conversation_id: cid,
                               paper_filenames: window.__selectedPapers ? window.__selectedPapers() : [] })
      });
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
  // --- conversation rail ---
  var railList = document.getElementById("kd-rail-list");
  var newChatBtn = document.getElementById("kd-newchat");
  var activeConv = null;
  window.__activeConv = function () { return activeConv; };

  function loadConversations() {
    if (!railList) return;
    fetch("/api/conversations").then(function (r) { return r.json(); }).then(function (j) {
      railList.innerHTML = "";
      (j.conversations || []).forEach(function (c) {
        var row = el("div", "kd-convo" + (c.id === activeConv ? " is-active" : ""));
        var title = el("span", "kd-convo__title", c.title);
        title.addEventListener("click", function () { openConversation(c.id); });
        var menu = el("div", "kd-convo__menu");
        var ren = el("button", "kd-convo__more", "✎");
        ren.addEventListener("click", function (e) { e.stopPropagation(); renameConversation(c.id, c.title); });
        var del = el("button", "kd-convo__more", "✕");
        del.addEventListener("click", function (e) { e.stopPropagation(); deleteConversation(c.id); });
        menu.appendChild(ren); menu.appendChild(del);
        row.appendChild(title); row.appendChild(menu);
        railList.appendChild(row);
      });
    });
  }

  function ensureConversation() {
    if (activeConv != null) return Promise.resolve(activeConv);
    return fetch("/api/conversations", { method: "POST" })
      .then(function (r) { return r.json(); })
      .then(function (j) { activeConv = j.id; loadConversations(); return activeConv; });
  }

  function openConversation(id) {
    activeConv = id;
    fetch("/api/conversations/" + id).then(function (r) { return r.json(); }).then(function (j) {
      thread.innerHTML = "";
      if (empty) { thread.appendChild(empty); empty.style.display = "none"; }
      (j.messages || []).forEach(function (m) {
        if (m.role === "user") { addUser(m.content); }
        else {
          var ai = addAi(); if (ai.typing) ai.typing.remove(); ai.text = m.content;
          ai.bubble.textContent = m.content;
          renderSources(ai.body, m.citations);
          addActions(ai.body, (function (t) { return function () { return t; }; })(m.content));
        }
      });
      loadConversations();
    });
  }

  function renameConversation(id, current) {
    var name = window.prompt(I18N.rename || "Rename", current);
    if (name == null) return;
    fetch("/api/conversations/" + id, {
      method: "PATCH", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: name })
    }).then(loadConversations);
  }

  function deleteConversation(id) {
    fetch("/api/conversations/" + id, { method: "DELETE" }).then(function () {
      if (id === activeConv) { activeConv = null; thread.innerHTML = ""; if (empty) { thread.appendChild(empty); empty.style.display = ""; } }
      loadConversations();
    });
  }

  if (newChatBtn) newChatBtn.addEventListener("click", function () {
    activeConv = null; thread.innerHTML = "";
    if (empty) { thread.appendChild(empty); empty.style.display = ""; }
    loadConversations();
  });
  loadConversations();

  // --- cite from library ---
  var citeOverlay = document.getElementById("kd-cite-overlay");
  var citeListEl = document.getElementById("kd-cite-list");
  var citeSearch = document.getElementById("kd-cite-search");
  var attachRow = document.getElementById("kd-attachrow");
  var selected = {};
  window.__selectedPapers = function () { return Object.keys(selected); };

  function openCite() { if (citeOverlay) { citeOverlay.classList.add("is-open"); loadPapers(""); } }
  function closeCite() { if (citeOverlay) citeOverlay.classList.remove("is-open"); }

  function loadPapers(q) {
    if (!citeListEl) return;
    fetch("/api/ask/papers?q=" + encodeURIComponent(q)).then(function (r) { return r.json(); })
      .then(function (j) {
        citeListEl.innerHTML = "";
        (j.papers || []).forEach(function (p) {
          var row = el("div", "kd-paper" + (selected[p.filename] ? " is-selected" : ""));
          var meta = el("div");
          meta.appendChild(el("div", "kd-paper__title", p.title));
          meta.appendChild(el("div", "kd-paper__authors", p.authors));
          row.appendChild(meta);
          row.addEventListener("click", function () {
            if (selected[p.filename]) { delete selected[p.filename]; row.classList.remove("is-selected"); }
            else { selected[p.filename] = { title: p.title }; row.classList.add("is-selected"); }
          });
          citeListEl.appendChild(row);
        });
      });
  }

  function renderChips() {
    if (!attachRow) return;
    attachRow.innerHTML = "";
    Object.keys(selected).forEach(function (fn) {
      var chip = el("span", "kd-chip");
      chip.appendChild(el("span", null, "\U0001F4C4 " + selected[fn].title));
      var x = el("button", "kd-iconbtn", "✕");
      x.addEventListener("click", function () { delete selected[fn]; renderChips(); });
      chip.appendChild(x);
      attachRow.appendChild(chip);
    });
  }

  var citeOpenBtn = document.getElementById("kd-cite-open");
  if (citeOpenBtn) citeOpenBtn.addEventListener("click", openCite);
  var citeCancel = document.getElementById("kd-cite-cancel");
  if (citeCancel) citeCancel.addEventListener("click", closeCite);
  var citeAdd = document.getElementById("kd-cite-add");
  if (citeAdd) citeAdd.addEventListener("click", function () { renderChips(); closeCite(); });
  if (citeSearch) citeSearch.addEventListener("input", function () { loadPapers(citeSearch.value); });
  if (citeOverlay) citeOverlay.addEventListener("click", function (e) { if (e.target === citeOverlay) closeCite(); });
})();
