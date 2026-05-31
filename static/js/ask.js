// static/js/ask.js — Keydion AI chat client (vanilla JS, no build step)
(function () {
  "use strict";
  var bootEl = document.getElementById("ask-boot");
  if (!bootEl) return;
  var BOOT = JSON.parse(bootEl.textContent);
  var I18N = BOOT.i18n || {};

  var thread = document.getElementById("kd-thread");
  var empty = document.getElementById("kd-empty");
  var input = document.getElementById("kd-composer-input");
  var sendBtn = document.getElementById("kd-send");
  var agent = document.getElementById("kd-agent");
  var mode = "flash";
  var busy = false;

  function el(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = text;
    return n;
  }

  function iconButton(label, svg) {
    var btn = el("button", "kd-iconbtn");
    btn.type = "button";
    btn.title = label;
    btn.setAttribute("aria-label", label);
    btn.innerHTML = svg;
    return btn;
  }

  function svgNode(svg) {
    var wrap = document.createElement("span");
    wrap.innerHTML = svg;
    return wrap.firstElementChild;
  }

  function messageIcon() {
    return svgNode('<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 15a4 4 0 0 1-4 4H8l-5 3V7a4 4 0 0 1 4-4h10a4 4 0 0 1 4 4z"/></svg>');
  }

  function parseConversationTime(c) {
    var raw = c.updated_at || c.created_at || "";
    var time = raw ? new Date(raw).getTime() : 0;
    return Number.isFinite(time) ? time : 0;
  }

  function conversationGroup(c) {
    var time = parseConversationTime(c);
    if (!time) return I18N.older || "Older";
    var dayMs = 24 * 60 * 60 * 1000;
    var today = new Date();
    var todayStart = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();
    var item = new Date(time);
    var itemStart = new Date(item.getFullYear(), item.getMonth(), item.getDate()).getTime();
    var diffDays = Math.floor((todayStart - itemStart) / dayMs);
    if (diffDays <= 0) return I18N.today || "Today";
    if (diffDays === 1) return I18N.yesterday || "Yesterday";
    if (diffDays <= 7) return I18N.previous_7_days || "Previous 7 days";
    return I18N.older || "Older";
  }

  // --- Flash / Thinking toggle ---
  if (agent) agent.addEventListener("click", function (e) {
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
  if (input) {
    input.addEventListener("input", function () {
      input.style.height = "auto";
      input.style.height = Math.min(input.scrollHeight, 220) + "px";
    });
    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); send(); }
    });
  }
  if (sendBtn) sendBtn.addEventListener("click", function () { send(); });

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
    var name = el("div", "kd-ai__name", "Keydion AI ");
    name.appendChild(el("span", "kd-ai__mode", mode === "think" ? (I18N.thinking || "Thinking") : (I18N.flash || "Flash")));
    var prose = el("div", "kd-prose");
    var bubble = el("p");
    var typing = el("div", "kd-typing");
    typing.appendChild(el("span")); typing.appendChild(el("span")); typing.appendChild(el("span"));
    prose.appendChild(typing);
    body.appendChild(name);
    body.appendChild(prose);
    msg.appendChild(body);
    thread.appendChild(msg);
    scroll();
    return { bubble: bubble, prose: prose, body: body, typing: typing, text: "" };
  }

  function renderSources(body, items) {
    if (!items || !items.length) return;
    var box = el("div", "kd-sources");
    box.appendChild(el("div", "kd-sources__label", I18N.sources || "Cited from your library"));
    var grid = el("div", "kd-sources__grid");
    items.forEach(function (it) {
      var a = el("a", "kd-source");
      a.href = it.url;
      a.appendChild(el("span", "kd-source__n", "[" + it.n + "]"));
      var meta = el("span", "kd-source__meta");
      meta.appendChild(el("span", "kd-source__title", it.title));
      if (it.authors) meta.appendChild(el("span", "kd-source__sub", it.authors));
      a.appendChild(meta);
      grid.appendChild(a);
    });
    box.appendChild(grid);
    body.querySelector(".kd-prose").appendChild(box);
  }

  function addActions(body, getText) {
    var bar = el("div", "kd-msg__actions");
    var copy = iconButton(I18N.copy || "Copy", '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>');
    copy.addEventListener("click", function () { navigator.clipboard.writeText(getText()); });
    var regen = iconButton(I18N.regenerate || "Regenerate", '<svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></svg>');
    regen.addEventListener("click", function () { if (window.__lastQuestion) send(window.__lastQuestion); });
    bar.appendChild(copy); bar.appendChild(regen);
    body.appendChild(bar);
  }

  function scroll() { var sc = document.querySelector(".kd-thread"); if (sc) sc.scrollTop = sc.scrollHeight; }

  function send(forced) {
    if (busy) return;
    var q = (forced != null ? forced : input.value).trim();
    if (!q) return;
    window.__lastQuestion = q;
    busy = true; if (sendBtn) sendBtn.disabled = true;
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
      if (ai.typing && ai.typing.parentNode) ai.typing.remove();
      ai.prose.appendChild(el("p", null, err.message || I18N.error || "Error"));
    }).finally(function () {
      busy = false; if (sendBtn) sendBtn.disabled = false;
      loadConversations();
    });
  }

  function ensureBubble(ai) {
    if (!ai.bubble.parentNode) ai.prose.appendChild(ai.bubble);
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
      ensureBubble(ai);
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
      ensureBubble(ai);
      ai.bubble.textContent = evt.message || I18N.error || "Error";
    }
  }

  // --- conversation rail ---
  var railList = document.getElementById("kd-rail-list");
  var railSearch = document.getElementById("kd-rail-search");
  var newChatBtn = document.getElementById("kd-newchat");
  var activeConv = null;
  var railConversations = [];
  window.__activeConv = function () { return activeConv; };

  function renderConversationGroup(label, items) {
    var group = el("div", "kd-rail__group");
    group.appendChild(el("div", "kd-rail__grouplabel", label));
    items.forEach(function (c) {
      var row = el("div", "kd-convo" + (c.id === activeConv ? " is-active" : ""));
      var main = el("button", "kd-convo__main");
      main.type = "button";
      main.appendChild(messageIcon());
      main.appendChild(el("span", "kd-convo__title", c.title));
      main.addEventListener("click", function () { openConversation(c.id); });
      var more = el("button", "kd-convo__more", "⋯");
      more.type = "button";
      more.title = I18N.rename || "Rename";
      more.addEventListener("click", function (e) { e.stopPropagation(); renameConversation(c.id, c.title); });
      var del = el("button", "kd-convo__more", "✕");
      del.type = "button";
      del.addEventListener("click", function (e) { e.stopPropagation(); deleteConversation(c.id); });
      row.appendChild(main); row.appendChild(more); row.appendChild(del);
      group.appendChild(row);
    });
    return group;
  }

  function renderConversationRail() {
    if (!railList) return;
    railList.innerHTML = "";
    var q = railSearch ? railSearch.value.trim().toLowerCase() : "";
    var visible = railConversations
      .filter(function (c) { return !q || (c.title || "").toLowerCase().indexOf(q) !== -1; })
      .slice()
      .sort(function (a, b) { return parseConversationTime(b) - parseConversationTime(a); });
    if (!visible.length) {
      railList.appendChild(el("div", "kd-rail__empty", I18N.no_conversations_match || "No conversations match."));
      return;
    }
    var groups = [];
    visible.forEach(function (c) {
      var label = conversationGroup(c);
      var group = groups.find(function (g) { return g.label === label; });
      if (!group) { group = { label: label, items: [] }; groups.push(group); }
      group.items.push(c);
    });
    groups.forEach(function (g) {
      railList.appendChild(renderConversationGroup(g.label, g.items));
    });
  }

  function loadConversations() {
    if (!railList) return;
    fetch("/api/conversations").then(function (r) { return r.json(); }).then(function (j) {
      railConversations = j.conversations || [];
      renderConversationRail();
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
          var ai = addAi(); if (ai.typing) ai.typing.remove();
          ensureBubble(ai); ai.text = m.content; ai.bubble.textContent = m.content;
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
  if (railSearch) railSearch.addEventListener("input", renderConversationRail);
  loadConversations();

  // --- attach "+" pop menu (upload item inert) ---
  var attachBtn = document.getElementById("kd-attach-btn");
  var attachPop = document.getElementById("kd-attach-pop");
  if (attachBtn && attachPop) {
    attachBtn.addEventListener("click", function (e) {
      e.stopPropagation();
      attachPop.classList.toggle("is-open");
    });
    document.addEventListener("mousedown", function (e) {
      if (!attachPop.contains(e.target) && e.target !== attachBtn) attachPop.classList.remove("is-open");
    });
  }
  var uploadItem = document.getElementById("kd-upload-item");
  if (uploadItem) uploadItem.addEventListener("click", function () { if (attachPop) attachPop.classList.remove("is-open"); });

  // --- web access toggle (visual only / inert) ---
  var webToggle = document.getElementById("kd-web-toggle");
  if (webToggle) webToggle.addEventListener("click", function () {
    var on = webToggle.classList.toggle("is-on");
    webToggle.setAttribute("aria-pressed", on ? "true" : "false");
  });

  // --- rail show/hide toggle ---
  var railToggle = document.getElementById("kd-railtoggle");
  var appEl = document.getElementById("kd-app");
  if (railToggle && appEl) railToggle.addEventListener("click", function () {
    appEl.setAttribute("data-rail", appEl.getAttribute("data-rail") === "hidden" ? "full" : "hidden");
  });

  // --- cite from library ---
  var citeOverlay = document.getElementById("kd-cite-overlay");
  var citeListEl = document.getElementById("kd-cite-list");
  var citeSearch = document.getElementById("kd-cite-search");
  var citeCount = document.getElementById("kd-cite-count");
  var attachRow = document.getElementById("kd-attachrow");
  var selected = {};
  window.__selectedPapers = function () { return Object.keys(selected); };

  function updateCount() {
    if (!citeCount) return;
    var n = Object.keys(selected).length;
    citeCount.textContent = n ? (n + " " + (I18N.selected || "selected")) : (I18N.select_hint || "Select papers to attach as citations");
  }

  function openCite() {
    if (attachPop) attachPop.classList.remove("is-open");
    if (citeOverlay) { citeOverlay.classList.add("is-open"); loadPapers(""); updateCount(); }
  }
  function closeCite() { if (citeOverlay) citeOverlay.classList.remove("is-open"); }

  function loadPapers(q) {
    if (!citeListEl) return;
    fetch("/api/ask/papers?q=" + encodeURIComponent(q)).then(function (r) { return r.json(); })
      .then(function (j) {
        citeListEl.innerHTML = "";
        (j.papers || []).forEach(function (p) {
          var row = el("div", "kd-paper" + (selected[p.filename] ? " is-selected" : ""));
          var check = el("button", "kd-check");
          if (selected[p.filename]) check.textContent = "✓";
          var meta = el("div", "kd-paper__meta");
          meta.appendChild(el("div", "kd-paper__title", p.title));
          if (p.authors) meta.appendChild(el("div", "kd-paper__authors", p.authors));
          if (p.category) {
            var tags = el("div", "kd-paper__tags");
            tags.appendChild(el("span", "kd-tag", p.category));
            meta.appendChild(tags);
          }
          row.appendChild(check); row.appendChild(meta);
          row.addEventListener("click", function () {
            if (selected[p.filename]) { delete selected[p.filename]; row.classList.remove("is-selected"); check.textContent = ""; }
            else { selected[p.filename] = { title: p.title }; row.classList.add("is-selected"); check.textContent = "✓"; }
            updateCount();
          });
          citeListEl.appendChild(row);
        });
      });
  }

  function renderChips() {
    if (!attachRow) return;
    attachRow.innerHTML = "";
    Object.keys(selected).forEach(function (fn) {
      var chip = el("span", "kd-chip kd-chip--paper");
      chip.appendChild(el("span", "kd-chip__name", selected[fn].title));
      var x = el("button", "kd-chip__x", "✕");
      x.addEventListener("click", function () { delete selected[fn]; renderChips(); updateCount(); });
      chip.appendChild(x);
      attachRow.appendChild(chip);
    });
  }

  var citeOpenBtn = document.getElementById("kd-cite-open");
  if (citeOpenBtn) citeOpenBtn.addEventListener("click", openCite);
  ["kd-cite-cancel", "kd-cite-cancel2"].forEach(function (id) {
    var b = document.getElementById(id);
    if (b) b.addEventListener("click", closeCite);
  });
  var citeAdd = document.getElementById("kd-cite-add");
  if (citeAdd) citeAdd.addEventListener("click", function () { renderChips(); closeCite(); });
  if (citeSearch) citeSearch.addEventListener("input", function () { loadPapers(citeSearch.value); });
  if (citeOverlay) citeOverlay.addEventListener("click", function (e) { if (e.target === citeOverlay) closeCite(); });
})();
