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

  // --- Markdown renderer (safe subset, XSS-proof) ---
  function renderMarkdown(src) {
    // Step 1: escape HTML in the raw source (critical XSS defence)
    var escaped = src
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");

    // Step 2: extract fenced code blocks to protect them from further processing
    var codeBlocks = [];
    escaped = escaped.replace(/```[\s\S]*?```/g, function (match) {
      var inner = match.slice(3, -3).replace(/^[^\n]*\n?/, ""); // strip language tag line
      codeBlocks.push("<pre><code>" + inner + "</code></pre>");
      return "\x00CODE" + (codeBlocks.length - 1) + "\x00";
    });

    // Step 3: extract inline code spans to protect them
    var codeSpans = [];
    escaped = escaped.replace(/`([^`]+)`/g, function (_, inner) {
      codeSpans.push("<code class=\"kd-icode\">" + inner + "</code>");
      return "\x00SPAN" + (codeSpans.length - 1) + "\x00";
    });

    // Step 4: process block-level markdown line by line
    var lines = escaped.split("\n");
    var out = [];
    var inUl = false;
    var inOl = false;

    function closeList() {
      if (inUl) { out.push("</ul>"); inUl = false; }
      if (inOl) { out.push("</ol>"); inOl = false; }
    }

    lines.forEach(function (line) {
      // fenced code block placeholder (whole line)
      if (/^\x00CODE\d+\x00$/.test(line.trim())) {
        closeList();
        out.push(line.trim());
        return;
      }
      // headings
      var hm = line.match(/^(#{1,6})\s+(.*)/);
      if (hm) {
        closeList();
        var level = Math.min(hm[1].length, 6);
        out.push("<h" + level + ">" + applyInline(hm[2]) + "</h" + level + ">");
        return;
      }
      // unordered list items
      var ulm = line.match(/^[ \t]*[-*]\s+(.*)/);
      if (ulm) {
        if (inOl) { out.push("</ol>"); inOl = false; }
        if (!inUl) { out.push("<ul>"); inUl = true; }
        out.push("<li>" + applyInline(ulm[1]) + "</li>");
        return;
      }
      // ordered list items
      var olm = line.match(/^[ \t]*\d+\.\s+(.*)/);
      if (olm) {
        if (inUl) { out.push("</ul>"); inUl = false; }
        if (!inOl) { out.push("<ol>"); inOl = true; }
        out.push("<li>" + applyInline(olm[1]) + "</li>");
        return;
      }
      // blank line — close lists, end paragraphs
      if (line.trim() === "") {
        closeList();
        out.push("<br>");
        return;
      }
      // regular paragraph line
      closeList();
      out.push("<p>" + applyInline(line) + "</p>");
    });

    closeList();

    var html = out.join("\n");

    // Step 5: restore code spans and code blocks
    html = html.replace(/\x00SPAN(\d+)\x00/g, function (_, i) { return codeSpans[+i]; });
    html = html.replace(/\x00CODE(\d+)\x00/g, function (_, i) { return codeBlocks[+i]; });

    return html;
  }

  function applyInline(text) {
    // bold: **x** or __x__
    text = text.replace(/\*\*(.+?)\*\*|__(.+?)__/g, function (_, a, b) {
      return "<strong>" + (a !== undefined ? a : b) + "</strong>";
    });
    // italic: *x* or _x_ (single, not double)
    text = text.replace(/\*([^*\n]+?)\*|_([^_\n]+?)_/g, function (_, a, b) {
      return "<em>" + (a !== undefined ? a : b) + "</em>";
    });
    // links: [text](url) — only http/https/relative; reject javascript:/data:/etc.
    text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, function (_, linkText, url) {
      var safe = /^(https?:\/\/|\/)/.test(url.trim());
      if (!safe) return linkText; // render as plain text, no anchor
      return "<a href=\"" + url.trim() + "\" target=\"_blank\" rel=\"noopener noreferrer\">" + linkText + "</a>";
    });
    // citation refs: [1], [23], etc. — standalone digit-only bracket refs
    text = text.replace(/\[(\d+)\]/g, "<sup class=\"kd-cite-ref\">[$1]</sup>");
    return text;
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

  function addUser(text, attachments) {
    if (empty) empty.style.display = "none";
    var msg = el("div", "kd-msg kd-msg--user");
    msg.appendChild(el("div", "kd-bubble", text));
    if (attachments && attachments.length) {
      var files = el("div", "kd-bubble__files");
      attachments.forEach(function (fn) {
        var chip = el("span", "kd-chip kd-chip--file kd-chip--sent");
        chip.appendChild(el("span", "kd-chip__name", fn));
        files.appendChild(chip);
      });
      msg.appendChild(files);
    }
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
    var bubble = el("div", "kd-md");
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
      var node = it.url ? el("a", "kd-source") : el("div", "kd-source kd-source--plain");
      if (it.url) node.href = it.url;
      node.appendChild(el("span", "kd-source__n", "[" + it.n + "]"));
      var meta = el("span", "kd-source__meta");
      meta.appendChild(el("span", "kd-source__title", it.title));
      if (it.authors) meta.appendChild(el("span", "kd-source__sub", it.authors));
      node.appendChild(meta);
      grid.appendChild(node);
    });
    box.appendChild(grid);
    body.querySelector(".kd-prose").appendChild(box);
  }

  function renderWebNote(body, items) {
    if (!items || !items.length) return;
    var box = el("div", "kd-webnote");
    box.appendChild(el("div", "kd-webnote__label", I18N.searched_web || "Searched the web"));
    var grid = el("div", "kd-webnote__grid");
    items.forEach(function (it) {
      var a = el("a", "kd-webnote__item");
      a.href = it.url; a.target = "_blank"; a.rel = "noopener noreferrer";
      a.appendChild(el("span", "kd-source__n", "[" + it.n + "]"));
      a.appendChild(el("span", "kd-webnote__title", it.title));
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
    var sentAttachments = Object.keys(window.__attachedDocs || {});
    addUser(q, sentAttachments);
    window.__attachedDocs = {};
    renderChips();
    input.value = ""; input.style.height = "auto";
    var ai = addAi();

    Promise.all((window.__attachUploads || []).slice()).then(function () {
      return ensureConversation();
    }).then(function (cid) {
      return fetch(BOOT.api_url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question: q, mode: mode, conversation_id: cid,
                               web: window.__webOn ? window.__webOn() : false,
                               message_attachments: sentAttachments,
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
      ai.bubble.innerHTML = renderMarkdown(ai.text);
      scroll();
    } else if (evt.type === "citations") {
      renderSources(ai.body, evt.items);
      scroll();
    } else if (evt.type === "web") {
      renderWebNote(ai.body, evt.items);
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
      .then(function (j) { 
        activeConv = j.id; 
        window.history.pushState(null, "", "/ask/" + activeConv);
        loadConversations(); 
        return activeConv; 
      });
  }

  function openConversation(id) {
    activeConv = id;
    window.history.pushState(null, "", "/ask/" + id);
    fetch("/api/conversations/" + id).then(function (r) { return r.json(); }).then(function (j) {
      window.__attachedDocs = {};
      renderChips();                                  // composer starts empty on reload
      thread.innerHTML = "";
      if (empty) { thread.appendChild(empty); empty.style.display = "none"; }
      (j.messages || []).forEach(function (m) {
        if (m.role === "user") { addUser(m.content, m.attachments || []); }
        else {
          var ai = addAi(); if (ai.typing) ai.typing.remove();
          ensureBubble(ai); ai.text = m.content; ai.bubble.innerHTML = renderMarkdown(m.content);
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
    window.__attachedDocs = {}; renderChips();
    window.history.pushState(null, "", "/ask");
    if (empty) { thread.appendChild(empty); empty.style.display = ""; }
    loadConversations();
  });
  if (railSearch) railSearch.addEventListener("input", renderConversationRail);
  loadConversations();
  if (BOOT.active_serial) {
    openConversation(BOOT.active_serial);
  }

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
  var fileInput = document.getElementById("kd-file-input");
  if (uploadItem) uploadItem.addEventListener("click", function () {
    if (attachPop) attachPop.classList.remove("is-open");
    if (fileInput) fileInput.click();
  });
  if (fileInput) fileInput.addEventListener("change", function () {
    var files = Array.prototype.slice.call(fileInput.files || []);
    fileInput.value = "";
    if (!files.length) return;
    ensureConversation().then(function (cid) {
      files.forEach(function (f) {
        window.__attachedDocs[f.name] = true;
        renderChips();
        var fd = new FormData();
        fd.append("file", f);
        fd.append("conversation_id", cid);
        var up = fetch("/api/ask/attach", { method: "POST", body: fd })
          .then(function (r) { return r.json(); })
          .then(function (j) {
            if (j && j.error) { delete window.__attachedDocs[f.name]; renderChips(); alert(j.error); }
          })
          .catch(function () { delete window.__attachedDocs[f.name]; renderChips(); })
          .then(function () {
            window.__attachUploads = window.__attachUploads.filter(function (p) { return p !== up; });
          });
        window.__attachUploads.push(up);
      });
    });
  });

  // --- web access toggle (visual only / inert) ---
  var webToggle = document.getElementById("kd-web-toggle");
  if (webToggle) webToggle.addEventListener("click", function () {
    var on = webToggle.classList.toggle("is-on");
    webToggle.setAttribute("aria-pressed", on ? "true" : "false");
  });
  window.__webOn = function () { return !!(webToggle && webToggle.classList.contains("is-on")); };

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
  var citeFiltersEl = document.getElementById("kd-cite-filters");
  var attachRow = document.getElementById("kd-attachrow");
  var selected = {};
  var allPapers = [];
  var activeFilter = "All";
  window.__selectedPapers = function () { return Object.keys(selected); };
  window.__attachedDocs = {};
  window.__attachUploads = [];   // in-flight /api/ask/attach promises

  function updateCount() {
    if (!citeCount) return;
    var n = Object.keys(selected).length;
    citeCount.textContent = n ? (n + " " + (I18N.selected || "selected")) : (I18N.select_hint || "Select papers to attach as citations");
  }

  function showPreview(paper) {
    var pane = document.querySelector(".kd-modal__preview");
    if (!pane) return;
    pane.innerHTML = "";
    var kicker = el("div", "kd-preview__kicker");
    kicker.textContent = paper.category || "";
    var title = el("h3", "kd-preview__title");
    title.textContent = paper.title || "";
    var authors = el("div", "kd-preview__authors");
    authors.textContent = paper.authors || "";
    var absLabel = el("div", "kd-preview__abslabel");
    absLabel.textContent = I18N.preview_abstract_label || "Abstract";
    var absText = el("div", "kd-preview__abstract");
    absText.textContent = paper.abstract || I18N.preview_no_abstract || "No abstract available.";
    if (paper.category) pane.appendChild(kicker);
    pane.appendChild(title);
    if (paper.authors) pane.appendChild(authors);
    pane.appendChild(absLabel);
    pane.appendChild(absText);
  }

  function resetPreview() {
    var pane = document.querySelector(".kd-modal__preview");
    if (!pane) return;
    pane.innerHTML = "";
    var empty = el("div", "kd-preview__empty");
    var icon = document.createElement("span");
    icon.innerHTML = '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>';
    var msg = el("p", null, I18N.preview_hint || "Hover a paper to preview its abstract before citing.");
    empty.appendChild(icon);
    empty.appendChild(msg);
    pane.appendChild(empty);
  }

  function renderPapers(papers) {
    if (!citeListEl) return;
    citeListEl.innerHTML = "";
    var visible = activeFilter === "All" ? papers : papers.filter(function (p) { return p.category === activeFilter; });
    visible.forEach(function (p) {
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
      row.addEventListener("mouseenter", function () { showPreview(p); });
      citeListEl.appendChild(row);
    });
  }

  function buildFilters(papers) {
    if (!citeFiltersEl) return;
    var categories = [];
    papers.forEach(function (p) {
      if (p.category && categories.indexOf(p.category) === -1) categories.push(p.category);
    });
    citeFiltersEl.innerHTML = "";
    var allBtn = el("button", "kd-filter" + (activeFilter === "All" ? " is-active" : ""), "All");
    allBtn.type = "button";
    allBtn.addEventListener("click", function () {
      activeFilter = "All";
      citeFiltersEl.querySelectorAll(".kd-filter").forEach(function (b) { b.classList.remove("is-active"); });
      allBtn.classList.add("is-active");
      renderPapers(allPapers);
    });
    citeFiltersEl.appendChild(allBtn);
    categories.forEach(function (cat) {
      var btn = el("button", "kd-filter" + (activeFilter === cat ? " is-active" : ""), cat);
      btn.type = "button";
      btn.addEventListener("click", function () {
        activeFilter = cat;
        citeFiltersEl.querySelectorAll(".kd-filter").forEach(function (b) { b.classList.remove("is-active"); });
        btn.classList.add("is-active");
        renderPapers(allPapers);
      });
      citeFiltersEl.appendChild(btn);
    });
  }

  function openCite() {
    if (attachPop) attachPop.classList.remove("is-open");
    if (citeOverlay) {
      citeOverlay.classList.add("is-open");
      activeFilter = "All";
      resetPreview();
      loadPapers("");
      updateCount();
    }
  }
  function closeCite() { if (citeOverlay) citeOverlay.classList.remove("is-open"); }

  function loadPapers(q) {
    if (!citeListEl) return;
    fetch("/api/ask/papers?q=" + encodeURIComponent(q)).then(function (r) { return r.json(); })
      .then(function (j) {
        allPapers = j.papers || [];
        activeFilter = "All";
        buildFilters(allPapers);
        renderPapers(allPapers);
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
    Object.keys(window.__attachedDocs).forEach(function (fn) {
      var chip = el("span", "kd-chip kd-chip--file");
      chip.appendChild(el("span", "kd-chip__name", fn));
      var x = el("button", "kd-chip__x", "✕");
      x.addEventListener("click", function () {
        delete window.__attachedDocs[fn];
        renderChips();
        var cid = window.__activeConv && window.__activeConv();
        if (cid) fetch("/api/ask/attach?conversation_id=" + encodeURIComponent(cid) +
                       "&filename=" + encodeURIComponent(fn), { method: "DELETE" });
      });
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
