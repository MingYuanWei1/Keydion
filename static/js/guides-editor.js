/* Keydion guides editor wiring.
   Owns: Quill init for EN/ZH editor cards, image upload, slug auto-suggest,
   published toggle, status pill, dirty tracker, Preview, Delete, callout/figure blots.
   Tasks add features incrementally — this baseline is feature-equivalent to
   the inline init that lived in guide_publish.html before Task 12.        */
(function () {
  var panel = document.querySelector('.kd-panel');
  if (!panel) return; /* not on the publish page */

  var uploadUrl = panel.dataset.uploadImageUrl;
  var CSRF = document.querySelector('meta[name="csrf-token"]')?.content || '';

  /* ─── Callout blot ─────────────────────────────────────────────── */
  var BlockEmbed = Quill.import('blots/block/embed');

  function escapeHtml(s) {
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function CalloutBlot() { BlockEmbed.apply(this, arguments); }
  CalloutBlot.prototype = Object.create(BlockEmbed.prototype);
  CalloutBlot.prototype.constructor = CalloutBlot;
  CalloutBlot.blotName = 'callout';
  CalloutBlot.tagName = 'div';
  CalloutBlot.className = 'kd-callout';
  CalloutBlot.create = function (value) {
    var node = BlockEmbed.create.call(this);
    node.setAttribute('class', 'kd-callout');
    var label = document.createElement('div');
    label.className = 'kd-callout-label';
    label.setAttribute('contenteditable', 'true');
    label.textContent = (value && value.label) || 'Note';
    var body = document.createElement('div');
    body.className = 'kd-callout-body';
    body.setAttribute('contenteditable', 'true');
    body.innerHTML = '<p>' + escapeHtml((value && value.body) || 'Type your callout here.') + '</p>';
    node.appendChild(label);
    node.appendChild(body);
    return node;
  };
  CalloutBlot.value = function (node) {
    var lbl = node.querySelector('.kd-callout-label');
    var bdy = node.querySelector('.kd-callout-body');
    return {
      label: lbl ? lbl.textContent.trim() : '',
      body: bdy ? bdy.textContent.trim() : '',
    };
  };
  Quill.register(CalloutBlot);

  /* ─── Figure blot ──────────────────────────────────────────────── */
  function isSafeImageSrc(src) {
    if (!src) return false;
    /* Same-origin /static/uploads/guides/... — relative URL */
    if (src.indexOf('/static/uploads/guides/') === 0) return true;
    /* Otherwise must be https:// */
    try {
      var u = new URL(src, window.location.origin);
      return u.protocol === 'https:';
    } catch (e) { return false; }
  }

  function FigureBlot() { BlockEmbed.apply(this, arguments); }
  FigureBlot.prototype = Object.create(BlockEmbed.prototype);
  FigureBlot.prototype.constructor = FigureBlot;
  FigureBlot.blotName = 'figure';
  FigureBlot.tagName = 'div';
  FigureBlot.className = 'kd-fig';
  FigureBlot.create = function (value) {
    var node = BlockEmbed.create.call(this);
    node.setAttribute('class', 'kd-fig');
    var src = (value && isSafeImageSrc(value.src)) ? value.src : '';
    var num = (value && value.num) || 'Fig.';
    var cap = (value && value.caption) || '';
    if (src) {
      var img = document.createElement('img');
      img.className = 'kd-fig-img';
      img.src = src;
      img.alt = cap;
      node.appendChild(img);
    } else {
      var placeholder = document.createElement('div');
      placeholder.className = 'kd-fig-img';
      placeholder.style.cssText = 'height:200px;display:flex;align-items:center;justify-content:center;color:var(--muted-2);font-family:var(--mono);font-size:11px;letter-spacing:.08em;text-transform:uppercase;background:var(--cream-2);';
      placeholder.textContent = 'No image';
      node.appendChild(placeholder);
    }
    var caption = document.createElement('div');
    caption.className = 'kd-fig-caption';
    caption.setAttribute('contenteditable', 'true');
    var numSpan = document.createElement('span');
    numSpan.className = 'num';
    numSpan.textContent = num;
    var capSpan = document.createElement('span');
    capSpan.className = 'caption-text';
    capSpan.textContent = cap;
    caption.appendChild(numSpan);
    caption.appendChild(document.createTextNode(' '));
    caption.appendChild(capSpan);
    node.appendChild(caption);
    return node;
  };
  FigureBlot.value = function (node) {
    var img = node.querySelector('img.kd-fig-img');
    var numSpan = node.querySelector('.kd-fig-caption .num');
    var capSpan = node.querySelector('.kd-fig-caption .caption-text');
    return {
      src: img ? img.getAttribute('src') : '',
      num: numSpan ? numSpan.textContent.trim() : 'Fig.',
      caption: capSpan ? capSpan.textContent.trim() : '',
    };
  };
  Quill.register(FigureBlot);

  var toolbar = [
    [{ header: [1, 2, 3, false] }],
    ['bold', 'italic', 'underline', 'strike'],
    [{ list: 'ordered' }, { list: 'bullet' }],
    ['blockquote', 'code-block'],
    ['link', 'image'],
    ['callout', 'figure'],
    ['clean'],
  ];

  function makeEditor(elId, hiddenId) {
    var hidden = document.getElementById(hiddenId);
    var editor = new Quill('#' + elId, { theme: 'snow', modules: { toolbar: toolbar } });
    if (hidden.value) editor.clipboard.dangerouslyPasteHTML(0, hidden.value);
    editor.getModule('toolbar').addHandler('image', function () {
      var input = document.createElement('input');
      input.type = 'file';
      input.accept = 'image/png,image/jpeg,image/gif,image/webp';
      input.onchange = function () {
        var file = input.files && input.files[0];
        if (!file) return;
        var fd = new FormData(); fd.append('file', file);
        fetch(uploadUrl, { method: 'POST', headers: { 'X-CSRFToken': CSRF }, body: fd })
          .then(function (r) { return r.json(); })
          .then(function (data) {
            if (data.url) {
              var range = editor.getSelection(true);
              editor.insertEmbed(range.index, 'image', data.url, 'user');
              editor.setSelection(range.index + 1);
            } else { alert(data.error || 'Upload failed'); }
          })
          .catch(function () { alert('Image upload failed.'); });
      };
      input.click();
    });
    editor.getModule('toolbar').addHandler('callout', function () {
      var range = editor.getSelection(true);
      editor.insertEmbed(range.index, 'callout',
        { label: 'Note', body: 'Type your callout here.' }, 'user');
      editor.setSelection(range.index + 1);
    });
    editor.getModule('toolbar').addHandler('figure', function () {
      var input = document.createElement('input');
      input.type = 'file';
      input.accept = 'image/png,image/jpeg,image/gif,image/webp';
      input.onchange = function () {
        var file = input.files && input.files[0];
        if (!file) return;
        var fd = new FormData(); fd.append('file', file);
        fetch(uploadUrl, { method: 'POST', headers: { 'X-CSRFToken': CSRF }, body: fd })
          .then(function (r) { return r.json(); })
          .then(function (data) {
            if (!data.url) { alert(data.error || 'Upload failed'); return; }
            var caption = window.prompt('Figure caption (optional):', '') || '';
            var num = window.prompt('Figure label:', 'Fig. 01') || 'Fig.';
            var range = editor.getSelection(true);
            editor.insertEmbed(range.index, 'figure',
              { src: data.url, num: num, caption: caption }, 'user');
            editor.setSelection(range.index + 1);
          })
          .catch(function () { alert('Image upload failed.'); });
      };
      input.click();
    });
    editor.on('text-change', function () { hidden.value = editor.root.innerHTML; });
    return { editor: editor, hidden: hidden };
  }

  var pairEn = makeEditor('editorEn', 'bodyEnField');
  var pairZh = makeEditor('editorZh', 'bodyZhField');

  document.querySelectorAll('button.ql-callout').forEach(function (btn) {
    btn.setAttribute('title', 'Insert callout');
    btn.innerHTML = '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="12" height="10"/><line x1="2" y1="6" x2="14" y2="6"/><circle cx="5" cy="9" r="0.6" fill="currentColor"/></svg>';
  });

  document.querySelectorAll('button.ql-figure').forEach(function (btn) {
    btn.setAttribute('title', 'Insert figure');
    btn.innerHTML = '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="12" height="10"/><circle cx="6" cy="7" r="1.2"/><path d="M2 11 L6 8 L9 10 L14 6"/></svg>';
  });

  document.getElementById('guideForm').addEventListener('submit', function () {
    pairEn.hidden.value = pairEn.editor.root.innerHTML;
    pairZh.hidden.value = pairZh.editor.root.innerHTML;
  });

  /* Slug auto-suggest from EN title (preserved verbatim from old inline init). */
  var slugInput = document.getElementById('slugInput');
  var titleEn = document.querySelector('input[name="title_en"]');
  titleEn.addEventListener('blur', function () {
    if (!slugInput.value && titleEn.value) {
      slugInput.value = titleEn.value.toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
    }
  });

  /* ─── Per-language status pill ───────────────────────────────────── */
  function updateStatus(lang, pair) {
    var card = document.querySelector('.kd-editor-card[data-lang="' + lang + '"]');
    if (!card) return;
    var dot = card.querySelector('[data-status] .dot');
    var label = card.querySelector('[data-status-label]');
    var titleInput = card.querySelector('input[name="title_' + lang + '"]');
    var summaryInput = card.querySelector('input[name="summary_' + lang + '"]');
    var bodyText = pair.editor.getText().trim();
    var msg, ok;
    if (!titleInput.value.trim()) { msg = 'Title missing'; ok = false; }
    else if (!summaryInput.value.trim()) { msg = 'Summary missing'; ok = false; }
    else if (!bodyText) { msg = 'Body missing'; ok = false; }
    else { msg = 'All fields filled'; ok = true; }
    label.textContent = msg;
    dot.style.background = ok ? '#2a9d5f' : '#c98a1a';
  }

  ['en', 'zh'].forEach(function (lang) {
    var pair = (lang === 'en') ? pairEn : pairZh;
    var card = document.querySelector('.kd-editor-card[data-lang="' + lang + '"]');
    if (!card) return;
    var inputs = card.querySelectorAll('input[data-required]');
    inputs.forEach(function (inp) {
      inp.addEventListener('input', function () { updateStatus(lang, pair); });
    });
    pair.editor.on('text-change', function () { updateStatus(lang, pair); });
    updateStatus(lang, pair); /* initial */
  });

  /* ─── Dirty tracker + beforeunload ───────────────────────────────── */
  var form = document.getElementById('guideForm');
  var dirtyEl = document.querySelector('[data-dirty-state]');

  function snapshot() {
    var fd = new FormData(form);
    var parts = [];
    fd.forEach(function (v, k) { parts.push(k + '=' + v); });
    parts.push('__body_en=' + pairEn.editor.root.innerHTML);
    parts.push('__body_zh=' + pairZh.editor.root.innerHTML);
    return parts.join('|');
  }

  var initial = snapshot();
  var isDirty = false;

  function beforeUnloadHandler(e) {
    e.preventDefault();
    e.returnValue = '';
    return '';
  }

  function checkDirty() {
    var now = snapshot();
    var nextDirty = (now !== initial);
    if (nextDirty === isDirty) return;
    isDirty = nextDirty;
    if (isDirty) {
      dirtyEl.textContent = '● Unsaved changes';
      dirtyEl.style.color = 'var(--accent)';
      window.addEventListener('beforeunload', beforeUnloadHandler);
    } else {
      dirtyEl.textContent = 'All changes saved';
      dirtyEl.style.color = '';
      window.removeEventListener('beforeunload', beforeUnloadHandler);
    }
  }

  form.addEventListener('input', checkDirty);
  form.addEventListener('change', checkDirty);
  pairEn.editor.on('text-change', checkDirty);
  pairZh.editor.on('text-change', checkDirty);

  form.addEventListener('submit', function () {
    window.removeEventListener('beforeunload', beforeUnloadHandler);
  });

  /* ─── Published toggle ───────────────────────────────────────────── */
  var toggle = document.querySelector('[data-toggle-published]');
  var publishedCheck = document.getElementById('publishedCheck');
  if (toggle && publishedCheck) {
    toggle.addEventListener('click', function (e) {
      e.preventDefault();
      var nowOn = !publishedCheck.checked;
      publishedCheck.checked = nowOn;
      toggle.classList.toggle('on', nowOn);
      var statusEl = toggle.querySelector('.kd-toggle-status');
      if (statusEl) statusEl.textContent = nowOn ? 'Live' : 'Draft';
      checkDirty();
    });
  }

  /* ─── Preview button ─────────────────────────────────────────────── */
  var previewBtn = document.querySelector('[data-preview-guide]');
  if (previewBtn) {
    previewBtn.addEventListener('click', function () {
      /* Sync editors into hidden fields first */
      pairEn.hidden.value = pairEn.editor.root.innerHTML;
      pairZh.hidden.value = pairZh.editor.root.innerHTML;
      var transient = document.createElement('form');
      transient.method = 'POST';
      transient.action = panel.dataset.previewUrl;
      transient.target = '_blank';
      var fd = new FormData(form);
      fd.forEach(function (v, k) {
        var input = document.createElement('input');
        input.type = 'hidden';
        input.name = k;
        input.value = v;
        transient.appendChild(input);
      });
      document.body.appendChild(transient);
      transient.submit();
      document.body.removeChild(transient);
    });
  }

  /* ─── Delete button ──────────────────────────────────────────────── */
  var deleteBtn = document.querySelector('[data-delete-guide]');
  var deleteForm = document.getElementById('deleteGuideForm');
  if (deleteBtn && deleteForm) {
    deleteBtn.addEventListener('click', function () {
      if (window.confirm('Delete this guide? This cannot be undone.')) {
        window.removeEventListener('beforeunload', beforeUnloadHandler);
        deleteForm.submit();
      }
    });
  }

  /* Hooks for the next tasks — exported on a namespace so each task can
     reach in without forcing another rewrite. */
  window.__guidesEditor = {
    pairs: { en: pairEn, zh: pairZh },
    uploadUrl: uploadUrl,
    panel: panel,
  };
})();
