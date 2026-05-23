/* Keydion guides editor wiring.
   Owns: Quill init for EN/ZH editor cards, image upload, slug auto-suggest,
   published toggle, status pill, dirty tracker, Preview, Delete, callout/figure blots.
   Tasks add features incrementally — this baseline is feature-equivalent to
   the inline init that lived in guide_publish.html before Task 12.        */
(function () {
  var panel = document.querySelector('.kd-panel');
  if (!panel) return; /* not on the publish page */

  var uploadUrl = panel.dataset.uploadImageUrl;

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

  var toolbar = [
    [{ header: [1, 2, 3, false] }],
    ['bold', 'italic', 'underline', 'strike'],
    [{ list: 'ordered' }, { list: 'bullet' }],
    ['blockquote', 'code-block'],
    ['link', 'image'],
    ['callout'],
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
        fetch(uploadUrl, { method: 'POST', body: fd })
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
    editor.on('text-change', function () { hidden.value = editor.root.innerHTML; });
    return { editor: editor, hidden: hidden };
  }

  var pairEn = makeEditor('editorEn', 'bodyEnField');
  var pairZh = makeEditor('editorZh', 'bodyZhField');

  document.querySelectorAll('button.ql-callout').forEach(function (btn) {
    btn.setAttribute('title', 'Insert callout');
    btn.innerHTML = '<svg viewBox="0 0 16 16" width="14" height="14" fill="none" stroke="currentColor" stroke-width="1.5"><rect x="2" y="3" width="12" height="10"/><line x1="2" y1="6" x2="14" y2="6"/><circle cx="5" cy="9" r="0.6" fill="currentColor"/></svg>';
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

  /* Hooks for the next tasks — exported on a namespace so each task can
     reach in without forcing another rewrite. */
  window.__guidesEditor = {
    pairs: { en: pairEn, zh: pairZh },
    uploadUrl: uploadUrl,
    panel: panel,
  };
})();
