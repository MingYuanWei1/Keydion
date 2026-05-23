/* Keydion guides editor wiring.
   Owns: Quill init for EN/ZH editor cards, image upload, slug auto-suggest,
   published toggle, status pill, dirty tracker, Preview, Delete, callout/figure blots.
   Tasks add features incrementally — this baseline is feature-equivalent to
   the inline init that lived in guide_publish.html before Task 12.        */
(function () {
  var panel = document.querySelector('.kd-panel');
  if (!panel) return; /* not on the publish page */

  var uploadUrl = panel.dataset.uploadImageUrl;
  var toolbar = [
    [{ header: [1, 2, 3, false] }],
    ['bold', 'italic', 'underline', 'strike'],
    [{ list: 'ordered' }, { list: 'bullet' }],
    ['blockquote', 'code-block'],
    ['link', 'image'],
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
    editor.on('text-change', function () { hidden.value = editor.root.innerHTML; });
    return { editor: editor, hidden: hidden };
  }

  var pairEn = makeEditor('editorEn', 'bodyEnField');
  var pairZh = makeEditor('editorZh', 'bodyZhField');

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
