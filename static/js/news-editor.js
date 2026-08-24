(function () {
  var form = document.getElementById('publishForm');
  if (!form || !window.Quill) return;

  var hidden = document.getElementById('bodyField');
  var csrf = document.querySelector('meta[name="csrf-token"]')?.content || '';
  var BlockEmbed = Quill.import('blots/block/embed');

  function safeImageSource(src) {
    if (!src) return '';
    try {
      var url = new URL(src, window.location.origin);
      return url.protocol === 'http:' || url.protocol === 'https:' ? src : '';
    } catch (e) { return ''; }
  }

  function FigureBlot() { BlockEmbed.apply(this, arguments); }
  FigureBlot.prototype = Object.create(BlockEmbed.prototype);
  FigureBlot.prototype.constructor = FigureBlot;
  FigureBlot.blotName = 'figure';
  FigureBlot.tagName = 'div';
  FigureBlot.className = 'news-figure';
  FigureBlot.create = function (value) {
    var node = BlockEmbed.create.call(this);
    var src = safeImageSource(value && value.src);
    var captionText = (value && value.caption) || '';
    if (src) {
      var image = document.createElement('img');
      image.src = src;
      image.alt = captionText;
      node.appendChild(image);
    }
    var caption = document.createElement('div');
    caption.className = 'news-figure-caption';
    caption.contentEditable = 'true';
    caption.textContent = captionText;
    node.appendChild(caption);
    return node;
  };
  FigureBlot.value = function (node) {
    var image = node.querySelector('img');
    var caption = node.querySelector('.news-figure-caption');
    return {
      src: image ? image.getAttribute('src') : '',
      caption: caption ? caption.textContent.trim() : '',
    };
  };

  function DividerBlot() { BlockEmbed.apply(this, arguments); }
  DividerBlot.prototype = Object.create(BlockEmbed.prototype);
  DividerBlot.prototype.constructor = DividerBlot;
  DividerBlot.blotName = 'divider';
  DividerBlot.tagName = 'hr';
  DividerBlot.className = 'news-divider';

  Quill.register(FigureBlot);
  Quill.register(DividerBlot);

  var editor = new Quill('#newsEditor', {
    theme: 'snow',
    modules: { toolbar: [
      [{ header: [1, 2, 3, false] }],
      ['bold', 'italic', 'underline', 'strike'],
      [{ list: 'ordered' }, { list: 'bullet' }],
      [{ color: [] }, { background: [] }],
      [{ align: [] }],
      ['blockquote', 'link'],
      ['figure', 'divider'],
      ['clean'],
    ] },
  });
  editor.root.setAttribute('aria-labelledby', 'newsBodyLabel');
  if (hidden.value) editor.clipboard.dangerouslyPasteHTML(0, hidden.value);

  var toolbar = editor.getModule('toolbar');
  toolbar.addHandler('divider', function () {
    var range = editor.getSelection(true);
    editor.insertEmbed(range.index, 'divider', true, 'user');
    editor.setSelection(range.index + 1);
  });
  toolbar.addHandler('figure', function () {
    var input = document.createElement('input');
    input.type = 'file';
    input.accept = 'image/png,image/jpeg,image/gif,image/webp';
    input.onchange = function () {
      var file = input.files && input.files[0];
      if (!file) return;
      var data = new FormData();
      data.append('file', file);
      fetch(form.dataset.uploadUrl, {
        method: 'POST', headers: { 'X-CSRFToken': csrf }, body: data,
      }).then(function (response) {
        return response.json().then(function (body) { return { ok: response.ok, body: body }; });
      }).then(function (result) {
        if (!result.ok || !result.body.url) {
          alert(result.body.error || form.dataset.uploadFailed); return;
        }
        var caption = window.prompt(form.dataset.imageCaption, '') || '';
        var range = editor.getSelection(true);
        editor.insertEmbed(range.index, 'figure', { src: result.body.url, caption: caption }, 'user');
        editor.setSelection(range.index + 1);
      }).catch(function () { alert(form.dataset.uploadFailed); });
    };
    input.click();
  });

  document.querySelectorAll('.ql-figure').forEach(function (button) {
    button.title = form.dataset.addImage; button.setAttribute('aria-label', form.dataset.addImage);
  });
  document.querySelectorAll('.ql-divider').forEach(function (button) {
    button.title = form.dataset.addDivider; button.setAttribute('aria-label', form.dataset.addDivider);
  });

  function syncBody() { hidden.value = editor.root.innerHTML; }
  editor.on('text-change', syncBody);
  form.addEventListener('submit', syncBody);
  document.addEventListener('keydown', function (event) {
    if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
      event.preventDefault(); document.getElementById('btnPublish').click();
    }
  });

  var coverInput = document.getElementById('coverInput');
  var coverPreview = document.getElementById('coverPreview');
  coverInput.addEventListener('change', function () {
    var file = coverInput.files && coverInput.files[0];
    if (!file) return;
    var reader = new FileReader();
    reader.onload = function (event) {
      coverPreview.innerHTML = '<img src="' + event.target.result + '" alt="' + form.dataset.coverPreview + '">';
      document.getElementById('removeImageFlag').value = '0';
    };
    reader.readAsDataURL(file);
  });
  var removeCover = document.getElementById('removeCover');
  if (removeCover) removeCover.addEventListener('click', function () {
    coverInput.value = '';
    coverPreview.innerHTML = '<span class="news-cover__empty">' + form.dataset.chooseImage + '</span>';
    document.getElementById('removeImageFlag').value = '1';
  });
})();
