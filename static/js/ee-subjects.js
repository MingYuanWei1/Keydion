(function () {
  var SAVE_URL = "/dashboard/admin/ee-subjects/save";
  var CSRF = document.querySelector('meta[name="csrf-token"]')?.content || '';

  function readJSON(id, fallback) {
    var el = document.getElementById(id);
    if (!el) return fallback;
    try { return JSON.parse(el.textContent); } catch (e) { return fallback; }
  }

  var groupsEl = document.getElementById('eeGroups');
  if (!groupsEl) return;
  var saveBar = document.getElementById('eeSaveBar');
  var saveSummary = document.getElementById('eeSaveSummary');
  var saveBtn = document.getElementById('eeSave');
  var discardBtn = document.getElementById('eeDiscard');
  var addGroupBtn = document.getElementById('eeAddGroup');
  var conflictsEl = document.getElementById('eeConflicts');
  var toastEl = document.getElementById('eeToast');

  var rootData = readJSON('eeData', { groups: [], interdisciplinary_subjects: [] });
  var i18n = readJSON('eeI18n', {});
  function t(k, d) { return (i18n && i18n[k]) || d; }

  var uid = 0;
  function nextUid() { return ++uid; }
  function escHtml(s) { var d = document.createElement('div'); d.textContent = (s == null ? '' : s); return d.innerHTML; }
  function ic(name) { return '<svg class="ee-ic" aria-hidden="true"><use href="#ee-' + name + '"></use></svg>'; }
  function find(arr, u) { for (var i = 0; i < arr.length; i++) if (arr[i].uid === u) return arr[i]; return null; }
  function groupByUid(u) { return find(model.groups, u); }
  function move(items, item, offset, kind, action) {
    var from = items.indexOf(item);
    var to = from + offset;
    if (from < 0 || to < 0 || to >= items.length) return;
    items.splice(to, 0, items.splice(from, 1)[0]);
    markDirty(); render();
    var selector = kind === 'group'
      ? '.ee-card[data-guid="' + item.uid + '"]'
      : '.ee-subj[data-suid="' + item.uid + '"]';
    var host = groupsEl.querySelector(selector);
    var focus = host && (host.querySelector('[data-act="' + action + '"]:not(:disabled)') ||
      host.querySelector('.ee-moves button:not(:disabled)'));
    if (focus) focus.focus();
  }

  function buildModel(data) {
    var inter = data.interdisciplinary_subjects || [];
    return { groups: (data.groups || []).map(function (g) {
      return { uid: nextUid(), id: (typeof g.id === 'number') ? g.id : null, name: g.name || '', collapsed: false,
        subjects: (g.subjects || []).map(function (name) {
          return { uid: nextUid(), name: name, original_name: name, interdisciplinary: inter.indexOf(name) !== -1 };
        }) };
    }) };
  }

  var model = buildModel(rootData);
  var dirty = false;

  function markDirty() { dirty = true; refreshSaveBar(); }
  function refreshSaveBar() {
    if (!saveBar) return;
    saveBar.style.display = dirty ? '' : 'none';
    if (dirty && saveSummary) saveSummary.textContent = summarize();
  }
  function summarize() {
    var renamed = 0, added = 0, origNow = {};
    model.groups.forEach(function (g) { g.subjects.forEach(function (s) {
      if (!s.original_name) added++; else { origNow[s.original_name] = true; if (s.original_name !== s.name) renamed++; }
    }); });
    var removed = 0;
    (rootData.groups || []).forEach(function (g) { (g.subjects || []).forEach(function (n) { if (!origNow[n]) removed++; }); });
    var parts = [];
    if (renamed) parts.push(renamed + ' ' + t('renamed', 'renamed'));
    if (added) parts.push(added + ' ' + t('added', 'added'));
    if (removed) parts.push(removed + ' ' + t('removed', 'removed'));
    return t('unsaved', 'Unsaved changes') + (parts.length ? ' · ' + parts.join(', ') : '');
  }

  function render() {
    groupsEl.innerHTML = '';
    model.groups.forEach(function (g) { groupsEl.appendChild(renderGroup(g)); });
    refreshSaveBar();
  }

  function renderGroup(g) {
    var card = document.createElement('div');
    card.className = 'ee-card';
    card.dataset.guid = g.uid;
    var head = document.createElement('div');
    head.className = 'ee-ghead';
    var groupIndex = model.groups.indexOf(g);
    var groupLabel = g.name || t('groupName', 'Group name');
    head.innerHTML =
      '<span class="ee-moves">' +
        '<button type="button" class="ee-iconbtn" data-act="move-group-up" aria-label="' + escHtml(t('moveUp', 'Move up') + ': ' + groupLabel) + '"' + (groupIndex === 0 ? ' disabled aria-disabled="true"' : '') + '>' + ic('chev-up') + '</button>' +
        '<button type="button" class="ee-iconbtn" data-act="move-group-down" aria-label="' + escHtml(t('moveDown', 'Move down') + ': ' + groupLabel) + '"' + (groupIndex === model.groups.length - 1 ? ' disabled aria-disabled="true"' : '') + '>' + ic('chev-down') + '</button>' +
      '</span>' +
      '<input class="ee-gname" value="' + escHtml(g.name).replace(/"/g, '&quot;') + '" placeholder="' + escHtml(t('groupName', 'Group name')) + '">' +
      '<span class="ee-count">' + g.subjects.length + '</span>' +
      '<button class="ee-iconbtn" data-act="del-group" title="' + escHtml(t('deleteGroup', 'Delete group')) + '">' + ic('trash') + '</button>' +
      '<button class="ee-iconbtn" data-act="toggle-group" title="' + escHtml(t('collapse', 'Collapse / expand')) + '">' + ic(g.collapsed ? 'chev-down' : 'chev-up') + '</button>';
    card.appendChild(head);
    if (!g.collapsed) {
      var list = document.createElement('div');
      list.className = 'ee-subjects';
      g.subjects.forEach(function (s) { list.appendChild(renderSubject(g, s)); });
      var addRow = document.createElement('div');
      addRow.className = 'ee-addrow';
      addRow.innerHTML = ic('plus') + '<input class="ee-addinput" placeholder="' + escHtml(t('addSubject', 'Add a subject to this group…')) + '">';
      list.appendChild(addRow);
      card.appendChild(list);
    }
    return card;
  }

  function renderSubject(g, s) {
    var row = document.createElement('div');
    row.className = 'ee-subj';
    row.dataset.suid = s.uid;
    var subjectIndex = g.subjects.indexOf(s);
    var subjectLabel = s.name || t('subject', 'Subject');
    row.innerHTML =
      '<span class="ee-moves">' +
        '<button type="button" class="ee-iconbtn" data-act="move-subj-up" aria-label="' + escHtml(t('moveUp', 'Move up') + ': ' + subjectLabel) + '"' + (subjectIndex === 0 ? ' disabled aria-disabled="true"' : '') + '>' + ic('chev-up') + '</button>' +
        '<button type="button" class="ee-iconbtn" data-act="move-subj-down" aria-label="' + escHtml(t('moveDown', 'Move down') + ': ' + subjectLabel) + '"' + (subjectIndex === g.subjects.length - 1 ? ' disabled aria-disabled="true"' : '') + '>' + ic('chev-down') + '</button>' +
      '</span>' +
      '<input class="ee-name" value="' + escHtml(s.name).replace(/"/g, '&quot;') + '">' +
      '<button class="ee-tag' + (s.interdisciplinary ? ' is-on' : '') + '" data-act="toggle-inter">' + ic(s.interdisciplinary ? 'check' : 'circle') + escHtml(t('interdisciplinary', 'Interdisciplinary')) + '</button>' +
      '<button class="ee-iconbtn" data-act="del-subj" title="' + escHtml(t('deleteSubject', 'Delete subject')) + '">' + ic('trash') + '</button>';
    return row;
  }

  groupsEl.addEventListener('input', function (e) {
    var el = e.target;
    if (el.classList.contains('ee-gname')) {
      var g = groupByUid(+el.closest('.ee-card').dataset.guid);
      if (g) { g.name = el.value; markDirty(); }
    } else if (el.classList.contains('ee-name')) {
      var card = el.closest('.ee-card');
      var g2 = groupByUid(+card.dataset.guid);
      var s = g2 && find(g2.subjects, +el.closest('.ee-subj').dataset.suid);
      if (s) { s.name = el.value; markDirty(); }
    }
  });

  groupsEl.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && e.target.classList.contains('ee-addinput')) {
      e.preventDefault();
      var g = groupByUid(+e.target.closest('.ee-card').dataset.guid);
      var name = e.target.value.trim();
      if (g && name) {
        g.subjects.push({ uid: nextUid(), name: name, original_name: null, interdisciplinary: false });
        markDirty(); render(); focusAdd(g.uid);
      }
    }
  });

  groupsEl.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-act]'); if (!btn) return;
    var card = btn.closest('.ee-card');
    var g = card && groupByUid(+card.dataset.guid); if (!g) return;
    var act = btn.dataset.act;
    if (act === 'move-group-up' || act === 'move-group-down') {
      move(model.groups, g, act === 'move-group-up' ? -1 : 1, 'group', act);
    } else if (act === 'move-subj-up' || act === 'move-subj-down') {
      var moved = find(g.subjects, +btn.closest('.ee-subj').dataset.suid);
      if (moved) move(g.subjects, moved, act === 'move-subj-up' ? -1 : 1, 'subject', act);
    } else if (act === 'del-group') {
      if (g.subjects.length && !confirm(t('confirmDeleteGroup', 'Delete this group and all its subjects?'))) return;
      model.groups = model.groups.filter(function (x) { return x.uid !== g.uid; });
      markDirty(); render();
    } else if (act === 'toggle-group') {
      g.collapsed = !g.collapsed; render();
    } else if (act === 'toggle-inter') {
      var s = find(g.subjects, +btn.closest('.ee-subj').dataset.suid);
      if (s) { s.interdisciplinary = !s.interdisciplinary; markDirty(); render(); }
    } else if (act === 'del-subj') {
      var suid = +btn.closest('.ee-subj').dataset.suid;
      g.subjects = g.subjects.filter(function (x) { return x.uid !== suid; });
      markDirty(); render();
    }
  });

  function focusAdd(guid) {
    var c = groupsEl.querySelector('.ee-card[data-guid="' + guid + '"]');
    if (c) { var inp = c.querySelector('.ee-addinput'); if (inp) inp.focus(); }
  }

  if (addGroupBtn) addGroupBtn.addEventListener('click', function () {
    model.groups.push({ uid: nextUid(), id: null, name: t('newGroup', 'New group'), collapsed: false, subjects: [] });
    markDirty(); render();
  });
  if (discardBtn) discardBtn.addEventListener('click', function () {
    model = buildModel(rootData); dirty = false;
    if (conflictsEl) conflictsEl.style.display = 'none';
    render();
  });
  if (saveBtn) saveBtn.addEventListener('click', save);

  function toPayload() {
    return { groups: model.groups.map(function (g) {
      return { id: g.id, name: (g.name || '').trim(), subjects: g.subjects.map(function (s) {
        return { name: (s.name || '').trim(), original_name: s.original_name, interdisciplinary: !!s.interdisciplinary };
      }) };
    }) };
  }

  function showToast(msg) {
    if (!toastEl) return;
    toastEl.textContent = msg; toastEl.classList.add('show');
    clearTimeout(toastEl._t); toastEl._t = setTimeout(function () { toastEl.classList.remove('show'); }, 2200);
  }

  function save() {
    if (conflictsEl) conflictsEl.style.display = 'none';
    saveBtn.disabled = true;
    fetch(SAVE_URL, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRFToken': CSRF }, body: JSON.stringify(toPayload()) })
      .then(function (r) { return r.json().then(function (d) { return { ok: r.ok, status: r.status, d: d }; }); })
      .then(function (res) {
        saveBtn.disabled = false;
        if (res.ok) {
          rootData = { groups: res.d.groups, interdisciplinary_subjects: res.d.interdisciplinary_subjects };
          model = buildModel(rootData); dirty = false; render();
          showToast(t('saved', 'Subjects saved.'));
        } else if (res.status === 409 && res.d.conflicts) {
          showConflicts(res.d.conflicts);
        } else {
          showToast(res.d.error || t('saveFailed', 'Could not save. Please try again.'));
        }
      })
      .catch(function () { saveBtn.disabled = false; showToast(t('saveFailed', 'Could not save. Please try again.')); });
  }

  function showConflicts(conflicts) {
    if (!conflictsEl) return;
    var items = conflicts.map(function (c) { return '<li>' + escHtml(c.subject) + ' — ' + c.paper_count + '</li>'; }).join('');
    conflictsEl.innerHTML = '<p>' + escHtml(t('conflictIntro', 'These subjects are still used by papers and can\'t be deleted:')) + '</p><ul>' + items + '</ul>';
    conflictsEl.style.display = '';
  }

  render();
})();
