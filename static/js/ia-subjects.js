(function () {
  var SAVE_URL = "/dashboard/admin/ia-subjects/save";
  var CSRF = document.querySelector('meta[name="csrf-token"]')?.content || '';

  function readJSON(id, fallback) {
    var el = document.getElementById(id);
    if (!el) return fallback;
    try { return JSON.parse(el.textContent); } catch (e) { return fallback; }
  }

  var groupsEl = document.getElementById('iaGroups');
  if (!groupsEl) return;
  // #iaSelBar is rendered as a sibling of #iaGroups, so selection-bar clicks
  // bubble to this host (the shared parent), not to groupsEl itself.
  var selbarHost = groupsEl.parentNode;
  var saveBar = document.getElementById('iaSaveBar');
  var saveSummary = document.getElementById('iaSaveSummary');
  var saveBtn = document.getElementById('iaSave');
  var discardBtn = document.getElementById('iaDiscard');
  var addGroupBtn = document.getElementById('iaAddGroup');
  var conflictsEl = document.getElementById('iaConflicts');
  var toastEl = document.getElementById('iaToast');

  var rootData = readJSON('iaData', { groups: [] });
  var i18n = readJSON('iaI18n', {});
  function t(k, d) { return (i18n && i18n[k]) || d; }

  var uid = 0;
  function nextUid() { return ++uid; }
  function escHtml(s) { var d = document.createElement('div'); d.textContent = (s == null ? '' : s); return d.innerHTML; }
  function attr(s) { return escHtml(s).replace(/"/g, '&quot;'); }
  function ic(name) { return '<svg class="ia-ic" aria-hidden="true"><use href="#ia-' + name + '"></use></svg>'; }
  function find(arr, u) { for (var i = 0; i < arr.length; i++) if (arr[i].uid === u) return arr[i]; return null; }
  function groupByUid(u) { return find(model.groups, u); }

  function buildModel(data) {
    return { groups: (data.groups || []).map(function (g) {
      return { uid: nextUid(), id: (typeof g.id === 'number') ? g.id : null, name: g.name || '', collapsed: false,
        bulkOpen: false,
        subjects: (g.subjects || []).map(function (s) {
          return { uid: nextUid(), name: s.name || '', original_name: s.name || '',
            collapsed: true, selected: false,
            criteria: (s.criteria || []).map(function (c) {
              return { uid: nextUid(), name: c.name || '', max: (typeof c.max === 'number') ? c.max : (parseInt(c.max, 10) || 0) };
            }) };
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
    (rootData.groups || []).forEach(function (g) { (g.subjects || []).forEach(function (s) { if (!origNow[s.name]) removed++; }); });
    var parts = [];
    if (renamed) parts.push(renamed + ' ' + t('renamed', 'renamed'));
    if (added) parts.push(added + ' ' + t('added', 'added'));
    if (removed) parts.push(removed + ' ' + t('removed', 'removed'));
    return t('unsaved', 'Unsaved changes') + (parts.length ? ' · ' + parts.join(', ') : '');
  }

  /* ── Selection (bulk delete) helpers ── */
  function allSelected() {
    var out = [];
    model.groups.forEach(function (g) { g.subjects.forEach(function (s) { if (s.selected) out.push(s); }); });
    return out;
  }

  function render() {
    groupsEl.innerHTML = '';
    renderSelBar();
    model.groups.forEach(function (g) { groupsEl.appendChild(renderGroup(g)); });
    refreshSaveBar();
    initSortables();
  }

  function renderSelBar() {
    var existing = selbarHost.querySelector('#iaSelBar');
    if (existing) existing.parentNode.removeChild(existing);
    var sel = allSelected();
    if (!sel.length) return;
    var bar = document.createElement('div');
    bar.className = 'ia-selbar';
    bar.id = 'iaSelBar';
    bar.innerHTML =
      '<span class="ia-selbar__count">' + sel.length + ' ' + escHtml(t('selected', 'selected')) + '</span>' +
      '<span class="ia-selbar__spacer"></span>' +
      '<button data-act="sel-clear">' + escHtml(t('clearSelection', 'Clear')) + '</button>' +
      '<button data-act="sel-delete">' + escHtml(t('deleteSelected', 'Delete selected')) + '</button>';
    selbarHost.insertBefore(bar, groupsEl);
  }

  function renderGroup(g) {
    var card = document.createElement('div');
    card.className = 'ia-card';
    card.dataset.guid = g.uid;
    var head = document.createElement('div');
    head.className = 'ia-ghead';
    head.innerHTML =
      '<span class="ia-grip" data-grip="group" title="' + attr(t('drag', 'Drag to reorder')) + '">' + ic('grip') + '</span>' +
      '<input class="ia-gname" value="' + attr(g.name) + '" placeholder="' + attr(t('groupName', 'Group name')) + '">' +
      '<span class="ia-count">' + g.subjects.length + '</span>' +
      '<button class="ia-iconbtn" data-act="bulk-toggle" title="' + attr(t('bulkAdd', 'Bulk add')) + '">' + ic('plus') + '</button>' +
      '<button class="ia-iconbtn" data-act="del-group" title="' + attr(t('deleteGroup', 'Delete group')) + '">' + ic('trash') + '</button>' +
      '<button class="ia-iconbtn" data-act="toggle-group" title="' + attr(t('collapse', 'Collapse / expand')) + '">' + ic(g.collapsed ? 'chev-down' : 'chev-up') + '</button>';
    card.appendChild(head);
    if (!g.collapsed) {
      var list = document.createElement('div');
      list.className = 'ia-subjects';
      g.subjects.forEach(function (s) { renderSubject(s).forEach(function (n) { list.appendChild(n); }); });
      var addRow = document.createElement('div');
      addRow.className = 'ia-addrow';
      addRow.innerHTML = ic('plus') + '<input class="ia-addinput" placeholder="' + attr(t('addSubject', 'Add a subject to this group…')) + '">';
      list.appendChild(addRow);
      card.appendChild(list);

      var bulk = document.createElement('div');
      bulk.className = 'ia-bulkpanel' + (g.bulkOpen ? ' is-open' : '');
      bulk.innerHTML =
        '<textarea class="ia-bulkbox" placeholder="' + attr(t('bulkPlaceholder', 'Paste one subject per line…')) + '"></textarea>' +
        '<div class="ia-bulkrow">' +
          '<button class="kp-btn kp-btn--primary kp-btn--sm" data-act="bulk-add-all">' + escHtml(t('addAll', 'Add all')) + '</button>' +
          '<button class="kp-btn kp-btn--ghost kp-btn--sm" data-act="bulk-cancel">' + escHtml(t('cancel', 'Cancel')) + '</button>' +
        '</div>';
      card.appendChild(bulk);
    }
    return card;
  }

  /* A subject renders as a row (header) + a body (criteria editor).
     Returns an array of nodes so SortableJS can keep them together by
     wrapping is unnecessary — both carry data-suid and we move both on drop. */
  function renderSubject(s) {
    var row = document.createElement('div');
    row.className = 'ia-subj';
    row.dataset.suid = s.uid;
    row.innerHTML =
      '<span class="ia-grip" data-grip="subject" title="' + attr(t('drag', 'Drag to reorder')) + '">' + ic('grip') + '</span>' +
      '<input class="ia-selcheck" type="checkbox" data-act="sel-toggle"' + (s.selected ? ' checked' : '') + '>' +
      '<input class="ia-name" value="' + attr(s.name) + '">' +
      '<span class="ia-summary" data-act="toggle-subj">' + ic(s.collapsed ? 'chev-down' : 'chev-up') + summaryText(s) + '</span>' +
      '<button class="ia-iconbtn" data-act="del-subj" title="' + attr(t('deleteSubject', 'Delete subject')) + '">' + ic('trash') + '</button>';

    var body = document.createElement('div');
    body.className = 'ia-subjbody' + (s.collapsed ? ' is-collapsed' : '');
    body.dataset.suidBody = s.uid;
    var html = '';
    if (!s.criteria.length) {
      html += '<span class="ia-nocrit">' + escHtml(t('noCriteria', 'no criteria yet')) + '</span>';
    } else {
      s.criteria.forEach(function (c) {
        html +=
          '<div class="ia-crit" data-cuid="' + c.uid + '">' +
            '<input class="ia-crit-name" value="' + attr(c.name) + '" placeholder="' + attr(t('criterionName', 'Criterion name')) + '">' +
            '<span class="ia-crit-maxlbl">' + escHtml(t('maxPoints', 'Max')) + '</span>' +
            '<input class="ia-crit-max" type="number" min="0" value="' + escHtml(String(c.max)) + '">' +
            '<button class="ia-iconbtn" data-act="del-crit" title="' + attr(t('deleteSubject', 'Delete')) + '">' + ic('trash') + '</button>' +
          '</div>';
      });
      html += '<span class="ia-totalmax">' + escHtml(t('totalMax', 'Total max')) + ' ' + totalMax(s) + '</span>';
    }
    html +=
      '<div class="ia-critadd">' + ic('plus') +
        '<input class="ia-critaddname" placeholder="' + attr(t('addCriterion', 'Add criterion')) + '">' +
        '<span class="ia-crit-maxlbl">' + escHtml(t('maxPoints', 'Max')) + '</span>' +
        '<input class="ia-critaddmax" type="number" min="0" value="">' +
        '<button class="ia-iconbtn" data-act="add-crit" title="' + attr(t('addCriterion', 'Add criterion')) + '">' + ic('plus') + '</button>' +
      '</div>';
    body.innerHTML = html;
    return [row, body];
  }

  function totalMax(s) {
    return s.criteria.reduce(function (sum, c) { return sum + (parseInt(c.max, 10) || 0); }, 0);
  }
  function summaryText(s) {
    if (!s.criteria.length) return ' ' + escHtml(t('noCriteria', 'no criteria yet'));
    var tpl = t('critSummary', '{n} criteria · max {m}');
    return ' ' + escHtml(tpl.replace('{n}', s.criteria.length).replace('{m}', totalMax(s)));
  }

  /* Find the subject object for any node inside a group card. */
  function subjFromNode(card, node) {
    var g = groupByUid(+card.dataset.guid); if (!g) return null;
    var host = node.closest('.ia-subj') || node.closest('.ia-subjbody');
    if (!host) return null;
    var suid = +(host.dataset.suid || host.dataset.suidBody);
    return { g: g, s: find(g.subjects, suid) };
  }

  groupsEl.addEventListener('input', function (e) {
    var el = e.target;
    var card = el.closest('.ia-card'); if (!card) return;
    if (el.classList.contains('ia-gname')) {
      var g = groupByUid(+card.dataset.guid);
      if (g) { g.name = el.value; markDirty(); }
    } else if (el.classList.contains('ia-name')) {
      var r = subjFromNode(card, el);
      if (r && r.s) { r.s.name = el.value; markDirty(); }
    } else if (el.classList.contains('ia-crit-name')) {
      var r2 = subjFromNode(card, el);
      var c = r2 && r2.s && find(r2.s.criteria, +el.closest('.ia-crit').dataset.cuid);
      if (c) { c.name = el.value; markDirty(); }
    } else if (el.classList.contains('ia-crit-max')) {
      var r3 = subjFromNode(card, el);
      var c2 = r3 && r3.s && find(r3.s.criteria, +el.closest('.ia-crit').dataset.cuid);
      if (c2) {
        c2.max = parseInt(el.value, 10) || 0;
        markDirty();
        // Live-update total-max badge + summary chip without a full re-render
        var body = el.closest('.ia-subjbody');
        var badge = body && body.querySelector('.ia-totalmax');
        if (badge) badge.textContent = t('totalMax', 'Total max') + ' ' + totalMax(r3.s);
        var sumEl = card.querySelector('.ia-subj[data-suid="' + r3.s.uid + '"] .ia-summary');
        if (sumEl) sumEl.innerHTML = ic(r3.s.collapsed ? 'chev-down' : 'chev-up') + summaryText(r3.s);
      }
    }
  });

  groupsEl.addEventListener('keydown', function (e) {
    if (e.key !== 'Enter') return;
    var card = e.target.closest('.ia-card'); if (!card) return;
    if (e.target.classList.contains('ia-addinput')) {
      e.preventDefault();
      var g = groupByUid(+card.dataset.guid);
      var name = e.target.value.trim();
      if (g && name) {
        g.subjects.push({ uid: nextUid(), name: name, original_name: null, collapsed: true, selected: false, criteria: [] });
        markDirty(); render(); focusAdd(g.uid);
      }
    } else if (e.target.classList.contains('ia-critaddname') || e.target.classList.contains('ia-critaddmax')) {
      e.preventDefault();
      addCriterion(card, e.target);
    }
  });

  function addCriterion(card, fromEl) {
    var r = subjFromNode(card, fromEl); if (!r || !r.s) return;
    var body = fromEl.closest('.ia-subjbody');
    var nameInp = body.querySelector('.ia-critaddname');
    var maxInp = body.querySelector('.ia-critaddmax');
    var nm = (nameInp.value || '').trim();
    if (!nm) return;
    r.s.criteria.push({ uid: nextUid(), name: nm, max: parseInt(maxInp.value, 10) || 0 });
    markDirty(); render();
    // Re-focus the add-criterion name field for fast entry
    var open = groupsEl.querySelector('.ia-subjbody[data-suid-body="' + r.s.uid + '"] .ia-critaddname');
    if (open) open.focus();
  }

  // Bound to selbarHost (the shared parent of #iaGroups and #iaSelBar) so the
  // selection-bar buttons — siblings of #iaGroups — are reachable too.
  selbarHost.addEventListener('click', function (e) {
    var btn = e.target.closest('[data-act]'); if (!btn) return;
    var act = btn.dataset.act;

    // Selection-bar actions live outside a card.
    if (act === 'sel-clear') { allSelected().forEach(function (s) { s.selected = false; }); render(); return; }
    if (act === 'sel-delete') {
      var sel = allSelected();
      model.groups.forEach(function (g) { g.subjects = g.subjects.filter(function (s) { return !s.selected; }); });
      if (sel.length) markDirty();
      render(); return;
    }

    var card = btn.closest('.ia-card');
    var g = card && groupByUid(+card.dataset.guid); if (!g) return;

    if (act === 'del-group') {
      if (g.subjects.length && !confirm(t('confirmDeleteGroup', 'Delete this group and all its subjects?'))) return;
      model.groups = model.groups.filter(function (x) { return x.uid !== g.uid; });
      markDirty(); render();
    } else if (act === 'toggle-group') {
      g.collapsed = !g.collapsed; render();
    } else if (act === 'bulk-toggle') {
      g.bulkOpen = !g.bulkOpen; if (g.bulkOpen) g.collapsed = false; render();
      var ta = card.querySelector('.ia-bulkbox'); if (ta) ta.focus();
    } else if (act === 'bulk-cancel') {
      g.bulkOpen = false; render();
    } else if (act === 'bulk-add-all') {
      var box = card.querySelector('.ia-bulkbox');
      var lines = (box ? box.value : '').split('\n').map(function (x) { return x.trim(); }).filter(Boolean);
      if (lines.length) {
        lines.forEach(function (nm) {
          g.subjects.push({ uid: nextUid(), name: nm, original_name: null, collapsed: true, selected: false, criteria: [] });
        });
        g.bulkOpen = false; markDirty();
      }
      render();
    } else if (act === 'sel-toggle') {
      var rs = subjFromNode(card, btn);
      if (rs && rs.s) { rs.s.selected = btn.checked; renderSelBar(); }
    } else if (act === 'toggle-subj') {
      var rt = subjFromNode(card, btn);
      if (rt && rt.s) { rt.s.collapsed = !rt.s.collapsed; render(); }
    } else if (act === 'del-subj') {
      var rd = subjFromNode(card, btn);
      if (rd && rd.s) { g.subjects = g.subjects.filter(function (x) { return x.uid !== rd.s.uid; }); markDirty(); render(); }
    } else if (act === 'add-crit') {
      addCriterion(card, btn);
    } else if (act === 'del-crit') {
      var rc = subjFromNode(card, btn);
      var cuid = +btn.closest('.ia-crit').dataset.cuid;
      if (rc && rc.s) { rc.s.criteria = rc.s.criteria.filter(function (x) { return x.uid !== cuid; }); markDirty(); render(); }
    }
  });

  function focusAdd(guid) {
    var c = groupsEl.querySelector('.ia-card[data-guid="' + guid + '"]');
    if (c) { var inp = c.querySelector('.ia-addinput'); if (inp) inp.focus(); }
  }

  /* ── Drag-reorder via SortableJS — live animation, same-group only ──
     Each subject is a header row (.ia-subj) plus a body (.ia-subjbody).
     We make ONLY the header rows draggable; on drop we re-derive order
     from the header rows and re-render so each body follows its row. */
  var sortables = [];
  function initSortables() {
    sortables.forEach(function (s) { try { s.destroy(); } catch (e) {} });
    sortables = [];
    if (!window.Sortable) return;
    var opts = {
      animation: 160, easing: 'cubic-bezier(.2,.7,.3,1)',
      ghostClass: 'ia-ghost', chosenClass: 'ia-chosen', dragClass: 'ia-drag',
      onEnd: commitOrder
    };
    sortables.push(Sortable.create(groupsEl, Object.assign({
      handle: '.ia-grip[data-grip="group"]', draggable: '.ia-card'
    }, opts)));
    groupsEl.querySelectorAll('.ia-subjects').forEach(function (list) {
      sortables.push(Sortable.create(list, Object.assign({
        handle: '.ia-grip[data-grip="subject"]', draggable: '.ia-subj', filter: '.ia-addrow'
      }, opts)));
    });
  }

  /* On drop, re-read group order and per-group subject order from the
     header rows, then re-render so each detached body re-attaches under
     its row in the new order. */
  function commitOrder() {
    var newGroups = [];
    groupsEl.querySelectorAll('.ia-card').forEach(function (card) {
      var g = groupByUid(+card.dataset.guid); if (!g) return;
      var subs = [];
      card.querySelectorAll('.ia-subj').forEach(function (rowEl) {
        var s = find(g.subjects, +rowEl.dataset.suid); if (s) subs.push(s);
      });
      if (subs.length === g.subjects.length) g.subjects = subs;
      newGroups.push(g);
    });
    if (newGroups.length === model.groups.length) model.groups = newGroups;
    markDirty();
    render();
  }

  if (addGroupBtn) addGroupBtn.addEventListener('click', function () {
    model.groups.push({ uid: nextUid(), id: null, name: t('newGroup', 'New group'), collapsed: false, bulkOpen: false, subjects: [] });
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
        return {
          name: (s.name || '').trim(),
          original_name: s.original_name,
          criteria: s.criteria.map(function (c) {
            return { name: (c.name || '').trim(), max: parseInt(c.max, 10) || 0 };
          })
        };
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
          rootData = { groups: res.d.groups };
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
