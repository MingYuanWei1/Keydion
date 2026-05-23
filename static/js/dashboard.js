/* =============================================================
   Keydion · Dashboard partial-load logic (production)

   - Sidebar state cycle: full → icons → hidden → full
     persisted to localStorage['keydion.sidebar'].
   - Sidebar links (and anything with [data-partial-href]) fetch the
     target URL with header `X-Partial-Content: 1`. The server returns
     just the inner content (see _bare.html). We swap that into
     #dashboardMain and pushState the URL.
   - Forms submitted INSIDE the main panel are intercepted and also
     posted with the partial header, so /upload, /change-password etc.
     keep the user in the dashboard shell.
   - Back/forward buttons restore previous panel via popstate.
   ============================================================= */
(function () {
  var shell = document.querySelector('.dashboard-shell');
  var main  = document.getElementById('dashboardMain');
  if (!shell || !main) return;

  var PARTIAL_HEADER = 'X-Partial-Content';

  /* ── Sidebar state ─────────────────────────────────────────────────── */
  var STATES = ['full', 'icons', 'hidden'];
  var saved = localStorage.getItem('keydion.sidebar');
  if (saved && STATES.indexOf(saved) >= 0) shell.dataset.sidebarState = saved;

  function cycleSidebar() {
    var cur = shell.dataset.sidebarState || 'full';
    var next = STATES[(STATES.indexOf(cur) + 1) % STATES.length];
    shell.dataset.sidebarState = next;
    localStorage.setItem('keydion.sidebar', next);
  }
  document.querySelectorAll('[data-cycle-sidebar]').forEach(function (btn) {
    btn.addEventListener('click', cycleSidebar);
  });

  /* ── Active item highlight ─────────────────────────────────────────── */
  function pathOf(href) {
    try { return new URL(href, location.origin).pathname; }
    catch (e) { return href; }
  }
  function activateNavForPath(p) {
    var match = null;
    document.querySelectorAll('.nav-item[data-partial-href]').forEach(function (a) {
      var ap = pathOf(a.dataset.partialHref);
      a.classList.remove('is-active');
      if (ap === p) match = a;
    });
    if (match) match.classList.add('is-active');
  }

  /* ── Core partial loader ───────────────────────────────────────────── */
  function loadPartial(url, opts) {
    opts = opts || {};
    main.classList.add('is-swapping');

    var init = {
      method: opts.method || 'GET',
      headers: { 'X-Requested-With': 'XMLHttpRequest' },
      credentials: 'same-origin',
      redirect: 'follow'
    };
    init.headers[PARTIAL_HEADER] = '1';
    if (opts.body) init.body = opts.body;

    fetch(url, init)
      .then(function (res) {
        // If the server redirected us off the dashboard (e.g. login wall),
        // fall back to a full navigation.
        if (res.redirected && res.url && pathOf(res.url) !== pathOf(url)) {
          window.location.href = res.url;
          return null;
        }
        return res.text();
      })
      .then(function (html) {
        if (html === null) return;
        main.innerHTML = html;
        main.classList.remove('is-swapping');
        main.scrollTop = 0;

        // Run any <script> tags inside the swapped HTML.
        main.querySelectorAll('script').forEach(function (oldScript) {
          var s = document.createElement('script');
          for (var i = 0; i < oldScript.attributes.length; i++) {
            s.setAttribute(oldScript.attributes[i].name, oldScript.attributes[i].value);
          }
          s.textContent = oldScript.textContent;
          oldScript.parentNode.replaceChild(s, oldScript);
        });

        var p = pathOf(url);
        activateNavForPath(p);
        if (opts.push !== false) {
          history.pushState({ partial: url }, '', url);
        }
        document.dispatchEvent(new CustomEvent('keydion:partial-loaded', { detail: { url: url } }));
      })
      .catch(function (err) {
        main.classList.remove('is-swapping');
        main.innerHTML = '<div class="panel"><div class="panel-placeholder">' +
          '<h3>Could not load this section.</h3><p>' + (err && err.message || err) + '</p></div></div>';
      });
  }

  /* ── Link interception ─────────────────────────────────────────────── */
  document.body.addEventListener('click', function (e) {
    var a = e.target.closest('a[data-partial-href], a[data-section]');
    if (!a) return;
    if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button === 1) return;
    var href = a.getAttribute('data-partial-href') || a.getAttribute('href');
    if (!href || href.charAt(0) === '#') return;
    e.preventDefault();
    loadPartial(href);
  });

  /* ── Form submission inside the panel ──────────────────────────────── */
  document.body.addEventListener('submit', function (e) {
    var form = e.target;
    if (!main.contains(form)) return;
    if (form.hasAttribute('data-skip-partial')) return;

    e.preventDefault();
    var action = form.getAttribute('action') || location.pathname;
    var method = (form.getAttribute('method') || 'GET').toUpperCase();
    var body;
    if (method === 'GET') {
      var params = new URLSearchParams(new FormData(form));
      action += (action.indexOf('?') >= 0 ? '&' : '?') + params.toString();
    } else {
      body = new FormData(form);
    }
    loadPartial(action, { method: method, body: body });
  });

  /* ── Browser back/forward ──────────────────────────────────────────── */
  window.addEventListener('popstate', function (e) {
    var url = (e.state && e.state.partial) || location.pathname + location.search;
    loadPartial(url, { push: false });
  });

  /* ── Boot: highlight current sidebar item for first paint ──────────── */
  activateNavForPath(location.pathname);
  history.replaceState({ partial: location.pathname + location.search }, '', location.pathname + location.search);
})();
