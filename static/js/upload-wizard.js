/* =============================================================
   Keydion · Paper-submission wizard
   Single IIFE module. Reads window.WIZARD_BOOT on init.
   ============================================================= */
(function () {
  'use strict';

  if (!window.WIZARD_BOOT) {
    console.error('[upload-wizard] WIZARD_BOOT missing; aborting.');
    return;
  }
  const BOOT = window.WIZARD_BOOT;
  const I18N = BOOT.i18n || {};

  // ─── i18n helper ───────────────────────────────────────────
  function t(key, fallback, vars) {
    let s = (I18N[key] != null ? I18N[key] : fallback) || '';
    if (vars) {
      Object.keys(vars).forEach(k => {
        s = s.replace('%(' + k + ')s', vars[k]);
      });
    }
    return s;
  }

  // ─── State ─────────────────────────────────────────────────
  const fd = BOOT.form_data || {};
  const state = {
    paperType: fd.is_ib_ee ? 'ee' : (fd.is_cp_paper ? 'cp' : (fd.title ? 'standard' : '')),
    title: fd.title || '',
    language: fd.language || '',
    category: fd.category || '',
    keywords: parseKeywords(fd.keywords),
    abstract: fd.abstract || '',
    isIbSample: !!fd.is_ib_sample,
    authors: parseAuthors(fd),
    // EE
    eeCoreSubject: fd.ib_ee_core_subject || '',
    eeInterSubject: fd.ib_ee_interdisciplinary_subject || '',
    eeScores: {
      A: fd.ib_crit_A_score || '', B: fd.ib_crit_B_score || '',
      C: fd.ib_crit_C_score || '', D: fd.ib_crit_D_score || '',
      E: fd.ib_crit_E_score || '',
    },
    eeIncludeComments: !!(fd.ib_crit_A_comment || fd.ib_crit_B_comment || fd.ib_crit_C_comment || fd.ib_crit_D_comment || fd.ib_crit_E_comment || fd.ib_holistic_comment),
    eeComments: {
      A: fd.ib_crit_A_comment || '', B: fd.ib_crit_B_comment || '',
      C: fd.ib_crit_C_comment || '', D: fd.ib_crit_D_comment || '',
      E: fd.ib_crit_E_comment || '', holistic: fd.ib_holistic_comment || '',
    },
    // CP
    cpGlobalContext: fd.cp_global_context || '',
    cpActionTypes: Array.isArray(fd.cp_action_types) ? fd.cp_action_types.slice() : [],
    cpScores: {
      A: fd.cp_crit_A_score || '', B: fd.cp_crit_B_score || '',
      C: fd.cp_crit_C_score || '', D: fd.cp_crit_D_score || '',
    },
    file: null,           // wizard tracks {name, size} only; real input lives in #uploadFormFile
    step: 0,
    visitedSteps: new Set([0]),
    lastModified: Date.now(),
  };

  function parseKeywords(raw) {
    if (!raw) return [];
    if (Array.isArray(raw)) return raw.slice();
    return raw.split(',').map(s => s.trim()).filter(Boolean);
  }
  function parseAuthors(fd) {
    const names = (fd.author_name || '').split(',').map(s => s.trim()).filter(Boolean);
    const emails = (fd.author_email || '').split(',').map(s => s.trim());
    const schools = (fd.author_school || '').split(',').map(s => s.trim());
    if (names.length === 0) return [{ name: '', email: '', school: '' }];
    return names.map((n, i) => ({
      name: n, email: emails[i] || '', school: schools[i] || ''
    }));
  }

  // ─── Step shape (dynamic per type / IB Sample) ─────────────
  function getSteps() {
    const steps = [{ id: 'type', name: t('step_name_type', 'Paper Type') }];
    if (!state.paperType) return steps;
    steps.push({ id: 'metadata', name: t('step_name_metadata', 'Metadata') });
    if (!state.isIbSample) {
      steps.push({ id: 'authors', name: t('step_name_authors', 'Authors') });
    }
    steps.push({ id: 'file', name: t('step_name_file', 'File') });
    steps.push({ id: 'review', name: t('step_name_review', 'Review') });
    return steps;
  }

  // ─── DOM refs ──────────────────────────────────────────────
  let stepperEl, stepsContainer, footerEl, autosaveEl;

  // ─── Render orchestration ──────────────────────────────────
  function render() {
    renderStepper();
    renderStep();
    renderFooter();
  }

  function renderStepper() {
    const steps = getSteps();
    stepperEl.innerHTML = '';
    steps.forEach((step, idx) => {
      const btn = document.createElement('button');
      btn.type = 'button';
      btn.className = 'wizard-step';
      if (idx === state.step) btn.classList.add('is-current');
      if (idx < state.step) btn.classList.add('is-done');
      btn.innerHTML = `
        <span class="wizard-step__num">${idx < state.step ? '✓' : idx + 1}</span>
        <span class="wizard-step__label">
          <span class="wizard-step__crumb">${t('step_label', 'Step %(n)s', { n: idx + 1 })}</span>
          <span class="wizard-step__name">${esc(step.name)}</span>
        </span>
      `;
      btn.addEventListener('click', () => {
        if (state.visitedSteps.has(idx) || idx <= state.step) goToStep(idx);
      });
      stepperEl.appendChild(btn);
    });
  }

  function renderStep() {
    const step = getSteps()[state.step];
    if (!step) { state.step = 0; renderStep(); return; }
    let html = '';
    switch (step.id) {
      case 'type': html = '<div class="wizard-card"><p>Step 1 placeholder</p></div>'; break;
      case 'metadata': html = '<div class="wizard-card"><p>Step 2 placeholder</p></div>'; break;
      case 'authors': html = '<div class="wizard-card"><p>Step 3 placeholder</p></div>'; break;
      case 'file': html = '<div class="wizard-card"><p>Step 4 placeholder</p></div>'; break;
      case 'review': html = '<div class="wizard-card"><p>Step 5 placeholder</p></div>'; break;
    }
    stepsContainer.innerHTML = html;
    // bindStep(step.id) — added in later tasks
  }

  function renderFooter() {
    const steps = getSteps();
    const isLast = state.step === steps.length - 1;
    const isFirst = state.step === 0;
    const nextLabel = isLast ? t('submit_paper', 'Submit Paper') : t('continue', 'Continue →');
    footerEl.innerHTML = `
      <div class="wizard-footer__left">
        ${!isFirst ? `<button type="button" class="btn btn--ghost" id="backBtn">${t('back', '← Back')}</button>` : ''}
      </div>
      <div class="wizard-footer__right">
        <button type="button" class="btn btn--text" id="saveBtn">${t('save_draft', 'Save Draft')}</button>
        <button type="button" class="btn btn--primary" id="nextBtn" ${state.step === 0 && !state.paperType ? 'disabled' : ''}>${nextLabel}</button>
      </div>
    `;
    const back = footerEl.querySelector('#backBtn');
    if (back) back.addEventListener('click', () => goToStep(state.step - 1));
    const next = footerEl.querySelector('#nextBtn');
    if (next) next.addEventListener('click', () => {
      if (isLast) { /* submit hooked up in later task */ }
      else goToStep(state.step + 1);
    });
    // saveBtn hooked up in Task 14
  }

  function goToStep(idx) {
    const steps = getSteps();
    if (idx < 0 || idx >= steps.length) return;
    state.step = idx;
    state.visitedSteps.add(idx);
    render();
    const main = document.getElementById('dashboardMain');
    if (main) main.scrollTo({ top: 0, behavior: 'smooth' });
  }

  // ─── Helpers ───────────────────────────────────────────────
  function esc(s) {
    if (s == null) return '';
    return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]);
  }
  function formatBytes(b) {
    if (b < 1024) return b + ' B';
    if (b < 1024 * 1024) return (b / 1024).toFixed(1) + ' KB';
    return (b / 1024 / 1024).toFixed(2) + ' MB';
  }

  // ─── Init ──────────────────────────────────────────────────
  function init() {
    stepperEl = document.getElementById('wizardStepper');
    stepsContainer = document.getElementById('wizardSteps');
    footerEl = document.getElementById('wizardFooter');
    autosaveEl = document.getElementById('autosaveIndicator');
    if (!stepperEl || !stepsContainer || !footerEl) {
      console.error('[upload-wizard] mount points missing');
      return;
    }
    render();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  // Expose for debugging only
  window.__uploadWizard = { state, goToStep, render };
})();
