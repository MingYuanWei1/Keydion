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
      case 'type': html = renderType(); break;
      case 'metadata': html = renderMetadata(); break;
      case 'authors': html = '<div class="wizard-card"><p>Step 3 placeholder</p></div>'; break;
      case 'file': html = '<div class="wizard-card"><p>Step 4 placeholder</p></div>'; break;
      case 'review': html = '<div class="wizard-card"><p>Step 5 placeholder</p></div>'; break;
    }
    stepsContainer.innerHTML = html;
    bindStep(step.id);
  }

  function bindStep(id) {
    if (id === 'type') bindType();
    if (id === 'metadata') bindMetadata();
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

  // ─── Step 1: Paper Type ────────────────────────────────────
  function renderType() {
    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: 1 })} · ${t('choose_paper_type', 'Choose paper type')}</div>
          <h2 class="wizard-card__title">${t('what_kind', 'What kind of paper are you submitting?')}</h2>
          <p class="wizard-card__sub">${t('what_kind_sub', "The fields you'll be asked for next depend on this. You can come back and change it before submitting.")}</p>
        </div>
        <div class="type-grid">
          ${renderTypeCard('standard',
            t('type_tag_standard', 'Independent Research'),
            t('type_title_standard', 'Standard Paper'),
            t('type_body_standard', 'A self-directed research paper, conference paper, or article that is not part of the IB Diploma framework.'),
            t('type_meta_standard', 'Title · authors · abstract · subject'))}
          ${renderTypeCard('ee',
            t('type_tag_ee', 'IB Diploma'),
            t('type_title_ee', 'Extended Essay (EE)'),
            t('type_body_ee', 'A 4,000-word IB Diploma research essay with structured criterion scores (A–E) and an EE subject from the six IB subject groups.'),
            t('type_meta_ee', 'Research Question · EE subject · criterion scores A–E'))}
          ${renderTypeCard('cp',
            t('type_tag_cp', 'IB Diploma'),
            t('type_title_cp', 'Community Project (CP)'),
            t('type_body_cp', 'An IB MYP Community Project graded against Criteria A–D, with a Global Context and a chosen type of action.'),
            t('type_meta_cp', 'Title · Global Context · type of action · criteria A–D'))}
        </div>
      </div>
    `;
  }

  function renderTypeCard(value, tag, title, body, meta) {
    const selected = state.paperType === value;
    return `
      <button type="button" class="type-card ${selected ? 'is-selected' : ''}" data-type="${value}">
        <span class="type-card__radio"></span>
        <span class="type-card__tag">${esc(tag)}</span>
        <h3 class="type-card__title">${esc(title)}</h3>
        <p class="type-card__body">${esc(body)}</p>
        <div class="type-card__meta">${esc(meta)}</div>
      </button>
    `;
  }

  function bindType() {
    stepsContainer.querySelectorAll('.type-card').forEach(card => {
      card.addEventListener('click', () => {
        state.paperType = card.dataset.type;
        if (state.paperType === 'standard') state.isIbSample = false;
        touch();
        render();
      });
    });
  }

  // ─── Step 2: Metadata ──────────────────────────────────────
  function renderMetadata() {
    const isEE = state.paperType === 'ee';
    const isCP = state.paperType === 'cp';
    const isIbType = isEE || isCP;
    const titleLabel = isEE ? t('research_question', 'Research Question') : t('paper_title', 'Paper Title');
    const titlePlaceholder = isEE
      ? t('research_question_ph', 'e.g. To what extent did monetary policy contribute to the 2008 financial crisis?')
      : t('paper_title_ph', 'Enter the complete paper title');
    const head = isEE ? t('tell_us_ee', 'Tell us about your essay')
      : isCP ? t('tell_us_cp', 'Tell us about your community project')
      : t('tell_us_std', 'Tell us about your paper');
    const sub = isIbType
      ? t('metadata_sub_ib', 'IB grading information and bibliographic details for the submission.')
      : t('metadata_sub_std', 'Bibliographic information that will appear on the public paper page.');

    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: 2 })} · ${t('paper_details', 'Paper details')}</div>
          <h2 class="wizard-card__title">${esc(head)}</h2>
          <p class="wizard-card__sub">${esc(sub)}</p>
        </div>

        <div class="section-sub">${t('bibliographic', 'Bibliographic')} <span class="req">*</span></div>
        <div class="form-grid">
          <div class="field">
            <label class="field__label" for="f-title">${esc(titleLabel)} <span class="req">*</span></label>
            <input class="input" type="text" id="f-title" value="${esc(state.title)}" placeholder="${esc(titlePlaceholder)}">
          </div>

          <div class="field field--6">
            <label class="field__label">${t('language', 'Language')} <span class="req">*</span></label>
            <div class="segmented" role="radiogroup">
              <button type="button" class="segmented__opt ${state.language === 'en' ? 'is-active' : ''}" data-lang="en">${t('english', 'English')}</button>
              <button type="button" class="segmented__opt ${state.language === 'zh' ? 'is-active' : ''}" data-lang="zh">${t('chinese', 'Chinese')}</button>
            </div>
          </div>

          <div class="field field--6">
            <label class="field__label" for="f-category">${t('subject_category', 'Subject Category')} <span class="req">*</span></label>
            <select class="select" id="f-category">
              <option value="">${t('choose_category', 'Choose a subject category…')}</option>
              ${(BOOT.paper_categories || []).map(c => {
                const value = typeof c === 'string' ? c : c.value;
                const label = typeof c === 'string' ? c : c.label;
                return `<option value="${esc(value)}" ${state.category === value ? 'selected' : ''}>${esc(label)}</option>`;
              }).join('')}
            </select>
          </div>

          ${!isIbType ? `
          <div class="field">
            <label class="field__label" for="f-keywords">${t('keywords', 'Keywords')} <span class="req">*</span></label>
            <div class="chips" id="chipsContainer">
              ${state.keywords.map((kw, i) => `<span class="chip">${esc(kw)}<button type="button" class="chip__x" data-i="${i}">\xd7</button></span>`).join('')}
              <input class="chips__input" id="f-keywords" type="text" placeholder="${state.keywords.length ? t('add_another', 'Add another…') : t('keyword_ph', 'Type a keyword and press Enter')}">
            </div>
            <div class="field__hint field__hint--inline">
              <span class="field__hint">${t('keyword_hint', 'Press Enter or comma to add. Aim for 3–6 keywords.')}</span>
              <span class="field__count">${state.keywords.length} ${t('added', 'added')}</span>
            </div>
          </div>

          <div class="field">
            <label class="field__label" for="f-abstract">${t('abstract', 'Abstract')} <span class="req">*</span></label>
            <textarea class="textarea" id="f-abstract" rows="6" placeholder="${t('abstract_ph', 'Briefly describe your research background, methods, and conclusions…')}">${esc(state.abstract)}</textarea>
            <div class="field__hint field__hint--inline">
              <span class="field__hint">${t('abstract_hint', 'A short summary that appears in search results.')}</span>
              <span class="field__count" id="abstractCount">${state.abstract.length} / 2000</span>
            </div>
          </div>
          ` : ''}

          ${isIbType ? `
            <div class="field">
              <label class="checkfield">
                <input type="checkbox" id="f-ibsample" ${state.isIbSample ? 'checked' : ''}>
                <span class="checkfield__body">
                  <span class="checkfield__title">${t('is_ib_sample', 'This is an IB Sample Paper')}</span>
                  <span class="checkfield__hint">${t('is_ib_sample_hint', 'Sample papers are reference essays without an identified author. Checking this will skip the Authors step.')}</span>
                </span>
              </label>
            </div>
          ` : ''}
        </div>

        ${isEE ? renderEEFieldset() : ''}
        ${isCP ? renderCPFieldset() : ''}
      </div>
    `;
  }

  function bindMetadata() {
    const titleEl = stepsContainer.querySelector('#f-title');
    if (titleEl) titleEl.addEventListener('input', e => { state.title = e.target.value; touch(); });

    stepsContainer.querySelectorAll('[data-lang]').forEach(b => {
      b.addEventListener('click', () => {
        state.language = b.dataset.lang;
        stepsContainer.querySelectorAll('[data-lang]').forEach(x => x.classList.toggle('is-active', x.dataset.lang === state.language));
        touch();
      });
    });

    const catEl = stepsContainer.querySelector('#f-category');
    if (catEl) catEl.addEventListener('change', e => { state.category = e.target.value; touch(); });

    const abstractEl = stepsContainer.querySelector('#f-abstract');
    const abstractCount = stepsContainer.querySelector('#abstractCount');
    if (abstractEl) abstractEl.addEventListener('input', e => {
      state.abstract = e.target.value;
      if (abstractCount) abstractCount.textContent = `${state.abstract.length} / 2000`;
      touch();
    });

    const chipsContainer = stepsContainer.querySelector('#chipsContainer');
    const chipsInput = stepsContainer.querySelector('#f-keywords');
    if (chipsInput) {
      chipsInput.addEventListener('keydown', e => {
        if (e.key === 'Enter' || e.key === ',') {
          e.preventDefault();
          const val = chipsInput.value.trim().replace(/,$/, '');
          if (val) {
            state.keywords.push(val);
            chipsInput.value = '';
            renderStep();
            const fresh = stepsContainer.querySelector('#f-keywords');
            if (fresh) fresh.focus();
            touch();
          }
        } else if (e.key === 'Backspace' && chipsInput.value === '' && state.keywords.length) {
          state.keywords.pop();
          renderStep();
          const fresh = stepsContainer.querySelector('#f-keywords');
          if (fresh) fresh.focus();
          touch();
        }
      });
    }
    if (chipsContainer) {
      chipsContainer.querySelectorAll('.chip__x').forEach(x => {
        x.addEventListener('click', () => {
          state.keywords.splice(parseInt(x.dataset.i, 10), 1);
          renderStep();
          touch();
        });
      });
    }

    const ibSampleEl = stepsContainer.querySelector('#f-ibsample');
    if (ibSampleEl) ibSampleEl.addEventListener('change', e => {
      state.isIbSample = e.target.checked;
      render();   // re-render stepper too (Authors step appears/disappears)
      touch();
    });

    if (state.paperType === 'ee') bindEEFieldset();
    if (state.paperType === 'cp') bindCPFieldset();   // hooked up in Task 11
    bindComboboxes();
  }

  // ─── EE fieldset ───────────────────────────────────────────
  function renderEEFieldset() {
    const total = sumScores(state.eeScores);
    const criteria = [
      ['A', t('crit_ee_A', 'Framework for the essay'), 6],
      ['B', t('crit_ee_B', 'Knowledge and understanding'), 6],
      ['C', t('crit_ee_C', 'Analysis and line of argument'), 6],
      ['D', t('crit_ee_D', 'Discussion and evaluation'), 8],
      ['E', t('crit_ee_E', 'Reflection'), 4],
    ];
    return `
      <div class="section-sub">${t('ee_subject', 'EE Subject')} <span class="req">*</span></div>
      <div class="form-grid">
        <div class="field field--6">
          <label class="field__label">${t('core_subject', 'Core Subject')} <span class="req">*</span></label>
          ${renderCombobox('ee-core', state.eeCoreSubject, t('select_core', 'Select a core subject…'), (BOOT.ee_subjects && BOOT.ee_subjects.groups) || [])}
        </div>
        <div class="field field--6">
          <label class="field__label">${t('inter_subject', 'Interdisciplinary Subject')} <span class="opt">${t('optional', 'Optional')}</span></label>
          ${renderCombobox('ee-inter', state.eeInterSubject, t('select_inter', 'Optional — select if applicable…'), (BOOT.ee_subjects && BOOT.ee_subjects.groups) || [])}
        </div>
      </div>

      <div class="section-sub">${t('crit_scores', 'Criterion Scores')} <span class="req">*</span></div>
      <table class="crit-table" id="eeCriteria">
        <thead><tr><th>${t('crit', 'Crit.')}</th><th>${t('criterion', 'Criterion')}</th><th style="width:140px;">${t('score', 'Score')}</th></tr></thead>
        <tbody>
          ${criteria.map(([k, name, max]) => `
            <tr>
              <td class="crit-letter">${k}</td>
              <td class="crit-name">${esc(name)}</td>
              <td class="crit-score">
                <span class="crit-score__input">
                  <input type="number" min="0" max="${max}" value="${esc(state.eeScores[k])}" data-crit="${k}" placeholder="0">
                  <span class="crit-score__max">/ ${max}</span>
                </span>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>

      <div class="total-readout">
        <div>
          <div class="total-readout__label">${t('overall_grade', 'Overall Grade')}</div>
          <div class="total-readout__sub">${t('overall_ee_sub', 'Calculated server-side from the criteria above')}</div>
        </div>
        <div class="total-readout__value"><span id="eeTotal">${total}</span><small>/ 30</small></div>
      </div>

      <div class="section-sub" style="margin-top:28px;">${t('crit_comments', 'Criterion Commentaries')} <span class="opt">${t('optional', 'Optional')}</span></div>
      <label class="checkfield">
        <input type="checkbox" id="eeIncComments" ${state.eeIncludeComments ? 'checked' : ''}>
        <span class="checkfield__body">
          <span class="checkfield__title">${t('include_comments', 'Include commentaries for all criteria')}</span>
          <span class="checkfield__hint">${t('include_comments_hint', 'Provide short remarks on each criterion plus an optional overall holistic commentary.')}</span>
        </span>
      </label>
      <div id="eeCommentsBox" class="${state.eeIncludeComments ? '' : 'is-hidden'}" style="margin-top:16px;">
        ${criteria.map(([k, name]) => `
          <div class="field" style="margin-bottom:14px;">
            <label class="field__label">${t('crit', 'Crit.')} ${k} — ${esc(name)}</label>
            <textarea class="textarea" rows="2" data-comment="${k}" placeholder="${t('crit_comment_ph', 'Commentary for Criterion %(k)s…', { k: k })}">${esc(state.eeComments[k] || '')}</textarea>
          </div>
        `).join('')}
        <div class="field">
          <label class="field__label">${t('holistic_comment', 'Holistic Commentary')} <span class="opt">${t('optional', 'Optional')}</span></label>
          <textarea class="textarea" rows="3" data-comment="holistic" placeholder="${t('holistic_ph', 'An overall holistic commentary for the essay…')}">${esc(state.eeComments.holistic || '')}</textarea>
        </div>
      </div>
    `;
  }

  function sumScores(obj) {
    return Object.values(obj).reduce((s, v) => s + (parseInt(v, 10) || 0), 0);
  }

  function bindEEFieldset() {
    stepsContainer.querySelectorAll('#eeCriteria input[data-crit]').forEach(inp => {
      inp.addEventListener('input', e => {
        const k = inp.dataset.crit;
        const max = parseInt(inp.max, 10);
        let v = parseInt(e.target.value, 10);
        if (!isNaN(v)) { if (v < 0) v = 0; if (v > max) v = max; }
        state.eeScores[k] = isNaN(v) ? '' : String(v);
        const totalEl = stepsContainer.querySelector('#eeTotal');
        if (totalEl) totalEl.textContent = sumScores(state.eeScores);
        touch();
      });
    });
    const inc = stepsContainer.querySelector('#eeIncComments');
    if (inc) inc.addEventListener('change', e => {
      state.eeIncludeComments = e.target.checked;
      const box = stepsContainer.querySelector('#eeCommentsBox');
      if (box) box.classList.toggle('is-hidden', !state.eeIncludeComments);
      touch();
    });
    stepsContainer.querySelectorAll('#eeCommentsBox textarea[data-comment]').forEach(ta => {
      ta.addEventListener('input', e => {
        state.eeComments[ta.dataset.comment] = e.target.value;
        touch();
      });
    });
  }

  // ─── CP fieldset ───────────────────────────────────────────
  function renderCPFieldset() {
    const criteria = [
      ['A', t('crit_cp_A', 'Investigating')],
      ['B', t('crit_cp_B', 'Planning')],
      ['C', t('crit_cp_C', 'Taking Action')],
      ['D', t('crit_cp_D', 'Reflecting')],
    ];
    const filled = Object.values(state.cpScores).filter(v => v !== '' && !isNaN(parseInt(v, 10)));
    const avg = filled.length ? Math.round(sumScores(state.cpScores) / 4) : 0;
    const contexts = BOOT.cp_global_contexts || [];
    const actions = BOOT.cp_action_types || [];

    return `
      <div class="section-sub">${t('global_context', 'Global Context')} <span class="req">*</span></div>
      <div class="field">
        ${renderCombobox('cp-global', state.cpGlobalContext, t('select_global', 'Select a Global Context…'),
          [{ name: t('global_contexts', 'Global Contexts'), subjects: contexts }])}
      </div>

      <div class="section-sub" style="margin-top:24px;">${t('type_of_action', 'Type of Action')} <span class="req">*</span></div>
      <div class="pill-checks">
        ${actions.map(a => `
          <label class="pill-check ${state.cpActionTypes.includes(a) ? 'is-checked' : ''}">
            <input type="checkbox" value="${esc(a)}" ${state.cpActionTypes.includes(a) ? 'checked' : ''}>${esc(a)}
          </label>
        `).join('')}
      </div>

      <div class="section-sub" style="margin-top:24px;">${t('crit_scores', 'Criterion Scores')} <span class="req">*</span></div>
      <table class="crit-table" id="cpCriteria">
        <thead><tr><th>${t('crit', 'Crit.')}</th><th>${t('criterion', 'Criterion')}</th><th style="width:140px;">${t('score', 'Score')}</th></tr></thead>
        <tbody>
          ${criteria.map(([k, name]) => `
            <tr>
              <td class="crit-letter">${k}</td>
              <td class="crit-name">${esc(name)}</td>
              <td class="crit-score">
                <span class="crit-score__input">
                  <input type="number" min="0" max="8" value="${esc(state.cpScores[k])}" data-crit="${k}" placeholder="0">
                  <span class="crit-score__max">/ 8</span>
                </span>
              </td>
            </tr>
          `).join('')}
        </tbody>
      </table>

      <div class="total-readout">
        <div>
          <div class="total-readout__label">${t('overall_grade', 'Overall Grade')}</div>
          <div class="total-readout__sub">${t('overall_cp_sub', 'Mean of the four criterion scores, rounded')}</div>
        </div>
        <div class="total-readout__value"><span id="cpTotal">${avg}</span><small>/ 8</small></div>
      </div>
    `;
  }

  function bindCPFieldset() {
    stepsContainer.querySelectorAll('#cpCriteria input[data-crit]').forEach(inp => {
      inp.addEventListener('input', e => {
        const k = inp.dataset.crit;
        let v = parseInt(e.target.value, 10);
        if (!isNaN(v)) { if (v < 0) v = 0; if (v > 8) v = 8; }
        state.cpScores[k] = isNaN(v) ? '' : String(v);
        const filled = Object.values(state.cpScores).filter(x => x !== '' && !isNaN(parseInt(x, 10)));
        const totalEl = stepsContainer.querySelector('#cpTotal');
        if (totalEl) totalEl.textContent = filled.length ? Math.round(sumScores(state.cpScores) / 4) : 0;
        touch();
      });
    });
    stepsContainer.querySelectorAll('.pill-check input[type="checkbox"]').forEach(cb => {
      cb.addEventListener('change', () => {
        const v = cb.value;
        if (cb.checked && !state.cpActionTypes.includes(v)) state.cpActionTypes.push(v);
        if (!cb.checked) state.cpActionTypes = state.cpActionTypes.filter(x => x !== v);
        cb.closest('.pill-check').classList.toggle('is-checked', cb.checked);
        touch();
      });
    });
  }

  // ─── Combobox component ────────────────────────────────────
  function renderCombobox(id, value, placeholder, groups) {
    return `
      <div class="combobox" data-cb="${id}">
        <button type="button" class="combobox__toggle ${value ? 'has-value' : ''}">
          <span class="${value ? '' : 'placeholder'}">${value ? esc(value) : esc(placeholder)}</span>
          <svg class="combobox__chevron" width="12" height="12" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.6"><path d="M3 6l5 5 5-5" stroke-linecap="round" stroke-linejoin="round"/></svg>
        </button>
        <div class="combobox__panel">
          <input class="combobox__search" type="text" placeholder="${t('search', 'Search…')}" autocomplete="off">
          <div class="combobox__list">
            ${groups.map(g => `
              <div class="combobox__group">
                ${g.name ? `<div class="combobox__group-label">${esc(g.name)}</div>` : ''}
                ${(g.subjects || []).map(s => `<button type="button" class="combobox__option ${value === s ? 'is-selected' : ''}" data-value="${esc(s)}">${esc(s)}</button>`).join('')}
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    `;
  }

  function bindComboboxes() {
    stepsContainer.querySelectorAll('.combobox').forEach(cb => {
      const id = cb.dataset.cb;
      const toggle = cb.querySelector('.combobox__toggle');
      const search = cb.querySelector('.combobox__search');
      const options = cb.querySelectorAll('.combobox__option');
      const labelEl = toggle.querySelector('span');

      toggle.addEventListener('click', (e) => {
        e.stopPropagation();
        const open = cb.classList.toggle('is-open');
        if (open) {
          stepsContainer.querySelectorAll('.combobox').forEach(other => { if (other !== cb) other.classList.remove('is-open'); });
          setTimeout(() => search && search.focus(), 50);
        }
      });
      if (search) search.addEventListener('input', () => {
        const q = search.value.toLowerCase();
        let anyMatch = 0;
        cb.querySelectorAll('.combobox__group').forEach(g => {
          let groupAny = false;
          g.querySelectorAll('.combobox__option').forEach(o => {
            const m = o.textContent.toLowerCase().includes(q);
            o.style.display = m ? '' : 'none';
            if (m) groupAny = true;
          });
          const lbl = g.querySelector('.combobox__group-label');
          if (lbl) lbl.style.display = groupAny ? '' : 'none';
          if (groupAny) anyMatch++;
        });
        let empty = cb.querySelector('.combobox__empty');
        if (anyMatch === 0) {
          if (!empty) {
            empty = document.createElement('div');
            empty.className = 'combobox__empty';
            empty.textContent = t('no_matches', 'No matches');
            cb.querySelector('.combobox__list').appendChild(empty);
          }
        } else if (empty) empty.remove();
      });

      options.forEach(opt => {
        opt.addEventListener('click', () => {
          const value = opt.dataset.value;
          if (id === 'ee-core') {
            if (state.eeInterSubject === value) state.eeInterSubject = '';
            state.eeCoreSubject = value;
          } else if (id === 'ee-inter') {
            if (state.eeCoreSubject === value) return;
            state.eeInterSubject = value;
          } else if (id === 'cp-global') {
            state.cpGlobalContext = value;
          }
          labelEl.textContent = value;
          labelEl.classList.remove('placeholder');
          toggle.classList.add('has-value');
          options.forEach(o => o.classList.toggle('is-selected', o === opt));
          cb.classList.remove('is-open');
          touch();
        });
      });
    });
  }

  document.addEventListener('click', (e) => {
    document.querySelectorAll('.combobox.is-open').forEach(cb => {
      if (!cb.contains(e.target)) cb.classList.remove('is-open');
    });
  });

  // ─── Mutation marker (used later by localStorage mirror) ───
  function touch() { state.lastModified = Date.now(); }

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
