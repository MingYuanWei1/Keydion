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
    paperType: fd.is_ia ? 'ia' : (fd.is_ib_ee ? 'ee' : (fd.is_cp_paper ? 'cp' : (fd.title ? 'standard' : ''))),
    title: fd.title || '',
    language: fd.language || '',
    category: fd.category || '',
    journal: fd.journal || '',
    keywords: parseKeywords(fd.keywords),
    abstract: fd.abstract || '',
    isIbSample: !!fd.is_ib_sample,
    isAnonymous: !fd.is_ib_sample && !!fd.is_anonymous,
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
    // EE auto-fill UI
    eeAutofillStatus: '',    // '' | 'loading' | 'ok' | 'partial' | 'error'
    eeAutofillMessage: '',
    // Abstract/keyword auto-fill UI (standard papers)
    metaAutofillStatus: '',  // '' | 'loading' | 'ok' | 'partial' | 'error'
    metaAutofillMessage: '',
    // CP
    cpGlobalContext: fd.cp_global_context || '',
    cpActionTypes: Array.isArray(fd.cp_action_types) ? fd.cp_action_types.slice() : [],
    cpScores: {
      A: fd.cp_crit_A_score || '', B: fd.cp_crit_B_score || '',
      C: fd.cp_crit_C_score || '', D: fd.cp_crit_D_score || '',
    },
    // IA
    iaSubject: fd.ia_subject || '',
    iaScores: parseIndexed(fd, 'ia_crit_', '_score'),     // { 0: '3', 1: '', ... }
    iaComments: parseIndexed(fd, 'ia_crit_', '_comment'),
    iaHolistic: fd.ia_holistic_comment || '',
    // IA auto-fill UI
    iaAutofillStatus: '',
    iaAutofillMessage: '',
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
  function parseIndexed(fd, prefix, suffix) {
    const out = {};
    Object.keys(fd || {}).forEach(key => {
      if (key.startsWith(prefix) && key.endsWith(suffix)) {
        const i = key.slice(prefix.length, key.length - suffix.length);
        if (/^\d+$/.test(i)) out[i] = fd[key];
      }
    });
    return out;
  }

  // ─── Step shape (dynamic per type / IB Sample) ─────────────
  function getSteps() {
    const steps = [{ id: 'type', name: t('step_name_type', 'Paper Type') }];
    if (!state.paperType) return steps;
    steps.push({ id: 'metadata', name: t('step_name_metadata', 'Metadata') });
    steps.push({ id: 'authors', name: t('step_name_authors', 'Authors') });
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
      case 'authors': html = renderAuthors(); break;
      case 'file': html = renderFile(); break;
      case 'review': html = renderReview(); break;
    }
    stepsContainer.innerHTML = html;
    bindStep(step.id);
  }

  function bindStep(id) {
    if (id === 'type') bindType();
    if (id === 'metadata') bindMetadata();
    if (id === 'authors') bindAuthors();
    if (id === 'file') bindFile();
    if (id === 'review') bindReview();
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
      if (isLast) {
        // Submit only when nothing is missing; otherwise re-render Review so
        // the missing-fields summary updates.
        if (getMissing().length) { renderStep(); return; }
        clearLocalStorage();
        serializeToForm();
      } else {
        goToStep(state.step + 1);
      }
    });
    const save = footerEl.querySelector('#saveBtn');
    if (save) save.addEventListener('click', () => {
      autosaveSaving();
      // For Save Draft we tolerate missing fields; the server only requires a title.
      serializeToForm([['save_draft', '1']]);
    });
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
          ${renderTypeCard('ia',
            t('type_tag_ia', 'IB Diploma'),
            t('type_title_ia', 'Internal Assessment (IA)'),
            t('type_body_ia', 'A subject-specific IB Internal Assessment graded against that subject’s assessment criteria.'),
            t('type_meta_ia', 'Title · IA subject · per-criterion scores'))}
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
    const isIA = state.paperType === 'ia';
    const isIbType = isEE || isCP || isIA;
    const titleLabel = isEE ? t('research_question', 'Research Question') : t('paper_title', 'Paper Title');
    const titlePlaceholder = isEE
      ? t('research_question_ph', 'e.g. To what extent did monetary policy contribute to the 2008 financial crisis?')
      : t('paper_title_ph', 'Enter the complete paper title');
    const head = isEE ? t('tell_us_ee', 'Tell us about your essay')
      : isCP ? t('tell_us_cp', 'Tell us about your community project')
      : isIA ? t('tell_us_ia', 'Tell us about your assessment')
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

          ${!isCP ? `
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
          ` : ''}

          <div class="field field--6">
            <label class="field__label" for="f-journal">${t('journal', 'Journal')}</label>
            <select class="select" id="f-journal">
              <option value="">${t('journal_none', '— None —')}</option>
              ${(BOOT.journals || []).map(j => `<option value="${esc(j)}" ${state.journal === j ? 'selected' : ''}>${esc(j)}</option>`).join('')}
            </select>
            <div class="field__hint">${t('journal_hint', 'Optional — assign this paper to a journal.')}</div>
          </div>

          ${!isIbType ? `
          ${BOOT.llm_metadata_enabled ? `
          <div class="ee-autofill">
            <button type="button" id="metaAutofillBtn" class="btn btn-outline-primary btn-sm" ${state.metaAutofillStatus === 'loading' ? 'disabled' : ''}>
              ${t('meta_autofill_btn', 'Generate abstract & keywords from PDF')}
            </button>
            <span id="metaAutofillStatus" class="ee-autofill__status ee-autofill__status--${state.metaAutofillStatus || 'idle'}">
              ${esc(state.metaAutofillMessage || '')}
            </span>
          </div>
          ` : ''}
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

        </div>

        ${isEE ? renderEEFieldset() : ''}
        ${isCP ? renderCPFieldset() : ''}
        ${isIA ? renderIAFieldset() : ''}
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

    const jrnEl = stepsContainer.querySelector('#f-journal');
    if (jrnEl) jrnEl.addEventListener('change', e => { state.journal = e.target.value; touch(); });

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

    // ── Abstract/keyword auto-fill (standard papers) ───────────
    const metaBtn = stepsContainer.querySelector('#metaAutofillBtn');
    if (metaBtn) {
      metaBtn.addEventListener('click', () => {
        const existing = document.getElementById('uploadFormFile');
        const chosen = existing && existing.files && existing.files[0];
        if (chosen) {
          runMetaAutofill(chosen);
        } else {
          state.metaAutofillStatus = 'error';
          state.metaAutofillMessage = t('meta_autofill_no_file',
            'Upload your PDF in the File step first.');
          render();
        }
      });
    }

    if (state.paperType === 'ee') bindEEFieldset();
    if (state.paperType === 'cp') bindCPFieldset();   // hooked up in Task 11
    if (state.paperType === 'ia') bindIAFieldset();
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
      <div class="ee-autofill">
        <button type="button" id="eeAutofillBtn" class="btn btn-outline-primary btn-sm" ${state.eeAutofillStatus === 'loading' ? 'disabled' : ''}>
          ${t('ee_autofill_btn', 'Auto-fill from commentary PDF')}
        </button>
        <input type="file" id="eeAutofillFile" accept="application/pdf,.pdf" hidden>
        <span id="eeAutofillStatus" class="ee-autofill__status ee-autofill__status--${state.eeAutofillStatus || 'idle'}">
          ${esc(state.eeAutofillMessage || '')}
        </span>
      </div>

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

    // ── EE auto-fill from commentary PDF ─────────────────────────
    const autoBtn = stepsContainer.querySelector('#eeAutofillBtn');
    const autoFile = stepsContainer.querySelector('#eeAutofillFile');
    if (autoBtn && autoFile) {
      autoBtn.addEventListener('click', () => autoFile.click());
      autoFile.addEventListener('change', async (e) => {
        const file = e.target.files && e.target.files[0];
        e.target.value = ''; // allow re-selecting the same file
        if (!file) return;
        await runEEAutofill(file);
      });
    }
  }

  async function runEEAutofill(file) {
    if (state.eeAutofillStatus === 'loading') return;  // re-entrancy guard
    state.eeAutofillStatus = 'loading';
    state.eeAutofillMessage = t('ee_autofill_extracting', 'Extracting…');
    render();

    try {
      const form = new FormData();
      form.append('file', file);
      const resp = await fetch('/api/upload/extract-ee-metadata', {
        method: 'POST',
        body: form,
        credentials: 'same-origin',
      });
      const data = await resp.json().catch(() => ({}));

      if (!resp.ok) {
        state.eeAutofillStatus = 'error';
        state.eeAutofillMessage = data.error || t('ee_autofill_error',
          'Auto-fill failed — try again or fill manually.');
        render();
        return;
      }

      if (isEEDirty()) {
        const ok = window.confirm(t('ee_autofill_overwrite',
          'Replace your existing EE entries with values from the PDF?'));
        if (!ok) {
          state.eeAutofillStatus = '';
          state.eeAutofillMessage = '';
          render();
          return;
        }
      }

      applyEEAutofill(data);
      const summary = summariseAutofill(data);
      state.eeAutofillStatus = summary.status;
      state.eeAutofillMessage = summary.message;
      touch();
      render();
    } catch (err) {
      state.eeAutofillStatus = 'error';
      state.eeAutofillMessage = t('ee_autofill_error',
        'Auto-fill failed — try again or fill manually.');
      render();
    }
  }

  function isEEDirty() {
    // Note: state.title is excluded — autofill no longer writes to the title.
    if ((state.eeCoreSubject || '').trim()) return true;
    if ((state.eeInterSubject || '').trim()) return true;
    for (const k of ['A','B','C','D','E']) {
      if ((state.eeScores[k] || '').toString().trim()) return true;
      if ((state.eeComments[k] || '').trim()) return true;
    }
    if ((state.eeComments.holistic || '').trim()) return true;
    return false;
  }

  function applyEEAutofill(data) {
    // Do NOT map research_question → state.title. EE research questions
    // routinely exceed the title column's 255-char limit, and the user can
    // type a concise title manually.
    state.eeCoreSubject = data.core_subject || '';
    state.eeInterSubject = data.interdisciplinary_subject || '';
    const criteria = data.criteria || {};
    ['A','B','C','D','E'].forEach(k => {
      const crit = criteria[k] || {};
      state.eeScores[k] = (crit.score === null || crit.score === undefined) ? '' : String(crit.score);
      state.eeComments[k] = crit.comment || '';
    });
    state.eeComments.holistic = data.holistic_comment || '';
    // Auto-reveal the commentary section if anything came back for it.
    const anyComment = ['A','B','C','D','E'].some(k => state.eeComments[k])
      || !!state.eeComments.holistic;
    if (anyComment) state.eeIncludeComments = true;
  }

  function summariseAutofill(data) {
    const warnings = (data.warnings || []);
    // Count populated fields out of the maximum. The research_question is
    // intentionally NOT counted because we no longer map it onto the title.
    // Max = 1 core_subject + 5 scores + 5 comments + 1 holistic = 12
    // (+1 for interdisciplinary_subject when the form is interdisciplinary).
    const max = (data.interdisciplinary_subject ? 13 : 12);
    let filled = 0;
    if (data.core_subject) filled++;
    if (data.interdisciplinary_subject) filled++;
    if (data.holistic_comment) filled++;
    ['A','B','C','D','E'].forEach(k => {
      const crit = (data.criteria || {})[k] || {};
      if (crit.score !== null && crit.score !== undefined) filled++;
      if (crit.comment) filled++;
    });
    if (warnings.length === 0 && filled >= max) {
      return { status: 'ok', message: t('ee_autofill_ok', 'Extracted all fields.') };
    }
    const tail = warnings.length ? ' ' + warnings.join(' ') : '';
    return {
      status: 'partial',
      message: t('ee_autofill_partial',
        'Extracted %(filled)s of %(total)s fields.', { filled: String(filled), total: String(max) })
        + tail,
    };
  }

  // ─── Abstract/keyword auto-fill (standard papers) ──────────
  async function runMetaAutofill(file) {
    if (state.metaAutofillStatus === 'loading') return;   // re-entrancy guard
    state.metaAutofillStatus = 'loading';
    state.metaAutofillMessage = t('meta_autofill_extracting', 'Generating…');
    render();

    try {
      const form = new FormData();
      form.append('file', file);
      form.append('language', state.language || 'en');
      const resp = await fetch('/api/upload/generate-abstract-keywords', {
        method: 'POST',
        body: form,
        credentials: 'same-origin',
      });
      const data = await resp.json().catch(() => ({}));

      if (!resp.ok) {
        state.metaAutofillStatus = 'error';
        state.metaAutofillMessage = data.error || t('meta_autofill_error',
          'Generation failed — try again or fill manually.');
        render();
        return;
      }

      if (isMetaDirty()) {
        const ok = window.confirm(t('meta_autofill_overwrite',
          'Replace your existing abstract and keywords with AI-generated ones?'));
        if (!ok) {
          state.metaAutofillStatus = '';
          state.metaAutofillMessage = '';
          render();
          return;
        }
      }

      state.abstract = (data.abstract || '').slice(0, 2000);
      state.keywords = Array.isArray(data.keywords) ? data.keywords.slice() : [];
      if ((data.title || '').trim()) {
        state.title = data.title.trim().slice(0, 255);
      }
      if (Array.isArray(data.authors) && data.authors.length) {
        data.authors.forEach((nm, i) => {
          const name = (nm || '').trim();
          if (!name) return;
          if (!state.authors[i]) state.authors[i] = { name: '', email: '', school: '' };
          state.authors[i].name = name;
        });
      }
      const warnings = data.warnings || [];
      if (warnings.length) {
        state.metaAutofillStatus = 'partial';
        state.metaAutofillMessage = warnings.join(' ');
      } else {
        state.metaAutofillStatus = 'ok';
        state.metaAutofillMessage = t('meta_autofill_ok', 'Generated abstract and keywords.');
      }
      touch();
      render();
    } catch (err) {
      state.metaAutofillStatus = 'error';
      state.metaAutofillMessage = t('meta_autofill_error',
        'Generation failed — try again or fill manually.');
      render();
    }
  }

  function isMetaDirty() {
    if ((state.abstract || '').trim()) return true;
    if (state.keywords && state.keywords.length > 0) return true;
    if ((state.title || '').trim()) return true;
    if (state.authors && state.authors.some(a => (a.name || '').trim())) return true;
    return false;
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

  // ─── Step 3: Authors ───────────────────────────────────────
  function authorMode() {
    return state.isIbSample ? 'ibsample' : (state.isAnonymous ? 'anonymous' : 'named');
  }

  function setAuthorMode(mode) {
    state.isIbSample = mode === 'ibsample' && state.paperType !== 'standard';
    state.isAnonymous = mode === 'anonymous';
  }

  function renderAuthorModeOption(value, title, hint) {
    return `
      <label class="checkfield">
        <input type="radio" name="f-author-mode" value="${value}" ${authorMode() === value ? 'checked' : ''}>
        <span class="checkfield__body">
          <span class="checkfield__title">${esc(title)}</span>
          <span class="checkfield__hint">${esc(hint)}</span>
        </span>
      </label>
    `;
  }

  function renderAuthors() {
    const isIbType = state.paperType === 'ee' || state.paperType === 'cp';
    const mode = authorMode();
    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: 3 })} · ${t('author_info', 'Author information')}</div>
          <h2 class="wizard-card__title">${t('who_wrote', 'Who wrote this?')}</h2>
          <p class="wizard-card__sub">${t('authors_sub', "The first author's contact details are required. Add co-authors as needed.")}</p>
        </div>

        <div class="field" id="authorModeChoice" style="display:flex;flex-direction:column;gap:10px;">
          ${renderAuthorModeOption('named', t('author_mode_named', 'Named authors'), t('author_mode_named_hint', 'List the authors with their contact details.'))}
          ${renderAuthorModeOption('anonymous', t('upload_anonymous', 'Upload as anonymous'), t('upload_anonymous_hint', 'The paper is published without any author information.'))}
          ${isIbType ? renderAuthorModeOption('ibsample', t('is_ib_sample', 'This is an IB Sample Paper'), t('is_ib_sample_hint', 'Sample papers are reference essays without an identified author.')) : ''}
        </div>

        ${mode === 'named' ? `
        <div id="authorsList">
          ${state.authors.map((a, i) => `
            <div class="author-row" data-i="${i}">
              <input class="input" type="text" placeholder="${t('name', 'Name')}${i === 0 ? ' *' : ''}" value="${esc(a.name)}" data-field="name">
              <input class="input" type="email" placeholder="${t('email', 'Email')}${i === 0 ? ' *' : ''}" value="${esc(a.email)}" data-field="email">
              <input class="input" type="text" placeholder="${t('school', 'School / Institution')}${i === 0 ? ' *' : ''}" value="${esc(a.school)}" data-field="school">
              <button type="button" class="author-row__remove" data-i="${i}" ${state.authors.length === 1 ? 'disabled' : ''} aria-label="${t('remove_author', 'Remove author')}">×</button>
            </div>
          `).join('')}
        </div>

        <button type="button" class="btn-add-author" id="addAuthorBtn">${t('add_author', '+ Add another author')}</button>
        ` : ''}
      </div>
    `;
  }

  function bindAuthors() {
    stepsContainer.querySelectorAll('#authorModeChoice input[name="f-author-mode"]').forEach(radio => {
      radio.addEventListener('change', () => {
        setAuthorMode(radio.value);
        renderStep();
        touch();
      });
    });
    stepsContainer.querySelectorAll('.author-row').forEach(row => {
      const i = parseInt(row.dataset.i, 10);
      row.querySelectorAll('input').forEach(inp => {
        inp.addEventListener('input', e => {
          state.authors[i][inp.dataset.field] = e.target.value;
          touch();
        });
      });
      const rem = row.querySelector('.author-row__remove');
      rem.addEventListener('click', () => {
        if (state.authors.length > 1) {
          state.authors.splice(i, 1);
          renderStep();
          touch();
        }
      });
    });
    const add = stepsContainer.querySelector('#addAuthorBtn');
    if (add) add.addEventListener('click', () => {
      state.authors.push({ name: '', email: '', school: '' });
      renderStep();
      touch();
    });
  }

  // ─── Step 4: File ──────────────────────────────────────────
  function renderFile() {
    const fileIdx = getSteps().findIndex(s => s.id === 'file') + 1;
    return `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: fileIdx })} · ${t('file_upload', 'File upload')}</div>
          <h2 class="wizard-card__title">${t('upload_pdf', 'Upload your PDF')}</h2>
          <p class="wizard-card__sub">${t('upload_pdf_sub', 'Submit a single PDF, up to 50 MB. You can change this before publishing.')}</p>
        </div>

        <label class="filefield" id="fileLabel">
          <span class="filefield__icon">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round">
              <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6"/>
            </svg>
          </span>
          <span class="filefield__body">
            <span class="filefield__name" id="fileName">${state.file ? esc(state.file.name) : t('no_file_chosen', 'No file chosen')}</span>
            <span class="filefield__meta" id="fileMeta">${state.file ? formatBytes(state.file.size) + ' · PDF' : t('pdf_only_single', 'PDF only · single file')}</span>
          </span>
          <span class="filefield__btn">
            <span class="btn-file" id="chooseFileBtn">${state.file ? t('replace_file', 'Replace') : t('choose_file', 'Choose file')}</span>
          </span>
        </label>
        <p class="field__hint" style="margin-top:10px;">${t('file_save_hint', "If you'd like to come back to this later, click Save Draft below — your form will be restored next time you visit.")}</p>
      </div>
    `;
  }

  function bindFile() {
    const choose = stepsContainer.querySelector('#chooseFileBtn');
    const realInput = document.getElementById('uploadFormFile');
    if (choose && realInput) {
      choose.addEventListener('click', () => realInput.click());
      realInput.addEventListener('change', () => {
        const f = realInput.files && realInput.files[0];
        if (f) {
          state.file = { name: f.name, size: f.size };
          const nameEl = stepsContainer.querySelector('#fileName');
          const metaEl = stepsContainer.querySelector('#fileMeta');
          if (nameEl) nameEl.textContent = f.name;
          if (metaEl) metaEl.textContent = formatBytes(f.size) + ' · PDF';
          choose.textContent = t('replace_file', 'Replace');
          touch();
        }
      });
    }
  }

  // ─── Step 5: Review + missing-field summary ───────────────
  function getMissing() {
    const steps = getSteps();
    const stepIdx = (id) => steps.findIndex(s => s.id === id);
    const missing = [];
    if (!state.paperType) missing.push({ label: t('paper_type', 'Paper type'), step: stepIdx('type') });
    if (!state.title.trim()) missing.push({
      label: state.paperType === 'ee' ? t('research_question', 'Research question') : t('paper_title', 'Paper title'),
      step: stepIdx('metadata'),
    });
    if (!state.language) missing.push({ label: t('language', 'Language'), step: stepIdx('metadata') });
    if (!state.category && state.paperType !== 'cp') missing.push({ label: t('subject_category', 'Subject category'), step: stepIdx('metadata') });
    if (state.paperType === 'standard') {
      if (!state.keywords.length) missing.push({ label: t('keywords', 'Keywords'), step: stepIdx('metadata') });
      if (!state.abstract.trim()) missing.push({ label: t('abstract', 'Abstract'), step: stepIdx('metadata') });
    }
    if (!state.isIbSample && !state.isAnonymous) {
      const a0 = state.authors[0] || {};
      if (!a0.name || !a0.email || !a0.school) {
        missing.push({ label: t('first_author', 'First author (name, email, school)'), step: stepIdx('authors') });
      }
    }
    if (state.paperType === 'ee') {
      if (!state.eeCoreSubject) missing.push({ label: t('ee_core', 'EE core subject'), step: stepIdx('metadata') });
      Object.entries(state.eeScores).forEach(([k, v]) => {
        if (v === '' || v == null) missing.push({ label: t('ee_score_x', 'EE criterion score %(k)s', { k }), step: stepIdx('metadata') });
      });
    }
    if (state.paperType === 'cp') {
      if (!state.cpGlobalContext) missing.push({ label: t('cp_global', 'Global context'), step: stepIdx('metadata') });
      if (!state.cpActionTypes.length) missing.push({ label: t('cp_action_label', 'Type of action'), step: stepIdx('metadata') });
      Object.entries(state.cpScores).forEach(([k, v]) => {
        if (v === '' || v == null) missing.push({ label: t('cp_score_x', 'CP criterion score %(k)s', { k }), step: stepIdx('metadata') });
      });
    }
    if (!state.file) missing.push({ label: t('pdf_file', 'PDF file'), step: stepIdx('file') });
    return missing.filter(m => m.step >= 0);
  }

  function renderReview() {
    const steps = getSteps();
    const idx = (id) => steps.findIndex(s => s.id === id);
    const missing = getMissing();
    const typeName = state.paperType === 'standard' ? t('type_standard', 'Independent Research Paper')
      : state.paperType === 'ee' ? t('type_ee', 'IB Extended Essay')
      : state.paperType === 'cp' ? t('type_cp', 'IB Community Project')
      : '—';
    const langName = state.language === 'en' ? t('english', 'English')
      : state.language === 'zh' ? t('chinese', 'Chinese') : '';
    const cats = BOOT.paper_categories || [];
    const matchedCat = cats.find(c => (typeof c === 'string' ? c : c.value) === state.category);
    const categoryName = matchedCat ? (typeof matchedCat === 'string' ? matchedCat : matchedCat.label) : '';

    let html = `
      <div class="wizard-card">
        <div class="wizard-card__head">
          <div class="wizard-card__crumb">${t('step_label', 'Step %(n)s', { n: steps.length })} · ${t('review_submit', 'Review & submit')}</div>
          <h2 class="wizard-card__title">${t('almost_there', 'Almost there — review your submission')}</h2>
          <p class="wizard-card__sub">${t('review_sub', 'Make sure everything looks right. You can jump back to any section to make changes.')}</p>
        </div>

        ${missing.length ? `
          <div class="review-missing">
            <div class="review-missing__head">
              ${missing.length === 1 ? t('missing_fields_one', '1 field still needs attention') : t('missing_fields_many', '%(n)s fields still need attention', { n: missing.length })}
            </div>
            <ul>
              ${missing.map(m => `<li><strong>${esc(m.label)}</strong> — <button type="button" class="review-section__edit" data-jump="${m.step}">${t('go_to', 'go to %(step)s', { step: steps[m.step].name })}</button></li>`).join('')}
            </ul>
          </div>
        ` : `
          <div class="review-missing review-missing--clean">
            <div class="review-missing__head">${t('everything_filled', 'Everything required is filled in.')}</div>
            <p style="margin:0;font-size:13.5px;">${t('submit_cta', 'Click Submit Paper below to send your submission for review.')}</p>
          </div>
        `}

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('paper_type', 'Paper Type')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('type')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid"><dt>${t('type', 'Type')}</dt><dd>${esc(typeName)}</dd></dl>
        </div>

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('metadata_title', 'Metadata')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('metadata')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid">
            <dt>${state.paperType === 'ee' ? t('research_q_short', 'Research Q.') : t('title_short', 'Title')}</dt>
            <dd${state.title ? '' : ' class="is-missing"'}>${state.title ? esc(state.title) : t('not_provided', 'Not provided')}</dd>
            <dt>${t('language', 'Language')}</dt><dd${langName ? '' : ' class="is-missing"'}>${langName || t('not_chosen', 'Not chosen')}</dd>
            ${state.paperType !== 'cp' ? `<dt>${t('subject', 'Subject')}</dt><dd${categoryName ? '' : ' class="is-missing"'}>${categoryName || t('not_chosen', 'Not chosen')}</dd>` : ''}
            ${state.paperType === 'standard' ? `
              <dt>${t('keywords', 'Keywords')}</dt><dd${state.keywords.length ? '' : ' class="is-missing"'}>${state.keywords.length ? state.keywords.map(esc).join(', ') : t('none', 'None')}</dd>
              <dt>${t('abstract', 'Abstract')}</dt><dd${state.abstract ? '' : ' class="is-missing"'}>${state.abstract ? esc(state.abstract.slice(0, 280)) + (state.abstract.length > 280 ? '…' : '') : t('not_written', 'Not written')}</dd>
            ` : ''}
          </dl>
        </div>

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('authors', 'Authors')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('authors')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid">
            ${state.isIbSample ? `
              <dt>${t('ib_sample', 'IB Sample')}</dt><dd>${t('yes_skipped', 'Yes — author info skipped')}</dd>
            ` : state.isAnonymous ? `
              <dt>${t('author', 'Author')}</dt><dd>${t('anonymous_skipped', 'Anonymous — author info skipped')}</dd>
            ` : state.authors.map((a, i) => `
              <dt>${t('author', 'Author')} ${i + 1}</dt>
              <dd${(i === 0 && (!a.name || !a.email || !a.school)) ? ' class="is-missing"' : ''}>
                ${a.name ? esc(a.name) : '<em>name?</em>'}${a.email ? ' · ' + esc(a.email) : ''}${a.school ? ' · ' + esc(a.school) : ''}
              </dd>
            `).join('')}
          </dl>
        </div>

        ${state.paperType === 'ee' ? renderReviewEE(idx('metadata')) : ''}
        ${state.paperType === 'cp' ? renderReviewCP(idx('metadata')) : ''}

        <div class="review-section">
          <div class="review-section__head">
            <div class="review-section__title">${t('file', 'File')}</div>
            <button type="button" class="review-section__edit" data-jump="${idx('file')}">${t('edit', 'Edit')}</button>
          </div>
          <dl class="review-grid">
            <dt>PDF</dt><dd${state.file ? '' : ' class="is-missing"'}>${state.file ? esc(state.file.name) + ' · ' + formatBytes(state.file.size) : t('no_file_uploaded', 'No file uploaded')}</dd>
          </dl>
        </div>
      </div>
    `;
    return html;
  }

  function renderReviewEE(jumpIdx) {
    const total = sumScores(state.eeScores);
    return `
      <div class="review-section">
        <div class="review-section__head">
          <div class="review-section__title">${t('ee_details', 'EE Details')}</div>
          <button type="button" class="review-section__edit" data-jump="${jumpIdx}">${t('edit', 'Edit')}</button>
        </div>
        <dl class="review-grid">
          <dt>${t('core_subject', 'Core Subject')}</dt><dd${state.eeCoreSubject ? '' : ' class="is-missing"'}>${esc(state.eeCoreSubject) || t('not_chosen', 'Not chosen')}</dd>
          ${state.eeInterSubject ? `<dt>${t('inter_subject', 'Interdisciplinary')}</dt><dd>${esc(state.eeInterSubject)}</dd>` : ''}
          <dt>${t('crit', 'Crit.')} A</dt><dd>${state.eeScores.A || 0} / 6</dd>
          <dt>${t('crit', 'Crit.')} B</dt><dd>${state.eeScores.B || 0} / 6</dd>
          <dt>${t('crit', 'Crit.')} C</dt><dd>${state.eeScores.C || 0} / 6</dd>
          <dt>${t('crit', 'Crit.')} D</dt><dd>${state.eeScores.D || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} E</dt><dd>${state.eeScores.E || 0} / 4</dd>
          <dt>${t('total', 'Total')}</dt><dd><strong>${total} / 30</strong></dd>
        </dl>
      </div>
    `;
  }

  function renderReviewCP(jumpIdx) {
    const sum = sumScores(state.cpScores);
    const avg = Math.round(sum / 4);
    return `
      <div class="review-section">
        <div class="review-section__head">
          <div class="review-section__title">${t('cp_details', 'CP Details')}</div>
          <button type="button" class="review-section__edit" data-jump="${jumpIdx}">${t('edit', 'Edit')}</button>
        </div>
        <dl class="review-grid">
          <dt>${t('global_context', 'Global Context')}</dt><dd${state.cpGlobalContext ? '' : ' class="is-missing"'}>${esc(state.cpGlobalContext) || t('not_chosen', 'Not chosen')}</dd>
          <dt>${t('type_of_action', 'Type of Action')}</dt><dd${state.cpActionTypes.length ? '' : ' class="is-missing"'}>${state.cpActionTypes.length ? state.cpActionTypes.map(esc).join(', ') : t('none_selected', 'None selected')}</dd>
          <dt>${t('crit', 'Crit.')} A</dt><dd>${state.cpScores.A || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} B</dt><dd>${state.cpScores.B || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} C</dt><dd>${state.cpScores.C || 0} / 8</dd>
          <dt>${t('crit', 'Crit.')} D</dt><dd>${state.cpScores.D || 0} / 8</dd>
          <dt>${t('avg_grade', 'Avg. Grade')}</dt><dd><strong>${avg} / 8</strong></dd>
        </dl>
      </div>
    `;
  }

  function bindReview() {
    stepsContainer.querySelectorAll('[data-jump]').forEach(b => {
      b.addEventListener('click', () => goToStep(parseInt(b.dataset.jump, 10)));
    });
  }

  // ─── Submit / Save Draft ───────────────────────────────────
  function serializeToForm(extraInputs) {
    const form = document.getElementById('uploadForm');
    if (!form) { console.error('[upload-wizard] #uploadForm missing'); return; }
    // Remove any previously injected hidden inputs (keep #uploadFormFile and draft_id).
    form.querySelectorAll('input[data-wiz]').forEach(el => el.remove());

    const add = (name, value) => {
      if (value == null) return;
      const i = document.createElement('input');
      i.type = 'hidden'; i.name = name; i.value = String(value);
      i.setAttribute('data-wiz', '1');
      form.appendChild(i);
    };

    if (state.paperType === 'ee') add('is_ib_ee', '1');
    if (state.paperType === 'cp') add('is_cp_paper', '1');
    if (state.isIbSample && state.paperType !== 'standard') add('is_ib_sample', '1');
    if (!state.isIbSample && state.isAnonymous) add('is_anonymous', '1');

    add('title', state.title);
    add('language', state.language);
    add('category', state.category);
    add('journal', state.journal);

    if (state.paperType === 'standard') {
      add('keywords', state.keywords.join(', '));
      add('abstract', state.abstract);
    }

    if (!state.isIbSample && !state.isAnonymous) {
      state.authors.forEach(a => {
        add('author_name', a.name);
        add('author_email', a.email);
        add('author_school', a.school);
      });
    }

    if (state.paperType === 'ee') {
      add('ib_ee_core_subject', state.eeCoreSubject);
      add('ib_ee_interdisciplinary_subject', state.eeInterSubject);
      ['A', 'B', 'C', 'D', 'E'].forEach(k => add(`ib_crit_${k}_score`, state.eeScores[k] || '0'));
      if (state.eeIncludeComments) {
        ['A', 'B', 'C', 'D', 'E'].forEach(k => add(`ib_crit_${k}_comment`, state.eeComments[k] || ''));
        add('ib_holistic_comment', state.eeComments.holistic || '');
      }
    }

    if (state.paperType === 'cp') {
      add('cp_global_context', state.cpGlobalContext);
      state.cpActionTypes.forEach(a => add('cp_action_type', a));
      ['A', 'B', 'C', 'D'].forEach(k => add(`cp_crit_${k}_score`, state.cpScores[k] || '0'));
    }

    (extraInputs || []).forEach(([n, v]) => add(n, v));

    // Save Draft posts the old-fashioned way (no file, instant). The real
    // paper submission goes through XHR so we can show upload progress and
    // land back on the upload page with a success banner.
    const isDraft = (extraInputs || []).some(([n]) => n === 'save_draft');
    if (isDraft) {
      form.submit();
    } else {
      submitViaXhr(form);
    }
  }

  // ─── XHR submit with upload progress ───────────────────────
  function submitViaXhr(form) {
    showUploadProgress(0);
    const xhr = new XMLHttpRequest();
    xhr.open('POST', form.action, true);
    xhr.setRequestHeader('X-Requested-With', 'XMLHttpRequest');
    xhr.upload.addEventListener('progress', e => {
      if (!e.lengthComputable) return;
      // Hold at 99% until the server confirms; jumping to 100 before the
      // response lands reads as "done" while the request is still in flight.
      showUploadProgress(Math.min(99, Math.round((e.loaded / e.total) * 100)));
    });
    xhr.addEventListener('load', () => {
      let resp = null;
      try { resp = JSON.parse(xhr.responseText); } catch (_) { /* non-JSON */ }
      if (xhr.status >= 200 && xhr.status < 300 && resp && resp.ok) {
        showUploadProgress(100, t('upload_finishing', 'Finishing up…'));
        try {
          sessionStorage.setItem('kd:upload-success', resp.message || t('upload_done', 'Upload successful!'));
        } catch (_) { /* storage disabled — banner just won't show */ }
        window.location = resp.redirect || form.action;
      } else {
        showUploadError((resp && resp.error) || t('upload_failed', 'Upload failed. Please check your connection and try again.'));
      }
    });
    xhr.addEventListener('error', () => {
      showUploadError(t('upload_failed', 'Upload failed. Please check your connection and try again.'));
    });
    xhr.send(new FormData(form));
  }

  function showUploadProgress(pct, label) {
    if (!footerEl) return;
    footerEl.innerHTML = `
      <div class="upload-progress" role="status" aria-live="polite">
        <div class="upload-progress__head">
          <span class="upload-progress__label">${esc(label || t('uploading', 'Uploading… %(pct)s%', { pct: pct }))}</span>
          <span class="upload-progress__pct">${pct}%</span>
        </div>
        <div class="upload-progress__track"><div class="upload-progress__fill" style="width:${pct}%"></div></div>
      </div>`;
  }

  function showUploadError(msg) {
    if (!footerEl) return;
    footerEl.innerHTML = `
      <div class="upload-error" role="alert">
        <span class="upload-error__msg">${esc(msg)}</span>
        <button type="button" class="btn btn--primary" id="retryBtn">${esc(t('try_again', 'Try again'))}</button>
      </div>`;
    const retry = footerEl.querySelector('#retryBtn');
    if (retry) retry.addEventListener('click', renderFooter);
  }

  function showSuccessBannerIfAny() {
    let msg = null;
    try {
      msg = sessionStorage.getItem('kd:upload-success');
      if (msg) sessionStorage.removeItem('kd:upload-success');
    } catch (_) { return; }
    if (!msg) return;
    const host = document.querySelector('.kd-upload-wizard');
    if (!host) return;
    const banner = document.createElement('div');
    banner.className = 'upload-banner';
    banner.setAttribute('role', 'status');
    banner.innerHTML = `
      <span class="upload-banner__icon" aria-hidden="true">✓</span>
      <span class="upload-banner__msg">${esc(msg)}</span>
      <button type="button" class="upload-banner__close" aria-label="${esc(t('discard_btn', 'Discard'))}">×</button>`;
    host.insertBefore(banner, host.firstChild);
    banner.querySelector('.upload-banner__close').addEventListener('click', () => banner.remove());
  }

  function autosaveSaving() {
    if (!autosaveEl) return;
    autosaveEl.classList.remove('autosave--idle', 'autosave--saved');
    autosaveEl.classList.add('autosave--saving');
    const text = autosaveEl.querySelector('.autosave__text');
    if (text) text.textContent = t('saving', 'Saving…');
  }
  function autosaveSaved() {
    if (!autosaveEl) return;
    autosaveEl.classList.remove('autosave--idle', 'autosave--saving');
    autosaveEl.classList.add('autosave--saved');
    const text = autosaveEl.querySelector('.autosave__text');
    if (text) {
      const now = new Date();
      const hh = String(now.getHours()).padStart(2, '0');
      const mm = String(now.getMinutes()).padStart(2, '0');
      text.textContent = t('draft_saved_at', 'Draft saved · %(time)s', { time: hh + ':' + mm });
    }
  }
  // ─── localStorage mirror ───────────────────────────────────
  const STORAGE_KEY = 'kd:upload-draft:' + (BOOT.user_key || 'anon') + (BOOT.draft_id ? ':' + BOOT.draft_id : '');
  let mirrorTimer = null;

  function mirrorToLocalStorage() {
    clearTimeout(mirrorTimer);
    mirrorTimer = setTimeout(() => {
      try {
        const payload = {
          ts: Date.now(),
          state: serializableState(),
        };
        localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
      } catch (e) { /* quota / disabled — silent fallback */ }
    }, 600);
  }

  function serializableState() {
    const s = Object.assign({}, state);
    s.visitedSteps = Array.from(state.visitedSteps);
    // The wizard's `file` is just {name, size}; the real File object lives in
    // #uploadFormFile and isn't restorable from localStorage anyway.
    return s;
  }

  function loadLocalStorage() {
    try {
      const raw = localStorage.getItem(STORAGE_KEY);
      if (!raw) return null;
      return JSON.parse(raw);
    } catch (e) { return null; }
  }

  function clearLocalStorage() {
    try { localStorage.removeItem(STORAGE_KEY); } catch (e) { /* silent */ }
  }

  function showRestoreBanner(stored) {
    const banner = document.createElement('div');
    banner.className = 'restore-banner';
    banner.innerHTML = `
      <div class="restore-banner__body">
        <div class="restore-banner__title">${t('restore_banner_title', 'Unsaved changes from earlier')}</div>
        <div class="restore-banner__sub">${t('restore_banner_body', "Your last session in this browser had changes you didn't save. Restore them?")}</div>
      </div>
      <div class="restore-banner__actions">
        <button type="button" class="btn btn--ghost" id="restoreDiscardBtn">${t('discard_btn', 'Discard')}</button>
        <button type="button" class="btn btn--primary" id="restoreApplyBtn">${t('restore_btn', 'Restore')}</button>
      </div>
    `;
    stepsContainer.appendChild(banner);
    banner.querySelector('#restoreApplyBtn').addEventListener('click', () => {
      Object.assign(state, stored.state);
      state.file = null;   // real file input is empty after a tab close; force re-pick
      state.visitedSteps = new Set(stored.state.visitedSteps || [0]);
      banner.remove();
      render();
    });
    banner.querySelector('#restoreDiscardBtn').addEventListener('click', () => {
      clearLocalStorage();
      banner.remove();
      render();
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

  // ─── Mutation marker ───────────────────────────────────────
  function touch() {
    state.lastModified = Date.now();
    autosaveSaving();
    mirrorToLocalStorage();
    // Mark as "saved" visually after the debounce period — note this only
    // reflects localStorage, NOT a server save. Server save is gated by
    // the Save Draft button. The visual lie matches user intent here: the
    // wizard remembers their work between tabs.
    setTimeout(() => autosaveSaved(), 700);
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
    showSuccessBannerIfAny();
    const stored = loadLocalStorage();
    const fdEmpty = !state.title && !state.paperType && state.keywords.length === 0 && !state.eeCoreSubject && !state.cpGlobalContext;
    if (stored && stored.state && (fdEmpty || stored.ts > (Number(fd.last_modified) || 0))) {
      showRestoreBanner(stored);
      return;   // wait for user to click Restore or Discard before rendering
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
