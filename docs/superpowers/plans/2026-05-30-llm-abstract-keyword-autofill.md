# LLM Abstract + Keyword Auto-fill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a button on the Standard-paper upload step that drafts an abstract + keywords from the paper's PDF via an OpenAI-compatible LLM, which the uploader edits before submitting.

**Architecture:** Mirror the existing EE auto-fill feature. A new standalone module `llm_metadata.py` (parallel to `ee_pdf_extractor.py`) extracts PDF text and calls an OpenAI-compatible chat endpoint, returning `{abstract, keywords, warnings}`. A new Flask route `/api/upload/generate-abstract-keywords` (parallel to `/api/upload/extract-ee-metadata`) wraps it. The wizard JS (`upload-wizard.js`) gains a button + handler parallel to `runEEAutofill`. The LLM client is fully config-driven (base URL, key, model) so it works with OpenAI, a local model, or any OpenAI-style provider.

**Tech Stack:** Python 3.9+, Flask, Flask-Babel (en/zh), PyPDF2, the `openai` SDK (used as an OpenAI-compatible client), vanilla JS wizard, `unittest` contract tests.

---

## File Structure

**Create:**
- `llm_metadata.py` — standalone module: PDF-bytes → text → LLM → `{abstract, keywords, warnings}`. One responsibility: produce draft metadata. No Flask imports (mirrors `ee_pdf_extractor.py`).
- `tests/test_llm_metadata.py` — unit tests for the module with the LLM client mocked.
- `tests/test_abstract_keywords_route_contract.py` — AST/source contract test for the new route + boot flag + i18n key.
- `tests/test_abstract_keywords_js_contract.py` — source contract test for the wizard JS wiring.
- `LLM_DEPLOYMENT_IDEAS.md` — repo-root catalog of the six LLM deployment ideas.

**Modify:**
- `requirements.txt` — add `openai>=1.0`.
- `app.py` — add import (line 48 area), add route (after EE route ~line 2279), add `llm_metadata_enabled` boot flag + 5 i18n keys in `_render_upload` (`app.py:1391-1558`).
- `static/js/upload-wizard.js` — add state, render block, bind handler, `runMetaAutofill`/`isMetaDirty`.
- `translations/zh/LC_MESSAGES/messages.po` — add zh translations for the 5 new strings.

**User action (not committed):** add `LLM_API_KEY` (and optional `LLM_BASE_URL`, `LLM_MODEL`) to `.env`.

---

## Task 1: Add the `openai` dependency

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add the package**

Append to `requirements.txt` (after the `gunicorn>=22.0` line):

```
openai>=1.0
```

- [ ] **Step 2: Install it**

Run: `pip3 install -r requirements.txt`
Expected: installs `openai` and its deps with no errors.

- [ ] **Step 3: Verify the import works**

Run: `python3 -c "from openai import OpenAI; print('ok')"`
Expected: prints `ok`.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "build: add openai SDK for LLM metadata assist"
```

**Note on config (no code change here — documented for the implementer and the user):**
The feature reads three optional env vars via the existing `os.environ.get(...)` pattern:
- `LLM_API_KEY` — required to enable the feature (button hidden if unset).
- `LLM_BASE_URL` — optional; omit for real OpenAI, set for a local/compatible provider (e.g. `http://localhost:11434/v1`).
- `LLM_MODEL` — optional; defaults to `gpt-4o-mini`.

---

## Task 2: Create the `llm_metadata.py` module (TDD)

**Files:**
- Create: `llm_metadata.py`
- Test: `tests/test_llm_metadata.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_llm_metadata.py`:

```python
import os
import unittest
from unittest import mock

import llm_metadata
from llm_metadata import (
    LLMMetadataError,
    generate_abstract_keywords,
    _complete,
    _build_client,
    _pdf_text_from_bytes,
    _normalise_keywords,
    _parse_json,
)


# ── A minimal fake mirroring client.chat.completions.create(...) ──
class _FakeMessage:
    def __init__(self, content):
        self.content = content


class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)


class _FakeResponse:
    def __init__(self, content):
        self.choices = [_FakeChoice(content)]


class FakeClient:
    def __init__(self, content):
        self._content = content
        self.captured = {}
        # client.chat.completions.create -> self.create
        self.chat = self
        self.completions = self

    def create(self, **kwargs):
        self.captured = kwargs
        return _FakeResponse(self._content)


class CompleteTest(unittest.TestCase):
    def test_parses_clean_json(self):
        client = FakeClient('{"abstract": "An abstract.", "keywords": ["alpha", "beta"]}')
        out = _complete(client, "paper text", "en")
        self.assertEqual(out["abstract"], "An abstract.")
        self.assertEqual(out["keywords"], ["alpha", "beta"])
        self.assertEqual(out["warnings"], [])

    def test_extracts_json_from_noisy_text(self):
        client = FakeClient('Sure!\n{"abstract": "x", "keywords": ["k1"]}\nHope that helps')
        out = _complete(client, "paper text", "en")
        self.assertEqual(out["abstract"], "x")
        self.assertEqual(out["keywords"], ["k1"])

    def test_normalises_string_keywords(self):
        client = FakeClient('{"abstract": "x", "keywords": "a, b, b, c"}')
        out = _complete(client, "t", "en")
        self.assertEqual(out["keywords"], ["a", "b", "c"])

    def test_warns_when_fields_missing(self):
        client = FakeClient('{"abstract": "", "keywords": []}')
        out = _complete(client, "t", "en")
        self.assertTrue(out["warnings"])

    def test_language_zh_in_prompt(self):
        client = FakeClient('{"abstract": "x", "keywords": ["k"]}')
        _complete(client, "t", "zh")
        system_msg = client.captured["messages"][0]["content"]
        self.assertIn("Chinese", system_msg)
        self.assertEqual(client.captured["response_format"], {"type": "json_object"})

    def test_unparseable_response_raises(self):
        client = FakeClient("totally not json")
        with self.assertRaises(LLMMetadataError):
            _complete(client, "t", "en")


class BuildClientTest(unittest.TestCase):
    def test_missing_key_raises(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(LLMMetadataError):
                _build_client()


class PdfTextTest(unittest.TestCase):
    def test_empty_bytes_raises(self):
        with self.assertRaises(LLMMetadataError):
            _pdf_text_from_bytes(b"")


class HelpersTest(unittest.TestCase):
    def test_normalise_keywords_dedupes_and_caps(self):
        self.assertEqual(
            _normalise_keywords(["a", "a", "b", "c", "d", "e", "f", "g"]),
            ["a", "b", "c", "d", "e", "f"],
        )

    def test_parse_json_none_on_garbage(self):
        self.assertIsNone(_parse_json("no json here"))


class GenerateEndToEndTest(unittest.TestCase):
    def test_orchestration(self):
        client = FakeClient('{"abstract": "done", "keywords": ["x"]}')
        with mock.patch.object(llm_metadata, "_pdf_text_from_bytes", return_value="text"), \
             mock.patch.object(llm_metadata, "_build_client", return_value=client):
            out = generate_abstract_keywords(b"%PDF-fake", "en")
        self.assertEqual(out["abstract"], "done")
        self.assertEqual(out["keywords"], ["x"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m unittest tests/test_llm_metadata.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'llm_metadata'`.

- [ ] **Step 3: Write the module**

Create `llm_metadata.py`:

```python
"""Draft an abstract + keywords from a paper PDF via an OpenAI-compatible LLM.

Public surface:
    generate_abstract_keywords(file_bytes, language="en") -> dict
    LLMMetadataError

Provider-agnostic: the client base URL, API key, and model are all read from
the environment (LLM_BASE_URL / LLM_API_KEY / LLM_MODEL), so the same code
works against OpenAI, a local model (Ollama/vLLM), or any OpenAI-style API.
"""

from __future__ import annotations

import io
import json
import os
import re

from PyPDF2 import PdfReader
from PyPDF2.errors import PdfReadError

MAX_PDF_CHARS = 12_000          # bound the prompt size / token cost
DEFAULT_MODEL = "gpt-4o-mini"
MAX_KEYWORDS = 6


class LLMMetadataError(Exception):
    """Raised when the PDF is unusable or the LLM request/response fails."""


def _pdf_text_from_bytes(file_bytes: bytes) -> str:
    """Extract concatenated text from a PDF given as bytes, capped to MAX_PDF_CHARS."""
    if not file_bytes:
        raise LLMMetadataError("Empty file")
    try:
        reader = PdfReader(io.BytesIO(file_bytes))
    except PdfReadError as exc:
        raise LLMMetadataError(f"Could not read PDF: {exc}") from exc
    if reader.is_encrypted:
        raise LLMMetadataError("PDF is encrypted")
    parts = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:  # PyPDF2 can throw a variety on odd pages
            parts.append("")
    text = "\n".join(parts).strip()
    if not text:
        raise LLMMetadataError("No readable text — is this a scanned image?")
    return text[:MAX_PDF_CHARS]


def _build_client():
    """Construct an OpenAI-compatible client from environment configuration."""
    api_key = os.environ.get("LLM_API_KEY")
    if not api_key:
        raise LLMMetadataError("AI assist is not configured.")
    try:
        from openai import OpenAI
    except ImportError as exc:  # openai not installed
        raise LLMMetadataError("openai package is not installed.") from exc
    base_url = os.environ.get("LLM_BASE_URL") or None
    return OpenAI(api_key=api_key, base_url=base_url)


def _parse_json(content: str):
    """Parse JSON, tolerating a model that wraps it in prose. Returns dict or None."""
    if not content:
        return None
    try:
        return json.loads(content)
    except (ValueError, TypeError):
        pass
    match = re.search(r"\{.*\}", content, re.DOTALL)
    if not match:
        return None
    try:
        return json.loads(match.group(0))
    except ValueError:
        return None


def _normalise_keywords(value) -> list:
    """Coerce the model's keywords into a clean, de-duplicated, capped list."""
    if isinstance(value, str):
        value = value.split(",")
    if not isinstance(value, list):
        return []
    out: list = []
    for item in value:
        s = str(item).strip()
        if s and s not in out:
            out.append(s)
    return out[:MAX_KEYWORDS]


def _complete(client, text: str, language: str) -> dict:
    """Call the chat endpoint and return {abstract, keywords, warnings}."""
    warnings: list = []
    model = os.environ.get("LLM_MODEL", DEFAULT_MODEL)
    lang_name = "Chinese" if language == "zh" else "English"
    system = (
        "You are an academic editor. Read the paper text and return a JSON object "
        f'with exactly two keys: "abstract" — a concise summary of at most 250 words '
        f"written in {lang_name} — and \"keywords\" — an array of 3 to 6 short topical "
        "keyword strings. Return ONLY the JSON object, no prose."
    )
    try:
        resp = client.chat.completions.create(
            model=model,
            temperature=0.2,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": text},
            ],
        )
    except Exception as exc:  # network/auth/rate-limit from any provider
        raise LLMMetadataError(f"AI request failed: {exc}") from exc

    content = (resp.choices[0].message.content or "").strip()
    data = _parse_json(content)
    if data is None:
        raise LLMMetadataError("The AI response could not be parsed.")

    abstract = (data.get("abstract") or "").strip()
    keywords = _normalise_keywords(data.get("keywords"))
    if not abstract:
        warnings.append("No abstract was generated — please write one manually.")
    if not keywords:
        warnings.append("No keywords were generated — please add them manually.")
    return {"abstract": abstract, "keywords": keywords, "warnings": warnings}


def generate_abstract_keywords(file_bytes: bytes, language: str = "en") -> dict:
    """Public entry point: PDF bytes -> {abstract, keywords, warnings}."""
    text = _pdf_text_from_bytes(file_bytes)
    client = _build_client()
    return _complete(client, text, language)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m unittest tests/test_llm_metadata.py -v`
Expected: PASS (all tests).

- [ ] **Step 5: Commit**

```bash
git add llm_metadata.py tests/test_llm_metadata.py
git commit -m "feat: add llm_metadata module for abstract/keyword generation"
```

---

## Task 3: Wire the route, boot flag, and i18n into `app.py` (TDD)

**Files:**
- Modify: `app.py` (import ~line 48; route after `app.py:2279`; boot flag + i18n in `_render_upload`, `app.py:1391-1558`)
- Test: `tests/test_abstract_keywords_route_contract.py`

- [ ] **Step 1: Write the failing contract test**

Create `tests/test_abstract_keywords_route_contract.py`:

```python
import ast
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATH = "/api/upload/generate-abstract-keywords"


class AbstractKeywordsRouteContractTest(unittest.TestCase):
    """The /api/upload/generate-abstract-keywords route must exist, require
    contributor (level=2) login, and delegate to generate_abstract_keywords."""

    @classmethod
    def setUpClass(cls):
        cls.source = (ROOT / "app.py").read_text(encoding="utf-8")
        ast.parse(cls.source)  # sanity: app.py still parses

    def test_route_path_present(self):
        self.assertIn(f'"{PATH}"', self.source)

    def test_imports_generator(self):
        self.assertTrue(
            re.search(
                r"from\s+llm_metadata\s+import\s+[^\n]*generate_abstract_keywords",
                self.source,
            ),
            "generate_abstract_keywords must be imported from llm_metadata",
        )

    def test_route_uses_require_login_level_2(self):
        idx = self.source.index(f'"{PATH}"')
        window = self.source[idx: idx + 4000]
        self.assertRegex(window, r"require_login\s*\(\s*level\s*=\s*2\s*\)")

    def test_route_calls_generator(self):
        idx = self.source.index(f'"{PATH}"')
        window = self.source[idx: idx + 4000]
        self.assertIn("generate_abstract_keywords(", window)

    def test_route_catches_llm_error(self):
        idx = self.source.index(f'"{PATH}"')
        window = self.source[idx: idx + 4000]
        self.assertIn("LLMMetadataError", window)

    def test_boot_exposes_enabled_flag(self):
        self.assertIn("llm_metadata_enabled", self.source)

    def test_i18n_key_present(self):
        self.assertIn('"meta_autofill_btn"', self.source)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests/test_abstract_keywords_route_contract.py -v`
Expected: FAIL — route path / import / flag not found.

- [ ] **Step 3: Add the module import**

In `app.py`, find line 48:

```python
from ee_pdf_extractor import extract_ee_metadata, EePdfExtractionError
```

Add immediately after it:

```python
from llm_metadata import generate_abstract_keywords, LLMMetadataError
```

- [ ] **Step 4: Add the boot flag**

In `_render_upload` (`app.py:1393-1401`), find:

```python
            "user_key": user.get("username", ""),
```

Add immediately after it:

```python
            "llm_metadata_enabled": bool(os.environ.get("LLM_API_KEY")),
```

- [ ] **Step 5: Add the 5 i18n keys**

In the same `i18n` dict, find the EE auto-fill keys ending at `app.py:1556`:

```python
                "ee_autofill_overwrite": _("Replace your existing EE entries with values from the PDF?"),
```

Add immediately after it:

```python
                "meta_autofill_btn": _("Generate abstract & keywords from PDF"),
                "meta_autofill_extracting": _("Generating…"),
                "meta_autofill_ok": _("Generated abstract and keywords."),
                "meta_autofill_error": _("Generation failed — try again or fill manually."),
                "meta_autofill_overwrite": _("Replace your existing abstract and keywords with AI-generated ones?"),
```

- [ ] **Step 6: Add the route**

In `app.py`, find the end of the EE route at `app.py:2279`:

```python
        return jsonify(result), 200

    @app.route("/dashboard/news/publish", methods=["GET", "POST"])
```

Insert the new route between them (after the `return jsonify(result), 200` of `api_extract_ee_metadata`, before `news_publish`):

```python

    @app.route("/api/upload/generate-abstract-keywords", methods=["POST"])
    def api_generate_abstract_keywords():
        user = require_login(level=2)
        if not user:
            return jsonify({"error": str(_("Unauthorized"))}), 401

        upload = request.files.get("file")
        if not upload or not upload.filename:
            return jsonify({"error": str(_("No file provided"))}), 400
        if not upload.filename.lower().endswith(".pdf"):
            return jsonify({"error": str(_("File must be a PDF"))}), 400

        raw = upload.read()
        if not raw.startswith(b"%PDF-"):
            return jsonify({"error": str(_("File is not a valid PDF"))}), 400

        language = request.form.get("language", "en")
        try:
            result = generate_abstract_keywords(raw, language)
        except LLMMetadataError as exc:
            return jsonify({"error": str(exc)}), 400

        return jsonify(result), 200
```

- [ ] **Step 7: Run the contract test to verify it passes**

Run: `python3 -m unittest tests/test_abstract_keywords_route_contract.py -v`
Expected: PASS.

- [ ] **Step 8: Verify app.py still imports cleanly**

Run: `python3 -c "import app; print('ok')"`
Expected: prints `ok` (no syntax/import error).

- [ ] **Step 9: Commit**

```bash
git add app.py tests/test_abstract_keywords_route_contract.py
git commit -m "feat: add generate-abstract-keywords route and wizard boot flag"
```

---

## Task 4: Add the wizard button + handler in `upload-wizard.js` (TDD)

**Files:**
- Modify: `static/js/upload-wizard.js`
- Test: `tests/test_abstract_keywords_js_contract.py`

- [ ] **Step 1: Write the failing JS contract test**

Create `tests/test_abstract_keywords_js_contract.py`:

```python
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class AbstractKeywordsJsContractTest(unittest.TestCase):
    """The wizard JS must wire the generate button to the new endpoint."""

    @classmethod
    def setUpClass(cls):
        cls.js = (ROOT / "static" / "js" / "upload-wizard.js").read_text(encoding="utf-8")

    def test_calls_endpoint(self):
        self.assertIn("/api/upload/generate-abstract-keywords", self.js)

    def test_has_handler(self):
        self.assertIn("runMetaAutofill", self.js)

    def test_has_button_id(self):
        self.assertIn("metaAutofillBtn", self.js)

    def test_gated_on_flag(self):
        self.assertIn("BOOT.llm_metadata_enabled", self.js)

    def test_sends_language(self):
        self.assertIn("form.append('language'", self.js)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m unittest tests/test_abstract_keywords_js_contract.py -v`
Expected: FAIL — strings not found.

- [ ] **Step 3: Add the two state fields**

In `static/js/upload-wizard.js`, find (lines 51-53):

```javascript
    // EE auto-fill UI
    eeAutofillStatus: '',    // '' | 'loading' | 'ok' | 'partial' | 'error'
    eeAutofillMessage: '',
```

Add immediately after:

```javascript
    // Abstract/keyword auto-fill UI (standard papers)
    metaAutofillStatus: '',  // '' | 'loading' | 'ok' | 'partial' | 'error'
    metaAutofillMessage: '',
```

- [ ] **Step 4: Render the button inside the standard-paper block**

In `renderMetadata()`, find the start of the `!isIbType` block (lines 302-304):

```javascript
          ${!isIbType ? `
          <div class="field">
            <label class="field__label" for="f-keywords">${t('keywords', 'Keywords')} <span class="req">*</span></label>
```

Replace with (adds the gated auto-fill control before the keywords field):

```javascript
          ${!isIbType ? `
          ${BOOT.llm_metadata_enabled ? `
          <div class="ee-autofill">
            <button type="button" id="metaAutofillBtn" class="btn btn-outline-primary btn-sm" ${state.metaAutofillStatus === 'loading' ? 'disabled' : ''}>
              ${t('meta_autofill_btn', 'Generate abstract & keywords from PDF')}
            </button>
            <input type="file" id="metaAutofillFile" accept="application/pdf,.pdf" hidden>
            <span id="metaAutofillStatus" class="ee-autofill__status ee-autofill__status--${state.metaAutofillStatus || 'idle'}">
              ${esc(state.metaAutofillMessage || '')}
            </span>
          </div>
          ` : ''}
          <div class="field">
            <label class="field__label" for="f-keywords">${t('keywords', 'Keywords')} <span class="req">*</span></label>
```

- [ ] **Step 5: Bind the button in `bindMetadata()`**

In `bindMetadata()`, find the end of the chips block (lines 391-399):

```javascript
    if (chipsContainer) {
      chipsContainer.querySelectorAll('.chip__x').forEach(x => {
        x.addEventListener('click', () => {
          state.keywords.splice(parseInt(x.dataset.i, 10), 1);
          renderStep();
          touch();
        });
      });
    }
```

Add immediately after it:

```javascript

    // ── Abstract/keyword auto-fill (standard papers) ───────────
    const metaBtn = stepsContainer.querySelector('#metaAutofillBtn');
    const metaFile = stepsContainer.querySelector('#metaAutofillFile');
    if (metaBtn && metaFile) {
      metaBtn.addEventListener('click', () => {
        // Reuse the PDF already chosen in the File step if present.
        const existing = document.getElementById('uploadFormFile');
        const chosen = existing && existing.files && existing.files[0];
        if (chosen) { runMetaAutofill(chosen); }
        else { metaFile.click(); }
      });
      metaFile.addEventListener('change', async (e) => {
        const file = e.target.files && e.target.files[0];
        e.target.value = '';   // allow re-selecting the same file
        if (!file) return;
        await runMetaAutofill(file);
      });
    }
```

- [ ] **Step 6: Add the `runMetaAutofill` + `isMetaDirty` functions**

In `static/js/upload-wizard.js`, find the end of `summariseAutofill` (the closing brace at line 647, immediately before the `// ─── CP fieldset ───` comment at line 649):

```javascript
    return {
      status: 'partial',
      message: t('ee_autofill_partial',
        'Extracted %(filled)s of %(total)s fields.', { filled: String(filled), total: String(max) })
        + tail,
    };
  }

  // ─── CP fieldset ───────────────────────────────────────────
```

Insert the new functions between the closing `}` and the `// ─── CP fieldset ───` comment:

```javascript
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
    return !!(state.abstract || '').trim() || (state.keywords && state.keywords.length > 0);
  }

  // ─── CP fieldset ───────────────────────────────────────────
```

- [ ] **Step 7: Run the JS contract test to verify it passes**

Run: `python3 -m unittest tests/test_abstract_keywords_js_contract.py -v`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add static/js/upload-wizard.js tests/test_abstract_keywords_js_contract.py
git commit -m "feat: add abstract/keyword auto-fill button to upload wizard"
```

---

## Task 5: Add zh translations for the new strings

**Files:**
- Modify: `translations/zh/LC_MESSAGES/messages.po`

- [ ] **Step 1: Append the new translation entries**

Append the following blocks to the end of `translations/zh/LC_MESSAGES/messages.po`:

```po
msgid "Generate abstract & keywords from PDF"
msgstr "从 PDF 生成摘要和关键词"

msgid "Generating…"
msgstr "生成中…"

msgid "Generated abstract and keywords."
msgstr "已生成摘要和关键词。"

msgid "Generation failed — try again or fill manually."
msgstr "生成失败——请重试或手动填写。"

msgid "Replace your existing abstract and keywords with AI-generated ones?"
msgstr "用 AI 生成的内容替换现有的摘要和关键词吗？"
```

- [ ] **Step 2: Compile translations**

Run: `python3 tools/compile_translations.py`
Expected: regenerates `translations/zh/LC_MESSAGES/messages.mo` with no errors.

- [ ] **Step 3: Commit**

```bash
git add translations/zh/LC_MESSAGES/messages.po translations/zh/LC_MESSAGES/messages.mo
git commit -m "i18n(zh): translate abstract/keyword auto-fill strings"
```

**Note:** the English UI works without this task (gettext falls back to the msgid). The dev server must be restarted to pick up the new `.mo`.

---

## Task 6: Write the LLM deployment ideas catalog

**Files:**
- Create: `LLM_DEPLOYMENT_IDEAS.md` (repo root)

- [ ] **Step 1: Create the file**

Create `LLM_DEPLOYMENT_IDEAS.md` with this content:

```markdown
# LLM Deployment Ideas — Keydion

Candidate places to apply an LLM in this repository app. #2 is implemented
(abstract + keyword auto-fill on upload). The rest are recorded for future work.
All user-facing output must be bilingual (en/zh) via Flask-Babel.

| # | Idea | Effort | Risk | Status |
|---|------|--------|------|--------|
| 1 | Grading assist (EE/CP draft scores + comments) | M | High (subjective IB grading) | Idea |
| 2 | Abstract + keyword auto-fill on upload | S | Low | Implemented |
| 3 | Reviewer triage / quality flags | M | Medium | Idea |
| 4 | Semantic search & "related papers" | L | Medium | Idea |
| 5 | Bilingual content assist (en↔zh) | S–M | Low–Medium | Idea |
| 6 | "Ask the library" RAG chat | L | Medium | Idea |

## 1. Grading assist (EE / CP)
Draft per-criterion scores + comments into the Curator review form for an
*ungraded* uploaded paper. Plugs into the review flow (`/dashboard/review/<id>`,
`review_accept` / `review_reject`) and the existing `ib_ee_data` criteria A–E /
`cp_data` criteria A–D structures (score + comment fields already exist).
**Always draft-only** — a Curator must edit and approve; never auto-publish. IB
grading is subjective and high-stakes, so this is the riskiest idea and needs
clear "AI-suggested, human-confirmed" UX.

## 2. Abstract + keyword auto-fill on upload  (implemented)
LLM reads the uploaded PDF and drafts the Abstract + Keywords on the Standard-
paper Metadata step; the uploader edits before submit. Mirrors the EE auto-fill
button. Endpoint `/api/upload/generate-abstract-keywords` -> `llm_metadata.py`.
Future extension: add abstract/keyword fields + generation for EE/CP papers too.

## 3. Reviewer triage / quality flags
Summarize each pending submission and flag issues (off-topic, missing sections,
possibly AI-generated, language mismatch) to help Curators prioritize the
`/dashboard/review` queue. Output is advisory metadata shown in the review list.

## 4. Semantic search & "related papers"
Replace/augment the current substring + full-text search (`search_papers`,
capped at 20 results) with embeddings-based semantic search and a "related
papers" block on paper pages. Needs an embedding store / vector index — the
largest infrastructure lift here.

## 5. Bilingual content assist (en ↔ zh)
Auto-draft translations of titles, abstracts, and news/journal bodies so content
is available in both `en` and `zh`. Fits the existing i18n workflow and the
block-based `NewsArticleModel.body`. Human reviews translations before publish.

## 6. "Ask the library" RAG chat
A Q&A assistant grounded in the published corpus (retrieval over paper text +
metadata). Students ask questions and get cited answers. Depends on the same
embedding/index infrastructure as #4; largest lift, highest product value.

## Cross-cutting notes
- **Provider:** OpenAI-compatible client (`openai` SDK with configurable
  `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL`); works with OpenAI, a local
  model (Ollama/vLLM), or any OpenAI-style provider. Cheap model `gpt-4o-mini`
  is the default for summarization-class tasks.
- **Privacy:** cloud providers receive paper text. For sensitive data, point
  `LLM_BASE_URL` at a self-hosted model so nothing leaves your network.
- **Cost control:** gate cost-incurring endpoints behind `require_login(level>=2)`,
  truncate input text, and prefer explicit user-triggered buttons over automatic
  background calls.
- **Human-in-the-loop:** every user-facing generation is a *draft* a human edits
  before anything is saved or published.
```

- [ ] **Step 2: Commit**

```bash
git add LLM_DEPLOYMENT_IDEAS.md
git commit -m "docs: add LLM deployment ideas catalog"
```

---

## Task 7: Full verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite**

Run: `python3 -m unittest discover -s tests -p "test_*.py" -v`
Expected: all tests pass, including the three new files. No regressions in the existing upload-wizard / EE contract tests.

- [ ] **Step 2: Verify graceful degradation (no key)**

With `LLM_API_KEY` unset, run: `./start_local.sh`, log in, open `/dashboard/upload`, pick "Standard Paper", go to the Metadata step.
Expected: the "Generate abstract & keywords from PDF" button is **absent** (boot flag false).

- [ ] **Step 3: Verify the happy path (with key)**

Set `LLM_API_KEY` (and `LLM_BASE_URL`/`LLM_MODEL` if using a non-OpenAI provider) in `.env`, restart `./start_local.sh`. As a Contributor, start a Standard paper, attach a real text-based PDF in the File step, return to Metadata, click the button.
Expected: status shows "Generating…", then the abstract textarea + keyword chips populate; the abstract char counter updates.

- [ ] **Step 4: Verify the overwrite guard**

With abstract/keywords already filled, click the button again.
Expected: a confirm dialog appears; clicking Cancel leaves existing values intact, OK replaces them.

- [ ] **Step 5: Verify error handling**

Click the button and select a scanned/image-only or empty PDF.
Expected: `#metaAutofillStatus` shows an error message (e.g. "No readable text — is this a scanned image?"); the form is unchanged.

- [ ] **Step 6: Verify i18n**

Switch the locale to zh (after `python3 tools/compile_translations.py` + server restart).
Expected: the button label and status messages render in Chinese.

- [ ] **Step 7: Final commit (if any verification fixes were needed)**

```bash
git add -A
git commit -m "test: verify abstract/keyword auto-fill end-to-end"
```

---

## Self-Review Notes

- **Spec coverage:** Deliverable 1 (abstract/keyword auto-fill) → Tasks 1–4 (deps, module, route+boot+i18n, frontend) + Task 5 (zh). Deliverable 2 (ideas MD in root) → Task 6. Verification → Task 7. ✓
- **Provider config:** `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL` used consistently in `llm_metadata.py`, the boot flag, and the ideas doc. ✓
- **Type/name consistency:** `generate_abstract_keywords`, `LLMMetadataError`, `_complete`, `_build_client`, `_pdf_text_from_bytes`, `_normalise_keywords`, `_parse_json` match across module + tests; `runMetaAutofill`, `isMetaDirty`, `metaAutofillBtn`, `metaAutofillFile`, `metaAutofillStatus`, `BOOT.llm_metadata_enabled`, and the 5 `meta_autofill_*` i18n keys match across JS + app.py + tests. ✓
- **No placeholders:** every code step shows complete code; every command shows expected output. ✓
```
