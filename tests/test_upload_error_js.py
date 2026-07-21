"""Executable client contract for upload lifecycle errors and Retry."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class UploadErrorJavascriptTest(unittest.TestCase):
    def test_formatter_xhr_rendering_and_retry_preserve_idempotency_key(self):
        script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const errors = require('./static/js/upload-error-format.js');
const fallback = 'Upload failed. Please try again.';
assert.equal(errors.formatUploadError('Legacy failure', fallback), 'Legacy failure');
assert.equal(
  errors.formatUploadError({
    message: 'Please correct the highlighted fields.',
    field_errors: { title: 'is invalid', author_email: 'is required' },
  }, fallback),
  'Please correct the highlighted fields. (title: is invalid; author_email: is required)',
);
assert.equal(errors.formatUploadError(null, fallback), fallback);
assert.equal(errors.formatUploadError({}, fallback), fallback);
assert.equal(errors.formatUploadError({ message: 42 }, fallback), fallback);
assert.equal(errors.formatUploadError([], fallback), fallback);

function genericElement() {
  return {
    innerHTML: '',
    textContent: '',
    value: '',
    dataset: {},
    style: {},
    classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
    appendChild() {},
    insertBefore() {},
    setAttribute() {},
    addEventListener() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
    scrollTo() {},
  };
}

const retryButton = genericElement();
retryButton.addEventListener = (_event, handler) => { retryButton.handler = handler; };
const footer = genericElement();
footer.querySelector = selector => selector === '#retryBtn' ? retryButton : null;
const elements = {
  wizardStepper: genericElement(),
  wizardSteps: genericElement(),
  wizardFooter: footer,
  autosaveIndicator: genericElement(),
  dashboardMain: genericElement(),
};
const keyInput = { value: 'publish-request-0001' };
const form = {
  action: '/dashboard/upload',
  querySelector: selector => selector === '[name="publishing_idempotency_key"]' ? keyInput : null,
  querySelectorAll: () => [],
};

let lastXhr = null;
class FakeXhr {
  constructor() {
    lastXhr = this;
    this.handlers = {};
    this.upload = { addEventListener() {} };
  }
  open() {}
  setRequestHeader() {}
  addEventListener(event, handler) { this.handlers[event] = handler; }
  send() {}
}

const context = {
  console,
  setTimeout,
  clearTimeout,
  FormData: class { constructor(value) { this.form = value; } },
  XMLHttpRequest: FakeXhr,
  localStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
  sessionStorage: { getItem() { return null; }, setItem() {}, removeItem() {} },
  document: {
    readyState: 'complete',
    getElementById: id => elements[id] || null,
    querySelector: () => null,
    querySelectorAll: () => [],
    createElement: genericElement,
    addEventListener() {},
  },
};
context.window = {
  WIZARD_BOOT: { form_data: {}, i18n: {} },
  CSS: { escape: value => String(value) },
  confirm: () => true,
  location: '',
};
context.window.window = context.window;
context.window.document = context.document;
context.window.localStorage = context.localStorage;
context.window.sessionStorage = context.sessionStorage;
vm.createContext(context);
vm.runInContext(fs.readFileSync('./static/js/upload-error-format.js', 'utf8'), context);
vm.runInContext(fs.readFileSync('./static/js/upload-wizard.js', 'utf8'), context);

context.window.__uploadWizard.submitViaXhr(form);
lastXhr.status = 422;
lastXhr.responseText = JSON.stringify({
  error: {
    message: 'Please correct the highlighted fields.',
    field_errors: { title: 'is <invalid>' },
  },
});
lastXhr.handlers.load();
assert.match(footer.innerHTML, /Please correct the highlighted fields\./);
assert.match(footer.innerHTML, /title: is &lt;invalid&gt;/);
assert.doesNotMatch(footer.innerHTML, /\[object Object\]/);
assert.equal(typeof retryButton.handler, 'function');
const originalKeyNode = form.querySelector('[name="publishing_idempotency_key"]');
retryButton.handler();
assert.equal(form.querySelector('[name="publishing_idempotency_key"]'), originalKeyNode);
assert.equal(keyInput.value, 'publish-request-0001');
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_formatter_script_loads_before_wizard(self):
        template = (ROOT / "templates" / "upload.html").read_text(encoding="utf-8")
        self.assertLess(
            template.index("upload-error-format.js"),
            template.index("upload-wizard.js"),
        )


if __name__ == "__main__":
    unittest.main()
