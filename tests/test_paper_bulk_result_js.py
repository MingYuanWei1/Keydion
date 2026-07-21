"""Executable browser/CommonJS contract for bulk Paper delete reporting."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PaperBulkResultJavascriptTest(unittest.TestCase):
    def test_formatter_reports_each_outcome_group_and_never_echoes_ids(self):
        script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const results = require('./static/js/paper-bulk-result-format.js');

const message = results.formatDeleteResult({
  deleted: ['deleted-secret-id'],
  deleting: ['deleting-secret-id', 'second-deleting-secret-id'],
  stale: [{ paper_id: 'stale-secret-id', current_version: 9 }],
  not_found: ['missing-secret-id'],
});
assert.equal(
  message,
  'Deleted 1 paper successfully · Deletion in progress for 2 papers · 1 paper changed while deleting; reload and try again · 1 paper was not found',
);
for (const secret of [
  'deleted-secret-id',
  'deleting-secret-id',
  'stale-secret-id',
  'missing-secret-id',
]) assert.doesNotMatch(message, new RegExp(secret));

assert.equal(
  results.formatDeleteResult({ stale: [{ paper_id: '<private>' }] }),
  '1 paper changed while deleting; reload and try again',
);
assert.equal(
  results.formatDeleteResult({ not_found: ['<private>'] }),
  '1 paper was not found',
);
assert.equal(results.formatDeleteResult({}), '');
assert.equal(results.formatDeleteResult(null), '');

const localized = results.formatDeleteResult(
  { stale: [{ paper_id: 'private' }], not_found: ['private'] },
  {
    stale: count => `stale=${count}`,
    notFound: count => `missing=${count}`,
  },
);
assert.equal(localized, 'stale=1 · missing=1');

const browser = { window: {} };
vm.createContext(browser);
vm.runInContext(
  fs.readFileSync('./static/js/paper-bulk-result-format.js', 'utf8'),
  browser,
);
assert.equal(
  browser.window.KeydionPaperBulkResults.formatDeleteResult({ stale: [{}] }),
  '1 paper changed while deleting; reload and try again',
);
"""
        completed = subprocess.run(
            ["node", "-e", script],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_formatter_loads_before_inline_management_javascript(self):
        template = (ROOT / "templates" / "paper_manage.html").read_text(
            encoding="utf-8"
        )
        self.assertLess(
            template.index("paper-bulk-result-format.js"),
            template.index("var BULK_URL"),
        )
        self.assertIn("KeydionPaperBulkResults.formatDeleteResult", template)


if __name__ == "__main__":
    unittest.main()
