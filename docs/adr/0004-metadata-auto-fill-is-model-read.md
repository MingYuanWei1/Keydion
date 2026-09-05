---
status: accepted
---

# Metadata auto-fill is model-read for all three targets

The Extended Essay marks extractor used to be the one metadata auto-fill target
with a deterministic offline parser: its fallback read the commentary form's
embedded text through pdfplumber table-walking plus anchor regexes, so EE
auto-fill worked with no model configured — but only on commentary forms that
matched the assumed fixed layout, silently returning blank scores elsewhere.
We removed that parser: EE marks are now transcribed the same way IA criterion
scores are — vision-first over the rendered pages, with an OCR+text-model
fallback path. The three extractors differ only in prompt and result shape, not
in how they read a PDF.

## Considered options

- **Keep the deterministic parser as the EE fallback.** Rejected: it was the
  only auto-fill path that could return confidently wrong-or-empty results on
  real-world layout drift, and it kept a pdfplumber dependency alive for one
  caller. IA's model-read fallback already proved more robust on the same
  class of forms.
- **Keep it behind a feature flag for offline deployments.** Rejected: no
  deployment runs without any model today, and maintaining two EE readers
  doubles the drift surface the removal exists to close.

## Consequences

EE auto-fill now requires a configured model (Worker vision or text capability),
exactly like the abstract/keywords and IA targets: with neither configured the
endpoint returns "AI assist is not configured." and the upload wizard hides the
EE auto-fill button behind the same extract-assist gate as the other two.
pdfplumber leaves `requirements.txt` (the lock file is regenerated separately).
The deterministic fixture PDF and its value tests are deleted with the parser.
ADR-0001 is untouched — the `VisionFirstExtractor` template-method cascade is
unchanged; only one extractor's fallback implementation was replaced.
