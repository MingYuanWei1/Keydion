# Keydion

Keydion (also PaperQuery) is an academic paper library: contributors publish papers — independent research and IB Diploma work (Extended Essays, Community Projects, Internal Assessments) — and readers search, read, and ask an AI assistant about them.

This is a glossary of terms whose meaning is specific to Keydion and easy to get wrong. It says what each term *is*, never how it is built — no module names, no implementation.

## PDF metadata extraction

When a contributor uploads a paper, Keydion can draft its metadata from the PDF for the contributor to review before publishing.

**Metadata auto-fill**:
Drafting a paper's metadata from its uploaded PDF so a contributor can review and edit it. Always advisory and editable — never authoritative. Covers three targets: an Extended Essay's examiner marks, an Internal Assessment's criterion scores, and a paper's abstract & keywords.
_Avoid_: extraction (too broad on its own — name the target), OCR (only one possible way of reading).

**Vision-first extraction**:
The default way Keydion reads an uploaded PDF for metadata: render the pages and have a vision model read them, resorting to the fallback path only when no vision model is configured or the vision read fails. Every metadata auto-fill is vision-first.
_Avoid_: vision extraction (drops the fallback, which is part of the concept).

**Fallback path**:
How Keydion reads a PDF without a vision model — from the PDF's embedded text, with OCR for scanned pages, and for some targets a direct read of the marks on the page. Yields the same shape of result as the vision read, so a caller cannot tell which path produced it.
_Avoid_: legacy path, OCR path (each names only part of it).
