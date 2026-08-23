# Keydion

Keydion (also PaperQuery) is an academic paper library: contributors publish papers — independent research and IB Diploma work (Extended Essays, Community Projects, Internal Assessments) — and readers search, read, and ask an AI assistant about them.

This is a glossary of terms whose meaning is specific to Keydion and easy to get wrong. It says what each term *is*, never how it is built — no module names, no implementation.

## Paper publishing

**Publishing lifecycle**:
The decisions and state changes through which a work is published directly or from an accepted Submission, revised over time, and eventually deleted. It includes Submission review when a Submission exists.
_Avoid_: Upload flow (upload is only one entry point), Submission lifecycle (directly published Papers have no Submission).

**Publishing worker**:
The independently operated background participant that completes durable Paper indexing and deletion work after a web request has committed the lifecycle decision. Its availability may delay indexing or deletion cleanup without reversing a committed publication decision or a Paper's immediate inaccessibility after deletion starts.
_Avoid_: Gunicorn worker (web request processes do not own this durable work), indexing thread (the work survives one process).

**Publishing migration**:
A coordinated maintenance event that assigns stable identities and revision storage to legacy Papers while treating the database, published PDFs, and pending Submission PDFs as one recoverable state. Normal application startup only validates the resulting schema; it does not perform this event.
_Avoid_: Startup migration (maintenance is explicit and offline), database migration (filesystem state is part of the same boundary).

**Paper**:
A work available in Keydion's library, either published directly by a Contributor or created when a Curator accepts a Submission. Its lifecycle is distinct from any Submission that produced it, and changes to its title or authors do not create a new Paper.

**Paper revision**:
A replacement of an existing Paper's PDF that preserves the Paper's identity, links, and relationship to any originating Submission. Only the current revision is available in the library; earlier revisions remain private for audit and restoration. Restoring earlier content creates another revision rather than rewinding history. Publishing a different work creates a new Paper instead.
_Avoid_: New Paper (the work remains the same), metadata edit (the Paper's content is unchanged).

**Paper deletion**:
Permanent removal of a Paper and all data owned by it, including its PDF, revisions, and search data. Any originating Submission remains as an independent review record but is no longer linked to a Paper.
_Avoid_: Withdrawal or archive (the Paper cannot be restored).

**Submission**:
A Reader's proposed Paper together with its review history. The Reader may cancel it before a Curator decides it. Acceptance creates a linked Paper; once accepted or rejected, the Submission remains as the permanent record of who submitted and reviewed it, when it was reviewed, and what decision was made.
_Avoid_: Pending paper (a Submission can also be accepted or rejected), Paper (the published library item is distinct).

## PDF metadata extraction

When a contributor uploads a paper, Keydion can draft its metadata from the PDF for the contributor to review before publishing.

**Metadata auto-fill**:
Drafting a paper's metadata from its uploaded PDF so a contributor can review and edit it. Always advisory and editable — never authoritative. Covers three targets: an Extended Essay's examiner marks, an Internal Assessment's criterion scores, and a paper's abstract & keywords.
_Avoid_: extraction (too broad on its own — name the target), OCR (only one possible way of reading).

**Vision-first extraction**:
The default way Keydion reads an uploaded PDF for metadata: render the pages and have a vision model read them, resorting to the fallback path only when no vision model is configured or the vision read fails. Every metadata auto-fill is vision-first.
_Avoid_: vision extraction (drops the fallback, which is part of the concept).

**Fallback path**:
How Keydion reads a PDF without a vision model — from the PDF's embedded text, with OCR for scanned pages, then a text model reading the result. Yields the same shape of result as the vision read, so a caller cannot tell which path produced it.
_Avoid_: legacy path, OCR path (each names only part of it).

## Keydion AI

**Ask turn**:
One round of a reader's conversation with the Keydion AI assistant: the question, the grounding gathered for it, the model's streamed answer, and the citations the answer actually references. A turn may run tool calls between the question and the final answer, fall back to a single-shot answer when tools are unavailable, hit a round cap that forces a tool-free final answer, and persist the assistant message when it finishes.
_Avoid_: Ask request (the HTTP call is one way to start a turn), conversation (many turns share one conversation), RAG retrieval (one input to a turn, not the turn itself).
