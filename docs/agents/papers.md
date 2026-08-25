# Papers and Submissions

Use the vocabulary in [CONTEXT.md](../../CONTEXT.md). Before changing Paper identity or publishing lifecycle behavior, read [ADR-0002](../adr/0002-stable-paper-identity-and-publishing-lifecycle.md); it is authoritative.

Paper metadata is database-backed; do not introduce a `data/paper_metadata.json` write path.

## Paper types and grading

Independent, EE, CP, and IA are mutually exclusive Paper types. The IB discriminators live inside `ib_ee_data`, `cp_data`, and `ia_data`; build those blobs through the existing server-side helpers.

- EE criterion scores are clamped to server-owned maxima, and the aggregate total is recomputed server-side; the readonly submitted total is ignored.
- IA criteria and maxima come from the selected subject in `data/ia_subjects.json`. Criterion-mode totals are recomputed; holistic-only `ia_total_score` is accepted but clamped server-side.
- The upload wizard offers `is_ib_sample` only for EE, CP, and IA. `is_anonymous` applies to any type and stores empty author fields; if both flags arrive, IB Sample wins.
- Preserve the legacy `author_name == "IB SAMPLE"` display compatibility when changing Paper views.
