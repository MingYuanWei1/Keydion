---
status: accepted
---

# Vision-first metadata extraction uses a template-method base class

The three metadata auto-fill extractors (Extended Essay marks, Internal Assessment scores, abstract & keywords) share an identical vision-first cascade — gate on the vision model, try it over the rendered pages, catch a vision failure, fall back to the text path, then post-process — but differ in their inputs, prompt, result shape, and fallback. We lifted that cascade into a single `VisionFirstExtractor` template-method base (`vision_extractor.py`): one concrete `extract()` drives the skeleton and each extractor overrides `build_prompt` / `shape_vision` / `fallback` / `post`, while the public `extract_*` functions stay as thin façades. This concentrates the cascade, its failure logging, and its page budget in one place so they change once, not three times.

## Considered options

- **Lift only the control-flow skeleton** into a higher-order function, injecting prompt/shaper/fallback as callables — less machinery, closer to the codebase's functional style. Rejected: it leaves result-shaping and the fallback structure unstandardised, and EE's shared post-step (subject normalisation applied to whichever branch ran) has no natural home — it would be duplicated inside both EE's vision and fallback branches.
- **Leave the three extractors as-is.** Rejected: the cascade is copy-pasted verbatim three times; a change to fallback policy, logging, retry, or page budget must be made in lockstep across all three.

## Consequences

A template-method class hierarchy for three implementations runs against this repo's simplicity-first ethos, and an automated architecture review may flag the base as a shallow abstraction. It is deliberate: the deletion test passes — remove the base and the cascade reappears verbatim in all three extractors. Revisit if the extractor count drops to one, or if a subclass needs to override `extract()` itself (a sign the skeleton is no longer truly shared).
