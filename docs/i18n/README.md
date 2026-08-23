# Bilingual documentation

English | [中文](README.zh.md)

This specification defines a universal standard for managing and translating technical documentation across languages (with English and Simplified Chinese as the reference pair). It establishes the pairing contract, structural synchronization, terminology rules, and quality verification.

- [translation-rules.md](translation-rules.md) defines the core translation and copywriting rules.
- [terminology.md](terminology.md) serves as the technical terminology source of truth.
- [style-samples.md](style-samples.md) provides reference style pairs across common technical genres.
- [translation-prompt.md](translation-prompt.md) provides a standardized LLM translation pipeline prompt.

## The pairing contract

- **Both languages carry equal authority.** A document may be authored and reviewed in either language first, with the counterpart translated from it. Neither language outranks the other; what binds them is that both must convey identical technical content, tone, and intent.
- **Co-located file pairing.** In file-based documentation repositories, each localized file sits as a sibling to its source in the same directory (e.g. `foo.md` and `foo.zh.md`), or follows an explicit locale directory mapping. Co-location avoids disconnected translation silos.
- **Language switcher.** Each localized document provides a clear bidirectional switcher immediately following its main heading (H1):
  - English source: `English | [中文](foo.zh.md)`
  - Chinese counterpart: `[English](foo.md) | 中文`
- **Structure mirrors the counterpart.** The structural frame of the document mirrors the counterpart one to one: heading depths and hierarchy, list types, item counts, table dimensions, verbatim code blocks, and link targets must align. See [translation-rules.md](translation-rules.md) for detailed preservation rules.

## Automated verification

1. **Pair completeness:** Every in-scope document has a corresponding counterpart in all target languages.
2. **Structural consistency:** Heading hierarchies, list item counts, table columns/rows, and fenced code block counts/kinds match across language counterparts.
3. **Verbatim code integrity:** Fenced code blocks and machine-readable identifiers remain byte-identical across translations.
4. **Link resolution:** All internal and relative links point to valid targets in both language versions without dead links.
5. **Synchronization tracking:** Modification of one side without updating its counterpart or updating synchronization metadata triggers a warning or verification failure.
