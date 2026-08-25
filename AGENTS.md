# Keydion

Keydion is a bilingual Flask/MySQL academic-paper library for publishing, searching, previewing, and discussing independent and IB papers, with optional AI-assisted extraction and RAG.

Runtime: Python 3.14+; dependencies use pip/pip-tools. There is no frontend build or typecheck step.

## Language and writing style

Use clear, human-understandable language in all natural-language replies to the user.

- Use a warm, direct tone.
- Be conversational when it improves understanding, and more pragmatic when explaining technical topics.
- Avoid jargon.

Apply these language and tone rules to prose written directly to the user, not to code, quoted text, or file content.

## Task-specific guidance

Read only the guides whose trigger matches the task:

- **Installing, configuring, or running locally:** [Development](docs/agents/development.md)
- **Changing startup, module boundaries, route placement, endpoint names, or dashboard partial rendering:** [Architecture](docs/agents/architecture.md)
- **Adding, changing, or running tests:** [Testing](docs/agents/testing.md)
- **Changing forms/fetch, authentication, sessions, redirects, stored HTML, or file paths:** [Security](docs/agents/security.md)
- **Changing Papers, Submissions, EE/CP/IA grading, authorship, or publishing:** [Papers](docs/agents/papers.md)
- **Changing Ask, LLM providers, vision/OCR, PDF extraction, embeddings, or RAG:** [AI and PDF](docs/agents/ai-and-pdf.md)
- **Changing user-facing text or model-generated output:** [Internationalization](docs/agents/i18n.md)
- **Bootstrapping a database or changing schema, migrations, workers, or deployment:** [Operations](docs/agents/operations.md)
- **Committing or creating local plans/specs:** [Git workflow](docs/agents/workflow.md)
- **Working with issues, PRDs, or triage:** [Issue tracker](docs/agents/issue-tracker.md); for triage, also read [Triage labels](docs/agents/triage-labels.md)
- **Exploring the codebase, naming domain concepts, or making domain decisions:** [Domain docs](docs/agents/domain.md)
