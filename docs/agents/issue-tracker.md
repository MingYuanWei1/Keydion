# Issue Tracker

Issues and PRDs live in this repository's GitHub Issues. Use the `gh` CLI from the clone so it infers the repository. External pull requests are not a request or triage surface.

- When a skill says to publish to the issue tracker, create a GitHub issue.
- When a skill says to fetch a ticket, run `gh issue view <number> --comments` and include its labels.
- GitHub shares one number space across issues and pull requests. For a bare `#<number>`, try `gh pr view <number>` and fall back to `gh issue view <number>` before deciding what it references.
