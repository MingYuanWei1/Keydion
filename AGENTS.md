# Repository Guidelines

## Project Structure & Module Organization
- `app.py` is the primary Flask application; `app_old.py` is legacy reference code.
- `templates/` holds Jinja2 HTML views (e.g., `login.html`, `search.html`).
- `static/` contains CSS and landing assets.
- `data/` stores CSV data (users, paper metadata) and session tracking JSON.
- `papers/` is the upload/download directory for PDF files.
- `translations/` contains Babel locale files (`translations/*/LC_MESSAGES/messages.po`).
- `tools/` includes utility scripts like `manage_passwords.py` and `compile_translations.py`.

## Build, Test, and Development Commands
- `pip install -r requirements.txt` installs Flask, Babel, and PDF parsing deps.
- `./start_local.sh` (macOS/Linux) or `.\start_local.ps1` (Windows) launches the local server; both accept `PAPERQUERY_*` env overrides.
- `python tools/manage_passwords.py set --username alice --password Secret123 --role 2 --registration-date 2024-09-01 --expiry-date 2025-09-01` manages PBKDF2 user hashes in `data/users.csv`.
- `python tools/compile_translations.py` rebuilds `.mo` files after editing `messages.po`.

## Coding Style & Naming Conventions
- Python follows standard PEP 8 style: 4-space indentation, snake_case names, and explicit imports.
- Keep new templates and static assets consistent with existing naming (lowercase, descriptive; use hyphens or underscores as needed).
- No formatter or linter is enforced; match the surrounding style and keep functions short and readable.

## Testing Guidelines
- No automated test suite is configured yet.
- Validate changes manually: start the server, log in with a test account, search/upload/download/delete PDFs, and switch languages in the navbar.
- Use sample PDFs in `papers/` to confirm search results and pagination.

## Commit & Pull Request Guidelines
- Commit messages follow a Conventional Commit style (`feat: ...`, `fix: ...`, optional scope like `fix(i18n): ...`).
- PRs should include a brief summary, test notes, and screenshots for UI changes.
- If you touch translations, include the compiled `.mo` updates and mention the command used.

## Security & Configuration Tips
- Set `PAPERQUERY_SECRET` in non-dev environments and avoid committing real user credentials.
- `data/users.csv` stores PBKDF2 hashes; never add plaintext passwords to the repo.
