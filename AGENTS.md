# Repository Guidelines

## Project Structure & Module Organization
- `app.py` is the primary Flask application; `app_old.py` is legacy reference code.
- `templates/` holds Jinja2 HTML views (e.g., `login.html`, `search.html`).
- `static/` contains CSS and landing assets.
- `data/` stores session tracking JSON and news/paper category configurations.
- `papers/` is the upload/download directory for PDF files.
- `translations/` contains Babel locale files (`translations/*/LC_MESSAGES/messages.po`).
- `tools/` includes utility scripts like `manage_passwords.py` and `compile_translations.py`.

## Build, Test, and Development Commands
- `pip install -r requirements.txt` installs Flask, Babel, SQLAlchemy, and PDF parsing deps.
- `docker-compose up -d` launches the application and its dependencies (MySQL).
- `./start_local.sh` (macOS/Linux) or `.\start_local.ps1` (Windows) launches the local server (requires a running MySQL instance).
- `python tools/manage_passwords.py set --username alice --password Secret123 --role 3` manages users directly in the SQL database.
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
- Set `PAPERQUERY_SECRET` and `PAPERQUERY_DATABASE_URL` in non-dev environments.
- Never commit real user credentials or secret keys to the repo.
