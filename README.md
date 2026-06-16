# Keydion - Academic Paper Management System

Keydion is a robust, scholarly-focused web application for managing, searching, and previewing academic papers. Built with Flask and MySQL, it features multi-language support, a premium design aesthetic, and specialized support for IB Extended Essays (EE).

## Features

- **Academic Search**: JSTOR-inspired search interface with filters for subjects, dates, and languages.
- **Paper Preview**: In-browser PDF preview with a custom sidebar for metadata.
- **IB Extended Essay Support**: Specialized metadata fields for IB EE papers, including criteria-based scores and comments.
- **Multi-language Support**: Full internationalization (i18n) for English and Chinese.
- **Microsoft Authentication**: Integrated MS Graph API support for user login and profile synchronization.
- **News Management**: Built-in system for publishing and managing academic news and announcements.

## Prerequisites

- **Python 3.11+**
- **MySQL 8.0+**
- **Tesseract OCR** (optional) — enables text extraction from *scanned* PDFs (chat attachments, the abstract/keyword generator, and the papers index). Install the engine plus the Chinese language data:
  - Debian/Ubuntu: `apt-get install -y tesseract-ocr tesseract-ocr-chi-sim`
  - macOS: `brew install tesseract tesseract-lang`

  Without it, scanned PDFs simply yield no extracted text; text-based PDFs are unaffected.

## Database Setup

Keydion reaches MySQL through the `PAPERQUERY_DATABASE_URL` connection string in
your `.env` / `.env.prod`. You must create the **database** (and, typically, a
dedicated user) before first start — the application creates and migrates all
**tables** automatically, but it does not create the database itself.

Connect as a MySQL admin (`mysql -u root -p`) and run:

```sql
-- utf8mb4 is required: PDF-extracted text and CJK metadata need 4-byte chars.
CREATE DATABASE keydion CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER 'keydion'@'localhost' IDENTIFIED BY 'change-me';
GRANT ALL PRIVILEGES ON keydion.* TO 'keydion'@'localhost';
FLUSH PRIVILEGES;
```

Then point your `.env` at it (keep the `?charset=utf8mb4` query string):

```
PAPERQUERY_DATABASE_URL="mysql+pymysql://keydion:change-me@127.0.0.1:3306/keydion?charset=utf8mb4"
```

On first start the app connects, creates every table, and runs its idempotent
`ALTER TABLE` migrations — there is no separate schema or migration step.

> **MySQL version & RAG:** semantic search and "Ask the Library" store
> embeddings in a binary `VECTOR` column that **requires MySQL 9.x**. On MySQL
> 8.x the app still runs — the `VECTOR` migration is silently skipped — but the
> RAG / semantic features stay disabled until you move to 9.x.

## Production Deployment (gunicorn under systemd, host nginx)

Production runs gunicorn directly under systemd, with the host's nginx as
the reverse proxy. The Flask development server (and its Werkzeug debugger)
must **never** be exposed publicly.

`run_prod.sh` sources `.env.prod` and execs gunicorn with the config in
`gunicorn.conf.py`. nginx serves `/static/*` directly from disk; PDF
download routes (`/papers/*`) proxy through to Flask so auth checks run.

1. Create a `.env.prod` (gitignored) in the repo root by copying
   [`.env.example`](.env.example) and filling in the values. Use a strong
   random `PAPERQUERY_SECRET` (NOT `dev-secret-key`) and set
   `PAPERQUERY_MS_REDIRECT_URI` to your public callback URL
   (e.g. `https://yourdomain.com/auth/callback`).

2. Create the virtualenv and install dependencies in the repo root:

   ```bash
   python3 -m venv .venv
   .venv/bin/pip install -r requirements.txt
   ```

3. Drop in the nginx site config. A reference is at
   [`deploy/keydion.nginx.conf`](deploy/keydion.nginx.conf); the critical
   bit is `proxy_set_header X-Forwarded-Proto $scheme;` (or `https` for a
   TLS-only vhost) so Flask's `ProxyFix` generates correct HTTPS URLs for
   OAuth callbacks.

   ```bash
   sudo cp deploy/keydion.nginx.conf /etc/nginx/sites-available/keydion
   sudo ln -s /etc/nginx/sites-available/keydion /etc/nginx/sites-enabled/
   sudo nginx -t && sudo systemctl reload nginx
   ```

   TLS is out of scope here — terminate HTTPS in nginx itself (certbot) or
   an upstream load balancer.

4. Install the systemd unit at `/etc/systemd/system/keydion.service`:

   ```ini
   [Unit]
   Description=Keydion (gunicorn)
   After=network.target

   [Service]
   User=<owner of the repo>
   WorkingDirectory=/Keydion
   ExecStart=/Keydion/run_prod.sh
   ExecReload=/bin/kill -HUP $MAINPID
   Restart=on-failure
   KillSignal=SIGTERM
   TimeoutStopSec=30

   [Install]
   WantedBy=multi-user.target
   ```

5. Enable and start the service:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now keydion
   sudo systemctl status keydion --no-pager
   ```

### Updating the server after `git pull`

Match the command to what actually changed:

| Change | Command |
|---|---|
| Python code (`app.py`, templates, etc.) | `sudo systemctl reload keydion` |
| `requirements.txt` (new/upgraded packages) | `.venv/bin/pip install -r requirements.txt && sudo systemctl restart keydion` |
| `gunicorn.conf.py` or `.env.prod` | `sudo systemctl restart keydion` |
| `run_prod.sh` or the systemd unit itself | `sudo systemctl daemon-reload && sudo systemctl restart keydion` |
| `.po` translation source files | `.venv/bin/python tools/compile_translations.py && sudo systemctl reload keydion` |
| Anything under `static/` | nothing — nginx serves it directly from disk |
| `nginx` config | `sudo nginx -t && sudo systemctl reload nginx` |

`reload` sends `SIGHUP` to the gunicorn master: new workers spawn with the
updated code, and old workers drain in-flight requests before exiting. No
dropped connections.

Quick post-deploy check:

```bash
sudo systemctl status keydion --no-pager      # active (running)
sudo journalctl -u keydion -n 30 --no-pager   # look for fresh "Booting worker"
curl -sI https://www.keydion.com/ | head -5   # 200/302, Server: nginx
```

If the journal shows a traceback instead of fresh worker boots, the new
code failed to import — fix on disk and reload again.

## Local Development

> **Warning:** the Flask dev server (Werkzeug) is for local testing only. Never expose it publicly.

### 1. Environment

Clone the repository and create a `.env` file in the root directory by copying
[`.env.example`](.env.example) and filling in the values. For local use, set
`PAPERQUERY_MS_REDIRECT_URI=http://localhost:5000/auth/callback`.

> The abstract/keyword button only appears for Contributors (role ≥ 2) when
> `LLM_API_KEY` is set. It drafts the abstract and keywords from the uploaded
> PDF; the uploader reviews and edits before submitting. The library assistant
> uses `LLM_EMBED_*` for retrieval embeddings when set, falling back to
> `LLM_API_KEY` / `LLM_BASE_URL` and `gemini-embedding-001` otherwise. See
> [`LLM_DEPLOYMENT_IDEAS.md`](LLM_DEPLOYMENT_IDEAS.md) for other planned LLM uses.
>
> `WEB_SEARCH_API_KEY` unset = web access toggle hidden on the Ask page.

### 2. Running the dev server

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Database**: ensure MySQL is running and that you've created the database (see [Database Setup](#database-setup)). Tables are created automatically on first start.

3. **Start the dev server**:
   ```bash
   ./start_local.sh
   ```

## User Management

You can manage users (create, update, list) using the provided CLI tool:

```bash
# Create an admin user
python tools/manage_passwords.py set --username admin --password MySecurePassword --role 3

# List all users
python tools/manage_passwords.py list
```

**Roles:**
- `1`: Reader (View & Download)
- `2`: Moderator (Upload Enabled)
- `3`: Admin (Full Access)

## Building the Search Index

Papers uploaded before LLM was configured are not automatically embedded. Run this once after setting `LLM_API_KEY` (and optionally `LLM_EMBED_*`) to index any missing papers:

```bash
python3 tools/build_embeddings.py
```

The script resumes by default — papers that already have stored chunks are skipped. To force a full re-index:

```bash
python3 tools/build_embeddings.py --rebuild
```

New papers uploaded after LLM is configured are indexed automatically on upload.

## Localization

The project uses Flask-Babel for translations. To update translations:

1. Re-extract translatable strings into `messages.pot`:
   ```bash
   python tools/extract_translations.py
   ```
   Use this script rather than a bare `pybabel extract`. Babel's default
   directory filter skips any directory whose name starts with `.` or `_`,
   which silently drops nested template packages such as
   `templates/_dashboard/`; the script passes the `--ignore-dirs` set that
   keeps them (and excludes the virtualenvs). See `babel.cfg`.
2. Merge new strings into each catalog (keeps existing translations):
   ```bash
   pybabel update -i messages.pot -d translations -l zh --no-fuzzy-matching
   pybabel update -i messages.pot -d translations -l en --no-fuzzy-matching
   ```
3. Fill in the new `msgstr` values in `translations/*/LC_MESSAGES/messages.po`.
4. Compile the translations:
   ```bash
   python tools/compile_translations.py
   ```

## Project Structure

- `app.py`: Core Flask application logic and SQLAlchemy models.
- `templates/`: Jinja2 templates for the web interface.
- `static/`: CSS, JavaScript, and image assets.
- `data/`: Dynamic configuration and session storage.
- `papers/`: Storage for uploaded PDF files.
- `tools/`: Administrative utility scripts.

## License

Copyright © 2026 Keydion. All rights reserved. This is proprietary software; see [LICENSE](LICENSE) for the full terms.
