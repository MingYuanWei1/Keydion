# Keydion - Academic Paper Management System

Keydion is a robust, scholarly-focused web application for managing, searching, and previewing academic papers. Built with Flask and MySQL, it features multi-language support, a premium design aesthetic, and specialized support for IB Essays.

## Features

- **Academic Search**: Advanced search interface with filters for subjects, dates, and languages.
- **Paper Preview**: In-browser PDF preview with a custom sidebar for metadata.
- **IB Extended Essay Support**: Specialized metadata fields for IB EE, IA, CP and other academic papers.
- **Multi-language Support**: Full internationalization (i18n) for English and Chinese.
- **Microsoft Authentication**: Integrated MS Graph API support for user login and profile synchronization.
- **News Management**: Built-in system for publishing and managing academic news and announcements.

## Prerequisites

- **Python 3.11+**
- **MySQL 9.x** (CI pins the official `mysql:9.7.1` image)
- **Tesseract OCR** (optional) — enables local text extraction from *scanned* PDFs (chat attachments, the abstract/keyword generator, and the papers index). Install the engine plus the Chinese language data:
  - Debian/Ubuntu: `apt-get install -y tesseract-ocr tesseract-ocr-chi-sim`
  - macOS: `brew install tesseract tesseract-lang`
  When LLM Vision is Configured, the system will prefer the LLM first, then local Tesseract extraction method.


## Database Setup

Keydion reaches MySQL through the `PAPERQUERY_DATABASE_URL` connection string in
your `.env` / `.env.prod`. You must create the **database** (and, typically, a
dedicated user) before deployment. Runtime startup validates that the database
is already at the expected Alembic revision; it does not upgrade a non-empty
schema or create the database itself.

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

For an existing installation, follow the coordinated
[Paper publishing migration runbook](docs/deployment/paper-publishing-migration.md)
before starting the new release. It owns preflight, backup, baseline stamping,
Alembic upgrade, validation, smoke tests, and rollback. Semantic search and
"Ask the Library" use MySQL 9.x binary `VECTOR` columns; unsupported MySQL or
schema shapes fail validation instead of silently degrading the migration.

## Production Deployment (gunicorn under systemd, host nginx)

Production runs gunicorn directly under systemd, with the host's nginx as
the reverse proxy. The Flask development server (and its Werkzeug debugger)
must **never** be exposed publicly.

The tracked web unit loads `/Keydion/.env.prod` and starts Gunicorn from the
shared `/Keydion/.venv`; the publishing worker has its own independently
enabled unit. nginx serves `/static/*` directly from disk; PDF download routes
(`/papers/*`) proxy through to Flask so auth checks run.

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

4. Verify the host uses the `keydion` account, `/Keydion`,
   `/Keydion/.env.prod`, and `/Keydion/.venv`, then install both tracked units:

   ```bash
   sudo cp deploy/keydion.service /etc/systemd/system/keydion.service
   sudo cp deploy/keydion-publishing-worker.service \
     /etc/systemd/system/keydion-publishing-worker.service
   sudo systemctl daemon-reload
   sudo systemd-analyze verify /etc/systemd/system/keydion.service \
     /etc/systemd/system/keydion-publishing-worker.service
   ```

   The web and worker use the same environment and Paper/pending storage. The
   worker is a separate process; Gunicorn never starts it from `post_fork`.

5. Enable and start the worker and web units independently:

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now keydion-publishing-worker
   sudo systemctl enable --now keydion
   sudo systemctl status keydion-publishing-worker --no-pager
   sudo systemctl status keydion --no-pager
   ```

   Existing databases must complete the linked migration runbook before either
   unit starts on the new release.

### Updating the server after `git pull`

Match the command to what actually changed:

| Change | Command |
|---|---|
| Python code (`app.py`, templates, worker/services) | `sudo systemctl restart keydion-publishing-worker && sudo systemctl reload keydion` |
| `requirements.txt` (new/upgraded packages) | `.venv/bin/pip install -r requirements.txt && sudo systemctl restart keydion-publishing-worker keydion` |
| `gunicorn.conf.py` or `.env.prod` | `sudo systemctl restart keydion-publishing-worker keydion` |
| Either tracked systemd unit | `sudo systemctl daemon-reload && sudo systemctl restart keydion-publishing-worker keydion` |
| `.po` translation source files | `.venv/bin/python tools/compile_translations.py && sudo systemctl reload keydion` |
| Anything under `static/` | nothing — nginx serves it directly from disk |
| `nginx` config | `sudo nginx -t && sudo systemctl reload nginx` |

`reload` sends `SIGHUP` to the gunicorn master: new workers spawn with the
updated code, and old workers drain in-flight requests before exiting. No
dropped connections.

Quick post-deploy check:

```bash
sudo systemctl status keydion-publishing-worker --no-pager
sudo systemctl status keydion --no-pager      # active (running)
sudo journalctl -u keydion-publishing-worker -n 30 --no-pager
sudo journalctl -u keydion -n 30 --no-pager   # look for fresh "Booting worker"
sudo -u keydion /Keydion/.venv/bin/python -m tools.publishing_worker --status
curl -sI https://www.keydion.com/ | head -5   # 200/302, Server: nginx
```

If the journal shows a traceback instead of fresh worker boots, the new
code failed to import — fix on disk and reload again.

## Docker reference stack

`docker-compose.prod.yml` is **not authoritative for production**. Production
operations, migration, worker supervision, and rollback use the tracked host
systemd units and the migration runbook. The unchanged Compose file runs only
`web` (Gunicorn) and `nginx`; it does not run the required independent
publishing worker.

For an explicitly non-production reference environment, the stack builds the
bundled [`Dockerfile`](Dockerfile) (Python 3.11 + Tesseract), so the OCR engine
and Chinese language data come baked in.

> **MySQL is not bundled.** The prod compose expects an **external MySQL 9.x**
> (see the `VECTOR` note under [Database Setup](#database-setup)). Point
> `PAPERQUERY_DATABASE_URL` in `.env.prod` at a host reachable *from the
> container* — inside the container `127.0.0.1` is the container itself, so use
> the Docker host's LAN IP or `host.docker.internal` for a MySQL on the host.

1. Create a `.env.prod` exactly as in the **Production Deployment** section
   above (strong `PAPERQUERY_SECRET`, a public `PAPERQUERY_MS_REDIRECT_URI`, and
   a container-reachable `PAPERQUERY_DATABASE_URL`).

2. Build and start the stack:

   ```bash
   docker compose -f docker-compose.prod.yml up -d --build
   ```

nginx listens on port 80; `web` and `nginx` communicate over a shared unix
socket (the `keydion-socket` volume), and nginx serves `/static/*` straight from
the mounted `./static`. TLS is out of scope — terminate HTTPS at a certbot /
load balancer in front, or add a `443` server block to
[`docker/nginx.conf`](docker/nginx.conf).

The `./papers`, `./data`, and `./static/uploads` directories are bind-mounted,
so uploaded PDFs and runtime data persist on the host across rebuilds. After a
`git pull`, rebuild and recreate:

```bash
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml logs -f web   # watch for "Booting worker"
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

Copyright © 2026 Keydion. Licensed under the Apache License, Version 2.0; see [LICENSE](LICENSE) for the full terms.
