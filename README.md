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
- **Docker & Docker Compose** (recommended)
- **MySQL 8.0+** (if running locally without Docker)

## Getting Started

### 1. Environment Setup

Clone the repository and create a `.env` file in the root directory:

```bash
PAPERQUERY_SECRET=your_secret_key_here
PAPERQUERY_DATABASE_URL="mysql+pymysql://user:password@host:port/dbname"

# Microsoft Integration (Optional)
PAPERQUERY_MS_CLIENT_ID=your_client_id
PAPERQUERY_MS_CLIENT_SECRET=your_client_secret
PAPERQUERY_MS_REDIRECT_URI=https://yourdomain.com/auth/callback
```

### 2. Using Docker (Recommended)

The easiest way to run the project is using Docker Compose:

```bash
docker-compose up -d
```

This will spin up the web application and a pre-configured MySQL database.

### 3. Local Development

If you prefer to run the application locally:

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Initialize Database**:
   Ensure your MySQL server is running and the database specified in `.env` exists. The application will automatically create the necessary tables on first start.

3. **Start the server**:
   ```bash
   ./start_local.sh
   ```

## Production Deployment (gunicorn + nginx)

The default `docker-compose.yml` runs the Flask development server with the
Werkzeug debugger enabled — **do not expose it publicly**. For production, use
`docker-compose.prod.yml`, which launches gunicorn behind an nginx reverse
proxy.

1. Create a `.env.prod` (gitignored) alongside `.env`:

   ```bash
   PAPERQUERY_SECRET=<strong random value, NOT dev-secret-key>
   PAPERQUERY_DATABASE_URL="mysql+pymysql://user:password@host:3306/dbname?charset=utf8mb4"

   # Microsoft OAuth — must match the Azure app registration redirect URI
   PAPERQUERY_MS_CLIENT_ID=...
   PAPERQUERY_MS_CLIENT_SECRET=...
   PAPERQUERY_MS_REDIRECT_URI=https://yourdomain.com/auth/callback

   # Optional gunicorn tuning
   GUNICORN_WORKERS=4
   GUNICORN_TIMEOUT=60
   PAPERQUERY_MAX_UPLOAD_MB=50
   ```

2. Build and start:

   ```bash
   docker-compose -f docker-compose.prod.yml up -d --build
   ```

   - nginx listens on `:80` and serves `/static/*` directly.
   - gunicorn runs application code with preforked workers behind a Unix
     socket. The Werkzeug debugger is not loaded.
   - PDF download routes (`/papers/*`) still go through Flask so auth checks
     run.

3. TLS is intentionally out of scope for this compose file — terminate HTTPS
   in a load balancer, Caddy/Traefik, or a separate certbot sidecar in front
   of nginx.

4. Graceful zero-downtime deploy after a code change:

   ```bash
   docker-compose -f docker-compose.prod.yml build web
   docker-compose -f docker-compose.prod.yml up -d web
   # or, to reload in place: docker exec keydion-web-prod kill -HUP 1
   ```

### Host-direct deployment (gunicorn under systemd, existing nginx)

If you already operate nginx on the host (instead of using the bundled
`docker-compose.prod.yml` stack), run gunicorn directly under systemd and
point your existing nginx at it. `run_prod.sh` sources `.env.prod` and
execs gunicorn with the config in `gunicorn.conf.py`.

A reference site config is at [`deploy/keydion.nginx.conf`](deploy/keydion.nginx.conf);
the critical bit is `proxy_set_header X-Forwarded-Proto $scheme;` (or
`https` for a TLS-only vhost) so Flask's `ProxyFix` generates correct
HTTPS URLs for OAuth callbacks.

A reference systemd unit (`/etc/systemd/system/keydion.service`):

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

## Localization

The project uses Flask-Babel for translations. To update translations:

1. Edit the translation strings in `translations/*/LC_MESSAGES/messages.po`.
2. Compile the translations:
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

Copyright © 2026 Keydion. All rights reserved.
