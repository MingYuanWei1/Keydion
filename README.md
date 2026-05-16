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
