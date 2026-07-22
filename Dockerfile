# syntax=docker/dockerfile:1.4@sha256:9ba7531bd80fb0a858632727cf7a112fbfd19b17e94c4e84ced81e24ef1a0dbc
FROM python:3.11-slim@sha256:e031123e3d85762b141ad1cbc56452ba69c6e722ebf2f042cc0dc86c47c0d8b3

ARG KEYDION_UID=10001
ARG KEYDION_GID=10001

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps: build tools for PyMySQL/pymssql + Tesseract OCR engine and
# Chinese language data for scanned-PDF extraction. The apt cache mounts reuse
# downloaded .debs and the package index across rebuilds (and keep them out of
# the final image, so no rm of /var/lib/apt/lists is needed).
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    freetds-dev \
    tesseract-ocr \
    tesseract-ocr-chi-sim \
    && groupadd --gid "$KEYDION_GID" keydion \
    && useradd --uid "$KEYDION_UID" --gid "$KEYDION_GID" \
        --no-create-home --home-dir /app --shell /usr/sbin/nologin keydion

COPY requirements.txt requirements.lock /app/
# pip cache mount: when requirements.lock changes, only newly selected wheels
# download; the rest are served from the persistent cache.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --require-hashes -r /app/requirements.lock

# Copy an explicit production allowlist.  Never replace this with COPY .: the
# build directory commonly contains .env.prod, private Papers, and databases.
COPY app.py wsgi.py config.py db.py models.py gunicorn.conf.py alembic.ini /app/
COPY ee_pdf_extractor.py ia_metadata.py library_tools.py llm_client.py /app/
COPY llm_metadata.py pdf_text.py rag_index.py vision_extractor.py vision_read.py web_search.py /app/
COPY routes /app/routes
COPY services /app/services
COPY migrations /app/migrations
COPY templates /app/templates
COPY static/css /app/static/css
COPY static/js /app/static/js
COPY static/landing /app/static/landing
COPY static/vendor /app/static/vendor
COPY static/K.png static/usricon.png /app/static/
COPY data/*.sample.json /app/data/
COPY translations /app/translations
COPY tools/__init__.py tools/attachment_worker.py tools/bootstrap_database.py /app/tools/
COPY tools/extract_attachment.py tools/publishing_worker.py tools/verify_alembic_state.py /app/tools/
COPY tools/verify_paper_integrity.py /app/tools/

# Runtime paths are private and writable only by the unprivileged service user.
RUN mkdir -p \
        /app/papers \
        /app/data/pending_papers \
        /app/resource_files \
        /app/static/uploads/news \
        /app/static/uploads/journal_covers \
        /app/static/uploads/guides \
        /var/run/keydion \
    && chown -R keydion:keydion \
        /app/papers \
        /app/data \
        /app/resource_files \
        /app/static/uploads \
        /var/run/keydion \
    && chmod 0750 /app/papers /app/data /app/resource_files /app/static/uploads /var/run/keydion

USER keydion:keydion

EXPOSE 4000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "wsgi:app"]
