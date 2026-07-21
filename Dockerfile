# syntax=docker/dockerfile:1.4
FROM python:3.11-slim

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
    tesseract-ocr-chi-sim

COPY requirements.txt /app/requirements.txt
# pip cache mount (no --no-cache-dir): when requirements.txt changes, only the
# newly added packages download; the rest are served from the persistent cache.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /app/requirements.txt

COPY . /app

# Ensure directories exist
RUN mkdir -p /app/papers /app/data /app/static/uploads/news /app/static/uploads/journal_covers /app/data/pending_papers

EXPOSE 4000

CMD ["gunicorn", "-c", "gunicorn.conf.py", "app:app"]
