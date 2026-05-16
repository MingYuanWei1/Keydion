FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install system dependencies for PyMySQL/pymssql
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    freetds-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

# Ensure directories exist
RUN mkdir -p /app/papers /app/data /app/static/uploads/news /app/static/uploads/journal_covers /app/data/pending_papers

EXPOSE 4000

CMD ["python", "-m", "flask", "--app", "app", "run", "--debug", "--host", "0.0.0.0", "--port", "4000"]
