# Dockerfile
# GTM UAE Partner Pipeline — container image
#
# Build:  docker compose build
# Run:    docker compose run --rm pipeline python graph.py
#         docker compose run --rm pipeline python scripts/migrate_excel_to_db.py

FROM python:3.11-slim

# ── System dependencies ──────────────────────────────────────────────────────
# libpq-dev  → needed by asyncpg / psycopg2 to compile Postgres client
# gcc        → needed for C extensions (asyncpg compiles a C layer)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Python dependencies (cached layer) ──────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ─────────────────────────────────────────────────────────
# Copied last so that code changes don't bust the pip-install cache layer.
# When using docker-compose with a volume mount (- .:/app), this layer is
# overridden at runtime — so you rarely need to rebuild for code changes.
COPY . .

# Backend service (docker-compose.yml's `backend`) serves on this port.
EXPOSE 8000

# ── Default command ──────────────────────────────────────────────────────────
CMD ["python", "graph.py"]