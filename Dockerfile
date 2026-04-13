FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY radio_cache/ radio_cache/
COPY radio_cache_api.py .
COPY templates/ templates/
COPY static/ static/

# Install exactly the versions in uv.lock — never resolves newer versions
RUN uv sync --frozen --no-dev --no-install-project

RUN mkdir -p recordings

ENV RECORDINGS_OUTPUT_DIR=/app/recordings

ENV RADIO_CACHE_GITHUB_URL=https://raw.githubusercontent.com/bumface11/radiocache/main/radio_cache_export.json

EXPOSE 8080
CMD ["/app/.venv/bin/uvicorn", "radio_cache_api:app", "--host", "0.0.0.0", "--port", "8080"]