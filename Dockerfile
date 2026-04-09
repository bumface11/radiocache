FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml .
COPY radio_cache/ radio_cache/
COPY radio_cache_api.py .
COPY templates/ templates/
COPY static/ static/
COPY radio_cache_export.json .

RUN pip install --no-cache-dir "fastapi>=0.115" "jinja2>=3.1" "uvicorn[standard]>=0.34"

RUN mkdir -p recordings

ENV RECORDINGS_OUTPUT_DIR=/app/recordings

EXPOSE 8080
CMD ["uvicorn", "radio_cache_api:app", "--host", "0.0.0.0", "--port", "8080"]