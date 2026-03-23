# BBC Radio Drama Cache

A cloud-hostable cache of BBC Radio drama programme metadata with a modern
search interface.

## Features

- **Daily refresh** from BBC Sounds feeds via GitHub Actions.
- **Full-text search** across titles, synopses, series, and categories.
- **Series grouping** -- episodes are grouped by parent series, with episode
  numbering for serialisations.
- **Brand hierarchy** -- series are grouped under their parent brands.
- **REST API** (FastAPI) for programmatic access at `/api/search`,
  `/api/series`, `/api/programme/{pid}`, and `/api/stats`.
- **Web UI** for searching and browsing, with one-click `get_iplayer` command
  copying for local downloads.
- **Static JSON export** (`radio_cache_export.json`) for cheap static hosting
  on GitHub Pages or similar.

## Quick Start

```bash
# Install uv (one-time; https://docs.astral.sh/uv/)

# Create a virtual environment and sync dependencies
uv sync --group dev

# Refresh the cache (fetches from BBC feeds)
uv run python -m radio_cache.refresh --verbose

# Or import from a JSON export
uv run python -m radio_cache.refresh --import-json radio_cache_export.json

# Start the web search UI
uv run uvicorn radio_cache_api:app --reload
```

Open `http://localhost:8000` to search and browse programmes.  Each programme
shows a copyable `get_iplayer` command for local download.

## Hosting Options (Cheap/Free)

| Option | Cost | Notes |
|---|---|---|
| **Render free tier** | Free | Deploy `radio_cache_api.py`; spins down on idle |
| **Fly.io** | Free tier | 3 shared VMs free |
| **Railway** | Free trial | Simple Docker deploy |
| **GitHub Pages** | Free | Host `radio_cache_export.json` as static file |
| **GitHub Actions** | Free | Daily cache refresh via cron workflow |

## Downloading Programmes

Find a programme in the web UI or JSON export, then use:

```bash
get_iplayer --pid=<PID> --type=radio
```

Requires [get_iplayer](https://github.com/get-iplayer/get_iplayer) installed
locally.

## Running Tests

```bash
uv sync --group dev
uv run pytest
```

## Project Structure

- `radio_cache/` -- core Python package (models, database, parser, search)
- `radio_cache_api.py` -- FastAPI web application
- `templates/radio_cache/` -- Jinja2 HTML templates
- `static/radio_cache/` -- CSS styles
- `tests/` -- unit tests (54 tests)
- `.github/workflows/refresh-radio-cache.yml` -- daily cache refresh cron job
