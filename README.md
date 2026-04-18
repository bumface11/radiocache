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
- **Downloadable SQLite snapshot** (`radio_cache.db.zip`) for fast deployment
  bootstrap without replaying a large JSON import.
- **Optional JSON export** (`radio_cache_export.json`) when a flat data dump is
  still useful.

## Quick Start

```bash
# Install uv (one-time; https://docs.astral.sh/uv/)

# Create a virtual environment and sync dependencies
uv sync --group dev

# Refresh the cache (fetches from BBC feeds)
uv run python -m radio_cache.refresh --verbose

# Or import from a SQLite snapshot
uv run python -m radio_cache.refresh --import-db-snapshot radio_cache.db.zip

# Start the web search UI
uv run uvicorn radio_cache_api:app --reload --reload-include="*.json"
```

Open `http://localhost:8000` to search and browse programmes.  Each programme
shows a copyable `get_iplayer` command for local download.

## Hosting Options (Cheap/Free)

| Option | Cost | Notes |
|---|---|---|
| **Render free tier** | Free | Deploy `radio_cache_api.py`; spins down on idle |
| **Fly.io** | Free tier | 3 shared VMs free |
| **Railway** | Free trial | Simple Docker deploy |
| **GitHub Releases** | Free | Host `radio_cache.db.zip` as a downloadable snapshot |
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

## BBC Programme Identifiers

The BBC API exposes several identifier fields for each programme.  Only the
**URN-derived PID** works reliably in BBC Sounds URLs and `get_iplayer`
commands:

| Field | Example | Purpose |
|---|---|---|
| `urn` | `urn:bbc:radio:episode:m002snjn` | Canonical identifier. The last colon-separated segment (`m002snjn`) is the **episode PID** used in BBC Sounds URLs and `get_iplayer`. |
| `pid` | `m002t10q` | **Version PID** — identifies a specific broadcast version of an episode, *not* the episode itself. Does not work in `/sounds/play/` URLs. |
| `id` | `p0n6f5q8` | Opaque API identifier. May differ from both the episode and version PIDs. Also does not work in `/sounds/play/` URLs. |

This project extracts the episode PID from the `urn` field, following the same
approach used by other BBC Sounds clients such as
[auntie-sounds](https://github.com/kieranhogg/auntie-sounds).

## Project Structure

- `radio_cache/` -- core Python package (models, database, parser, search)
- `radio_cache_api.py` -- FastAPI web application
- `templates/radio_cache/` -- Jinja2 HTML templates
- `static/radio_cache/` -- CSS styles
- `tests/` -- unit tests (61 tests)
- `.github/workflows/refresh-radio-cache.yml` -- daily cache refresh and SQLite snapshot publishing

## Deployment Bootstrap

Set one of these environment variables for empty deployments:

- `RADIO_CACHE_DB_SNAPSHOT` -- path to a local `.db` or `.zip` snapshot.
- `RADIO_CACHE_DB_SNAPSHOT_URL` -- URL of a downloadable `.db` or `.zip` snapshot.

For Cloud Run, the default deployment target is:

- `RADIO_CACHE_DB_SNAPSHOT_URL=https://github.com/bumface11/radiocache/releases/latest/download/radio_cache.db.zip`

The app will only bootstrap from a snapshot or JSON export when the target
database is missing or empty. If a populated `RADIO_CACHE_DB` already exists,
startup skips the import step.
