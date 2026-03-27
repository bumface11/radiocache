"""FastAPI web application for the BBC Radio Drama programme cache.

Provides a REST API and a server-rendered search interface for
browsing cached radio drama programmes.  Designed to run on cheap
hosting (Render free tier, Fly.io, Railway, etc.).

Run locally with::

    uv run uvicorn radio_cache_api:app --reload --reload-include="*.json"
"""

from __future__ import annotations

import io
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Final, Literal

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme, format_duration
from radio_cache.refresh import import_from_json
from radio_cache.search import (
    group_by_series,
    search_programmes,
)

logger = logging.getLogger(__name__)

_DB_PATH: Final[str] = os.environ.get("RADIO_CACHE_DB", "radio_cache.db")
_JSON_PATH: Final[str] = os.environ.get(
    "RADIO_CACHE_JSON", "radio_cache_export.json"
)
_BASE_DIR: Final[Path] = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Import cache from the JSON export file on startup.

    When the JSON export (populated by GitHub Actions) exists, its
    contents are loaded into the SQLite database so the web UI always
    reflects the latest data.
    """
    json_path = Path(_JSON_PATH)
    if json_path.exists():
        try:
            count = import_from_json(str(json_path), _DB_PATH)
            logger.info(
                "Loaded %d programmes from %s on startup", count, json_path
            )
        except Exception:
            logger.exception("Failed to import cache from %s", json_path)
    yield


app = FastAPI(
    title="BBC Radio Drama Cache",
    description="Search and browse BBC Radio drama programmes",
    version="1.0.0",
    lifespan=lifespan,
)

app.mount(
    "/static",
    StaticFiles(directory=str(_BASE_DIR / "static" / "radio_cache")),
    name="static",
)
templates = Jinja2Templates(directory=str(_BASE_DIR / "templates" / "radio_cache"))
templates.env.filters["format_duration"] = format_duration


def format_short_date(value: str) -> str:
    """Format an ISO-like timestamp string as d/m/yy.

    Args:
        value: Date/time string, typically ISO-8601.

    Returns:
        Date in d/m/yy format, or the original input when parsing fails.
    """
    if not value:
        return ""

    dt = _parse_iso_datetime(value)
    if dt is None:
        return value

    return f"{dt.day}/{dt.month}/{dt.strftime('%y')}"


def _parse_iso_datetime(value: str) -> datetime | None:
    """Parse a date/time string into an aware UTC datetime."""
    if not value:
        return None

    cleaned = value.strip()
    try:
        dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = datetime.fromisoformat(cleaned[:10])
        except ValueError:
            return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def is_recent_broadcast(value: str) -> bool:
    """Return True when ``first_broadcast`` is within the last 3 days."""
    dt = _parse_iso_datetime(value)
    if dt is None:
        return False

    delta = datetime.now(timezone.utc) - dt
    return timedelta(0) <= delta <= timedelta(days=3)


def is_expiring_soon(value: str) -> bool:
    """Return True when ``available_until`` is within the next 7 days."""
    dt = _parse_iso_datetime(value)
    if dt is None:
        return False

    delta = dt - datetime.now(timezone.utc)
    return timedelta(0) <= delta <= timedelta(days=7)


SortOption = Literal[
    "series_order",
    "broadcast_newest",
    "broadcast_oldest",
    "expiry_soonest",
    "title_az",
]


def _utc_seconds(dt: datetime) -> float:
    """Convert a UTC datetime to seconds since epoch without using timestamp()."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (dt - epoch).total_seconds()


def _sort_series_order_key(ep: Programme) -> tuple[object, ...]:
    """Default logical order: numbered episodes first, then dated items."""
    return (
        0 if ep.episode_number > 0 else 1,
        ep.episode_number if ep.episode_number > 0 else 2147483647,
        0 if ep.first_broadcast else 1,
        ep.first_broadcast or "9999-99-99T99:99:99Z",
        ep.title.lower(),
        ep.pid,
    )


def _sort_episodes(episodes: list[Programme], sort_by: SortOption) -> list[Programme]:
    """Return a sorted copy of ``episodes`` according to the selected order."""
    if sort_by == "broadcast_newest":
        return sorted(
            episodes,
            key=lambda ep: (
                _parse_iso_datetime(ep.first_broadcast) is None,
                -_utc_seconds(_parse_iso_datetime(ep.first_broadcast) or datetime.max.replace(tzinfo=timezone.utc)),
                ep.title.lower(),
                ep.pid,
            ),
        )
    if sort_by == "broadcast_oldest":
        return sorted(
            episodes,
            key=lambda ep: (
                _parse_iso_datetime(ep.first_broadcast) is None,
                _utc_seconds(_parse_iso_datetime(ep.first_broadcast) or datetime.max.replace(tzinfo=timezone.utc)),
                ep.title.lower(),
                ep.pid,
            ),
        )
    if sort_by == "expiry_soonest":
        return sorted(
            episodes,
            key=lambda ep: (
                _parse_iso_datetime(ep.available_until) is None,
                _utc_seconds(_parse_iso_datetime(ep.available_until) or datetime.max.replace(tzinfo=timezone.utc)),
                ep.title.lower(),
                ep.pid,
            ),
        )
    if sort_by == "title_az":
        return sorted(
            episodes,
            key=lambda ep: (
                ep.title.lower(),
                _parse_iso_datetime(ep.first_broadcast) is None,
                _utc_seconds(_parse_iso_datetime(ep.first_broadcast) or datetime.max.replace(tzinfo=timezone.utc)),
                ep.pid,
            ),
        )
    return sorted(episodes, key=_sort_series_order_key)


templates.env.filters["format_short_date"] = format_short_date
templates.env.filters["is_recent_broadcast"] = is_recent_broadcast
templates.env.filters["is_expiring_soon"] = is_expiring_soon


def _get_db() -> CacheDB:
    """Open a cache database connection.

    Returns:
        CacheDB instance.
    """
    return CacheDB(_DB_PATH)


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    """Render the main search page.

    Args:
        request: Incoming HTTP request.

    Returns:
        Rendered HTML page.
    """
    with _get_db() as db:
        stats = db.stats()
        recent = db.recent_programmes(limit=20)
    return templates.TemplateResponse(
        request,
        "index.html",
        {"request": request, "stats": stats, "recent": recent},
    )


@app.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(default="", description="Search query"),
    page: int = Query(default=1, ge=1, description="Page number"),
) -> HTMLResponse:
    """Render search results.

    Args:
        request: Incoming HTTP request.
        q: Free-text search query.
        page: Page number for pagination.

    Returns:
        Rendered HTML with search results.
    """
    per_page = 50
    offset = (page - 1) * per_page

    with _get_db() as db:
        if q:
            programmes = search_programmes(db, q, limit=per_page, offset=offset)
        else:
            programmes = db.recent_programmes(limit=per_page)
        stats = db.stats()

    series_groups = group_by_series(programmes)
    return templates.TemplateResponse(
        request,
        "search_results.html",
        {
            "request": request,
            "query": q,
            "programmes": programmes,
            "series_groups": series_groups,
            "stats": stats,
            "page": page,
            "per_page": per_page,
        },
    )


@app.get("/series", response_class=HTMLResponse)
async def series_list(request: Request) -> HTMLResponse:
    """List all series in the cache.

    Args:
        request: Incoming HTTP request.

    Returns:
        Rendered HTML page listing all series.
    """
    with _get_db() as db:
        series = db.list_series()
        stats = db.stats()
    return templates.TemplateResponse(
        request,
        "series_list.html",
        {"request": request, "series": series, "stats": stats},
    )


@app.get("/series/{series_pid}", response_class=HTMLResponse)
async def series_detail(
    request: Request,
    series_pid: str,
    sort: SortOption = Query(default="series_order"),
    prev: str = Query(default=""),
) -> HTMLResponse:
    """Show episodes in a series.

    Args:
        request: Incoming HTTP request.
        series_pid: BBC series PID.

    Returns:
        Rendered HTML page showing series episodes.
    """
    with _get_db() as db:
        episodes = db.get_series_episodes(series_pid)
        stats = db.stats()
    episodes = _sort_episodes(episodes, sort)
    series_title = episodes[0].series_title if episodes else series_pid
    if prev.startswith("/") and not prev.startswith("//"):
        previous_url = prev
    else:
        referer = request.headers.get("referer", "")
        base_url = str(request.base_url)
        if referer.startswith(base_url):
            previous_url = "/" + referer[len(base_url) :].lstrip("/")
        elif referer.startswith("/"):
            previous_url = referer
        else:
            previous_url = "/series"

    if previous_url.startswith(f"/series/{series_pid}"):
        previous_url = "/series"

    sort_options = [
        {"value": "series_order", "label": "Series order"},
        {"value": "broadcast_newest", "label": "Broadcast date (newest first)"},
        {"value": "broadcast_oldest", "label": "Broadcast date (oldest first)"},
        {"value": "expiry_soonest", "label": "Expiry date (soonest first)"},
        {"value": "title_az", "label": "Title (A-Z)"},
    ]
    return templates.TemplateResponse(
        request,
        "series_detail.html",
        {
            "request": request,
            "series_pid": series_pid,
            "series_title": series_title,
            "episodes": episodes,
            "sort": sort,
            "sort_options": sort_options,
            "previous_url": previous_url,
            "stats": stats,
        },
    )


@app.get("/brands", response_class=HTMLResponse)
async def brand_list(request: Request) -> HTMLResponse:
    """List all brands in the cache.

    Args:
        request: Incoming HTTP request.

    Returns:
        Rendered HTML page listing all brands.
    """
    with _get_db() as db:
        brands = db.list_brands()
        stats = db.stats()
    return templates.TemplateResponse(
        request,
        "brand_list.html",
        {"request": request, "brands": brands, "stats": stats},
    )


@app.get("/brands/{brand_pid}", response_class=HTMLResponse)
async def brand_detail(request: Request, brand_pid: str) -> HTMLResponse:
    """Show series within a brand.

    Args:
        request: Incoming HTTP request.
        brand_pid: BBC brand PID.

    Returns:
        Rendered HTML page showing brand series.
    """
    with _get_db() as db:
        series = db.get_brand_series(brand_pid)
        stats = db.stats()
    return templates.TemplateResponse(
        request,
        "brand_detail.html",
        {
            "request": request,
            "brand_pid": brand_pid,
            "series": series,
            "stats": stats,
        },
    )


# ── JSON API endpoints ────────────────────────────────────────────────


@app.get("/api/search")
async def api_search(
    q: str = Query(default="", description="Search query"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """JSON search endpoint for programmatic access.

    Args:
        q: Free-text search query.
        limit: Max results.
        offset: Pagination offset.

    Returns:
        Dict with ``results`` and ``count``.
    """
    with _get_db() as db:
        if q:
            programmes = search_programmes(db, q, limit=limit, offset=offset)
        else:
            programmes = db.recent_programmes(limit=limit)
    return {
        "query": q,
        "count": len(programmes),
        "results": [_prog_dict(p) for p in programmes],
    }


@app.get("/api/series")
async def api_series() -> dict:
    """JSON endpoint listing all series.

    Returns:
        Dict with ``series`` list.
    """
    with _get_db() as db:
        series = db.list_series()
    return {"count": len(series), "series": series}


@app.get("/api/series/{series_pid}")
async def api_series_detail(series_pid: str) -> dict:
    """JSON endpoint for series episodes.

    Args:
        series_pid: BBC series PID.

    Returns:
        Dict with ``episodes`` list.
    """
    with _get_db() as db:
        episodes = db.get_series_episodes(series_pid)
    return {
        "series_pid": series_pid,
        "count": len(episodes),
        "episodes": [_prog_dict(e) for e in episodes],
    }


@app.get("/api/programme/{pid}")
async def api_programme(pid: str) -> dict:
    """JSON endpoint for a single programme.

    Args:
        pid: BBC programme PID.

    Returns:
        Programme dict or error.
    """
    with _get_db() as db:
        prog = db.get_programme(pid)
    if prog is None:
        return {"error": "not_found", "pid": pid}
    return _prog_dict(prog)


@app.get("/api/stats")
async def api_stats() -> dict:
    """JSON endpoint for cache statistics.

    Returns:
        Cache stats dict.
    """
    with _get_db() as db:
        stats = db.stats()
    return {
        "total_programmes": stats.total_programmes,
        "total_series": stats.total_series,
        "total_brands": stats.total_brands,
        "last_refreshed": stats.last_refreshed,
    }


@app.get("/export/radio.cache", response_class=PlainTextResponse)
async def export_radio_cache() -> PlainTextResponse:
    """Stream the get_iplayer-compatible radio cache flat file.

    Returns:
        Plain-text pipe-delimited cache in the native get_iplayer v3.36 format::

            #index|type|name|episode|seriesnum|episodenum|pid|channel|available|expires|duration|desc|web|thumbnail|timeadded
    """
    buf = io.StringIO()
    with _get_db() as db:
        db.export_get_iplayer_cache(buf)
    return PlainTextResponse(
        buf.getvalue(),
        media_type="text/plain; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="radio.cache"'},
    )


def _prog_dict(prog: object) -> dict:
    """Convert a Programme to a JSON-serialisable dict.

    Args:
        prog: Programme instance.

    Returns:
        Dictionary representation.
    """
    from radio_cache.models import Programme

    if not isinstance(prog, Programme):
        return {}
    return {
        "pid": prog.pid,
        "title": prog.title,
        "synopsis": prog.synopsis,
        "duration": format_duration(prog.duration_secs),
        "duration_secs": prog.duration_secs,
        "available_until": prog.available_until,
        "first_broadcast": prog.first_broadcast,
        "programme_type": prog.programme_type,
        "series_pid": prog.series_pid,
        "series_title": prog.series_title,
        "brand_pid": prog.brand_pid,
        "brand_title": prog.brand_title,
        "episode_number": prog.episode_number,
        "channel": prog.channel,
        "thumbnail_url": prog.thumbnail_url,
        "categories": prog.categories,
        "url": prog.url,
        "get_iplayer_cmd": f"get_iplayer --pid={prog.pid} --type=radio",
    }
