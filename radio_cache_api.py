"""FastAPI web application for the BBC Radio Drama programme cache.

Provides a REST API and a server-rendered search interface for
browsing cached radio drama programmes.  Designed to run on cheap
hosting (Render free tier, Fly.io, Railway, etc.).

Run locally with::

    uvicorn radio_cache_api:app --reload
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Final

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from radio_cache.cache_db import CacheDB
from radio_cache.models import format_duration
from radio_cache.search import (
    group_by_series,
    search_programmes,
)

_DB_PATH: Final[str] = os.environ.get("RADIO_CACHE_DB", "radio_cache.db")
_BASE_DIR: Final[Path] = Path(__file__).resolve().parent

app = FastAPI(
    title="BBC Radio Drama Cache",
    description="Search and browse BBC Radio drama programmes",
    version="1.0.0",
)

app.mount(
    "/static",
    StaticFiles(directory=str(_BASE_DIR / "static" / "radio_cache")),
    name="static",
)
templates = Jinja2Templates(directory=str(_BASE_DIR / "templates" / "radio_cache"))
templates.env.filters["format_duration"] = format_duration


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
        "series_list.html",
        {"request": request, "series": series, "stats": stats},
    )


@app.get("/series/{series_pid}", response_class=HTMLResponse)
async def series_detail(request: Request, series_pid: str) -> HTMLResponse:
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
    series_title = episodes[0].series_title if episodes else series_pid
    return templates.TemplateResponse(
        "series_detail.html",
        {
            "request": request,
            "series_pid": series_pid,
            "series_title": series_title,
            "episodes": episodes,
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
