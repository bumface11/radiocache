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
import threading
import time
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
from radio_cache.refresh import import_from_github, import_from_json
from radio_cache.search import (
    group_by_series,
    search_programmes,
)

logger = logging.getLogger(__name__)

_DB_PATH: Final[str] = os.environ.get("RADIO_CACHE_DB", "radio_cache.db")
_JSON_PATH: Final[str] = os.environ.get(
    "RADIO_CACHE_JSON", "radio_cache_export.json"
)
_GITHUB_URL: Final[str] = os.environ.get("RADIO_CACHE_GITHUB_URL", "")
_BASE_DIR: Final[Path] = Path(__file__).resolve().parent


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Import cache from the JSON export file on startup.

    When the JSON export (populated by GitHub Actions) exists, its
    contents are loaded into the SQLite database so the web UI always
    reflects the latest data.  If the JSON file is missing and
    ``RADIO_CACHE_GITHUB_URL`` is set, the export is fetched from
    GitHub instead.
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
    elif _GITHUB_URL:
        try:
            count = import_from_github(_GITHUB_URL, _DB_PATH)
            logger.info(
                "Loaded %d programmes from GitHub on startup", count
            )
        except Exception:
            logger.exception("Failed to import cache from GitHub")
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


@app.get("/recordings", response_class=HTMLResponse)
async def recordings_page(request: Request) -> HTMLResponse:
    """Render the recordings management page.

    Args:
        request: Incoming HTTP request.

    Returns:
        Rendered HTML page showing recording jobs and a live-station form.
    """
    with _get_db() as db:
        stats = db.stats()
    return templates.TemplateResponse(
        request,
        "recordings.html",
        {"request": request, "stats": stats},
    )


# ── JSON API endpoints ────────────────────────────────────────────────


@app.get("/api/search")
async def api_search(
    q: str = Query(default="", description="Search query"),
    category: str = Query(default="", description="Filter by category tag"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    """JSON search endpoint for programmatic access.

    Args:
        q: Free-text search query.
        category: Optional category tag to filter results.
        limit: Max results.
        offset: Pagination offset.

    Returns:
        Dict with ``results`` and ``count``.
    """
    with _get_db() as db:
        if q:
            programmes = search_programmes(db, q, limit=limit, offset=offset)
            if category:
                programmes = [
                    p for p in programmes
                    if category.lower() in [c.strip().lower() for c in p.categories.split(",")]
                ]
        elif category:
            programmes = db.programmes_by_category(category, limit=limit, offset=offset)
        else:
            programmes = db.recent_programmes(limit=limit)
    return {
        "query": q,
        "category": category,
        "count": len(programmes),
        "results": [_prog_dict(p) for p in programmes],
    }


@app.get("/api/categories")
async def api_categories() -> dict:
    """JSON endpoint listing all distinct category tags.

    Returns:
        Dict with ``categories`` list, each entry having ``category``
        and ``programme_count`` keys.
    """
    with _get_db() as db:
        categories = db.list_categories()
    return {"count": len(categories), "categories": categories}

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


# ── Recording endpoints ──────────────────────────────────────────────────

from radio_cache.recording.job_manager import get_job_manager  # noqa: E402
from radio_cache.recording.models import (  # noqa: E402
    RecordingRequest,
    RecordingStatus,
    job_to_dict,
)
from radio_cache.recording import recorder as _recorder  # noqa: E402
from radio_cache.recording.stream_resolver import (  # noqa: E402
    StreamNotSupportedError,
    StreamUnavailableError,
    resolve_live_stream,
    resolve_programme_stream,
)


_recording_worker_lock = threading.Lock()
_recording_worker_thread: threading.Thread | None = None
_PROGRAMME_DURATION_BUFFER_SECONDS: Final[int] = int(
    os.environ.get("PROGRAMME_DURATION_BUFFER_SECONDS", "120")
)


def _buffered_programme_duration(duration_secs: int) -> int:
    """Return a duration cap with slack to avoid clipping programme endings."""
    ten_percent = int(duration_secs * 0.1)
    return duration_secs + max(_PROGRAMME_DURATION_BUFFER_SECONDS, ten_percent)


def _run_recording_queue_worker() -> None:
    """Process queued recording jobs sequentially in one background thread."""
    manager = get_job_manager()
    try:
        while True:
            queued_jobs = manager.list_jobs(status="queued", limit=200)
            if not queued_jobs:
                return

            # Process oldest queued job first to preserve FIFO behavior.
            next_job = min(queued_jobs, key=lambda j: j.created_at)

            # Job may have been cancelled between listing and execution.
            current = manager.get_job(next_job.job_id)
            if current is None or current.status != "queued":
                continue

            # Claim this queued job so it is not picked again.
            manager.update_status(
                next_job.job_id,
                "running",
                started_at=datetime.now(timezone.utc).isoformat(),
            )
            try:
                _run_recording_job(next_job.job_id)
            except Exception:
                logger.exception("Recording queue worker failed for job %s", next_job.job_id)
                manager.update_status(
                    next_job.job_id,
                    "failed",
                    error_code="failed",
                    error_message="Unexpected worker error",
                    completed_at=datetime.now(timezone.utc).isoformat(),
                )
            time.sleep(0.05)
    finally:
        global _recording_worker_thread
        with _recording_worker_lock:
            _recording_worker_thread = None


def _ensure_recording_worker() -> None:
    """Start the sequential recording worker if it is not already running."""
    global _recording_worker_thread
    with _recording_worker_lock:
        if _recording_worker_thread is not None and _recording_worker_thread.is_alive():
            return
        _recording_worker_thread = threading.Thread(
            target=_run_recording_queue_worker,
            name="recording-queue-worker",
            daemon=True,
        )
        _recording_worker_thread.start()


def _run_recording_job(job_id: str) -> None:
    """Background task: resolve stream, record, update job state.

    This function runs in a thread spawned by FastAPI BackgroundTasks.
    It transitions the job through queued → running → completed/failed/
    not_supported, writing structured log entries at each step.

    Args:
        job_id: ID of the job to execute.
    """
    manager = get_job_manager()
    job = manager.get_job(job_id)
    if job is None:
        logger.error("Background task: job %s not found", job_id)
        return

    from datetime import UTC, datetime

    # ── 1. Resolve stream ────────────────────────────────────────────────
    try:
        if job.source_type == "live":
            resolved = resolve_live_stream(job.source_id)
        else:
            resolved = resolve_programme_stream(job.source_id)
    except StreamNotSupportedError as exc:
        logger.warning("Job %s: stream not supported: %s", job_id, exc)
        manager.update_status(
            job_id,
            "not_supported",
            error_code="not_supported",
            error_message=str(exc),
            completed_at=datetime.now(UTC).isoformat(),
        )
        return
    except StreamUnavailableError as exc:
        logger.warning("Job %s: stream unavailable: %s", job_id, exc)
        manager.update_status(
            job_id,
            "failed",
            error_code="unavailable",
            error_message=str(exc),
            completed_at=datetime.now(UTC).isoformat(),
        )
        return
    except Exception as exc:
        logger.exception("Job %s: unexpected error during stream resolution", job_id)
        manager.update_status(
            job_id,
            "failed",
            error_code="failed",
            error_message=f"Stream resolution error: {exc}",
            completed_at=datetime.now(UTC).isoformat(),
        )
        return

    manager.update_status(
        job_id,
        "running",
        manifest_url=resolved.manifest_url,
        started_at=datetime.now(UTC).isoformat(),
    )
    logger.info("Job %s: resolved manifest %s", job_id, resolved.manifest_url)

    # ── 2. Resolve metadata from the catalogue (best-effort) ─────────────
    title = job.source_id
    station = ""
    programme = ""
    date = ""
    if job.source_type == "programme":
        with _get_db() as db:
            prog = db.get_programme(job.source_id)
        if prog:
            title = prog.title
            station = prog.channel
            programme = prog.series_title or prog.brand_title
            date = (prog.first_broadcast or "")[:10]
            if job.duration_seconds is None and prog.duration_secs > 0:
                job.duration_seconds = _buffered_programme_duration(prog.duration_secs)
                manager.update_status(
                    job_id,
                    "running",
                    duration_seconds=job.duration_seconds,
                )

    # ── 3. Build output path ─────────────────────────────────────────────
    output_path = _recorder.build_output_path(job, title=title)

    # ── 4. Record ────────────────────────────────────────────────────────
    def _on_progress(elapsed_secs: int) -> None:
        manager.update_status(job_id, "running", progress_seconds=elapsed_secs)

    try:
        _recorder.record_stream(
            job=job,
            manifest_url=resolved.manifest_url,
            output_path=output_path,
            title=title,
            station=station,
            programme=programme,
            date=date,
            progress_cb=_on_progress,
        )
    except FileNotFoundError as exc:
        logger.error("Job %s: ffmpeg not found: %s", job_id, exc)
        manager.update_status(
            job_id,
            "failed",
            error_code="failed",
            error_message=str(exc),
            completed_at=datetime.now(UTC).isoformat(),
        )
        return
    except ValueError as exc:
        # Duration cap exceeded
        logger.warning("Job %s: invalid parameters: %s", job_id, exc)
        manager.update_status(
            job_id,
            "failed",
            error_code="failed",
            error_message=str(exc),
            completed_at=datetime.now(UTC).isoformat(),
        )
        return
    except Exception as exc:
        logger.exception("Job %s: recording failed", job_id)
        manager.update_status(
            job_id,
            "failed",
            error_code="failed",
            error_message=f"Recording error: {exc}",
            completed_at=datetime.now(UTC).isoformat(),
        )
        return

    # ── 5. Check if cancelled mid-run ────────────────────────────────────
    refreshed = manager.get_job(job_id)
    if refreshed and refreshed.status == "cancelled":
        logger.info("Job %s: marked cancelled after capture", job_id)
        return

    manager.update_status(
        job_id,
        "completed",
        output_path=str(output_path),
        completed_at=datetime.now(UTC).isoformat(),
    )
    logger.info("Job %s: completed -> %s", job_id, output_path)


@app.post("/api/recordings", status_code=201)
async def create_recording(
    body: RecordingRequest,
) -> dict:
    """Submit a new recording job.

    The job is queued immediately and executed asynchronously.  Poll
    ``GET /api/recordings/{job_id}`` to track progress.

    Args:
        body: Recording request parameters.

    Returns:
        Dict with ``job_id`` and initial ``status``.
    """
    if body.source_type == "live" and body.duration_seconds is None:
        body.duration_seconds = 1800

    manager = get_job_manager()
    job = manager.create_job(
        source_type=body.source_type,
        source_id=body.source_id,
        output_format=body.output_format,
        duration_seconds=body.duration_seconds,
    )
    _ensure_recording_worker()
    logger.info(
        "Created recording job %s for %s/%s",
        job.job_id,
        body.source_type,
        body.source_id,
    )
    return {"job_id": job.job_id, "status": "queued", "created_at": job.created_at}


@app.get("/api/recordings")
async def list_recordings(
    status: RecordingStatus | None = Query(default=None, description="Filter by status"),
    limit: int = Query(default=50, ge=1, le=200),
) -> dict:
    """List recent recording jobs.

    Args:
        status: Optional status filter
            (``queued|running|completed|failed|not_supported|cancelled``).
        limit: Maximum number of results (newest first).

    Returns:
        Dict with ``count`` and ``jobs`` list.
    """
    manager = get_job_manager()
    jobs = manager.list_jobs(status=status, limit=limit)
    return {"count": len(jobs), "jobs": [job_to_dict(j) for j in jobs]}


@app.get("/api/recordings/{job_id}")
async def get_recording(job_id: str) -> dict:
    """Return the current state of a recording job.

    Args:
        job_id: UUID job identifier returned by ``POST /api/recordings``.

    Returns:
        Full job dict, or ``{"error": "not_found", "job_id": "..."}``
        with HTTP 404 when the job does not exist.
    """
    from fastapi import HTTPException

    manager = get_job_manager()
    job = manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "job_id": job_id})
    return job_to_dict(job)


@app.delete("/api/recordings/{job_id}")
async def cancel_recording(job_id: str) -> dict:
    """Cancel a queued or running recording job.

    Marks the job as ``"cancelled"`` and, if ffmpeg is running,
    terminates the subprocess.  Already-terminal jobs (completed,
    failed, not_supported) cannot be cancelled.

    Args:
        job_id: UUID job identifier.

    Returns:
        Dict with ``job_id`` and updated ``status``.

    Raises:
        HTTPException 404: Job not found.
        HTTPException 409: Job is already in a terminal state.
    """
    from fastapi import HTTPException

    manager = get_job_manager()
    job = manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "job_id": job_id})
    if job.status not in ("queued", "running"):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "not_cancellable",
                "job_id": job_id,
                "status": job.status,
            },
        )
    # Stop the ffmpeg process if one is running for this job.
    _recorder.terminate_job(job_id)
    updated = manager.cancel_job(job_id)
    if updated is None:
        raise HTTPException(status_code=404, detail={"error": "not_found", "job_id": job_id})
    logger.info("Cancelled recording job %s", job_id)
    return {"job_id": updated.job_id, "status": updated.status}
