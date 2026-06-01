"""Schedule-first cache refresh (Approach 2).

This module implements an alternative ingestion strategy modelled after
get_iplayer's schedule-based sweep:

1. **Phase 1 — Programme discovery** (fast): Sweep all available
   programmes on allowed channels by querying the BBC RMS playable API
   filtered by network ID.  This returns *all* currently-available
   episodes without needing category slugs or container backfill.

2. **Phase 2 — Category enrichment** (targeted): For programmes missing
   category metadata, fetch ``/programmes/{pid}.json`` individually to
   obtain the three-level ``broader.category`` hierarchy.

The output is written to **separate** database and cache files so it
does not interfere with the category-first approach:

- ``radio_cache_schedule.db`` (SQLite database)
- ``radio_schedule.cache`` (get_iplayer-compatible flat file)

Run as a CLI::

    python -m radio_cache.schedule_refresh [--verbose] [--max-pages N]
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from typing import Final

from radio_cache.bbc_feed_parser import (
    _fetch_json,
    _is_allowed_channel,
    _parse_programme_item,
)
from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme

logger = logging.getLogger(__name__)

# ── BBC RMS API constants ─────────────────────────────────────────────

_BBC_PLAYABLE_API: Final[str] = (
    "https://rms.api.bbc.co.uk/v2/programmes/playable"
)
_BBC_PROGRAMMES_API: Final[str] = "https://www.bbc.co.uk/programmes"

# BBC network IDs for channels we care about.
# These map to the allowed channels: Radio 4, Radio 4 Extra, Radio 3.
_NETWORK_IDS: Final[list[str]] = [
    "bbc_radio_fourfm",
    "bbc_radio_four_extra",
    "bbc_radio_three",
]

_PAGE_LIMIT: Final[int] = 30
_DEFAULT_MAX_PAGES: Final[int] = 200
_REQUEST_DELAY_SECS: Final[float] = 1.0
_REQUEST_TIMEOUT_SECS: Final[int] = 30
_MAX_RETRIES: Final[int] = 3
_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (compatible; RadioCacheBot/1.0; "
    "+https://github.com/bumface11/radiocache)"
)

# Default output paths (separate from the category-first approach)
_DEFAULT_DB_PATH: Final[str] = "radio_cache_schedule.db"
_DEFAULT_CACHE_PATH: Final[str] = "radio_schedule.cache"


# ── Phase 1: Schedule sweep ──────────────────────────────────────────


def _fetch_network_programmes(
    network_id: str,
    max_pages: int = _DEFAULT_MAX_PAGES,
    delay: float = _REQUEST_DELAY_SECS,
) -> list[Programme]:
    """Fetch all available programmes for a BBC network.

    Uses the RMS playable API without a category filter, just the
    network constraint.  This mirrors get_iplayer's approach of
    sweeping all content on a channel.

    Args:
        network_id: BBC network identifier (e.g. ``"bbc_radio_fourfm"``).
        max_pages: Maximum pages to fetch.
        delay: Seconds to sleep between requests.

    Returns:
        List of discovered :class:`Programme` objects.
    """
    programmes: list[Programme] = []
    for page in range(max_pages):
        offset = page * _PAGE_LIMIT
        url = (
            f"{_BBC_PLAYABLE_API}?network={network_id}"
            f"&sort=date&offset={offset}&limit={_PAGE_LIMIT}"
        )
        data = _fetch_json(url)
        if data is None or not isinstance(data, dict):
            logger.warning(
                "Network %s page %d: no data returned; stopping.",
                network_id, page + 1,
            )
            break

        items = data.get("data") or []
        if not items:
            break

        for item in items:
            prog = _parse_programme_item(item)
            if prog is not None and _is_allowed_channel(prog.channel):
                programmes.append(prog)

        total: int = data.get("total", 0)
        if offset + len(items) >= total or len(items) < _PAGE_LIMIT:
            break

        if page % 10 == 9:
            logger.info(
                "  network %s: page %d, %d programmes so far",
                network_id, page + 1, len(programmes),
            )

        time.sleep(delay)

    logger.info(
        "Network %s: fetched %d programmes in %d pages",
        network_id, len(programmes), min(page + 1, max_pages),
    )
    return programmes


def fetch_schedule_programmes(
    network_ids: list[str] | None = None,
    max_pages: int = _DEFAULT_MAX_PAGES,
    delay: float = _REQUEST_DELAY_SECS,
) -> list[Programme]:
    """Phase 1: Discover all available programmes via network sweep.

    Args:
        network_ids: Network IDs to sweep; defaults to Radio 4/4X/3.
        max_pages: Max pages per network.
        delay: Inter-request delay.

    Returns:
        De-duplicated list of programmes (by PID).
    """
    networks = network_ids or _NETWORK_IDS
    pid_to_prog: dict[str, Programme] = {}

    for network_id in networks:
        logger.info("Phase 1: sweeping network %s", network_id)
        progs = _fetch_network_programmes(network_id, max_pages, delay)
        for prog in progs:
            if prog.pid not in pid_to_prog:
                pid_to_prog[prog.pid] = prog
        time.sleep(delay)

    logger.info(
        "Phase 1 complete: %d unique programmes from %d networks",
        len(pid_to_prog), len(networks),
    )
    return list(pid_to_prog.values())


# ── Phase 2: Category enrichment ─────────────────────────────────────


def _fetch_json_with_retry(
    url: str,
    max_retries: int = _MAX_RETRIES,
    delay: float = _REQUEST_DELAY_SECS,
) -> dict | list | None:
    """Fetch JSON with retry and exponential backoff for transient errors.

    Args:
        url: URL to fetch.
        max_retries: Number of retry attempts.
        delay: Base delay between retries (doubled each attempt).

    Returns:
        Parsed JSON, or ``None`` on persistent failure.
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        },
    )
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(
                req, timeout=_REQUEST_TIMEOUT_SECS
            ) as resp:
                result: dict | list = json.loads(resp.read())
                return result
        except (
            urllib.error.URLError,
            json.JSONDecodeError,
            TimeoutError,
            ConnectionError,
            OSError,
        ) as exc:
            if attempt < max_retries:
                wait = delay * (2 ** attempt)
                logger.debug(
                    "Retry %d/%d for %s after error: %s (wait %.1fs)",
                    attempt + 1, max_retries, url, exc, wait,
                )
                time.sleep(wait)
            else:
                logger.warning("Failed to fetch %s after %d attempts: %s",
                               url, max_retries + 1, exc)
                return None
    return None  # pragma: no cover


def _parse_categories_from_programmes_json(data: dict) -> str:
    """Extract categories from a ``/programmes/{pid}.json`` response.

    The ``/programmes/{pid}.json`` endpoint returns a structure like::

        {"programme": {"categories": [{"title": "Drama", "broader": ...}]}}

    This function traverses the three-level ``broader.category`` hierarchy
    (matching get_iplayer behaviour) and returns a comma-separated string.

    Args:
        data: Parsed JSON response from the programmes endpoint.

    Returns:
        Comma-separated category string, or empty string.
    """
    prog_data = data.get("programme") or data
    categories_list = prog_data.get("categories") or []
    if not isinstance(categories_list, list) or not categories_list:
        return ""

    cats1: list[str] = []
    cats2: list[str] = []
    cats3: list[str] = []
    for cat in categories_list:
        if not isinstance(cat, dict):
            continue
        title = cat.get("title") or cat.get("id") or ""
        if title:
            cats1.append(title)
        broader = (cat.get("broader") or {}).get("category") or {}
        if broader:
            bt = broader.get("title") or broader.get("id") or ""
            if bt:
                cats2.append(bt)
            grandparent = (
                (broader.get("broader") or {}).get("category") or {}
            )
            if grandparent:
                gt = grandparent.get("title") or grandparent.get("id") or ""
                if gt:
                    cats3.append(gt)

    # Assemble deduplicated list broadest-first (mirrors get_iplayer)
    seen: set[str] = set()
    all_cats: list[str] = []
    for cat_title in cats3 + cats2 + cats1:
        if cat_title and cat_title not in seen:
            seen.add(cat_title)
            all_cats.append(cat_title)
    return ",".join(all_cats)


def _fetch_categories_for_pid(
    pid: str, delay: float = _REQUEST_DELAY_SECS,
) -> str:
    """Fetch categories for a single PID from the programmes API.

    Args:
        pid: BBC programme/brand/series PID.
        delay: Base delay for retries.

    Returns:
        Comma-separated category string, or empty string on failure.
    """
    url = f"{_BBC_PROGRAMMES_API}/{pid}.json"
    data = _fetch_json_with_retry(url, delay=delay)
    if not isinstance(data, dict):
        return ""
    return _parse_categories_from_programmes_json(data)


def enrich_categories(
    programmes: list[Programme],
    delay: float = _REQUEST_DELAY_SECS,
    max_enrichments: int = 0,
) -> list[Programme]:
    """Phase 2: Fetch category data for programmes missing it.

    Enriches at the **brand/series level** to minimise requests: all
    episodes sharing a brand or series PID inherit the same categories.
    Only falls back to per-episode lookup when no brand/series PID is
    available.

    Args:
        programmes: List from phase 1.
        delay: Inter-request delay.
        max_enrichments: Max brand/series PIDs to look up (0 = unlimited).

    Returns:
        Updated programme list with categories populated where possible.
    """
    needs_enrichment = [p for p in programmes if not p.categories]
    logger.info(
        "Phase 2: %d/%d programmes need category enrichment",
        len(needs_enrichment), len(programmes),
    )

    # Group by brand_pid or series_pid to avoid per-episode lookups.
    # All episodes in a brand/series share the same categories.
    container_to_pids: dict[str, list[str]] = {}
    orphan_pids: list[str] = []
    for prog in needs_enrichment:
        container = prog.brand_pid or prog.series_pid
        if container:
            container_to_pids.setdefault(container, []).append(prog.pid)
        else:
            orphan_pids.append(prog.pid)

    lookup_pids = list(container_to_pids.keys()) + orphan_pids
    logger.info(
        "  %d unique containers + %d orphans = %d lookups needed "
        "(vs %d per-episode)",
        len(container_to_pids), len(orphan_pids),
        len(lookup_pids), len(needs_enrichment),
    )

    if max_enrichments > 0:
        lookup_pids = lookup_pids[:max_enrichments]
        logger.info("  (capped to %d lookups)", max_enrichments)

    # Fetch categories for each unique container/orphan PID
    pid_categories: dict[str, str] = {}
    success_count = 0
    for i, pid in enumerate(lookup_pids):
        cats = _fetch_categories_for_pid(pid, delay=delay)
        if cats:
            pid_categories[pid] = cats
            success_count += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "  looked up %d/%d (%d successful)",
                i + 1, len(lookup_pids), success_count,
            )
        time.sleep(delay)

    logger.info(
        "Phase 2 complete: %d/%d lookups returned categories",
        success_count, len(lookup_pids),
    )

    # Apply categories to all programmes
    result: list[Programme] = []
    enriched_count = 0
    for prog in programmes:
        if prog.categories:
            result.append(prog)
            continue

        # Try container first, then the episode PID itself
        container = prog.brand_pid or prog.series_pid
        cats = (
            (pid_categories.get(container) if container else None)
            or pid_categories.get(prog.pid, "")
        )
        if cats:
            result.append(dataclasses.replace(prog, categories=cats))
            enriched_count += 1
        else:
            result.append(prog)

    logger.info(
        "  Applied categories to %d/%d programmes",
        enriched_count, len(needs_enrichment),
    )
    return result


# ── Orchestrator ─────────────────────────────────────────────────────


def schedule_refresh(
    db_path: str = _DEFAULT_DB_PATH,
    cache_path: str = _DEFAULT_CACHE_PATH,
    max_pages: int = _DEFAULT_MAX_PAGES,
    max_enrichments: int = 0,
    delay: float = _REQUEST_DELAY_SECS,
    purge_expired: bool = True,
    export_cache: bool = True,
) -> int:
    """Run the full schedule-first refresh pipeline.

    Args:
        db_path: SQLite database output path (separate from category approach).
        cache_path: get_iplayer cache output path (separate from category approach).
        max_pages: Max pages per network in phase 1.
        max_enrichments: Max PIDs to enrich in phase 2 (0 = all).
        delay: Inter-request delay.
        purge_expired: Remove expired programmes from the DB.
        export_cache: Whether to export get_iplayer-compatible cache file.

    Returns:
        Total programmes in the database after refresh.
    """
    logger.info("=" * 60)
    logger.info("SCHEDULE-FIRST REFRESH (Approach 2)")
    logger.info("  DB: %s | Cache: %s", db_path, cache_path)
    logger.info("=" * 60)

    # Phase 1: fast sweep
    programmes = fetch_schedule_programmes(max_pages=max_pages, delay=delay)

    # Phase 2: targeted category enrichment
    programmes = enrich_categories(
        programmes, delay=delay, max_enrichments=max_enrichments,
    )

    # Store to separate DB
    with CacheDB(db_path) as db:
        upserted = db.upsert_programmes(programmes)
        logger.info("Upserted %d programmes", upserted)

        if purge_expired:
            purged = db.purge_expired()
            if purged:
                logger.info("Purged %d expired programmes", purged)

        now = datetime.now(UTC).isoformat()
        db.set_meta("last_refreshed", now)
        db.set_meta("refresh_strategy", "schedule-first")

        stats = db.stats()
        logger.info(
            "Cache stats: %d programmes, %d series, %d brands",
            stats.total_programmes, stats.total_series, stats.total_brands,
        )

        if export_cache:
            count = db.export_get_iplayer_cache(cache_path)
            logger.info(
                "Exported %d programmes to %s", count, cache_path,
            )

        return stats.total_programmes


# ── CLI ──────────────────────────────────────────────────────────────


def main() -> None:
    """CLI entry point for schedule-first refresh."""
    parser = argparse.ArgumentParser(
        description=(
            "Schedule-first BBC Radio cache refresh (Approach 2). "
            "Sweeps programmes by network, then enriches categories."
        )
    )
    parser.add_argument(
        "--db",
        default=_DEFAULT_DB_PATH,
        help=f"SQLite database path (default: {_DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--cache",
        default=_DEFAULT_CACHE_PATH,
        help=f"get_iplayer cache path (default: {_DEFAULT_CACHE_PATH})",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=_DEFAULT_MAX_PAGES,
        help="Max pages per network (default: 200)",
    )
    parser.add_argument(
        "--max-enrichments",
        type=int,
        default=0,
        help="Max PIDs to enrich for categories (0=all, default: 0)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=_REQUEST_DELAY_SECS,
        help="Seconds between requests (default: 1.0)",
    )
    parser.add_argument(
        "--no-cache-export",
        action="store_true",
        help="Skip get_iplayer cache file export",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose/debug logging",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    count = schedule_refresh(
        db_path=args.db,
        cache_path=args.cache,
        max_pages=args.max_pages,
        max_enrichments=args.max_enrichments,
        delay=args.delay,
        export_cache=not args.no_cache_export,
    )
    logger.info("Schedule-first cache contains %d programmes", count)


if __name__ == "__main__":
    main()
