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
import logging
import time
from datetime import UTC, datetime
from typing import Final

from radio_cache.bbc_feed_parser import (
    _fetch_json,
    _is_allowed_channel,
    _parse_programme_item,
    fetch_programme_detail,
)
from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme

logger = logging.getLogger(__name__)

# ── BBC RMS API constants ─────────────────────────────────────────────

_BBC_PLAYABLE_API: Final[str] = (
    "https://rms.api.bbc.co.uk/v2/programmes/playable"
)

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


def enrich_categories(
    programmes: list[Programme],
    delay: float = _REQUEST_DELAY_SECS,
    max_enrichments: int = 0,
) -> list[Programme]:
    """Phase 2: Fetch category data for programmes missing it.

    Calls ``/programmes/{pid}.json`` for each programme that has no
    categories, extracting the ``broader.category`` hierarchy.

    Args:
        programmes: List from phase 1.
        delay: Inter-request delay.
        max_enrichments: Max PIDs to enrich (0 = unlimited).

    Returns:
        Updated programme list with categories populated where possible.
    """
    needs_enrichment = [p for p in programmes if not p.categories]
    logger.info(
        "Phase 2: %d/%d programmes need category enrichment",
        len(needs_enrichment), len(programmes),
    )

    if max_enrichments > 0:
        needs_enrichment = needs_enrichment[:max_enrichments]
        logger.info("  (capped to %d enrichments)", max_enrichments)

    enriched_map: dict[str, Programme] = {}
    success_count = 0
    for i, prog in enumerate(needs_enrichment):
        detail = fetch_programme_detail(prog.pid)
        if detail and detail.categories:
            enriched_map[prog.pid] = Programme(
                pid=prog.pid,
                title=prog.title,
                synopsis=prog.synopsis or detail.synopsis,
                duration_secs=prog.duration_secs or detail.duration_secs,
                available_until=prog.available_until or detail.available_until,
                first_broadcast=prog.first_broadcast or detail.first_broadcast,
                programme_type=prog.programme_type,
                series_pid=prog.series_pid or detail.series_pid,
                series_title=prog.series_title or detail.series_title,
                brand_pid=prog.brand_pid or detail.brand_pid,
                brand_title=prog.brand_title or detail.brand_title,
                episode_number=prog.episode_number or detail.episode_number,
                channel=prog.channel or detail.channel,
                thumbnail_url=prog.thumbnail_url or detail.thumbnail_url,
                categories=detail.categories,
                url=prog.url,
            )
            success_count += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "  enriched %d/%d (%d successful)",
                i + 1, len(needs_enrichment), success_count,
            )
        time.sleep(delay)

    logger.info(
        "Phase 2 complete: enriched %d/%d programmes with categories",
        success_count, len(needs_enrichment),
    )

    # Build final list replacing enriched items
    result: list[Programme] = []
    for prog in programmes:
        if prog.pid in enriched_map:
            result.append(enriched_map[prog.pid])
        else:
            result.append(prog)
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
