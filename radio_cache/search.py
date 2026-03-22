"""Search and grouping logic for the BBC Radio Drama cache.

Provides higher-level search operations including series grouping,
brand hierarchies, and relevance ranking on top of the raw
:class:`CacheDB` queries.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC

from radio_cache.cache_db import CacheDB
from radio_cache.models import BrandGroup, Programme, SeriesGroup


def search_programmes(
    db: CacheDB,
    query: str,
    limit: int = 50,
    offset: int = 0,
) -> list[Programme]:
    """Search programmes by free-text query.

    Falls back to a ``LIKE`` query if FTS returns no results (handles
    partial word matches that FTS5 would miss).

    Args:
        db: Open cache database.
        query: User search string.
        limit: Maximum results.
        offset: Pagination offset.

    Returns:
        Matching programmes.
    """
    stripped = query.strip()
    if not stripped:
        return []

    results = db.search(stripped, limit=limit, offset=offset)
    if results:
        return results

    pattern = f"%{stripped}%"
    rows = db.query(
        "SELECT * FROM programmes "
        "WHERE title LIKE ? OR synopsis LIKE ? OR series_title LIKE ? "
        "OR brand_title LIKE ? OR categories LIKE ? "
        "ORDER BY first_broadcast DESC LIMIT ? OFFSET ?",
        (pattern, pattern, pattern, pattern, pattern, limit, offset),
    )
    from radio_cache.cache_db import _row_to_programme

    return [_row_to_programme(r) for r in rows]


def group_by_series(programmes: list[Programme]) -> list[SeriesGroup]:
    """Group a list of programmes by their parent series.

    Programmes without a ``series_pid`` are placed in a synthetic
    ``"standalone"`` group.

    Args:
        programmes: Flat list of programmes.

    Returns:
        Series groups with episodes sorted by episode number.
    """
    buckets: dict[str, list[Programme]] = defaultdict(list)
    series_meta: dict[str, tuple[str, str, str]] = {}

    for prog in programmes:
        key = prog.series_pid or "standalone"
        buckets[key].append(prog)
        if key != "standalone" and key not in series_meta:
            series_meta[key] = (
                prog.series_title,
                prog.brand_pid,
                prog.brand_title,
            )

    groups: list[SeriesGroup] = []
    for spid, eps in sorted(
        buckets.items(), key=lambda kv: (kv[0] == "standalone", kv[0])
    ):
        eps_sorted = sorted(eps, key=lambda p: (p.episode_number, p.first_broadcast))
        meta = series_meta.get(spid, ("Standalone", "", ""))
        groups.append(
            SeriesGroup(
                series_pid=spid,
                series_title=meta[0],
                brand_pid=meta[1],
                brand_title=meta[2],
                episode_count=len(eps_sorted),
                episodes=eps_sorted,
            )
        )
    return groups


def group_by_brand(programmes: list[Programme]) -> list[BrandGroup]:
    """Group programmes into brand > series > episode hierarchy.

    Programmes without a ``brand_pid`` are placed under a synthetic
    ``"unbranded"`` brand.

    Args:
        programmes: Flat list of programmes.

    Returns:
        Brand groups, each containing series groups.
    """
    brand_buckets: dict[str, list[Programme]] = defaultdict(list)
    brand_titles: dict[str, str] = {}

    for prog in programmes:
        key = prog.brand_pid or "unbranded"
        brand_buckets[key].append(prog)
        if key != "unbranded" and key not in brand_titles:
            brand_titles[key] = prog.brand_title

    groups: list[BrandGroup] = []
    for bpid, progs in sorted(
        brand_buckets.items(), key=lambda kv: (kv[0] == "unbranded", kv[0])
    ):
        series = group_by_series(progs)
        groups.append(
            BrandGroup(
                brand_pid=bpid,
                brand_title=brand_titles.get(bpid, "Unbranded"),
                series_count=len(series),
                total_episodes=len(progs),
                series=series,
            )
        )
    return groups


def filter_available(programmes: list[Programme]) -> list[Programme]:
    """Return only currently available programmes.

    Removes programmes whose ``available_until`` has passed.

    Args:
        programmes: Input programme list.

    Returns:
        Filtered list containing only available programmes.
    """
    from datetime import datetime

    now = datetime.now(UTC).isoformat()
    return [
        p
        for p in programmes
        if not p.available_until or p.available_until >= now
    ]
