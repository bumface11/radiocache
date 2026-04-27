"""Search and grouping logic for the BBC Radio Drama cache.

Provides higher-level search operations including series grouping,
brand hierarchies, and relevance ranking on top of the raw
:class:`CacheDB` queries.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC
from typing import Literal

from radio_cache.cache_db import CacheDB
from radio_cache.models import BrandGroup, Programme, SeriesGroup

SearchSortOption = Literal[
    "relevance",
    "title-asc",
    "title-desc",
    "date-desc",
    "date-asc",
    "duration-desc",
    "duration-asc",
]


def normalise_search_sort(sort: str, *, has_query: bool) -> SearchSortOption:
    """Return a valid server-side sort option for search pages."""
    valid: set[str] = {
        "relevance",
        "title-asc",
        "title-desc",
        "date-desc",
        "date-asc",
        "duration-desc",
        "duration-asc",
    }
    chosen = sort if sort in valid else ("relevance" if has_query else "date-desc")
    if chosen == "relevance" and not has_query:
        return "date-desc"
    return chosen  # type: ignore[return-value]


def search_programmes_count(
    db: CacheDB,
    query: str,
    category: str = "",
) -> int:
    """Count programmes matching a search query.

    Mirrors the logic in :func:`search_programmes` — tries FTS first,
    falls back to ``LIKE``.

    Args:
        db: Open cache database.
        query: User search string.
        category: Optional category tag filter.

    Returns:
        Total number of matching programmes.
    """
    stripped = query.strip()
    if not stripped:
        return 0

    fts_count = db.search_count(stripped, category=category)
    if fts_count:
        return fts_count

    pattern = f"%{stripped}%"
    cat_clause = ""
    params: list[object] = [pattern, pattern, pattern, pattern, pattern]
    if category:
        cat_clause = (
            " AND ',' || LOWER(categories) || ',' LIKE '%,' || LOWER(?) || ',%'"
        )
        params.append(category)
    rows = db.query(
        "SELECT COUNT(*) FROM programmes "
        "WHERE (title LIKE ? OR synopsis LIKE ? OR series_title LIKE ? "
        "OR brand_title LIKE ? OR categories LIKE ?)"
        f"{cat_clause}",
        tuple(params),
    )
    return int(rows[0][0]) if rows else 0


def search_programmes(
    db: CacheDB,
    query: str,
    limit: int = 50,
    offset: int = 0,
    category: str = "",
) -> list[Programme]:
    """Search programmes by free-text query.

    Falls back to a ``LIKE`` query if FTS returns no results (handles
    partial word matches that FTS5 would miss).

    Args:
        db: Open cache database.
        query: User search string.
        limit: Maximum results.
        offset: Pagination offset.
        category: Optional category tag to filter results at the database
            level.  The match is case-insensitive.

    Returns:
        Matching programmes.
    """
    stripped = query.strip()
    if not stripped:
        return []

    results = db.search(stripped, limit=limit, offset=offset, category=category)
    if results:
        return results

    pattern = f"%{stripped}%"
    cat_clause = ""
    params: list[object] = [pattern, pattern, pattern, pattern, pattern]
    if category:
        cat_clause = (
            " AND ',' || LOWER(categories) || ',' LIKE '%,' || LOWER(?) || ',%'"
        )
        params.append(category)
    params.extend([limit, offset])
    rows = db.query(
        "SELECT * FROM programmes "
        "WHERE (title LIKE ? OR synopsis LIKE ? OR series_title LIKE ? "
        "OR brand_title LIKE ? OR categories LIKE ?)"
        f"{cat_clause} "
        "ORDER BY first_broadcast DESC LIMIT ? OFFSET ?",
        tuple(params),
    )
    from radio_cache.cache_db import _row_to_programme

    return [_row_to_programme(r) for r in rows]


def search_groups_count(
    db: CacheDB,
    query: str,
    category: str = "",
    brand_pid: str = "",
) -> int:
    """Count distinct series groups matching a search query.

    Each series counts as one result; standalone programmes (no series)
    each count as one result.  Mirrors :func:`search_programmes_count`
    with a FTS-first, LIKE-fallback strategy.

    Args:
        db: Open cache database.
        query: User search string.
        category: Optional category tag filter.

    Returns:
        Number of distinct series groups containing a match.
    """
    stripped = query.strip()
    if not stripped:
        return 0

    fts_count = db.search_groups_count(stripped, category=category, brand_pid=brand_pid)
    if fts_count:
        return fts_count

    pattern = f"%{stripped}%"
    cat_clause = ""
    params: list[object] = [pattern, pattern, pattern, pattern, pattern]
    if category:
        cat_clause = (
            " AND ',' || LOWER(categories) || ',' LIKE '%,' || LOWER(?) || ',%'"
        )
        params.append(category)
    brand_clause = ""
    if brand_pid:
        brand_clause = " AND brand_pid = ?"
        params.append(brand_pid)
    rows = db.query(
        "SELECT COUNT(DISTINCT COALESCE(NULLIF(series_pid, ''), pid)) "
        "FROM programmes "
        "WHERE (title LIKE ? OR synopsis LIKE ? OR series_title LIKE ? "
        "OR brand_title LIKE ? OR categories LIKE ?)"
        f"{cat_clause}{brand_clause}",
        tuple(params),
    )
    return int(rows[0][0]) if rows else 0


def search_by_groups(
    db: CacheDB,
    query: str,
    limit: int = 50,
    offset: int = 0,
    category: str = "",
    sort: SearchSortOption = "relevance",
    brand_pid: str = "",
) -> list[Programme]:
    """Return matching programmes for a page of series groups.

    Uses the same FTS-first, LIKE-fallback strategy as
    :func:`search_programmes`.  The caller receives all matching episodes
    belonging to the *limit* groups at the requested *offset*.

    Args:
        db: Open cache database.
        query: User search string.
        limit: Number of series groups per page.
        offset: Group-level pagination offset.
        category: Optional category tag filter.

    Returns:
        Matching programmes for the requested page of groups.
    """
    stripped = query.strip()
    if not stripped:
        return []

    results = db.search_by_groups(
        stripped, limit=limit, offset=offset, category=category, sort=sort, brand_pid=brand_pid
    )
    if results:
        return results

    # LIKE fallback ──────────────────────────────────────────────────
    pattern = f"%{stripped}%"
    cat_clause = ""
    params: list[object] = [pattern, pattern, pattern, pattern, pattern]
    if category:
        cat_clause = (
            " AND ',' || LOWER(categories) || ',' LIKE '%,' || LOWER(?) || ',%'"
        )
        params.append(category)
    brand_clause = ""
    if brand_pid:
        brand_clause = " AND brand_pid = ?"
        params.append(brand_pid)

    group_order = {
        "relevance": "MAX(first_broadcast) DESC, LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) ASC, grp ASC",
        "title-asc": "LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) ASC, grp ASC",
        "title-desc": "LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) DESC, grp DESC",
        "date-desc": "MAX(first_broadcast) DESC, LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) ASC, grp ASC",
        "date-asc": "COALESCE(MIN(NULLIF(first_broadcast, '')), '9999-99-99T99:99:99Z') ASC, LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) ASC, grp ASC",
        "duration-desc": "MAX(duration_secs) DESC, LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) ASC, grp ASC",
        "duration-asc": "MIN(duration_secs) ASC, LOWER(MIN(COALESCE(NULLIF(series_title, ''), title))) ASC, grp ASC",
    }[sort]

    group_params = list(params) + [limit, offset]
    group_rows = db.query(
        "SELECT COALESCE(NULLIF(series_pid, ''), pid) AS grp "
        "FROM programmes "
        "WHERE (title LIKE ? OR synopsis LIKE ? OR series_title LIKE ? "
        "OR brand_title LIKE ? OR categories LIKE ?)"
        f"{cat_clause}{brand_clause} "
        "GROUP BY grp "
        f"ORDER BY {group_order} "
        "LIMIT ? OFFSET ?",
        tuple(group_params),
    )
    group_keys = [r[0] for r in group_rows]
    if not group_keys:
        return []

    placeholders = ",".join("?" * len(group_keys))
    episode_params = list(params) + group_keys
    rows = db.query(
        "SELECT * FROM programmes "
        "WHERE (title LIKE ? OR synopsis LIKE ? OR series_title LIKE ? "
        "OR brand_title LIKE ? OR categories LIKE ?)"
        f"{cat_clause}{brand_clause} "
        f"AND COALESCE(NULLIF(series_pid, ''), pid) IN ({placeholders}) "
        "ORDER BY first_broadcast DESC",
        tuple(episode_params),
    )
    from radio_cache.cache_db import _row_to_programme

    return [_row_to_programme(r) for r in rows]


def category_groups_count(db: CacheDB, category: str) -> int:
    """Count distinct series groups within a category.

    Args:
        db: Open cache database.
        category: Category tag filter.

    Returns:
        Number of distinct series groups in the category.
    """
    stripped = category.strip()
    if not stripped:
        return 0
    return db.programme_groups_by_category_count(stripped)


def category_programmes_by_groups(
    db: CacheDB,
    category: str,
    limit: int = 50,
    offset: int = 0,
    sort: SearchSortOption = "date-desc",
) -> list[Programme]:
    """Fetch category results for a page of distinct series groups.

    Args:
        db: Open cache database.
        category: Category tag filter.
        limit: Number of result groups per page.
        offset: Group-level pagination offset.

    Returns:
        Matching programmes for the requested page of groups.
    """
    stripped = category.strip()
    if not stripped:
        return []
    return db.programme_groups_by_category(
        stripped, limit=limit, offset=offset, sort=sort
    )


def sort_programmes(
    programmes: list[Programme],
    sort: SearchSortOption,
) -> list[Programme]:
    """Sort a flat programme list for server-rendered paging."""
    if sort == "title-asc":
        return sorted(programmes, key=lambda p: (p.title.casefold(), p.pid))
    if sort == "title-desc":
        return sorted(
            programmes,
            key=lambda p: (p.title.casefold(), p.pid),
            reverse=True,
        )
    if sort == "date-asc":
        return sorted(
            programmes,
            key=lambda p: (
                1 if not p.first_broadcast else 0,
                p.first_broadcast or "9999-99-99T99:99:99Z",
                p.title.casefold(),
                p.pid,
            ),
        )
    if sort == "duration-desc":
        return sorted(
            programmes,
            key=lambda p: (-p.duration_secs, p.title.casefold(), p.pid),
        )
    if sort == "duration-asc":
        return sorted(
            programmes,
            key=lambda p: (p.duration_secs, p.title.casefold(), p.pid),
        )
    return sorted(
        programmes,
        key=lambda p: (p.first_broadcast or "", p.title.casefold(), p.pid),
        reverse=True,
    )


def group_by_series(
    programmes: list[Programme],
    sort: SearchSortOption | Literal["series_order"] = "series_order",
    preserve_group_order: bool = False,
) -> list[SeriesGroup]:
    """Group a list of programmes by their parent series.

    Programmes without a ``series_pid`` are placed in a synthetic
    ``"standalone"`` group.

    Args:
        programmes: Flat list of programmes.

    Returns:
        Series groups with episodes sorted according to the requested mode.
    """
    buckets: dict[str, list[Programme]] = defaultdict(list)
    series_meta: dict[str, tuple[str, str, str]] = {}
    group_order: dict[str, int] = {}

    for prog in programmes:
        key = prog.series_pid or "standalone"
        buckets[key].append(prog)
        group_order.setdefault(key, len(group_order))
        if key != "standalone" and key not in series_meta:
            series_meta[key] = (
                prog.series_title,
                prog.brand_pid,
                prog.brand_title,
            )

    groups: list[SeriesGroup] = []
    bucket_items = list(buckets.items())
    if preserve_group_order:
        bucket_items.sort(key=lambda kv: group_order[kv[0]])
    else:
        bucket_items.sort(key=lambda kv: (kv[0] == "standalone", kv[0]))

    for spid, eps in bucket_items:
        if sort == "title-asc":
            eps_sorted = sorted(eps, key=lambda p: (p.title.casefold(), p.pid))
        elif sort == "title-desc":
            eps_sorted = sorted(
                eps,
                key=lambda p: (p.title.casefold(), p.pid),
                reverse=True,
            )
        elif sort == "date-desc":
            eps_sorted = sorted(
                eps,
                key=lambda p: (p.first_broadcast or "", p.title.casefold(), p.pid),
                reverse=True,
            )
        elif sort == "date-asc":
            eps_sorted = sorted(
                eps,
                key=lambda p: (
                    1 if not p.first_broadcast else 0,
                    p.first_broadcast or "9999-99-99T99:99:99Z",
                    p.title.casefold(),
                    p.pid,
                ),
            )
        elif sort == "duration-desc":
            eps_sorted = sorted(
                eps,
                key=lambda p: (-p.duration_secs, p.title.casefold(), p.pid),
            )
        elif sort == "duration-asc":
            eps_sorted = sorted(
                eps,
                key=lambda p: (p.duration_secs, p.title.casefold(), p.pid),
            )
        else:
            eps_sorted = sorted(
                eps,
                key=lambda p: (
                    0 if p.episode_number > 0 else 1,
                    p.episode_number if p.episode_number > 0 else 2147483647,
                    0 if p.first_broadcast else 1,
                    p.first_broadcast or "9999-99-99T99:99:99Z",
                    p.title,
                    p.pid,
                ),
            )
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
