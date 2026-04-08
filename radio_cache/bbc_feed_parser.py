"""BBC programme feed parser for radio drama content.

Fetches programme metadata from BBC Sounds / iPlayer JSON feeds and
converts it into :class:`Programme` objects suitable for caching.

The BBC exposes programme data through several endpoints.  This module
targets the publicly accessible category and programme detail feeds
used by the BBC Sounds web interface.

Category ingestion follows the same strategy as get_iplayer:

* The BBC RMS playable API (`/v2/programmes/playable?category=<slug>`)
  is used to discover all programmes in a genre.  Its per-item response
  typically does **not** include a populated ``categories`` array, so the
  slug name is recorded as the authoritative category for every programme
  found in that listing.
* When a programme appears in multiple slug searches its category set is
  **merged** rather than discarded.
* The ``/programmes/{pid}.json`` endpoint (also used by get_iplayer)
  returns a richer category structure with a three-level
  ``broader.category`` hierarchy; this module handles that format so
  :func:`fetch_programme_detail` returns accurate category data.
* :func:`fetch_all_category_slugs` queries the BBC categories API to
  discover the full list of audio genre slugs at runtime, falling back to
  the built-in :data:`_CATEGORY_SLUGS` list when the API is unavailable.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import time
import urllib.error
import urllib.request
from typing import Final

from radio_cache.models import Programme, programme_sounds_url

logger = logging.getLogger(__name__)

_BBC_PLAYABLE_API: Final[str] = (
    "https://rms.api.bbc.co.uk/v2/programmes/playable"
)
_BBC_PROGRAMMES_API: Final[str] = "https://www.bbc.co.uk/programmes"
_BBC_CATEGORIES_API: Final[str] = (
    "https://rms.api.bbc.co.uk/v2/categories"
)

_PAGE_LIMIT: Final[int] = 30
_DEFAULT_MAX_PAGES: Final[int] = 50
_REQUEST_DELAY_SECS: Final[float] = 1.0
_REQUEST_TIMEOUT_SECS: Final[int] = 30
_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (compatible; RadioCacheBot/1.0; "
    "+https://github.com/bumface11/radiocache)"
)

# Known BBC Sounds radio drama / comedy genre slugs used by the RMS playable
# API.  These serve as the fallback when :func:`fetch_all_category_slugs`
# cannot reach the BBC categories endpoint.
_CATEGORY_SLUGS: Final[list[str]] = [
    "drama",
    "dramatisations",
    "scifi",
    "comedy",
    "thriller",
    "horror",
    "classic-drama",
    "crime",
    "comedy-drama",
    "period-drama",
]

# Human-readable display names for category slugs.  Used when the BBC API
# does not return a ``categories`` array in the programme item (which is the
# common case for the RMS playable endpoint).
_SLUG_DISPLAY_NAMES: Final[dict[str, str]] = {
    "drama": "Drama",
    "dramatisations": "Dramatisations",
    "scifi": "Sci-Fi",
    "comedy": "Comedy",
    "thriller": "Thriller",
    "horror": "Horror",
    "classic-drama": "Classic Drama",
    "crime": "Crime",
    "comedy-drama": "Comedy Drama",
    "period-drama": "Period Drama",
}


def _fetch_json(url: str) -> dict | list | None:
    """Fetch JSON from *url* with polite request headers.

    Args:
        url: The URL to fetch.

    Returns:
        Parsed JSON, or ``None`` on failure.
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": _USER_AGENT,
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_SECS) as resp:
            return json.loads(resp.read())
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
        logger.error("Failed to fetch %s: %s", url, exc)
        return None


def _parse_programme_item(item: dict) -> Programme | None:
    """Convert a BBC API programme item dict into a :class:`Programme`.

    Args:
        item: Raw dict from the BBC API response.

    Returns:
        A ``Programme`` instance, or ``None`` if essential fields are missing.
    """
    # The URN (e.g. "urn:bbc:radio:episode:m002snjn") contains the
    # correct episode PID as its last colon-separated segment.  The
    # top-level ``pid`` field is typically a *version* PID and the
    # ``id`` field may be an opaque API identifier – neither works
    # reliably in BBC Sounds ``/sounds/play/…`` URLs.
    urn = item.get("urn") or ""
    if not urn or ":" not in urn:
        return None

    pid = urn.rsplit(":", 1)[-1].strip()
    if not pid:
        return None

    titles = item.get("titles") or {}
    primary_title = titles.get("primary") or item.get("title") or ""
    secondary_title = titles.get("secondary") or ""
    full_title = (
        f"{primary_title}: {secondary_title}"
        if secondary_title
        else primary_title
    )

    synopses = item.get("synopses") or {}
    synopsis = (
        synopses.get("short")
        or synopses.get("medium")
        or synopses.get("long")
        or item.get("synopsis")
        or ""
    )

    duration = item.get("duration") or {}
    duration_secs = duration.get("value") or item.get("duration_secs") or 0

    availability = item.get("availability") or {}
    available_until = availability.get("to") or ""

    release = item.get("release") or {}
    first_broadcast = release.get("date") or item.get("first_broadcast") or ""

    container = item.get("container") or {}
    series_pid = container.get("id") or ""
    series_title = (container.get("title") or "")

    brand = item.get("brand") or item.get("master_brand") or {}
    brand_pid = brand.get("id") or ""
    brand_title = brand.get("title") or ""

    network = item.get("network") or {}
    channel = network.get("short_title") or network.get("id") or ""

    image = item.get("image_url") or ""
    if image and "{recipe}" in image:
        image = image.replace("{recipe}", "624x624")

    episode_number = item.get("episode_number") or 0

    categories_list = item.get("categories") or []
    if isinstance(categories_list, list):
        # get_iplayer traverses a three-level ``broader.category`` hierarchy
        # present in the ``/programmes/{pid}.json`` response format.  The RMS
        # playable endpoint usually returns a flat list (or none at all); this
        # code handles both forms gracefully.
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
                grandparent = (broader.get("broader") or {}).get("category") or {}
                if grandparent:
                    gt = grandparent.get("title") or grandparent.get("id") or ""
                    if gt:
                        cats3.append(gt)
        # Assemble deduplicated list broadest-first (mirrors get_iplayer ordering)
        seen: set[str] = set()
        all_cats: list[str] = []
        for cat_title in cats3 + cats2 + cats1:
            if cat_title and cat_title not in seen:
                seen.add(cat_title)
                all_cats.append(cat_title)
        categories = ",".join(all_cats)
    else:
        categories = ""

    return Programme(
        pid=pid,
        title=full_title,
        synopsis=synopsis,
        duration_secs=int(duration_secs) if duration_secs else 0,
        available_until=available_until,
        first_broadcast=first_broadcast,
        programme_type=item.get("type") or "episode",
        series_pid=series_pid,
        series_title=series_title,
        brand_pid=brand_pid,
        brand_title=brand_title,
        episode_number=int(episode_number) if episode_number else 0,
        channel=channel,
        thumbnail_url=image,
        categories=categories,
        url=programme_sounds_url(pid),
    )


def fetch_all_category_slugs() -> list[str]:
    """Fetch radio genre category slugs from the BBC categories API.

    Queries ``/v2/categories?medium=audio`` and extracts all slug / id
    values.  Falls back to the built-in :data:`_CATEGORY_SLUGS` list if
    the API is unreachable or returns an unexpected payload.

    Returns:
        List of category slug strings suitable for use as the ``category``
        parameter of the RMS playable API.
    """
    url = f"{_BBC_CATEGORIES_API}?medium=audio&kind=genre"
    data = _fetch_json(url)
    if not isinstance(data, dict):
        logger.warning(
            "BBC categories API returned unexpected type; using built-in slugs"
        )
        return list(_CATEGORY_SLUGS)

    items = data.get("data") or []
    if not isinstance(items, list) or not items:
        logger.warning(
            "BBC categories API returned no items; using built-in slugs"
        )
        return list(_CATEGORY_SLUGS)

    slugs: list[str] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        slug = item.get("id") or item.get("key") or item.get("slug") or ""
        if slug:
            slugs.append(str(slug))

    if not slugs:
        logger.warning(
            "Could not extract slugs from BBC categories API; using built-in slugs"
        )
        return list(_CATEGORY_SLUGS)

    logger.info("Discovered %d category slugs from BBC API", len(slugs))
    return slugs


def fetch_drama_programmes(
    category_slugs: list[str] | None = None,
    max_pages: int = _DEFAULT_MAX_PAGES,
    delay: float = _REQUEST_DELAY_SECS,
) -> list[Programme]:
    """Fetch radio drama programme metadata from BBC Sounds feeds.

    Iterates through category listing pages and extracts programme
    metadata.  Respects rate limits by sleeping between requests.

    Category data is built from two complementary sources:

    1. **API response ``categories`` field** – when the BBC RMS API
       includes a populated ``categories`` array in a programme item,
       those titles are used directly (with ``broader.category`` hierarchy
       traversal mirroring get_iplayer).
    2. **Slug fallback** – the RMS playable endpoint frequently omits the
       ``categories`` field.  The human-readable display name of the slug
       used to discover the programme (e.g. ``"thriller"`` → ``"Thriller"``)
       is always added to its category set so that at least one category is
       recorded.

    When a programme appears in **multiple** slug searches its category
    sets are **merged**, so a programme found in both ``"drama"`` and
    ``"thriller"`` will end up with ``"Drama,Thriller"`` rather than just
    the first slug encountered.

    Args:
        category_slugs: Category URL slugs to scan; defaults to
            built-in drama categories.
        max_pages: Maximum pages to fetch per category.
        delay: Seconds to sleep between HTTP requests.

    Returns:
        De-duplicated list of :class:`Programme` objects with merged
        category data.
    """
    slugs = category_slugs or _CATEGORY_SLUGS
    # pid → first Programme parsed (base metadata)
    pid_to_prog: dict[str, Programme] = {}
    # pid → merged set of category strings
    pid_to_cats: dict[str, set[str]] = {}

    for slug in slugs:
        slug_display = _SLUG_DISPLAY_NAMES.get(slug) or slug.replace("-", " ").title()
        logger.info("Fetching category: %s (%s)", slug, slug_display)
        for page in range(max_pages):
            offset = page * _PAGE_LIMIT
            url = (
                f"{_BBC_PLAYABLE_API}?category={slug}"
                f"&sort=date&tleoDistinct=true"
                f"&offset={offset}&limit={_PAGE_LIMIT}"
            )
            data = _fetch_json(url)
            if data is None:
                break

            items = data.get("data") or []
            if not items:
                break

            new_this_page = 0
            for item in items:
                prog = _parse_programme_item(item)
                if prog is None:
                    continue

                # Gather categories from the parsed item
                item_cats: set[str] = set(
                    c.strip() for c in prog.categories.split(",") if c.strip()
                )
                # Always include the slug display name as a category so
                # programmes discovered via this slug are always tagged, even
                # when the BBC API returns an empty categories array.
                item_cats.add(slug_display)

                if prog.pid not in pid_to_prog:
                    pid_to_prog[prog.pid] = prog
                    pid_to_cats[prog.pid] = item_cats
                    new_this_page += 1
                else:
                    # Programme already seen – merge new categories in
                    pid_to_cats[prog.pid].update(item_cats)

            logger.info(
                "  page %d: %d items (%d new, %d total unique)",
                page + 1,
                len(items),
                new_this_page,
                len(pid_to_prog),
            )

            total = data.get("total", 0)
            if offset + len(items) >= total or len(items) < _PAGE_LIMIT:
                break

            if page + 1 >= max_pages:
                logger.warning(
                    "Reached max_pages=%d for category '%s' before exhausting "
                    "results (offset=%d, total=%d). Some episodes may be missed.",
                    max_pages,
                    slug,
                    offset + len(items),
                    total,
                )
                break

            time.sleep(delay)

        time.sleep(delay)

    # Build the final programme list, replacing each programme's categories
    # with the fully merged set gathered across all slug searches.
    # Categories from multiple slugs are peer-level tags (e.g. "Drama",
    # "Thriller") with no parent-child hierarchy, so alphabetical order
    # gives a consistent, reproducible output across runs.
    programmes: list[Programme] = []
    for pid, prog in pid_to_prog.items():
        merged = ",".join(sorted(pid_to_cats[pid]))
        if merged != prog.categories:
            prog = dataclasses.replace(prog, categories=merged)
        programmes.append(prog)

    logger.info("Fetched %d unique programmes", len(programmes))
    return programmes


def fetch_programme_detail(pid: str) -> Programme | None:
    """Fetch detailed metadata for a single programme by PID.

    Args:
        pid: BBC programme identifier.

    Returns:
        A :class:`Programme`, or ``None`` on failure.
    """
    url = f"{_BBC_PROGRAMMES_API}/{pid}.json"
    data = _fetch_json(url)
    if data is None:
        return None

    prog_data = data.get("programme") or data
    return _parse_programme_item(prog_data)
