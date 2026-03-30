"""BBC programme feed parser for radio drama content.

Fetches programme metadata from BBC Sounds / iPlayer JSON feeds and
converts it into :class:`Programme` objects suitable for caching.

The BBC exposes programme data through several endpoints.  This module
targets the publicly accessible category and programme detail feeds
used by the BBC Sounds web interface.
"""

from __future__ import annotations

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

_PAGE_LIMIT: Final[int] = 30
_DEFAULT_MAX_PAGES: Final[int] = 50
_REQUEST_DELAY_SECS: Final[float] = 1.0
_REQUEST_TIMEOUT_SECS: Final[int] = 30
_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (compatible; RadioCacheBot/1.0; "
    "+https://github.com/bumface11/radiocache)"
)

_CATEGORY_SLUGS: Final[list[str]] = [
    "drama",
    "dramatisations",
    "scifi",
    "comedy",
    "thriller",
    "horror",
    "classic-drama",
    "audiobooks",
    "discussionandtalk",
    "docudramas",
    "documentaries",
    "entertainment",
    "factual",
    "gamesandquizzes",
    "learning",
    "magazinesandreviews",
    "readings"
]


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
        categories = ",".join(
            c.get("title") or c.get("id") or ""
            for c in categories_list
            if isinstance(c, dict)
        )
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


def fetch_drama_programmes(
    category_slugs: list[str] | None = None,
    max_pages: int = _DEFAULT_MAX_PAGES,
    delay: float = _REQUEST_DELAY_SECS,
) -> list[Programme]:
    """Fetch radio drama programme metadata from BBC Sounds feeds.

    Iterates through category listing pages and extracts programme
    metadata.  Respects rate limits by sleeping between requests.

    Args:
        category_slugs: Category URL slugs to scan; defaults to
            built-in drama categories.
        max_pages: Maximum pages to fetch per category.
        delay: Seconds to sleep between HTTP requests.

    Returns:
        List of :class:`Programme` objects (may contain duplicates
        across categories; the caller should de-duplicate).
    """
    slugs = category_slugs or _CATEGORY_SLUGS
    programmes: list[Programme] = []
    seen_pids: set[str] = set()

    for slug in slugs:
        logger.info("Fetching category: %s", slug)
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

            for item in items:
                prog = _parse_programme_item(item)
                if prog is not None and prog.pid not in seen_pids:
                    seen_pids.add(prog.pid)
                    programmes.append(prog)

            logger.info(
                "  page %d: %d items (total %d)",
                page + 1,
                len(items),
                len(programmes),
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
