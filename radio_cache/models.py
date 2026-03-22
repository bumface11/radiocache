"""Data models for BBC Radio Drama programme cache.

Defines the core types used throughout the radio cache system:
programmes, episodes, series, and search results.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

PROGRAMME_TYPE_EPISODE: Final[str] = "episode"
PROGRAMME_TYPE_BRAND: Final[str] = "brand"
PROGRAMME_TYPE_SERIES: Final[str] = "series"
PROGRAMME_TYPE_CLIP: Final[str] = "clip"


@dataclass(frozen=True, slots=True)
class Programme:
    """A single BBC Radio programme (episode or clip).

    Attributes:
        pid: BBC programme identifier (e.g. ``"b09xyz12"``).
        title: Full programme title.
        synopsis: Short description of the programme.
        duration_secs: Duration in seconds.
        available_until: ISO-8601 expiry timestamp, or empty string.
        first_broadcast: ISO-8601 first-broadcast timestamp.
        programme_type: One of ``episode``, ``clip``, ``brand``, ``series``.
        series_pid: Parent series PID, or empty string.
        series_title: Parent series title, or empty string.
        brand_pid: Top-level brand PID, or empty string.
        brand_title: Top-level brand title, or empty string.
        episode_number: Numeric position within a series, or ``0``.
        channel: Broadcasting channel/station name.
        thumbnail_url: URL to programme thumbnail image.
        categories: Comma-separated category tags.
        url: BBC Sounds permalink for this programme.
    """

    pid: str
    title: str
    synopsis: str = ""
    duration_secs: int = 0
    available_until: str = ""
    first_broadcast: str = ""
    programme_type: str = PROGRAMME_TYPE_EPISODE
    series_pid: str = ""
    series_title: str = ""
    brand_pid: str = ""
    brand_title: str = ""
    episode_number: int = 0
    channel: str = ""
    thumbnail_url: str = ""
    categories: str = ""
    url: str = ""


@dataclass(frozen=True, slots=True)
class SeriesGroup:
    """A group of episodes belonging to the same series.

    Attributes:
        series_pid: BBC series PID.
        series_title: Series title.
        brand_pid: Parent brand PID.
        brand_title: Parent brand title.
        episode_count: Number of episodes in this group.
        episodes: List of episodes ordered by episode number.
    """

    series_pid: str
    series_title: str
    brand_pid: str = ""
    brand_title: str = ""
    episode_count: int = 0
    episodes: list[Programme] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class BrandGroup:
    """A brand containing one or more series.

    Attributes:
        brand_pid: BBC brand PID.
        brand_title: Brand title.
        series_count: Number of distinct series.
        total_episodes: Total episode count across all series.
        series: List of series groups.
    """

    brand_pid: str
    brand_title: str
    series_count: int = 0
    total_episodes: int = 0
    series: list[SeriesGroup] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SearchResult:
    """Search result with relevance metadata.

    Attributes:
        programme: The matched programme.
        score: Relevance score (higher is better).
        match_field: Which field matched (``title``, ``synopsis``, etc.).
    """

    programme: Programme
    score: float = 0.0
    match_field: str = ""


@dataclass(frozen=True, slots=True)
class CacheStats:
    """Summary statistics for the programme cache.

    Attributes:
        total_programmes: Total programme count.
        total_series: Distinct series count.
        total_brands: Distinct brand count.
        last_refreshed: ISO-8601 timestamp of last cache refresh.
        oldest_available: Earliest ``available_until`` in the cache.
    """

    total_programmes: int = 0
    total_series: int = 0
    total_brands: int = 0
    last_refreshed: str = ""
    oldest_available: str = ""


def programme_sounds_url(pid: str) -> str:
    """Build the BBC Sounds URL for a programme PID.

    Args:
        pid: BBC programme identifier.

    Returns:
        Full BBC Sounds URL.
    """
    return f"https://www.bbc.co.uk/sounds/play/{pid}"


def format_duration(secs: int) -> str:
    """Format a duration in seconds as ``HH:MM:SS`` or ``MM:SS``.

    Args:
        secs: Duration in seconds.

    Returns:
        Human-readable duration string.
    """
    if secs <= 0:
        return "0:00"
    hours, remainder = divmod(secs, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{seconds:02d}"
    return f"{minutes}:{seconds:02d}"
