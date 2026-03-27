"""Tests for helper utilities in radio_cache_api."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from radio_cache.models import Programme
from radio_cache_api import (
    _sort_episodes,
    format_short_date,
    is_expiring_soon,
    is_recent_broadcast,
)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def test_format_short_date() -> None:
    assert format_short_date("2026-03-27T12:34:56Z") == "27/3/26"


def test_recent_broadcast_window() -> None:
    now = datetime.now(timezone.utc)
    assert is_recent_broadcast(_iso(now - timedelta(days=1))) is True
    assert is_recent_broadcast(_iso(now - timedelta(days=5))) is False


def test_expiring_soon_window() -> None:
    now = datetime.now(timezone.utc)
    assert is_expiring_soon(_iso(now + timedelta(days=3))) is True
    assert is_expiring_soon(_iso(now + timedelta(days=9))) is False
    assert is_expiring_soon(_iso(now - timedelta(hours=1))) is False


def test_sort_episodes_broadcast_newest() -> None:
    episodes = [
        Programme(pid="a", title="Alpha", first_broadcast="2026-03-01T00:00:00Z"),
        Programme(pid="b", title="Bravo", first_broadcast="2026-03-10T00:00:00Z"),
        Programme(pid="c", title="Charlie", first_broadcast=""),
    ]
    sorted_eps = _sort_episodes(episodes, "broadcast_newest")
    assert [ep.pid for ep in sorted_eps] == ["b", "a", "c"]


def test_sort_episodes_expiry_soonest() -> None:
    episodes = [
        Programme(pid="a", title="Alpha", available_until="2026-03-30T00:00:00Z"),
        Programme(pid="b", title="Bravo", available_until="2026-03-28T00:00:00Z"),
        Programme(pid="c", title="Charlie", available_until=""),
    ]
    sorted_eps = _sort_episodes(episodes, "expiry_soonest")
    assert [ep.pid for ep in sorted_eps] == ["b", "a", "c"]
