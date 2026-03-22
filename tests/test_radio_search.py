"""Tests for the radio_cache.search module."""

from __future__ import annotations

import pytest

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme
from radio_cache.search import (
    filter_available,
    group_by_brand,
    group_by_series,
    search_programmes,
)


@pytest.fixture()
def populated_db() -> CacheDB:
    """Create a populated in-memory database."""
    db = CacheDB(":memory:")
    programmes = [
        Programme(
            pid="ep1",
            title="Mystery Hour: Part 1",
            synopsis="First episode of mystery",
            series_pid="s_mystery",
            series_title="Mystery Hour",
            brand_pid="b_drama",
            brand_title="BBC Drama",
            episode_number=1,
        ),
        Programme(
            pid="ep2",
            title="Mystery Hour: Part 2",
            synopsis="Second episode of mystery",
            series_pid="s_mystery",
            series_title="Mystery Hour",
            brand_pid="b_drama",
            brand_title="BBC Drama",
            episode_number=2,
        ),
        Programme(
            pid="ep3",
            title="Sci-Fi Anthology: Robot Dreams",
            synopsis="A robot discovers consciousness",
            series_pid="s_scifi",
            series_title="Sci-Fi Anthology",
            brand_pid="b_drama",
            brand_title="BBC Drama",
            episode_number=1,
        ),
        Programme(
            pid="ep4",
            title="Standalone Thriller",
            synopsis="One-off nail-biter",
        ),
    ]
    db.upsert_programmes(programmes)
    return db


class TestSearchProgrammes:
    """Tests for the search_programmes function."""

    def test_fts_search(self, populated_db: CacheDB) -> None:
        """FTS search finds matching programmes."""
        results = search_programmes(populated_db, "mystery")
        assert len(results) >= 1

    def test_like_fallback(self, populated_db: CacheDB) -> None:
        """LIKE fallback finds partial matches."""
        results = search_programmes(populated_db, "nail")
        assert len(results) >= 1

    def test_empty_query(self, populated_db: CacheDB) -> None:
        """Empty query returns empty list."""
        results = search_programmes(populated_db, "")
        assert results == []


class TestGroupBySeries:
    """Tests for group_by_series."""

    def test_groups_by_series_pid(self) -> None:
        """Programmes are grouped by series_pid."""
        progs = [
            Programme(pid="a", title="A", series_pid="s1", series_title="S1"),
            Programme(pid="b", title="B", series_pid="s1", series_title="S1"),
            Programme(pid="c", title="C", series_pid="s2", series_title="S2"),
        ]
        groups = group_by_series(progs)
        assert len(groups) == 2
        s1 = [g for g in groups if g.series_pid == "s1"]
        assert len(s1) == 1
        assert s1[0].episode_count == 2

    def test_standalone_grouped(self) -> None:
        """Programmes without series go to standalone group."""
        progs = [
            Programme(pid="a", title="A"),
            Programme(pid="b", title="B"),
        ]
        groups = group_by_series(progs)
        assert len(groups) == 1
        assert groups[0].series_pid == "standalone"

    def test_episodes_sorted(self) -> None:
        """Episodes within a group are sorted by episode number."""
        progs = [
            Programme(
                pid="b",
                title="Ep 2",
                series_pid="s1",
                series_title="S",
                episode_number=2,
            ),
            Programme(
                pid="a",
                title="Ep 1",
                series_pid="s1",
                series_title="S",
                episode_number=1,
            ),
        ]
        groups = group_by_series(progs)
        assert groups[0].episodes[0].episode_number == 1
        assert groups[0].episodes[1].episode_number == 2


class TestGroupByBrand:
    """Tests for group_by_brand."""

    def test_groups_by_brand(self) -> None:
        """Programmes are grouped by brand, then by series."""
        progs = [
            Programme(
                pid="a",
                title="A",
                series_pid="s1",
                series_title="S1",
                brand_pid="b1",
                brand_title="Brand 1",
            ),
            Programme(
                pid="b",
                title="B",
                series_pid="s2",
                series_title="S2",
                brand_pid="b1",
                brand_title="Brand 1",
            ),
            Programme(
                pid="c",
                title="C",
                brand_pid="b2",
                brand_title="Brand 2",
            ),
        ]
        groups = group_by_brand(progs)
        assert len(groups) == 2
        b1 = [g for g in groups if g.brand_pid == "b1"]
        assert len(b1) == 1
        assert b1[0].total_episodes == 2

    def test_unbranded_grouped(self) -> None:
        """Programmes without brand go to unbranded group."""
        progs = [Programme(pid="a", title="A")]
        groups = group_by_brand(progs)
        assert len(groups) == 1
        assert groups[0].brand_pid == "unbranded"


class TestFilterAvailable:
    """Tests for filter_available."""

    def test_removes_expired(self) -> None:
        """Expired programmes are filtered out."""
        progs = [
            Programme(
                pid="a",
                title="Expired",
                available_until="2020-01-01T00:00:00Z",
            ),
            Programme(pid="b", title="No expiry"),
            Programme(
                pid="c",
                title="Future",
                available_until="2030-01-01T00:00:00Z",
            ),
        ]
        filtered = filter_available(progs)
        pids = {p.pid for p in filtered}
        assert "a" not in pids
        assert "b" in pids
        assert "c" in pids
