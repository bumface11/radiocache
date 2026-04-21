"""Tests for the radio_cache.search module."""

from __future__ import annotations

import pytest

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme
from radio_cache.search import (
    category_groups_count,
    category_programmes_by_groups,
    filter_available,
    group_by_brand,
    group_by_series,
    normalise_search_sort,
    search_by_groups,
    search_groups_count,
    search_programmes,
    search_programmes_count,
    sort_programmes,
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
            categories="audiobooks",
            first_broadcast="2024-01-03T00:00:00+00:00",
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
            categories="audiobooks",
            first_broadcast="2024-01-02T00:00:00+00:00",
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
            categories="audiobooks",
            first_broadcast="2024-01-01T00:00:00+00:00",
        ),
        Programme(
            pid="ep4",
            title="Standalone Thriller",
            synopsis="One-off nail-biter",
            categories="audiobooks",
            first_broadcast="2023-12-31T00:00:00+00:00",
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

    def test_count_fts(self, populated_db: CacheDB) -> None:
        """search_programmes_count returns total for FTS matches."""
        count = search_programmes_count(populated_db, "mystery")
        assert count >= 2

    def test_count_like_fallback(self, populated_db: CacheDB) -> None:
        """search_programmes_count falls back to LIKE for partial matches."""
        count = search_programmes_count(populated_db, "nail")
        assert count >= 1

    def test_count_empty(self, populated_db: CacheDB) -> None:
        """search_programmes_count returns 0 for empty query."""
        assert search_programmes_count(populated_db, "") == 0


class TestSearchByGroups:
    """Tests for the group-paginated search functions."""

    def test_groups_count_fts(self, populated_db: CacheDB) -> None:
        """search_groups_count counts series groups, not individual episodes."""
        count = search_groups_count(populated_db, "mystery")
        # ep1 and ep2 share series s_mystery → 1 group
        assert count == 1

    def test_groups_count_like_fallback(self, populated_db: CacheDB) -> None:
        """search_groups_count falls back to LIKE for partial matches."""
        count = search_groups_count(populated_db, "nail")
        assert count == 1

    def test_groups_count_empty(self, populated_db: CacheDB) -> None:
        """search_groups_count returns 0 for empty query."""
        assert search_groups_count(populated_db, "") == 0

    def test_search_by_groups_returns_all_episodes(
        self, populated_db: CacheDB
    ) -> None:
        """search_by_groups returns all matching episodes for the page."""
        results = search_by_groups(populated_db, "mystery")
        pids = {p.pid for p in results}
        assert "ep1" in pids
        assert "ep2" in pids

    def test_search_by_groups_like_fallback(self, populated_db: CacheDB) -> None:
        """search_by_groups falls back to LIKE for partial word matches."""
        results = search_by_groups(populated_db, "nail")
        assert any(p.pid == "ep4" for p in results)

    def test_search_by_groups_empty(self, populated_db: CacheDB) -> None:
        """search_by_groups returns empty list for empty query."""
        assert search_by_groups(populated_db, "") == []

    def test_groups_count_less_than_programmes_count(
        self, populated_db: CacheDB
    ) -> None:
        """Group count is less than episode count when series have multiple episodes."""
        ep_count = search_programmes_count(populated_db, "mystery")
        grp_count = search_groups_count(populated_db, "mystery")
        assert ep_count >= 2
        assert grp_count == 1


class TestCategoryProgrammesByGroups:
    """Tests for group-paginated category browsing."""

    def test_category_groups_count(self, populated_db: CacheDB) -> None:
        """Category count uses distinct groups rather than episode count."""
        assert category_groups_count(populated_db, "audiobooks") == 3

    def test_category_programmes_by_groups_first_page(
        self, populated_db: CacheDB
    ) -> None:
        """First page returns all episodes for the top group only."""
        results = category_programmes_by_groups(
            populated_db, "audiobooks", limit=1, offset=0
        )
        assert {programme.pid for programme in results} == {"ep1", "ep2"}

    def test_category_programmes_by_groups_second_page(
        self, populated_db: CacheDB
    ) -> None:
        """Later pages do not repeat the previous series group."""
        results = category_programmes_by_groups(
            populated_db, "audiobooks", limit=1, offset=1
        )
        assert {programme.pid for programme in results} == {"ep3"}

    def test_category_programmes_by_groups_title_sort(
        self, populated_db: CacheDB
    ) -> None:
        """Category group paging respects server-side title sorting."""
        results = category_programmes_by_groups(
            populated_db, "audiobooks", limit=1, offset=0, sort="title-desc"
        )
        assert {programme.pid for programme in results} == {"ep4"}


class TestServerSideSorting:
    """Tests for server-side sort helpers."""

    def test_group_by_series_preserves_input_group_order(self) -> None:
        """Search result groups can preserve database-selected ordering."""
        progs = [
            Programme(pid="b", title="B", series_pid="s2", series_title="S2"),
            Programme(pid="a", title="A", series_pid="s1", series_title="S1"),
        ]
        groups = group_by_series(progs, preserve_group_order=True)
        assert [group.series_pid for group in groups] == ["s2", "s1"]

    def test_group_by_series_date_sort_orders_episodes(self) -> None:
        """Server-side episode sorting follows the requested sort option."""
        progs = [
            Programme(
                pid="old",
                title="Old",
                series_pid="s1",
                series_title="Series",
                first_broadcast="2024-01-01T00:00:00+00:00",
            ),
            Programme(
                pid="new",
                title="New",
                series_pid="s1",
                series_title="Series",
                first_broadcast="2024-02-01T00:00:00+00:00",
            ),
        ]
        groups = group_by_series(progs, sort="date-desc", preserve_group_order=True)
        assert [episode.pid for episode in groups[0].episodes] == ["new", "old"]

    def test_sort_programmes_title_asc(self) -> None:
        """Flat programme pages can be sorted on the server."""
        progs = [
            Programme(pid="b", title="Zulu"),
            Programme(pid="a", title="Alpha"),
        ]
        sorted_programmes = sort_programmes(progs, "title-asc")
        assert [programme.pid for programme in sorted_programmes] == ["a", "b"]

    def test_normalise_search_sort_without_query(self) -> None:
        """Relevance falls back to newest-first when no text query exists."""
        assert normalise_search_sort("relevance", has_query=False) == "date-desc"


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

    def test_unnumbered_episodes_sort_after_numbered(self) -> None:
        """Episodes with episode_number=0 sort after numbered episodes."""
        progs = [
            Programme(
                pid="u1",
                title="Unnumbered",
                series_pid="s1",
                series_title="S",
                episode_number=0,
                first_broadcast="2024-01-01T00:00:00Z",
            ),
            Programme(
                pid="n2",
                title="Ep 2",
                series_pid="s1",
                series_title="S",
                episode_number=2,
            ),
            Programme(
                pid="n1",
                title="Ep 1",
                series_pid="s1",
                series_title="S",
                episode_number=1,
            ),
        ]
        groups = group_by_series(progs)
        eps = groups[0].episodes
        assert eps[0].pid == "n1"
        assert eps[1].pid == "n2"
        assert eps[2].pid == "u1"

    def test_unnumbered_no_broadcast_sorts_last(self) -> None:
        """Unnumbered episodes with empty first_broadcast sort after those with a date."""
        progs = [
            Programme(
                pid="no_date",
                title="No Date",
                series_pid="s1",
                series_title="S",
                episode_number=0,
                first_broadcast="",
            ),
            Programme(
                pid="has_date",
                title="Has Date",
                series_pid="s1",
                series_title="S",
                episode_number=0,
                first_broadcast="2024-06-01T00:00:00Z",
            ),
        ]
        groups = group_by_series(progs)
        eps = groups[0].episodes
        assert eps[0].pid == "has_date"
        assert eps[1].pid == "no_date"


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
