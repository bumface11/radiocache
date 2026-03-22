"""Tests for the radio_cache.cache_db module."""

from __future__ import annotations

import pytest

from radio_cache.cache_db import CacheDB, _sanitise_fts_query
from radio_cache.models import Programme


@pytest.fixture()
def db() -> CacheDB:
    """Create an in-memory cache database for testing."""
    return CacheDB(":memory:")


@pytest.fixture()
def populated_db(db: CacheDB) -> CacheDB:
    """Seed the in-memory database with sample programmes."""
    programmes = [
        Programme(
            pid="p001",
            title="The Archers",
            synopsis="Long-running rural drama",
            duration_secs=900,
            series_pid="s_archers",
            series_title="The Archers",
            brand_pid="b_archers",
            brand_title="The Archers Brand",
            episode_number=1,
            channel="Radio 4",
            categories="Drama",
        ),
        Programme(
            pid="p002",
            title="The Archers: Episode 2",
            synopsis="Continuing rural drama",
            duration_secs=900,
            series_pid="s_archers",
            series_title="The Archers",
            brand_pid="b_archers",
            brand_title="The Archers Brand",
            episode_number=2,
            channel="Radio 4",
            categories="Drama",
        ),
        Programme(
            pid="p003",
            title="Dracula",
            synopsis="Gothic horror classic",
            duration_secs=3600,
            series_pid="s_dracula",
            series_title="Dracula Series",
            brand_pid="b_dracula",
            brand_title="Dracula Brand",
            episode_number=1,
            channel="Radio 4",
            categories="Drama,Horror",
        ),
        Programme(
            pid="p004",
            title="Standalone Play",
            synopsis="A one-off thriller",
            duration_secs=2700,
            channel="Radio 3",
            categories="Drama,Thriller",
        ),
    ]
    db.upsert_programmes(programmes)
    return db


class TestCacheDB:
    """Tests for CacheDB operations."""

    def test_upsert_and_get(self, db: CacheDB) -> None:
        """Can insert and retrieve a programme."""
        prog = Programme(pid="test1", title="Test Programme")
        db.upsert_programme(prog)
        result = db.get_programme("test1")
        assert result is not None
        assert result.pid == "test1"
        assert result.title == "Test Programme"

    def test_upsert_update(self, db: CacheDB) -> None:
        """Upserting with same PID updates the record."""
        prog1 = Programme(pid="test1", title="Original")
        prog2 = Programme(pid="test1", title="Updated")
        db.upsert_programme(prog1)
        db.upsert_programme(prog2)
        result = db.get_programme("test1")
        assert result is not None
        assert result.title == "Updated"

    def test_get_missing(self, db: CacheDB) -> None:
        """Getting a non-existent PID returns None."""
        assert db.get_programme("nonexistent") is None

    def test_bulk_upsert(self, db: CacheDB) -> None:
        """Bulk upsert inserts multiple programmes."""
        progs = [
            Programme(pid=f"bulk{i}", title=f"Bulk {i}")
            for i in range(10)
        ]
        count = db.upsert_programmes(progs)
        assert count == 10
        assert db.programme_count() == 10

    def test_search_fts(self, populated_db: CacheDB) -> None:
        """Full-text search finds matching programmes."""
        results = populated_db.search("Archers")
        assert len(results) >= 1
        assert any("Archers" in r.title for r in results)

    def test_search_horror(self, populated_db: CacheDB) -> None:
        """FTS matches category text."""
        results = populated_db.search("horror")
        assert len(results) >= 1

    def test_search_empty(self, populated_db: CacheDB) -> None:
        """Empty search returns no results."""
        results = populated_db.search("")
        assert results == []

    def test_list_series(self, populated_db: CacheDB) -> None:
        """List series returns distinct series with counts."""
        series = populated_db.list_series()
        assert len(series) >= 2
        archers = [s for s in series if s["series_pid"] == "s_archers"]
        assert len(archers) == 1
        assert archers[0]["episode_count"] == 2

    def test_get_series_episodes(self, populated_db: CacheDB) -> None:
        """Get series episodes returns sorted episodes."""
        episodes = populated_db.get_series_episodes("s_archers")
        assert len(episodes) == 2
        assert episodes[0].episode_number <= episodes[1].episode_number

    def test_list_brands(self, populated_db: CacheDB) -> None:
        """List brands returns distinct brands."""
        brands = populated_db.list_brands()
        assert len(brands) >= 2

    def test_get_brand_series(self, populated_db: CacheDB) -> None:
        """Get brand series returns series within a brand."""
        series = populated_db.get_brand_series("b_archers")
        assert len(series) == 1

    def test_recent_programmes(self, populated_db: CacheDB) -> None:
        """Recent programmes returns results."""
        recent = populated_db.recent_programmes(limit=2)
        assert len(recent) <= 2

    def test_programme_count(self, populated_db: CacheDB) -> None:
        """Programme count matches inserted data."""
        assert populated_db.programme_count() == 4

    def test_stats(self, populated_db: CacheDB) -> None:
        """Stats returns correct counts."""
        stats = populated_db.stats()
        assert stats.total_programmes == 4
        assert stats.total_series == 2
        assert stats.total_brands == 2

    def test_meta(self, db: CacheDB) -> None:
        """Metadata can be set and retrieved."""
        db.set_meta("test_key", "test_value")
        assert db.get_meta("test_key") == "test_value"

    def test_meta_missing(self, db: CacheDB) -> None:
        """Missing metadata returns empty string."""
        assert db.get_meta("nonexistent") == ""

    def test_purge_expired(self, db: CacheDB) -> None:
        """Purge removes expired programmes."""
        db.upsert_programme(
            Programme(
                pid="expired1",
                title="Expired",
                available_until="2020-01-01T00:00:00Z",
            )
        )
        db.upsert_programme(
            Programme(pid="current1", title="Current", available_until="")
        )
        purged = db.purge_expired()
        assert purged == 1
        assert db.programme_count() == 1

    def test_context_manager(self) -> None:
        """CacheDB works as a context manager."""
        with CacheDB(":memory:") as db:
            db.upsert_programme(Programme(pid="cm1", title="Context"))
            assert db.programme_count() == 1

    def test_all_programmes(self, populated_db: CacheDB) -> None:
        """All programmes returns everything ordered by title."""
        progs = populated_db.all_programmes()
        assert len(progs) == 4
        titles = [p.title for p in progs]
        assert titles == sorted(titles)


class TestSanitiseFtsQuery:
    """Tests for the FTS query sanitiser."""

    def test_simple(self) -> None:
        assert _sanitise_fts_query("hello") == '"hello"'

    def test_multiple_words(self) -> None:
        result = _sanitise_fts_query("hello world")
        assert result == '"hello" "world"'

    def test_empty(self) -> None:
        assert _sanitise_fts_query("") == ""

    def test_special_chars(self) -> None:
        result = _sanitise_fts_query('test "quoted"')
        assert '""' in result

    def test_whitespace_only(self) -> None:
        assert _sanitise_fts_query("   ") == ""
