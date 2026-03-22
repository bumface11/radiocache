"""Tests for the radio_cache.models module."""

from __future__ import annotations

import pytest

from radio_cache.models import (
    BrandGroup,
    CacheStats,
    Programme,
    SeriesGroup,
    format_duration,
    programme_sounds_url,
)


class TestProgramme:
    """Tests for the Programme dataclass."""

    def test_create_minimal(self) -> None:
        """Programme can be created with just a PID and title."""
        prog = Programme(pid="b09abc12", title="Test Drama")
        assert prog.pid == "b09abc12"
        assert prog.title == "Test Drama"
        assert prog.synopsis == ""
        assert prog.duration_secs == 0

    def test_create_full(self) -> None:
        """Programme can be created with all fields."""
        prog = Programme(
            pid="b09abc12",
            title="Test Drama",
            synopsis="A gripping tale",
            duration_secs=1800,
            available_until="2026-04-01T00:00:00Z",
            first_broadcast="2026-03-01T14:00:00Z",
            programme_type="episode",
            series_pid="b09ser01",
            series_title="Test Series",
            brand_pid="b09bra01",
            brand_title="Test Brand",
            episode_number=3,
            channel="Radio 4",
            thumbnail_url="https://example.com/thumb.jpg",
            categories="Drama,Thriller",
            url="https://www.bbc.co.uk/sounds/play/b09abc12",
        )
        assert prog.duration_secs == 1800
        assert prog.episode_number == 3
        assert prog.channel == "Radio 4"

    def test_frozen(self) -> None:
        """Programme is immutable."""
        prog = Programme(pid="b09abc12", title="Test")
        with pytest.raises(AttributeError):
            prog.title = "Changed"  # type: ignore[misc]


class TestSeriesGroup:
    """Tests for SeriesGroup."""

    def test_empty(self) -> None:
        """SeriesGroup can be created empty."""
        group = SeriesGroup(series_pid="s01", series_title="Test")
        assert group.episodes == []
        assert group.episode_count == 0


class TestBrandGroup:
    """Tests for BrandGroup."""

    def test_empty(self) -> None:
        """BrandGroup can be created empty."""
        group = BrandGroup(brand_pid="b01", brand_title="Brand")
        assert group.series == []
        assert group.total_episodes == 0


class TestCacheStats:
    """Tests for CacheStats."""

    def test_defaults(self) -> None:
        """CacheStats starts with zero counts."""
        stats = CacheStats()
        assert stats.total_programmes == 0
        assert stats.total_series == 0


class TestFormatDuration:
    """Tests for the format_duration helper."""

    def test_zero(self) -> None:
        assert format_duration(0) == "0:00"

    def test_seconds(self) -> None:
        assert format_duration(45) == "0:45"

    def test_minutes(self) -> None:
        assert format_duration(600) == "10:00"

    def test_hours(self) -> None:
        assert format_duration(3661) == "1:01:01"

    def test_negative(self) -> None:
        assert format_duration(-5) == "0:00"


class TestProgrammeSoundsUrl:
    """Tests for programme_sounds_url."""

    def test_url(self) -> None:
        url = programme_sounds_url("b09abc12")
        assert url == "https://www.bbc.co.uk/sounds/play/b09abc12"
