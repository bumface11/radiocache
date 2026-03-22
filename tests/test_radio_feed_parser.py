"""Tests for the radio_cache.bbc_feed_parser module."""

from __future__ import annotations

from radio_cache.bbc_feed_parser import _parse_programme_item


class TestParseProgrammeItem:
    """Tests for _parse_programme_item."""

    def test_minimal_item(self) -> None:
        """Parses an item with only an id."""
        item = {"id": "b09test", "title": "Test"}
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "b09test"

    def test_missing_id(self) -> None:
        """Returns None when no id or pid is present."""
        item = {"title": "No ID"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_full_item(self) -> None:
        """Parses a fully populated BBC API item."""
        item = {
            "id": "b09full",
            "titles": {
                "primary": "Drama Hour",
                "secondary": "Episode 1",
            },
            "synopses": {
                "short": "Short desc",
                "medium": "Medium desc",
            },
            "duration": {"value": 1800},
            "availability": {"to": "2026-06-01T00:00:00Z"},
            "release": {"date": "2026-03-01T14:00:00Z"},
            "container": {"id": "s_drama", "title": "Drama Hour Series"},
            "network": {"short_title": "Radio 4"},
            "image_url": "https://example.com/{recipe}/thumb.jpg",
            "episode_number": 1,
            "type": "episode",
            "categories": [
                {"id": "drama", "title": "Drama"},
                {"id": "thriller", "title": "Thriller"},
            ],
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "b09full"
        assert prog.title == "Drama Hour: Episode 1"
        assert prog.synopsis == "Short desc"
        assert prog.duration_secs == 1800
        assert prog.series_pid == "s_drama"
        assert prog.channel == "Radio 4"
        assert "624x624" in prog.thumbnail_url
        assert "Drama" in prog.categories

    def test_pid_fallback(self) -> None:
        """Uses 'pid' field when 'id' is not present."""
        item = {"pid": "b09pid", "title": "PID fallback"}
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "b09pid"

    def test_synopsis_fallback(self) -> None:
        """Falls back through synopsis fields."""
        item = {
            "id": "b09syn",
            "synopses": {"long": "Long description only"},
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.synopsis == "Long description only"
