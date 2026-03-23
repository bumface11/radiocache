"""Tests for the radio_cache.bbc_feed_parser module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from radio_cache.bbc_feed_parser import (
    _PAGE_LIMIT,
    _parse_programme_item,
    fetch_drama_programmes,
)


class TestParseProgrammeItem:
    """Tests for _parse_programme_item."""

    def test_minimal_item(self) -> None:
        """Parses an item using the episode PID from URN."""
        item = {"urn": "urn:bbc:radio:episode:b09test", "title": "Test"}
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "b09test"

    def test_missing_urn(self) -> None:
        """Returns None when no URN is present."""
        item = {"title": "No ID"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_full_item(self) -> None:
        """Parses a fully populated BBC API item."""
        item = {
            "urn": "urn:bbc:radio:episode:b09full",
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

    def test_urn_extracts_episode_pid(self) -> None:
        """Extracts episode PID from the URN, ignoring pid and id fields."""
        item = {
            "id": "p0n6f5q8",
            "pid": "m002t10q",
            "urn": "urn:bbc:radio:episode:m002snjn",
            "title": "URN Episode",
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "m002snjn"

    def test_urn_clip(self) -> None:
        """Extracts clip PID from a clip URN."""
        item = {
            "id": "some_id",
            "urn": "urn:bbc:radio:clip:p0abc123",
            "title": "Clip",
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.pid == "p0abc123"

    def test_id_fallback_without_urn(self) -> None:
        """Rejects 'id' when 'urn' is not present."""
        item = {"id": "b09test", "title": "ID fallback"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_pid_fallback_without_urn_or_id(self) -> None:
        """Rejects version pid when no URN is present."""
        item = {"pid": "b09pid", "title": "PID only"}
        prog = _parse_programme_item(item)
        assert prog is None

    def test_synopsis_fallback(self) -> None:
        """Falls back through synopsis fields."""
        item = {
            "urn": "urn:bbc:radio:episode:b09syn",
            "synopses": {"long": "Long description only"},
        }
        prog = _parse_programme_item(item)
        assert prog is not None
        assert prog.synopsis == "Long description only"


class TestFetchDramaProgrammes:
    """Tests for fetch_drama_programmes."""

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_uses_playable_endpoint(self, mock_fetch: MagicMock) -> None:
        """Uses the /v2/programmes/playable endpoint."""
        mock_fetch.return_value = {"data": [], "total": 0}
        fetch_drama_programmes(category_slugs=["drama"], max_pages=1, delay=0)
        url = mock_fetch.call_args[0][0]
        assert "/v2/programmes/playable?" in url
        assert "category=drama" in url
        assert "inline/categories" not in url

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_pagination_uses_offset(self, mock_fetch: MagicMock) -> None:
        """Pagination uses offset and limit query parameters."""
        items = [
            {
                "urn": f"urn:bbc:radio:episode:p{i:04d}",
                "title": f"P{i}",
            }
            for i in range(_PAGE_LIMIT)
        ]
        mock_fetch.side_effect = [
            {"data": items, "total": _PAGE_LIMIT + 5},
            {
                "data": [
                    {
                        "urn": "urn:bbc:radio:episode:p9999",
                        "title": "Last",
                    }
                ],
                "total": _PAGE_LIMIT + 5,
            },
        ]
        fetch_drama_programmes(category_slugs=["drama"], max_pages=5, delay=0)
        urls = [c[0][0] for c in mock_fetch.call_args_list]
        assert "offset=0" in urls[0]
        assert f"offset={_PAGE_LIMIT}" in urls[1]

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_includes_tleo_distinct(self, mock_fetch: MagicMock) -> None:
        """URL includes tleoDistinct=true."""
        mock_fetch.return_value = {"data": [], "total": 0}
        fetch_drama_programmes(category_slugs=["drama"], max_pages=1, delay=0)
        url = mock_fetch.call_args[0][0]
        assert "tleoDistinct=true" in url

    @patch("radio_cache.bbc_feed_parser._fetch_json")
    def test_deduplicates_across_categories(self, mock_fetch: MagicMock) -> None:
        """Programmes seen in multiple categories are returned once."""
        item = {"urn": "urn:bbc:radio:episode:b09dup", "title": "Shared Drama"}
        mock_fetch.return_value = {"data": [item], "total": 1}
        result = fetch_drama_programmes(
            category_slugs=["drama", "thriller"], max_pages=1, delay=0
        )
        assert len(result) == 1
        assert result[0].pid == "b09dup"
