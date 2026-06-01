"""Tests for radio_cache.schedule_refresh (Approach 2)."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import patch

from radio_cache.models import Programme
from radio_cache.schedule_refresh import (
    _DEFAULT_CACHE_PATH,
    _DEFAULT_DB_PATH,
    _NETWORK_IDS,
    _parse_categories_from_programmes_json,
    enrich_categories,
    fetch_schedule_programmes,
    schedule_refresh,
)


def _make_programme(
    pid: str,
    categories: str = "",
    channel: str = "BBC Radio 4",
    brand_pid: str = "",
    series_pid: str = "",
) -> Programme:
    return Programme(
        pid=pid,
        title=f"Test Programme {pid}",
        synopsis="A test synopsis.",
        duration_secs=1800,
        available_until="2026-12-01T00:00:00Z",
        first_broadcast="2026-01-01T00:00:00Z",
        programme_type="episode",
        channel=channel,
        categories=categories,
        brand_pid=brand_pid,
        series_pid=series_pid,
        url=f"https://www.bbc.co.uk/sounds/play/{pid}",
    )


def _make_api_response(
    pids: list[str], total: int | None = None,
) -> dict[str, Any]:
    """Build a fake BBC RMS API response."""
    items = []
    for pid in pids:
        items.append({
            "urn": f"urn:bbc:radio:episode:{pid}",
            "titles": {"primary": f"Show {pid}", "secondary": ""},
            "synopses": {"short": "A test episode."},
            "duration": {"value": 1800},
            "availability": {"to": "2026-12-01T00:00:00Z"},
            "release": {"date": "2026-01-01T00:00:00Z"},
            "network": {"short_title": "Radio 4", "id": "bbc_radio_fourfm"},
            "container": {
                "type": "series", "id": "s001", "title": "Test Series",
            },
            "image_url": "",
            "categories": [],
            "type": "episode",
        })
    return {
        "data": items,
        "total": total if total is not None else len(items),
    }


class TestFetchScheduleProgrammes:
    """Tests for Phase 1: network sweep."""

    def test_deduplicates_across_networks(self) -> None:
        """Same PID on multiple networks is only stored once."""
        response = _make_api_response(["m001abc", "m001def"])

        with patch(
            "radio_cache.schedule_refresh._fetch_json",
            side_effect=[response, {"data": [], "total": 0}] * 3,
        ):
            progs = fetch_schedule_programmes(delay=0)

        pids = [p.pid for p in progs]
        assert len(pids) == len(set(pids))

    def test_returns_programmes_from_single_network(self) -> None:
        """Can fetch from a single network."""
        response = _make_api_response(["m001abc", "m001def"])
        empty = {"data": [], "total": 0}

        with patch(
            "radio_cache.schedule_refresh._fetch_json",
            side_effect=[response, empty],
        ):
            progs = fetch_schedule_programmes(
                network_ids=["bbc_radio_fourfm"], delay=0,
            )

        assert len(progs) == 2

    def test_handles_api_failure_gracefully(self) -> None:
        """Returns empty list when API returns None."""
        with patch(
            "radio_cache.schedule_refresh._fetch_json",
            return_value=None,
        ):
            progs = fetch_schedule_programmes(
                network_ids=["bbc_radio_fourfm"], delay=0,
            )
        assert progs == []


class TestParseCategoriesFromProgrammesJson:
    """Tests for the /programmes/{pid}.json category parser."""

    def test_parses_three_level_hierarchy(self) -> None:
        """Extracts broadest-first from broader.category nesting."""
        data = {
            "programme": {
                "categories": [
                    {
                        "title": "Thriller",
                        "broader": {
                            "category": {
                                "title": "Drama",
                                "broader": {
                                    "category": {"title": "Audio"}
                                },
                            }
                        },
                    }
                ]
            }
        }
        result = _parse_categories_from_programmes_json(data)
        assert result == "Audio,Drama,Thriller"

    def test_returns_empty_for_no_categories(self) -> None:
        """Returns empty string when categories list is empty."""
        data: dict[str, Any] = {"programme": {"categories": []}}
        assert _parse_categories_from_programmes_json(data) == ""

    def test_deduplicates_categories(self) -> None:
        """Same category appearing multiple times is deduplicated."""
        data = {
            "programme": {
                "categories": [
                    {"title": "Drama"},
                    {"title": "Drama"},
                ]
            }
        }
        assert _parse_categories_from_programmes_json(data) == "Drama"


class TestEnrichCategories:
    """Tests for Phase 2: category enrichment."""

    def test_enriches_programmes_via_brand_pid(self) -> None:
        """Programmes sharing a brand_pid are enriched with one lookup."""
        prog1 = _make_programme(
            "m001abc", categories="", brand_pid="b001",
        )
        prog2 = _make_programme(
            "m001def", categories="", brand_pid="b001",
        )
        prog_with_cat = _make_programme("m001ghi", categories="Drama")

        with patch(
            "radio_cache.schedule_refresh._fetch_categories_for_pid",
            return_value="Thriller,Drama",
        ) as mock_fetch:
            result = enrich_categories(
                [prog1, prog2, prog_with_cat], delay=0,
            )

        # Only one lookup for the shared brand PID
        assert mock_fetch.call_count == 1
        assert mock_fetch.call_args[0][0] == "b001"
        # Both episodes get the categories
        assert result[0].categories == "Thriller,Drama"
        assert result[1].categories == "Thriller,Drama"
        # Already-categorised programme untouched
        assert result[2].categories == "Drama"

    def test_respects_max_enrichments_cap(self) -> None:
        """Only looks up max_enrichments PIDs."""
        progs = [
            _make_programme(f"m00{i}", categories="", brand_pid=f"b00{i}")
            for i in range(5)
        ]

        call_count = 0

        def mock_fetch(pid: str, delay: float = 1.0) -> str:
            nonlocal call_count
            call_count += 1
            return "Comedy"

        with patch(
            "radio_cache.schedule_refresh._fetch_categories_for_pid",
            side_effect=mock_fetch,
        ):
            enrich_categories(progs, delay=0, max_enrichments=2)

        assert call_count == 2

    def test_handles_failed_fetch(self) -> None:
        """Programmes stay un-enriched when fetch fails."""
        prog = _make_programme("m001abc", categories="", brand_pid="b001")

        with patch(
            "radio_cache.schedule_refresh._fetch_categories_for_pid",
            return_value="",
        ):
            result = enrich_categories([prog], delay=0)

        assert result[0].categories == ""

    def test_orphan_programmes_enriched_by_own_pid(self) -> None:
        """Programmes without brand/series PID use their own PID."""
        prog = _make_programme(
            "m001abc", categories="", brand_pid="", series_pid="",
        )

        with patch(
            "radio_cache.schedule_refresh._fetch_categories_for_pid",
            return_value="Sci-Fi",
        ) as mock_fetch:
            result = enrich_categories([prog], delay=0)

        mock_fetch.assert_called_once_with("m001abc", delay=0)
        assert result[0].categories == "Sci-Fi"


class TestScheduleRefresh:
    """Integration tests for the full pipeline."""

    def test_creates_separate_db_and_cache(self, tmp_path: Path) -> None:
        """Outputs go to separate files from the category approach."""
        db_path = str(tmp_path / "schedule.db")
        cache_path = str(tmp_path / "schedule.cache")

        progs = [_make_programme("m001abc", categories="Drama")]

        with patch(
            "radio_cache.schedule_refresh.fetch_schedule_programmes",
            return_value=progs,
        ), patch(
            "radio_cache.schedule_refresh.enrich_categories",
            return_value=progs,
        ):
            count = schedule_refresh(
                db_path=db_path,
                cache_path=cache_path,
                export_cache=True,
            )

        assert count >= 1
        assert Path(db_path).exists()
        assert Path(cache_path).exists()

    def test_default_paths_are_separate_from_category_approach(self) -> None:
        """Default output paths differ from the category-first module."""
        assert _DEFAULT_DB_PATH != "radio_cache.db"
        assert _DEFAULT_CACHE_PATH != "radio.cache"

    def test_network_ids_cover_allowed_channels(self) -> None:
        """Network IDs list covers Radio 4, Radio 4 Extra, Radio 3."""
        assert "bbc_radio_fourfm" in _NETWORK_IDS
        assert "bbc_radio_four_extra" in _NETWORK_IDS
        assert "bbc_radio_three" in _NETWORK_IDS
