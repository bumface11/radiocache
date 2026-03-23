"""Tests for the radio_cache.refresh module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme
from radio_cache.refresh import (
    _export_get_iplayer_cache,
    _export_json,
    _programme_to_dict,
    import_from_json,
    refresh_cache,
)


class TestProgrammeToDict:
    """Tests for _programme_to_dict serialisation."""

    def test_round_trip(self) -> None:
        """Dict contains all programme fields."""
        prog = Programme(
            pid="rt1",
            title="Round Trip",
            synopsis="Testing",
            duration_secs=1200,
        )
        d = _programme_to_dict(prog)
        assert d["pid"] == "rt1"
        assert d["title"] == "Round Trip"
        assert d["duration_secs"] == 1200


class TestExportJson:
    """Tests for JSON export functionality."""

    def test_export_creates_file(self, tmp_path: Path) -> None:
        """Exporting creates a valid JSON file."""
        db = CacheDB(":memory:")
        db.upsert_programme(Programme(pid="ex1", title="Export Test"))
        json_path = str(tmp_path / "export.json")
        _export_json(db, json_path)

        data = json.loads(Path(json_path).read_text())
        assert data["meta"]["total_programmes"] == 1
        assert len(data["programmes"]) == 1
        assert data["programmes"][0]["pid"] == "ex1"


class TestImportFromJson:
    """Tests for JSON import functionality."""

    def test_import_round_trip(self, tmp_path: Path) -> None:
        """Programmes survive export -> import round trip."""
        db = CacheDB(":memory:")
        db.upsert_programme(
            Programme(pid="imp1", title="Import Test", synopsis="Round trip")
        )
        json_path = str(tmp_path / "import.json")
        _export_json(db, json_path)
        db.close()

        db_path = str(tmp_path / "import.db")
        count = import_from_json(json_path, db_path)
        assert count == 1

        with CacheDB(db_path) as db2:
            prog = db2.get_programme("imp1")
            assert prog is not None
            assert prog.title == "Import Test"


class TestExportGetIplayerCache:
    """Tests for get_iplayer cache export functionality."""

    def test_export_creates_file(self, tmp_path: Path) -> None:
        """Exporting creates a pipe-delimited cache file."""
        db = CacheDB(":memory:")
        db.upsert_programme(
            Programme(
                pid="gi1",
                title="Episode One",
                brand_title="My Drama",
                synopsis="A synopsis",
                duration_secs=1800,
                channel="BBC Radio 4",
                url="https://www.bbc.co.uk/sounds/play/gi1",
            )
        )
        cache_path = str(tmp_path / "radio.cache")
        _export_get_iplayer_cache(db, cache_path)

        lines = Path(cache_path).read_text().splitlines()
        assert lines[0].startswith("#index|thumbnail|pid|")
        assert lines[1].startswith("ENTRY|")

    def test_entry_fields(self, tmp_path: Path) -> None:
        """ENTRY line contains correct field values."""
        db = CacheDB(":memory:")
        db.upsert_programme(
            Programme(
                pid="gi2",
                title="Episode Title",
                brand_title="Brand Name",
                synopsis="Some desc",
                duration_secs=3600,
                channel="BBC Radio 4",
                episode_number=3,
                url="https://www.bbc.co.uk/sounds/play/gi2",
                available_until="2026-12-31T00:00:00Z",
                first_broadcast="2026-01-01T10:00:00Z",
                thumbnail_url="https://example.com/thumb.jpg",
                categories="Drama,Crime",
            )
        )
        cache_path = str(tmp_path / "radio.cache")
        _export_get_iplayer_cache(db, cache_path)

        entry = Path(cache_path).read_text().splitlines()[1]
        assert entry.startswith("ENTRY|")
        fields = entry.split("|")
        # fields[0] is "ENTRY", then positional fields follow in heading order
        assert fields[1] == "1"  # index
        assert fields[3] == "gi2"  # pid
        assert fields[6] == "radio"  # type
        assert fields[7] == "Brand Name"  # name
        assert fields[8] == "Episode Title"  # episode
        assert fields[9] == "default"  # versions
        assert fields[10] == "3600"  # duration
        assert fields[11] == "Some desc"  # desc
        assert fields[12] == "BBC Radio 4"  # channel
        assert fields[13] == "Drama,Crime"  # categories
        assert fields[16] == "https://www.bbc.co.uk/sounds/play/gi2"  # web
        assert fields[18] == "3"  # episodenum
        assert fields[19] == "<filename>"  # filename
        assert fields[20] == "default"  # mode

    def test_pipe_chars_escaped(self, tmp_path: Path) -> None:
        """Pipe characters in field values are replaced to avoid breaking the format."""
        db = CacheDB(":memory:")
        db.upsert_programme(
            Programme(
                pid="gi3",
                title="Title|With|Pipes",
                synopsis="Desc|with|pipes",
                brand_title="Brand|Name",
            )
        )
        cache_path = str(tmp_path / "radio.cache")
        _export_get_iplayer_cache(db, cache_path)

        content = Path(cache_path).read_text()
        entry = [ln for ln in content.splitlines() if ln.startswith("ENTRY|")][0]
        # Each ENTRY line must have exactly the expected number of fields
        assert entry.count("|") == 20  # ENTRY + 20 fields = 21 items, 20 separators

    def test_empty_db(self, tmp_path: Path) -> None:
        """Exporting an empty database writes only the header line."""
        db = CacheDB(":memory:")
        cache_path = str(tmp_path / "empty.cache")
        _export_get_iplayer_cache(db, cache_path)

        lines = [ln for ln in Path(cache_path).read_text().splitlines() if ln]
        assert len(lines) == 1
        assert lines[0].startswith("#")

    def test_name_fallback(self, tmp_path: Path) -> None:
        """Name falls back to series_title then title when brand_title is absent."""
        db = CacheDB(":memory:")
        db.upsert_programme(
            Programme(pid="gi4", title="Only Title", series_title="Series Title")
        )
        cache_path = str(tmp_path / "fallback.cache")
        _export_get_iplayer_cache(db, cache_path)

        entry = Path(cache_path).read_text().splitlines()[1]
        fields = entry.split("|")
        assert fields[7] == "Series Title"  # name uses series_title fallback


class TestRefreshCacheGetIplayer:
    """Tests for get_iplayer cache generation via refresh_cache."""

    def test_refresh_creates_get_iplayer_cache(self, tmp_path: Path) -> None:
        """refresh_cache generates a get_iplayer cache file when enabled."""
        mock_programmes = [
            Programme(pid="rc1", title="Radio Prog 1", brand_title="Show A"),
            Programme(pid="rc2", title="Radio Prog 2", brand_title="Show B"),
        ]
        db_path = str(tmp_path / "refresh.db")
        json_path = str(tmp_path / "refresh.json")
        cache_path = str(tmp_path / "radio.cache")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ):
            count = refresh_cache(
                db_path=db_path,
                export_json=True,
                json_path=json_path,
                export_get_iplayer=True,
                get_iplayer_path=cache_path,
            )

        assert count == 2
        assert Path(cache_path).exists()
        entry_lines = [
            ln
            for ln in Path(cache_path).read_text().splitlines()
            if ln.startswith("ENTRY|")
        ]
        assert len(entry_lines) == 2

    def test_refresh_skips_get_iplayer_cache_when_disabled(
        self, tmp_path: Path
    ) -> None:
        """refresh_cache does not generate a get_iplayer cache when disabled."""
        mock_programmes = [Programme(pid="rc3", title="Radio Prog 3")]
        db_path = str(tmp_path / "refresh2.db")
        cache_path = str(tmp_path / "radio.cache")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ):
            refresh_cache(
                db_path=db_path,
                export_json=False,
                export_get_iplayer=False,
                get_iplayer_path=cache_path,
            )

        assert not Path(cache_path).exists()


class TestRefreshCache:
    """Tests for the cache refresh workflow."""

    def test_refresh_with_mock_feed(self, tmp_path: Path) -> None:
        """Refresh populates the cache from mocked feed data."""
        mock_programmes = [
            Programme(pid="ref1", title="Refresh 1"),
            Programme(pid="ref2", title="Refresh 2"),
        ]

        db_path = str(tmp_path / "refresh.db")
        json_path = str(tmp_path / "refresh.json")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ):
            count = refresh_cache(
                db_path=db_path,
                export_json=True,
                json_path=json_path,
            )

        assert count == 2

        with CacheDB(db_path) as db:
            assert db.programme_count() == 2
            assert db.get_meta("last_refreshed") != ""

        data = json.loads(Path(json_path).read_text())
        assert data["meta"]["total_programmes"] == 2
