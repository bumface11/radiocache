"""Tests for the radio_cache.refresh module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme
from radio_cache.refresh import (
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
