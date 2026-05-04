"""Tests for the radio_cache_api lifespan and startup behaviour."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme
from radio_cache.refresh import _export_json, export_db_snapshot


class TestLifespanImport:
    """Tests for the lifespan startup cache import."""

    def test_lifespan_imports_db_snapshot_on_startup(self, tmp_path: Path) -> None:
        """App imports a SQLite snapshot into an empty DB on startup."""
        source_db = tmp_path / "source.db"
        with CacheDB(str(source_db)) as db:
            db.upsert_programme(Programme(pid="snap1", title="Snapshot Test"))

        snapshot_path = tmp_path / "radio_cache.db.zip"
        export_db_snapshot(str(source_db), str(snapshot_path))

        db_path = str(tmp_path / "startup.db")

        with (
            patch("radio_cache_api._DB_SNAPSHOT_PATH", str(snapshot_path)),
            patch("radio_cache_api._DB_SNAPSHOT_URL", ""),
            patch("radio_cache_api._JSON_PATH", str(tmp_path / "missing.json")),
            patch("radio_cache_api._DB_PATH", db_path),
        ):
            from radio_cache_api import app

            with TestClient(app):
                pass

        with CacheDB(db_path) as db2:
            prog = db2.get_programme("snap1")
            assert prog is not None
            assert prog.title == "Snapshot Test"

    def test_lifespan_imports_json_on_startup(self, tmp_path: Path) -> None:
        """App imports radio_cache_export.json into the DB on startup."""
        # Create a JSON export file with known data.
        db = CacheDB(":memory:")
        db.upsert_programme(
            Programme(pid="ls1", title="Lifespan Test")
        )
        db.set_meta("last_refreshed", "2025-07-01T04:00:00+00:00")
        json_path = str(tmp_path / "radio_cache_export.json")
        _export_json(db, json_path)
        db.close()

        db_path = str(tmp_path / "startup.db")

        with (
            patch("radio_cache_api._DB_SNAPSHOT_PATH", ""),
            patch("radio_cache_api._DB_SNAPSHOT_URL", ""),
            patch("radio_cache_api._JSON_PATH", json_path),
            patch("radio_cache_api._DB_PATH", db_path),
        ):
            # Re-import app so the patched values are used in the lifespan.
            from radio_cache_api import app

            with TestClient(app):
                pass  # lifespan runs on enter

        with CacheDB(db_path) as db2:
            assert db2.programme_count() == 1
            prog = db2.get_programme("ls1")
            assert prog is not None
            assert prog.title == "Lifespan Test"
            assert db2.get_meta("last_refreshed") == "2025-07-01T04:00:00+00:00"

    def test_lifespan_no_json_file(self, tmp_path: Path) -> None:
        """App starts normally when the JSON file does not exist."""
        missing = str(tmp_path / "nonexistent.json")
        db_path = str(tmp_path / "empty.db")

        with (
            patch("radio_cache_api._DB_SNAPSHOT_PATH", ""),
            patch("radio_cache_api._DB_SNAPSHOT_URL", ""),
            patch("radio_cache_api._JSON_PATH", missing),
            patch("radio_cache_api._DB_PATH", db_path),
        ):
            from radio_cache_api import app

            with TestClient(app):
                pass  # should not raise


class TestSeriesTotalsOnPages:
    """Page responses show total series counts, not only page slices."""

    def test_search_page_and_series_page_show_total_episode_count(
        self, tmp_path: Path
    ) -> None:
        db_path = str(tmp_path / "series-counts.db")

        with CacheDB(db_path) as db:
            db.upsert_programmes(
                [
                    Programme(
                        pid=f"big-{index:03d}",
                        title=f"Big Serial Episode {index}",
                        synopsis="Long-running test serial",
                        series_pid="s_big_serial",
                        series_title="Big Serial",
                        brand_pid="b_big_serial",
                        brand_title="Big Serial Brand",
                        episode_number=index,
                        channel="Radio 4",
                        categories="Drama",
                    )
                    for index in range(1, 61)
                ]
            )

        with (
            patch("radio_cache_api._DB_SNAPSHOT_PATH", ""),
            patch("radio_cache_api._DB_SNAPSHOT_URL", ""),
            patch("radio_cache_api._JSON_PATH", str(tmp_path / "missing.json")),
            patch("radio_cache_api._DB_PATH", db_path),
        ):
            from radio_cache_api import app

            with TestClient(app) as client:
                search_resp = client.get("/search", params={"q": "Big Serial"})
                assert search_resp.status_code == 200
                assert "20 results on this page" in search_resp.text
                assert "Big Serial" in search_resp.text
                assert ">60<" in search_resp.text

                series_resp = client.get("/series/s_big_serial")
                assert series_resp.status_code == 200
                assert "60 episodes" in series_resp.text

                filtered_series_resp = client.get(
                    "/series/s_big_serial",
                    params={"q": "Episode 17"},
                )
                assert filtered_series_resp.status_code == 200
                assert 'showing 1 matches for "Episode 17"' in filtered_series_resp.text
                assert "Big Serial Episode 17" in filtered_series_resp.text
                assert "Big Serial Episode 18" not in filtered_series_resp.text
