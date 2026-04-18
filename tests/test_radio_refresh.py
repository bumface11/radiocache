"""Tests for the radio_cache.refresh module."""

from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from unittest.mock import patch

from radio_cache.cache_db import CacheDB
from radio_cache.models import Programme
from radio_cache.refresh import (
    _export_get_iplayer_cache,
    _export_json,
    _programme_to_dict,
    export_db_snapshot,
    import_db_snapshot_from_github,
    import_from_db_snapshot,
    import_from_json,
    refresh_cache,
    _RECENT_MAX_PAGES,
    _FULL_MAX_PAGES,
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

    def test_import_preserves_last_refreshed_from_json(
        self, tmp_path: Path
    ) -> None:
        """Import uses the last_refreshed timestamp from the JSON metadata."""
        original_ts = "2025-06-15T04:00:00+00:00"
        db = CacheDB(":memory:")
        db.upsert_programme(Programme(pid="ts1", title="Timestamp Test"))
        db.set_meta("last_refreshed", original_ts)
        json_path = str(tmp_path / "ts.json")
        _export_json(db, json_path)
        db.close()

        db_path = str(tmp_path / "ts.db")
        import_from_json(json_path, db_path)

        with CacheDB(db_path) as db2:
            assert db2.get_meta("last_refreshed") == original_ts


class TestDbSnapshot:
    """Tests for SQLite snapshot export/import."""

    def test_export_and_import_zipped_snapshot(self, tmp_path: Path) -> None:
        """A zipped DB snapshot round-trips programme data and metadata."""
        db_path = tmp_path / "source.db"
        with CacheDB(str(db_path)) as db:
            db.upsert_programme(Programme(pid="db1", title="DB Snapshot"))
            db.set_meta("last_refreshed", "2026-04-18T04:00:00+00:00")

        snapshot_path = tmp_path / "radio_cache.db.zip"
        export_db_snapshot(str(db_path), str(snapshot_path))

        restored_path = tmp_path / "restored.db"
        count = import_from_db_snapshot(str(snapshot_path), str(restored_path))

        assert count == 1
        with CacheDB(str(restored_path)) as restored:
            prog = restored.get_programme("db1")
            assert prog is not None
            assert prog.title == "DB Snapshot"
            assert restored.get_meta("last_refreshed") == "2026-04-18T04:00:00+00:00"

    def test_import_snapshot_from_url(self, tmp_path: Path) -> None:
        """A downloaded zipped snapshot is restored into the destination DB."""
        db_path = tmp_path / "source.db"
        with CacheDB(str(db_path)) as db:
            db.upsert_programme(Programme(pid="db2", title="Remote Snapshot"))

        snapshot_path = tmp_path / "radio_cache.db.zip"
        export_db_snapshot(str(db_path), str(snapshot_path))
        payload = snapshot_path.read_bytes()

        class _Response:
            def __enter__(self) -> _Response:
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def read(self) -> bytes:
                return payload

        restored_path = tmp_path / "downloaded.db"
        with patch("urllib.request.urlopen", return_value=_Response()):
            count = import_db_snapshot_from_github(
                "https://example.com/radio_cache.db.zip",
                str(restored_path),
            )

        assert count == 1
        with CacheDB(str(restored_path)) as restored:
            prog = restored.get_programme("db2")
            assert prog is not None
            assert prog.title == "Remote Snapshot"


class TestExportGetIplayerCache:
    """Tests for get_iplayer cache export functionality."""

    def test_export_creates_file(self, tmp_path: Path) -> None:
        """Exporting creates a pipe-delimited cache file in the new format."""
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
        expected_header = (
            "#index|type|name|episode|seriesnum|episodenum"
            "|pid|channel|available|expires|duration|desc|web|thumbnail|timeadded"
        )
        assert lines[0] == expected_header
        # entry line is plain pipe-delimited (no ENTRY prefix)
        assert not lines[1].startswith("ENTRY|")
        assert lines[1].startswith("1|radio|")

    def test_entry_fields(self, tmp_path: Path) -> None:
        """Entry line contains correct field values in the new column order."""
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
        fields = entry.split("|")
        # index|type|name|episode|seriesnum|episodenum|pid|channel|...
        assert fields[0] == "1"            # index
        assert fields[1] == "radio"        # type
        assert fields[2] == "Brand Name"   # name
        assert fields[3] == "Episode Title"  # episode
        assert fields[4] == ""             # seriesnum (always empty)
        assert fields[5] == "3"            # episodenum
        assert fields[6] == "gi2"          # pid
        assert fields[7] == "BBC Radio 4"  # channel
        assert fields[10] == "3600"        # duration
        assert fields[11] == "Some desc"   # desc
        assert fields[12] == "https://www.bbc.co.uk/sounds/play/gi2"  # web
        assert fields[13] == "https://example.com/thumb.jpg"  # thumbnail
        # available (fields[8]) and expires (fields[9]) should be Unix timestamps
        assert fields[8].isdigit()
        assert fields[9].isdigit()
        # timeadded (fields[14]) should be a Unix timestamp
        assert fields[14].isdigit()

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
        entry = [ln for ln in content.splitlines() if not ln.startswith("#")][0]
        # 15 fields, 14 separators per data line
        assert entry.count("|") == 14

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
        # name (col index 2) uses series_title fallback
        assert fields[2] == "Series Title"


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
            if ln and not ln.startswith("#")
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
                get_iplayer_path=str(tmp_path / "radio.cache"),
            )

        assert count == 2

        with CacheDB(db_path) as db:
            assert db.programme_count() == 2
            assert db.get_meta("last_refreshed") != ""

        data = json.loads(Path(json_path).read_text())
        assert data["meta"]["total_programmes"] == 2

    def test_refresh_can_export_db_snapshot(self, tmp_path: Path) -> None:
        """refresh_cache can write a compressed DB snapshot for deployment."""
        mock_programmes = [Programme(pid="ref3", title="Snapshot Export")]
        db_path = str(tmp_path / "refresh.db")
        snapshot_path = tmp_path / "radio_cache.db.zip"

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ):
            count = refresh_cache(
                db_path=db_path,
                export_json=False,
                db_snapshot_path=str(snapshot_path),
                export_get_iplayer=False,
            )

        assert count == 1
        assert snapshot_path.exists()
        with zipfile.ZipFile(snapshot_path) as archive:
            assert any(name.endswith(".db") for name in archive.namelist())


class TestRefreshDepth:
    """Tests for the depth parameter controlling page limits."""

    def test_recent_depth_limits_pages(self, tmp_path: Path) -> None:
        """depth='recent' passes a small max_pages and no backfill."""
        mock_programmes = [Programme(pid="d1", title="Depth Test")]
        db_path = str(tmp_path / "depth.db")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ) as mock_fetch:
            refresh_cache(
                db_path=db_path,
                export_json=False,
                export_get_iplayer=False,
                depth="recent",
            )
            mock_fetch.assert_called_once()
            _, kwargs = mock_fetch.call_args
            assert kwargs["max_pages"] == _RECENT_MAX_PAGES
            assert kwargs["backfill_containers"] is False

    def test_full_depth_uses_large_page_limit(self, tmp_path: Path) -> None:
        """depth='full' passes the full max_pages and enables backfill."""
        mock_programmes = [Programme(pid="d2", title="Full Depth")]
        db_path = str(tmp_path / "depth_full.db")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ) as mock_fetch:
            refresh_cache(
                db_path=db_path,
                export_json=False,
                export_get_iplayer=False,
                depth="full",
            )
            mock_fetch.assert_called_once()
            _, kwargs = mock_fetch.call_args
            assert kwargs["max_pages"] == _FULL_MAX_PAGES
            assert kwargs["backfill_containers"] is True

    def test_depth_records_per_bucket_metadata(self, tmp_path: Path) -> None:
        """Each depth level records its own last_refreshed timestamp."""
        mock_programmes = [Programme(pid="d3", title="Bucket Meta")]
        db_path = str(tmp_path / "bucket_meta.db")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ):
            refresh_cache(
                db_path=db_path,
                export_json=False,
                export_get_iplayer=False,
                depth="recent",
            )

        with CacheDB(db_path) as db:
            assert db.get_meta("last_refreshed") != ""
            assert db.get_meta("last_refreshed_recent") != ""
            assert db.get_meta("last_refreshed_full") == ""

    def test_categories_passed_with_depth(self, tmp_path: Path) -> None:
        """category_slugs are forwarded regardless of depth."""
        mock_programmes = [Programme(pid="d4", title="Cat Depth")]
        db_path = str(tmp_path / "cat_depth.db")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ) as mock_fetch:
            refresh_cache(
                db_path=db_path,
                export_json=False,
                export_get_iplayer=False,
                category_slugs=["drama", "comedy"],
                depth="recent",
            )
            mock_fetch.assert_called_once()
            _, kwargs = mock_fetch.call_args
            assert kwargs["category_slugs"] == ["drama", "comedy"]
            assert kwargs["max_pages"] == _RECENT_MAX_PAGES

    def test_default_depth_is_full(self, tmp_path: Path) -> None:
        """Omitting depth defaults to 'full'."""
        mock_programmes = [Programme(pid="d5", title="Default Depth")]
        db_path = str(tmp_path / "default_depth.db")

        with patch(
            "radio_cache.refresh.fetch_drama_programmes",
            return_value=mock_programmes,
        ) as mock_fetch:
            refresh_cache(
                db_path=db_path,
                export_json=False,
                export_get_iplayer=False,
            )
            _, kwargs = mock_fetch.call_args
            assert kwargs["max_pages"] == _FULL_MAX_PAGES
