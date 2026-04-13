"""API tests for recording endpoints.

Tests:
- POST /api/recordings — 201 with job_id for live and programme sources.
- POST /api/recordings — 422 for invalid request body.
- GET /api/recordings/{job_id} — 200 job dict.
- GET /api/recordings/{job_id} — 404 when not found.
- GET /api/recordings — 200 list with optional status filter.
- DELETE /api/recordings/{job_id} — 200 cancelled.
- DELETE /api/recordings/{job_id} — 404 when not found.
- DELETE /api/recordings/{job_id} — 409 when job is already terminal.
- Existing catalogue endpoints (/api/search, /api/stats) are unchanged.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from radio_cache.recording.job_manager import JobManager


@pytest.fixture()
def fresh_manager() -> JobManager:
    """Return a fresh :class:`JobManager` for each test."""
    return JobManager()


@pytest.fixture()
def client(fresh_manager: JobManager) -> TestClient:
    """Return a TestClient wired to a fresh JobManager and CacheDB."""
    with (
        patch("radio_cache_api._DB_PATH", ":memory:"),
        patch("radio_cache_api._JSON_PATH", "/nonexistent/path.json"),
        patch("radio_cache_api.get_job_manager", return_value=fresh_manager),
        # Prevent the background task from actually running ffmpeg.
        patch("radio_cache_api._run_recording_job"),
    ):
        from radio_cache_api import app
        with TestClient(app) as c:
            yield c


# ── POST /api/recordings ─────────────────────────────────────────────────


class TestCreateRecording:
    def test_live_recording_returns_201(self, client: TestClient) -> None:
        resp = client.post(
            "/api/recordings",
            json={
                "source_type": "live",
                "source_id": "bbc_radio_fourfm",
                "duration_seconds": 1800,
                "output_format": "m4a",
            },
        )
        assert resp.status_code == 201
        body = resp.json()
        assert "job_id" in body
        assert body["status"] == "queued"
        assert "created_at" in body

    def test_programme_recording_returns_201(self, client: TestClient) -> None:
        resp = client.post(
            "/api/recordings",
            json={
                "source_type": "programme",
                "source_id": "m002snjn",
                "output_format": "mp3",
            },
        )
        assert resp.status_code == 201
        assert resp.json()["status"] == "queued"

    def test_default_format_is_m4a(self, client: TestClient) -> None:
        resp = client.post(
            "/api/recordings",
            json={"source_type": "live", "source_id": "bbc_radio_one"},
        )
        assert resp.status_code == 201

    def test_invalid_source_type_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/api/recordings",
            json={"source_type": "invalid", "source_id": "bbc_radio_one"},
        )
        assert resp.status_code == 422

    def test_invalid_format_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/api/recordings",
            json={
                "source_type": "live",
                "source_id": "bbc_radio_one",
                "output_format": "flac",
            },
        )
        assert resp.status_code == 422

    def test_missing_source_id_returns_422(self, client: TestClient) -> None:
        resp = client.post(
            "/api/recordings",
            json={"source_type": "live"},
        )
        assert resp.status_code == 422


# ── GET /api/recordings/{job_id} ─────────────────────────────────────────


class TestGetRecording:
    def test_returns_200_with_job_dict(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("live", "bbc_radio_fourfm", "m4a", 600)
        resp = client.get(f"/api/recordings/{job.job_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["job_id"] == job.job_id
        assert body["status"] == "queued"
        assert body["source_id"] == "bbc_radio_fourfm"

    def test_unknown_job_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/recordings/not-a-real-id")
        assert resp.status_code == 404

    def test_completed_job_has_output_path(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("programme", "m000abc1", "m4a")
        fresh_manager.update_status(
            job.job_id,
            "completed",
            output_path="/recordings/file.m4a",
        )
        resp = client.get(f"/api/recordings/{job.job_id}")
        assert resp.status_code == 200
        assert resp.json()["output_path"] == "/recordings/file.m4a"

    def test_not_supported_job_has_error_fields(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("programme", "drm_pid", "m4a")
        fresh_manager.update_status(
            job.job_id,
            "not_supported",
            error_code="not_supported",
            error_message="DRM-only stream",
        )
        resp = client.get(f"/api/recordings/{job.job_id}")
        assert resp.json()["status"] == "not_supported"
        assert resp.json()["error_code"] == "not_supported"


# ── GET /api/recordings ──────────────────────────────────────────────────


class TestListRecordings:
    def test_returns_200_with_jobs_list(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        fresh_manager.create_job("live", "bbc_radio_one", "m4a", 60)
        fresh_manager.create_job("live", "bbc_radio_two", "m4a", 60)
        resp = client.get("/api/recordings")
        assert resp.status_code == 200
        body = resp.json()
        assert body["count"] == 2
        assert len(body["jobs"]) == 2

    def test_status_filter_queued(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        j1 = fresh_manager.create_job("live", "bbc_radio_one", "m4a", 60)
        j2 = fresh_manager.create_job("live", "bbc_radio_two", "m4a", 60)
        fresh_manager.update_status(j1.job_id, "running")
        resp = client.get("/api/recordings?status=queued")
        body = resp.json()
        assert body["count"] == 1
        assert body["jobs"][0]["job_id"] == j2.job_id

    def test_empty_list_when_no_jobs(self, client: TestClient) -> None:
        resp = client.get("/api/recordings")
        assert resp.status_code == 200
        assert resp.json()["count"] == 0

    def test_limit_parameter_respected(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        for _ in range(5):
            fresh_manager.create_job("live", "bbc_radio_one", "m4a", 60)
        resp = client.get("/api/recordings?limit=2")
        assert resp.json()["count"] == 2


# ── DELETE /api/recordings/{job_id} ──────────────────────────────────────


class TestCancelRecording:
    def test_cancel_queued_returns_200(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("live", "bbc_radio_three", "m4a", 120)
        with patch("radio_cache_api._recorder.terminate_job"):
            resp = client.delete(f"/api/recordings/{job.job_id}")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "cancelled"
        assert body["job_id"] == job.job_id

    def test_cancel_running_returns_200(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("live", "bbc_radio_three", "m4a", 120)
        fresh_manager.update_status(job.job_id, "running")
        with patch("radio_cache_api._recorder.terminate_job"):
            resp = client.delete(f"/api/recordings/{job.job_id}")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"

    def test_cancel_nonexistent_returns_404(self, client: TestClient) -> None:
        with patch("radio_cache_api._recorder.terminate_job"):
            resp = client.delete("/api/recordings/no-such-job")
        assert resp.status_code == 404

    def test_cancel_completed_returns_409(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("programme", "m000fin1", "m4a")
        fresh_manager.update_status(job.job_id, "completed")
        with patch("radio_cache_api._recorder.terminate_job"):
            resp = client.delete(f"/api/recordings/{job.job_id}")
        assert resp.status_code == 409
        assert resp.json()["detail"]["error"] == "not_cancellable"

    def test_cancel_failed_returns_409(
        self, client: TestClient, fresh_manager: JobManager
    ) -> None:
        job = fresh_manager.create_job("programme", "m000err1", "m4a")
        fresh_manager.update_status(job.job_id, "failed")
        with patch("radio_cache_api._recorder.terminate_job"):
            resp = client.delete(f"/api/recordings/{job.job_id}")
        assert resp.status_code == 409


# ── Existing catalogue endpoints unchanged ────────────────────────────────


class TestExistingEndpointsUnchanged:
    def test_api_stats_still_works(self, client: TestClient) -> None:
        resp = client.get("/api/stats")
        assert resp.status_code == 200
        body = resp.json()
        assert "total_programmes" in body

    def test_api_search_still_works(self, client: TestClient) -> None:
        resp = client.get("/api/search?q=drama")
        assert resp.status_code == 200
        body = resp.json()
        assert "results" in body

    def test_api_series_still_works(self, client: TestClient) -> None:
        resp = client.get("/api/series")
        assert resp.status_code == 200
        assert "series" in resp.json()

    def test_api_programme_not_found_still_works(self, client: TestClient) -> None:
        resp = client.get("/api/programme/notexist")
        assert resp.status_code == 200
        assert resp.json()["error"] == "not_found"


class TestCategoriesEndpoint:
    """Tests for GET /api/categories."""

    def test_categories_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/categories")
        assert resp.status_code == 200
        body = resp.json()
        assert "count" in body
        assert "categories" in body
        assert isinstance(body["categories"], list)

    def test_api_search_category_filter(self, client: TestClient) -> None:
        """Search endpoint accepts a category query param."""
        resp = client.get("/api/search?category=Drama")
        assert resp.status_code == 200
        body = resp.json()
        assert "results" in body
        assert body["category"] == "Drama"


# ── Podcast feed ─────────────────────────────────────────────────────────


class TestPodcastFeed:
    """Tests for GET /api/podcast.xml."""

    @pytest.fixture()
    def podcast_client(
        self, fresh_manager: JobManager, tmp_path: Path
    ) -> TestClient:
        """Client with a completed job and matching programme in the DB."""
        from datetime import UTC, datetime

        from radio_cache.cache_db import CacheDB
        from radio_cache.models import Programme

        # Create a small DB with one programme.
        db_path = str(tmp_path / "test.db")
        db = CacheDB(db_path)
        db.upsert_programme(
            Programme(
                pid="p00test1",
                title="Test Drama Episode",
                synopsis="A gripping test drama.",
                duration_secs=1800,
                series_title="Test Drama",
                episode_number=3,
                channel="Radio 4",
                thumbnail_url="https://example.com/thumb.jpg",
                categories="Drama",
                url="https://www.bbc.co.uk/sounds/play/p00test1",
            )
        )
        db.close()

        # Create a dummy output file.
        out_file = tmp_path / "recording.m4a"
        out_file.write_bytes(b"\x00" * 100)

        # Register a completed job.
        job = fresh_manager.create_job(
            source_type="programme",
            source_id="p00test1",
            output_format="m4a",
        )
        fresh_manager.update_status(
            job.job_id,
            "completed",
            output_path=str(out_file),
            completed_at=datetime.now(UTC).isoformat(),
            duration_seconds=1800,
        )

        with (
            patch("radio_cache_api._DB_PATH", db_path),
            patch("radio_cache_api._JSON_PATH", "/nonexistent/path.json"),
            patch("radio_cache_api.get_job_manager", return_value=fresh_manager),
            patch("radio_cache_api._run_recording_job"),
        ):
            from radio_cache_api import app

            with TestClient(app) as c:
                yield c

    def test_podcast_feed_returns_xml(self, podcast_client: TestClient) -> None:
        resp = podcast_client.get("/api/podcast.xml")
        assert resp.status_code == 200
        assert "application/rss+xml" in resp.headers["content-type"]

    def test_podcast_feed_contains_itunes_tags(
        self, podcast_client: TestClient
    ) -> None:
        import xml.etree.ElementTree as ET

        resp = podcast_client.get("/api/podcast.xml")
        root = ET.fromstring(resp.content)
        ns = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}

        items = root.findall(".//item")
        assert len(items) == 1
        item = items[0]

        assert item.findtext("title") == "Test Drama Episode"
        assert item.findtext("description") == "A gripping test drama."
        assert item.findtext("link") == "https://www.bbc.co.uk/sounds/play/p00test1"
        assert item.findtext("itunes:summary", namespaces=ns) == "A gripping test drama."
        assert item.findtext("itunes:subtitle", namespaces=ns) == "Test Drama - Episode 3"
        assert item.findtext("itunes:author", namespaces=ns) == "Radio 4"
        assert item.findtext("itunes:episode", namespaces=ns) == "3"
        assert item.findtext("itunes:episodeType", namespaces=ns) == "full"
        assert item.findtext("itunes:keywords", namespaces=ns) == "Drama"
        assert item.find("itunes:image", namespaces=ns).get("href") == "https://example.com/thumb.jpg"

    def test_podcast_feed_episode_label_no_number(
        self, tmp_path: Path
    ) -> None:
        """Episode label uses series title alone when episode_number is 0."""
        from datetime import UTC, datetime

        from radio_cache.cache_db import CacheDB
        from radio_cache.models import Programme

        db_path = str(tmp_path / "test2.db")
        db = CacheDB(db_path)
        db.upsert_programme(
            Programme(
                pid="p00test2",
                title="Loose Ends",
                series_title="Loose Ends",
                episode_number=0,
                channel="Radio 4",
            )
        )
        db.close()

        out_file = tmp_path / "recording2.m4a"
        out_file.write_bytes(b"\x00" * 50)

        mgr = JobManager()
        job = mgr.create_job(
            source_type="programme", source_id="p00test2", output_format="m4a"
        )
        mgr.update_status(
            job.job_id,
            "completed",
            output_path=str(out_file),
            completed_at=datetime.now(UTC).isoformat(),
        )

        with (
            patch("radio_cache_api._DB_PATH", db_path),
            patch("radio_cache_api._JSON_PATH", "/nonexistent/path.json"),
            patch("radio_cache_api.get_job_manager", return_value=mgr),
            patch("radio_cache_api._run_recording_job"),
        ):
            import xml.etree.ElementTree as ET

            from radio_cache_api import app

            with TestClient(app) as c:
                resp = c.get("/api/podcast.xml")

        ns = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd"}
        root = ET.fromstring(resp.content)
        item = root.findall(".//item")[0]
        assert item.findtext("itunes:subtitle", namespaces=ns) == "Loose Ends"
        # No <itunes:episode> when episode_number is 0
        assert item.find("itunes:episode", namespaces=ns) is None
