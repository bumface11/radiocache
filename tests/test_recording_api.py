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
