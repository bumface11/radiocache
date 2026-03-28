"""Unit tests for the recording JobManager.

Covers:
- Job creation in queued state.
- Status transitions (update_status).
- Cancellation of queued and running jobs.
- Attempting to cancel a terminal job.
- List filtering and limit.
- Thread safety of create / update under concurrent access.
"""

from __future__ import annotations

import threading

import pytest

from radio_cache.recording.job_manager import JobManager


@pytest.fixture()
def manager() -> JobManager:
    """Return a fresh :class:`JobManager` for each test."""
    return JobManager()


class TestCreateJob:
    def test_new_job_is_queued(self, manager: JobManager) -> None:
        job = manager.create_job("live", "bbc_radio_fourfm", "m4a", 1800)
        assert job.status == "queued"
        assert job.source_type == "live"
        assert job.source_id == "bbc_radio_fourfm"
        assert job.output_format == "m4a"
        assert job.duration_seconds == 1800
        assert job.job_id  # non-empty UUID string

    def test_create_programme_job_no_duration(self, manager: JobManager) -> None:
        job = manager.create_job("programme", "m002snjn", "mp3", None)
        assert job.duration_seconds is None
        assert job.source_type == "programme"

    def test_each_job_has_unique_id(self, manager: JobManager) -> None:
        j1 = manager.create_job("live", "bbc_radio_one", "m4a", 60)
        j2 = manager.create_job("live", "bbc_radio_one", "m4a", 60)
        assert j1.job_id != j2.job_id

    def test_job_stored_and_retrievable(self, manager: JobManager) -> None:
        job = manager.create_job("live", "bbc_6music", "m4a", 300)
        retrieved = manager.get_job(job.job_id)
        assert retrieved is not None
        assert retrieved.job_id == job.job_id

    def test_get_nonexistent_returns_none(self, manager: JobManager) -> None:
        assert manager.get_job("not-a-real-id") is None


class TestUpdateStatus:
    def test_queued_to_running(self, manager: JobManager) -> None:
        job = manager.create_job("live", "bbc_radio_two", "m4a", 600)
        updated = manager.update_status(
            job.job_id, "running", started_at="2026-03-28T10:00:00+00:00"
        )
        assert updated is not None
        assert updated.status == "running"
        assert updated.started_at == "2026-03-28T10:00:00+00:00"

    def test_running_to_completed(self, manager: JobManager) -> None:
        job = manager.create_job("programme", "m000abc1", "m4a")
        manager.update_status(job.job_id, "running")
        updated = manager.update_status(
            job.job_id,
            "completed",
            output_path="/recordings/file.m4a",
            progress_seconds=1800,
        )
        assert updated is not None
        assert updated.status == "completed"
        assert updated.output_path == "/recordings/file.m4a"
        assert updated.progress_seconds == 1800

    def test_update_nonexistent_returns_none(self, manager: JobManager) -> None:
        assert manager.update_status("ghost", "running") is None

    def test_error_fields_set_on_failure(self, manager: JobManager) -> None:
        job = manager.create_job("programme", "m000xyz9", "m4a")
        updated = manager.update_status(
            job.job_id,
            "failed",
            error_code="unavailable",
            error_message="HTTP 404",
        )
        assert updated is not None
        assert updated.error_code == "unavailable"
        assert updated.error_message == "HTTP 404"

    def test_not_supported_status(self, manager: JobManager) -> None:
        job = manager.create_job("programme", "m000drm1", "m4a")
        updated = manager.update_status(
            job.job_id,
            "not_supported",
            error_code="not_supported",
            error_message="DRM-only stream",
        )
        assert updated is not None
        assert updated.status == "not_supported"
        assert updated.error_code == "not_supported"


class TestCancelJob:
    def test_cancel_queued_job(self, manager: JobManager) -> None:
        job = manager.create_job("live", "bbc_radio_three", "m4a", 120)
        cancelled = manager.cancel_job(job.job_id)
        assert cancelled is not None
        assert cancelled.status == "cancelled"
        assert cancelled.completed_at  # timestamp set

    def test_cancel_running_job(self, manager: JobManager) -> None:
        job = manager.create_job("live", "bbc_radio_three", "m4a", 120)
        manager.update_status(job.job_id, "running")
        cancelled = manager.cancel_job(job.job_id)
        assert cancelled is not None
        assert cancelled.status == "cancelled"

    def test_cancel_completed_job_returns_unchanged(self, manager: JobManager) -> None:
        job = manager.create_job("programme", "m000fin1", "m4a")
        manager.update_status(job.job_id, "completed")
        result = manager.cancel_job(job.job_id)
        assert result is not None
        assert result.status == "completed"  # unchanged

    def test_cancel_nonexistent_returns_none(self, manager: JobManager) -> None:
        assert manager.cancel_job("does-not-exist") is None


class TestListJobs:
    def test_returns_all_jobs_newest_first(self, manager: JobManager) -> None:
        j1 = manager.create_job("live", "bbc_radio_one", "m4a", 60)
        j2 = manager.create_job("live", "bbc_radio_two", "m4a", 60)
        jobs = manager.list_jobs()
        assert jobs[0].job_id == j2.job_id
        assert jobs[1].job_id == j1.job_id

    def test_status_filter_running(self, manager: JobManager) -> None:
        j1 = manager.create_job("live", "bbc_radio_one", "m4a", 60)
        j2 = manager.create_job("live", "bbc_radio_two", "m4a", 60)
        manager.update_status(j1.job_id, "running")
        running = manager.list_jobs(status="running")
        assert len(running) == 1
        assert running[0].job_id == j1.job_id

    def test_limit_is_respected(self, manager: JobManager) -> None:
        for i in range(10):
            manager.create_job("live", f"bbc_radio_{i}", "m4a", 60)
        jobs = manager.list_jobs(limit=3)
        assert len(jobs) == 3

    def test_empty_list_when_no_jobs(self, manager: JobManager) -> None:
        assert manager.list_jobs() == []


class TestThreadSafety:
    def test_concurrent_creates_all_unique(self, manager: JobManager) -> None:
        """Concurrent job creation must produce unique IDs without data races."""
        ids: list[str] = []
        lock = threading.Lock()

        def create() -> None:
            job = manager.create_job("live", "bbc_radio_one", "m4a", 60)
            with lock:
                ids.append(job.job_id)

        threads = [threading.Thread(target=create) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(set(ids)) == 50
