"""In-memory job registry for recording jobs.

Thread-safe via :class:`threading.Lock`.  All state lives in process
memory; jobs do not survive a restart.  For a production deployment
with multiple workers or crash-recovery requirements, replace the
``_jobs`` dict with a SQLite table or Redis store (adapting the same
interface).
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime
from uuid import uuid4

from radio_cache.recording.models import (
    OutputFormat,
    RecordingJob,
    RecordingStatus,
    SourceType,
)


class JobManager:
    """Thread-safe store for :class:`~models.RecordingJob` instances."""

    def __init__(self) -> None:
        self._jobs: dict[str, RecordingJob] = {}
        self._lock = threading.Lock()

    # ── Lifecycle ────────────────────────────────────────────────────────

    def create_job(
        self,
        source_type: SourceType,
        source_id: str,
        output_format: OutputFormat,
        duration_seconds: int | None = None,
    ) -> RecordingJob:
        """Create a new ``queued`` job and register it.

        Args:
            source_type: ``"live"`` or ``"programme"``.
            source_id: Station ID or episode PID.
            output_format: Audio container format.
            duration_seconds: Capture ceiling; ``None`` means record
                until stream EOF.

        Returns:
            The new :class:`~models.RecordingJob`.
        """
        job = RecordingJob(
            job_id=str(uuid4()),
            source_type=source_type,
            source_id=source_id,
            output_format=output_format,
            duration_seconds=duration_seconds,
            status="queued",
            created_at=datetime.now(UTC).isoformat(),
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def get_job(self, job_id: str) -> RecordingJob | None:
        """Return the job with *job_id*, or ``None``.

        Args:
            job_id: UUID job identifier.
        """
        with self._lock:
            return self._jobs.get(job_id)

    def list_jobs(
        self,
        status: RecordingStatus | None = None,
        limit: int = 50,
    ) -> list[RecordingJob]:
        """Return jobs sorted newest-first, optionally filtered by status.

        Args:
            status: Optional status to filter by.
            limit: Maximum number of results.

        Returns:
            Matching :class:`~models.RecordingJob` list.
        """
        with self._lock:
            jobs = list(self._jobs.values())
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        return jobs[:limit]

    def update_status(
        self,
        job_id: str,
        status: RecordingStatus,
        **kwargs: object,
    ) -> RecordingJob | None:
        """Update a job's status and any additional fields.

        Accepted keyword arguments mirror :class:`~models.RecordingJob`
        attributes: ``output_path``, ``error_code``, ``error_message``,
        ``started_at``, ``completed_at``, ``progress_seconds``,
        ``manifest_url``.

        Args:
            job_id: Target job identifier.
            status: New status value.
            **kwargs: Extra job fields to set atomically.

        Returns:
            Updated job, or ``None`` if *job_id* is not found.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            job.status = status
            for key, value in kwargs.items():
                if hasattr(job, key):
                    setattr(job, key, value)
        return job

    def cancel_job(self, job_id: str) -> RecordingJob | None:
        """Mark a job as cancelled if it is in a cancellable state.

        Only ``"queued"`` and ``"running"`` jobs can be cancelled.
        To actually stop a running ffmpeg process, the caller should
        also invoke :func:`~recorder.terminate_job` with the same
        *job_id*.

        Args:
            job_id: Job to cancel.

        Returns:
            Updated job (status ``"cancelled"``), the unchanged job if
            it is already in a terminal state, or ``None`` if not found.
        """
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            if job.status not in ("queued", "running"):
                return job
            job.status = "cancelled"
            job.completed_at = datetime.now(UTC).isoformat()
        return job


# ── Module-level singleton ───────────────────────────────────────────────

_manager = JobManager()


def get_job_manager() -> JobManager:
    """Return the shared :class:`JobManager` singleton."""
    return _manager
