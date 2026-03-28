"""Data models for the recording service.

Defines the job state machine and Pydantic request/response schemas
used by the FastAPI recording endpoints.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field

# ── Type aliases ────────────────────────────────────────────────────────

RecordingStatus = Literal[
    "queued", "running", "completed", "failed", "not_supported", "cancelled"
]

RecordingErrorCode = Literal["not_supported", "unavailable", "failed"]

OutputFormat = Literal["m4a", "mp3"]

SourceType = Literal["live", "programme"]


# ── Internal job state ──────────────────────────────────────────────────


@dataclass
class RecordingJob:
    """Mutable runtime state for one recording job.

    Attributes:
        job_id: UUID-based unique identifier.
        source_type: ``"live"`` or ``"programme"``.
        source_id: Station ID or programme PID.
        output_format: Target audio container.
        duration_seconds: Requested capture duration, or ``None`` for
            full catch-up programme (EOF-driven).
        status: Current lifecycle status.
        output_path: Absolute path of the written audio file.
        error_code: Stable error classification when status is
            ``"failed"`` or ``"not_supported"``.
        error_message: Human-readable error detail.
        created_at: ISO-8601 UTC creation timestamp.
        started_at: ISO-8601 UTC timestamp when capture began.
        completed_at: ISO-8601 UTC timestamp when the job finished.
        progress_seconds: Best-effort elapsed capture seconds.
        manifest_url: Resolved HLS manifest URL for this job.
    """

    job_id: str
    source_type: SourceType
    source_id: str
    output_format: OutputFormat
    duration_seconds: int | None = None
    status: RecordingStatus = "queued"
    output_path: str = ""
    error_code: RecordingErrorCode | None = None
    error_message: str = ""
    created_at: str = field(
        default_factory=lambda: datetime.now(UTC).isoformat()
    )
    started_at: str = ""
    completed_at: str = ""
    progress_seconds: int = 0
    manifest_url: str = ""


# ── Pydantic API schemas ────────────────────────────────────────────────


class RecordingRequest(BaseModel):
    """Request body for ``POST /api/recordings``."""

    source_type: SourceType = Field(
        description="'live' to record a live station or 'programme' for catch-up."
    )
    source_id: str = Field(
        description=(
            "BBC station ID (e.g. 'bbc_radio_fourfm') for live, "
            "or programme PID (e.g. 'm002snjn') for catch-up."
        )
    )
    duration_seconds: int | None = Field(
        default=None,
        ge=1,
        description=(
            "Duration to capture in seconds.  Required for live recordings; "
            "optional (uses programme duration) for catch-up."
        ),
    )
    output_format: OutputFormat = Field(
        default="m4a",
        description="Output audio container.  'm4a' (default) or 'mp3'.",
    )


def job_to_dict(job: RecordingJob) -> dict:
    """Serialise a :class:`RecordingJob` to a JSON-compatible dict."""
    return {
        "job_id": job.job_id,
        "source_type": job.source_type,
        "source_id": job.source_id,
        "output_format": job.output_format,
        "duration_seconds": job.duration_seconds,
        "status": job.status,
        "output_path": job.output_path,
        "error_code": job.error_code,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "completed_at": job.completed_at,
        "progress_seconds": job.progress_seconds,
        "manifest_url": job.manifest_url,
    }
