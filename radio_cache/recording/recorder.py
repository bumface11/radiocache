"""FFmpeg-based audio stream recorder.

Handles:
* Building ffmpeg command lines for HLS capture with inline metadata.
* Running ffmpeg as a subprocess with stdout/stderr capture.
* Parsing ffmpeg progress output to report elapsed seconds.
* Exponential-backoff retry on transient subprocess failures.
* A module-level process registry so that running jobs can be
  terminated on demand (e.g. from the DELETE /api/recordings endpoint).
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from radio_cache.recording.config import (
    FFMPEG_PATH,
    HTTP_RETRY_COUNT,
    HTTP_TIMEOUT_SECONDS,
    MAX_LIVE_RECORDING_SECONDS,
    RECORDINGS_OUTPUT_DIR,
)
from radio_cache.recording.models import RecordingJob

logger = logging.getLogger(__name__)

# ── Process registry (for cancellation) ─────────────────────────────────

_active_processes: dict[str, subprocess.Popen[str]] = {}
_processes_lock = threading.Lock()


def register_process(job_id: str, proc: subprocess.Popen[str]) -> None:
    """Store a running ffmpeg process so it can be terminated later.

    Args:
        job_id: Owning job identifier.
        proc: Running ffmpeg :class:`subprocess.Popen` instance.
    """
    with _processes_lock:
        _active_processes[job_id] = proc


def terminate_job(job_id: str) -> bool:
    """Terminate the ffmpeg process for *job_id* if it is still running.

    Args:
        job_id: Job whose process should be stopped.

    Returns:
        ``True`` if a process was found and termination was requested,
        ``False`` if no running process was registered for that job.
    """
    with _processes_lock:
        proc = _active_processes.pop(job_id, None)
    if proc is not None and proc.poll() is None:
        proc.terminate()
        logger.info("Terminated ffmpeg process for job %s", job_id)
        return True
    return False


def _deregister_process(job_id: str) -> None:
    """Remove a job's process entry without terminating it."""
    with _processes_lock:
        _active_processes.pop(job_id, None)


# ── Filename helpers ─────────────────────────────────────────────────────

_UNSAFE_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')


def safe_filename(name: str) -> str:
    """Replace filesystem-unsafe characters with underscores.

    Args:
        name: Raw text intended as a filename fragment.

    Returns:
        Sanitised string, at most 200 characters.
    """
    cleaned = _UNSAFE_CHARS.sub("_", name.strip())
    return cleaned[:200] or "recording"


def build_output_path(job: RecordingJob, title: str = "") -> Path:
    """Construct the output file path for *job*.

    The filename encodes a UTC timestamp plus the sanitised title so
    that successive recordings of the same content do not collide.

    Args:
        job: The recording job.
        title: Optional human-readable title for the filename; falls
            back to ``job.source_id`` when empty.

    Returns:
        Absolute :class:`~pathlib.Path` ready for writing.
    """
    out_dir = Path(RECORDINGS_OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    label = safe_filename(title or job.source_id)
    filename = f"{date_str}_{label}.{job.output_format}"
    return out_dir / filename


# ── Public recording entry point ─────────────────────────────────────────


def record_stream(
    job: RecordingJob,
    manifest_url: str,
    output_path: Path,
    title: str = "",
    station: str = "",
    programme: str = "",
    date: str = "",
    progress_cb: Callable[[int], None] | None = None,
) -> None:
    """Capture an HLS stream to a local audio file using ffmpeg.

    Duration is taken from ``job.duration_seconds``.  For live
    recordings the value is capped at :data:`~config.MAX_LIVE_RECORDING_SECONDS`.
    Catch-up (``job.duration_seconds is None``) records until the
    stream ends (EOF-driven).

    Metadata is embedded via ffmpeg ``-metadata`` flags; no separate
    post-processing step is needed.

    Retries up to :data:`~config.HTTP_RETRY_COUNT` times with
    exponential back-off on ffmpeg failures.

    Args:
        job: The owning recording job.
        manifest_url: HLS ``.m3u8`` URL to record from.
        output_path: Destination file path.
        title: Programme title for embedded metadata.
        station: Station name written to the ``artist`` tag.
        programme: Series/brand name written to the ``album`` tag.
        date: Broadcast date written to the ``date`` tag.
        progress_cb: Optional callable invoked with elapsed seconds
            as ffmpeg progresses.  Useful for polling job progress.

    Raises:
        ValueError: Requested duration exceeds the configured cap.
        FileNotFoundError: ffmpeg binary not found.
        RuntimeError: ffmpeg exited non-zero after all retries.
    """
    ffmpeg_exe = _check_ffmpeg(FFMPEG_PATH)

    duration = job.duration_seconds
    if job.source_type == "live" and duration is not None:
        if duration > MAX_LIVE_RECORDING_SECONDS:
            raise ValueError(
                f"Requested duration {duration}s exceeds the maximum "
                f"{MAX_LIVE_RECORDING_SECONDS}s allowed for live recordings."
            )

    cmd = _build_ffmpeg_command(
        ffmpeg_exe=ffmpeg_exe,
        manifest_url=manifest_url,
        output_path=output_path,
        duration=duration,
        output_format=job.output_format,
        title=title,
        station=station,
        programme=programme,
        date=date,
    )

    logger.info(
        "Recording job %s: starting ffmpeg -> %s", job.job_id, output_path
    )

    last_exc: Exception | None = None
    for attempt in range(1, HTTP_RETRY_COUNT + 1):
        try:
            _run_ffmpeg(cmd, job.job_id, progress_cb)
            logger.info(
                "Recording job %s: completed -> %s", job.job_id, output_path
            )
            return
        except subprocess.CalledProcessError as exc:
            last_exc = exc
            if attempt < HTTP_RETRY_COUNT:
                wait = 2**attempt
                logger.warning(
                    "Recording job %s: ffmpeg attempt %d/%d failed, "
                    "retrying in %ds",
                    job.job_id,
                    attempt,
                    HTTP_RETRY_COUNT,
                    wait,
                )
                time.sleep(wait)
            else:
                logger.error(
                    "Recording job %s: ffmpeg failed after %d attempts",
                    job.job_id,
                    HTTP_RETRY_COUNT,
                )

    raise RuntimeError(
        f"ffmpeg failed after {HTTP_RETRY_COUNT} attempts"
    ) from last_exc


# ── Private helpers ──────────────────────────────────────────────────────


def _check_ffmpeg(ffmpeg_path: str) -> str:
    """Resolve the ffmpeg executable path.

    Args:
        ffmpeg_path: Configured path or bare command name.

    Returns:
        Absolute path string.

    Raises:
        FileNotFoundError: ffmpeg is not on PATH or at the given path.
    """
    resolved = shutil.which(ffmpeg_path)
    if resolved is None:
        raise FileNotFoundError(
            f"ffmpeg not found at '{ffmpeg_path}'.  Install ffmpeg and "
            "ensure it is on PATH, or set the FFMPEG_PATH environment variable."
        )
    return resolved


def _build_ffmpeg_command(
    ffmpeg_exe: str,
    manifest_url: str,
    output_path: Path,
    duration: int | None,
    output_format: str,
    title: str,
    station: str,
    programme: str,
    date: str,
) -> list[str]:
    """Assemble the ffmpeg command list.

    ``-reconnect`` flags tell ffmpeg to re-open the HTTP connection
    automatically on transient drops, which is common during long HLS
    captures.  ``-timeout`` is given in microseconds as required by the
    ffmpeg HTTP protocol handler.

    Args:
        ffmpeg_exe: Absolute path to ffmpeg.
        manifest_url: Source HLS URL.
        output_path: Destination file.
        duration: Capture duration in seconds, or ``None`` for full stream.
        output_format: ``"m4a"`` or ``"mp3"``.
        title: ``title`` metadata tag value.
        station: ``artist`` metadata tag value.
        programme: ``album`` metadata tag value.
        date: ``date`` metadata tag value.

    Returns:
        List of command-line arguments ready for :class:`subprocess.Popen`.
    """
    cmd: list[str] = [
        ffmpeg_exe,
        "-reconnect", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "5",
        "-timeout", str(HTTP_TIMEOUT_SECONDS * 1_000_000),
        "-i", manifest_url,
    ]

    if duration is not None:
        cmd += ["-t", str(duration)]

    if output_format == "mp3":
        cmd += ["-vn", "-c:a", "libmp3lame", "-q:a", "2"]
    else:
        # Copy AAC audio stream directly into an MPEG-4 container.
        # -movflags +faststart writes the moov atom early so partial
        # files are playable.
        cmd += ["-vn", "-c:a", "copy", "-movflags", "+faststart"]

    if title:
        cmd += ["-metadata", f"title={title}"]
    if station:
        cmd += ["-metadata", f"artist={station}"]
    if programme:
        cmd += ["-metadata", f"album={programme}"]
    if date:
        cmd += ["-metadata", f"date={date}"]

    cmd += ["-y", str(output_path)]
    return cmd


_TIME_RE = re.compile(r"time=(\d+):(\d+):(\d+(?:\.\d+)?)")


def _run_ffmpeg(
    cmd: list[str],
    job_id: str,
    progress_cb: Callable[[int], None] | None,
) -> None:
    """Run ffmpeg and parse progress from stderr.

    The process is registered in :data:`_active_processes` so that
    :func:`terminate_job` can stop it.

    Args:
        cmd: Full ffmpeg command list.
        job_id: Owning job identifier (used for logging and process registry).
        progress_cb: Called with elapsed integer seconds when ffmpeg
            outputs a ``time=HH:MM:SS`` progress line.

    Raises:
        subprocess.CalledProcessError: ffmpeg exited with non-zero status.
    """
    proc: subprocess.Popen[str] = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    register_process(job_id, proc)
    try:
        assert proc.stderr is not None
        for line in proc.stderr:
            m = _TIME_RE.search(line)
            if m and progress_cb:
                h = int(m.group(1))
                mn = int(m.group(2))
                s = float(m.group(3))
                progress_cb(int(h * 3600 + mn * 60 + s))
        proc.wait()
    finally:
        _deregister_process(job_id)

    if proc.returncode != 0:
        raise subprocess.CalledProcessError(proc.returncode, cmd)
