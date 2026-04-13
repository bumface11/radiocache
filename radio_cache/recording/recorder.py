"""FFmpeg-based audio stream recorder.

Handles:
* Building ffmpeg command lines for HLS capture with inline metadata.
* Running ffmpeg as a subprocess with stdout/stderr capture.
* Parsing ffmpeg progress output to report elapsed seconds.
* Exponential-backoff retry on transient subprocess failures.
* A module-level process registry so that running jobs can be
  terminated on demand (e.g. from the DELETE /api/recordings endpoint).
* Embedding rich metadata tags (get-iplayer style) including cover art.
"""

from __future__ import annotations

import logging
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.request
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
    *,
    genre: str = "",
    synopsis: str = "",
    track: int = 0,
    episode_id: str = "",
    url: str = "",
    thumbnail_url: str = "",
) -> None:
    """Capture an HLS stream to a local audio file using ffmpeg.

    Duration is taken from ``job.duration_seconds``.  For live
    recordings the value is capped at :data:`~config.MAX_LIVE_RECORDING_SECONDS`.
    Catch-up (``job.duration_seconds is None``) records until the
    stream ends (EOF-driven).

    Metadata is embedded via ffmpeg ``-metadata`` flags in a style
    similar to get-iplayer: title, artist, album_artist, album, genre,
    date, track, comment/description, show, episode_id, network,
    copyright, and encoder.  If *thumbnail_url* is provided the image
    is downloaded and embedded as cover art.

    Retries up to :data:`~config.HTTP_RETRY_COUNT` times with
    exponential back-off on ffmpeg failures.

    Args:
        job: The owning recording job.
        manifest_url: HLS ``.m3u8`` URL to record from.
        output_path: Destination file path.
        title: Programme title for embedded metadata.
        station: Station name written to ``artist``/``album_artist``.
        programme: Series/brand name written to ``album``/``show``.
        date: Broadcast date written to the ``date`` tag.
        progress_cb: Optional callable invoked with elapsed seconds
            as ffmpeg progresses.  Useful for polling job progress.
        genre: Category/genre tag (e.g. "Drama").
        synopsis: Programme description for ``comment``/``description``.
        track: Episode number for the ``track`` tag.
        episode_id: BBC PID written to the ``episode_id`` tag.
        url: BBC Sounds URL written to ``comment`` alongside synopsis.
        thumbnail_url: URL of cover art to download and embed.

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

    # Download cover art to a temporary file (best-effort).
    artwork_path = _download_thumbnail(thumbnail_url) if thumbnail_url else None

    try:
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
            genre=genre,
            synopsis=synopsis,
            track=track,
            episode_id=episode_id,
            url=url,
            artwork_path=artwork_path,
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
    finally:
        if artwork_path:
            try:
                Path(artwork_path).unlink(missing_ok=True)
            except OSError:
                pass


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


def _download_thumbnail(url: str) -> str | None:
    """Download a thumbnail image to a temporary file.

    Returns the path to the temp file, or ``None`` on failure.
    The caller is responsible for deleting the file.
    """
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
            data = resp.read()
        suffix = ".jpg" if b"\xff\xd8" in data[:4] else ".png"
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tmp.write(data)
        tmp.close()
        logger.info("Downloaded thumbnail (%d bytes) -> %s", len(data), tmp.name)
        return tmp.name
    except Exception:
        logger.warning("Failed to download thumbnail from %s", url)
        return None


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
    *,
    genre: str = "",
    synopsis: str = "",
    track: int = 0,
    episode_id: str = "",
    url: str = "",
    artwork_path: str | None = None,
) -> list[str]:
    """Assemble the ffmpeg command list.

    ``-reconnect`` flags tell ffmpeg to re-open the HTTP connection
    automatically on transient drops, which is common during long HLS
    captures.  ``-timeout`` is given in microseconds as required by the
    ffmpeg HTTP protocol handler.

    Embeds rich metadata in a style similar to get-iplayer:
    title, artist, album_artist, album, genre, date, track,
    comment, description, show, episode_id, network, copyright,
    and encoder.  If *artwork_path* is set, the image is embedded
    as cover art.

    Args:
        ffmpeg_exe: Absolute path to ffmpeg.
        manifest_url: Source HLS URL.
        output_path: Destination file.
        duration: Capture duration in seconds, or ``None`` for full stream.
        output_format: ``"m4a"`` or ``"mp3"``.
        title: ``title`` metadata tag value.
        station: ``artist`` / ``album_artist`` / ``network`` tag value.
        programme: ``album`` / ``show`` metadata tag value.
        date: ``date`` metadata tag value.
        genre: Category/genre tag value.
        synopsis: Written to ``description`` and ``comment`` tags.
        track: Episode number for the ``track`` tag.
        episode_id: BBC PID for the ``episode_id`` tag.
        url: BBC Sounds URL, appended to the comment tag.
        artwork_path: Local path to cover art image, or ``None``.

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

    # Optional cover-art input.
    has_artwork = artwork_path is not None
    if has_artwork:
        cmd += ["-i", artwork_path]

    if duration is not None:
        cmd += ["-t", str(duration)]

    if output_format == "mp3":
        cmd += ["-map", "0:a"]
        if has_artwork:
            cmd += ["-map", "1:v"]
        cmd += ["-c:a", "libmp3lame", "-q:a", "2"]
        if has_artwork:
            cmd += ["-c:v", "copy", "-id3v2_version", "3"]
    else:
        # Copy AAC audio stream directly into an MPEG-4 container.
        # -movflags +faststart writes the moov atom early so partial
        # files are playable.
        cmd += ["-map", "0:a"]
        if has_artwork:
            cmd += ["-map", "1:v"]
        cmd += ["-c:a", "copy", "-movflags", "+faststart"]
        if has_artwork:
            cmd += ["-c:v", "mjpeg"]

    if has_artwork:
        cmd += ["-disposition:v:0", "attached_pic"]

    # ── Metadata tags (get-iplayer style) ────────────────────────────────
    if title:
        cmd += ["-metadata", f"title={title}"]
    if station:
        cmd += ["-metadata", f"artist={station}"]
        cmd += ["-metadata", f"album_artist={station}"]
    if programme:
        cmd += ["-metadata", f"album={programme}"]
        cmd += ["-metadata", f"show={programme}"]
    if date:
        cmd += ["-metadata", f"date={date}"]
    if genre:
        cmd += ["-metadata", f"genre={genre}"]
    if synopsis:
        cmd += ["-metadata", f"description={synopsis}"]
    # Build a comment combining synopsis and URL (like get-iplayer).
    comment_parts = [p for p in (synopsis, url) if p]
    if comment_parts:
        cmd += ["-metadata", f"comment={chr(10).join(comment_parts)}"]
    if track > 0:
        cmd += ["-metadata", f"track={track}"]
    if episode_id:
        cmd += ["-metadata", f"episode_id={episode_id}"]
    if station:
        cmd += ["-metadata", f"network={station}"]
    cmd += ["-metadata", "copyright=BBC"]
    cmd += ["-metadata", "encoder=RadioCache"]

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
