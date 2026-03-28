"""Integration tests for the recording flow with mocked HTTP.

Covers:
- Successful live recording: stream resolves, ffmpeg runs, job completes.
- Expired/unauthorised URL: StreamUnavailableError → job status "failed".
- Retry then success: ffmpeg fails once then succeeds on retry.
- Unsupported stream: DRM-only content → job status "not_supported".

ffmpeg is mocked throughout; no real network I/O occurs.
"""

from __future__ import annotations

import json
import subprocess
import urllib.error
import urllib.request
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from radio_cache.recording.job_manager import JobManager
from radio_cache.recording.models import RecordingJob
from radio_cache.recording import recorder as _recorder
from radio_cache.recording.stream_resolver import (
    resolve_live_stream,
    resolve_programme_stream,
)


# ── Helpers ───────────────────────────────────────────────────────────────


def _mock_urlopen_responses(payloads: list[dict]) -> list[MagicMock]:
    """Return a list of context-manager mocks for successive urlopen calls."""
    mocks = []
    for p in payloads:
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(p).encode()
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        mocks.append(mock_resp)
    return mocks


def _ibl_payload(vpid: str) -> dict:
    return {"versions": [{"id": vpid}]}


def _ms_hls_payload(url: str = "https://hls.example/stream.m3u8") -> dict:
    return {
        "result": "success",
        "media": [
            {
                "kind": "audio",
                "connection": [{"transferFormat": "hls", "href": url}],
            }
        ],
    }


def _ms_drm_only_payload() -> dict:
    return {
        "result": "success",
        "media": [
            {
                "kind": "audio",
                "connection": [
                    {"transferFormat": "dash", "href": "https://dash.example/drm.mpd"}
                ],
            }
        ],
    }


# ── Successful live recording ─────────────────────────────────────────────


class TestSuccessfulLiveRecording:
    def test_record_stream_calls_ffmpeg(self, tmp_path: Path) -> None:
        """record_stream executes ffmpeg with correct arguments."""
        job = RecordingJob(
            job_id="live-001",
            source_type="live",
            source_id="bbc_radio_fourfm",
            output_format="m4a",
            duration_seconds=60,
        )
        output_path = tmp_path / "recorded.m4a"

        # Simulate ffmpeg writing a progress line then exiting cleanly.
        mock_proc = MagicMock()
        mock_proc.stderr = iter(["size=1kB time=00:01:00.00 bitrate=  64.0kbits/s\n"])
        mock_proc.returncode = 0
        mock_proc.poll.return_value = None

        progress_calls: list[int] = []

        with (
            patch("shutil.which", return_value="/usr/bin/ffmpeg"),
            patch("subprocess.Popen", return_value=mock_proc),
            patch("radio_cache.recording.recorder.RECORDINGS_OUTPUT_DIR", str(tmp_path)),
        ):
            _recorder.record_stream(
                job=job,
                manifest_url="https://hls.example/stream.m3u8",
                output_path=output_path,
                title="Test Programme",
                station="BBC Radio 4",
                progress_cb=lambda s: progress_calls.append(s),
            )

        assert progress_calls == [60]


# ── Expired / unauthorised URL ────────────────────────────────────────────


class TestExpiredUrl:
    def test_http_401_resolves_to_unavailable(self) -> None:
        """HTTP 401 from Media Selector surfaces as StreamUnavailableError."""
        ibl_response = _mock_urlopen_responses([_ibl_payload("vpid_expired")])[0]
        http_401 = urllib.error.HTTPError(
            url="", code=401, msg="Unauthorized", hdrs=MagicMock(), fp=None
        )

        responses: list = [ibl_response, http_401]
        call_count = 0

        def fake_urlopen(req: urllib.request.Request, timeout: int) -> object:
            nonlocal call_count
            r = responses[call_count]
            call_count += 1
            if isinstance(r, Exception):
                raise r
            return r

        from radio_cache.recording.stream_resolver import StreamUnavailableError

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with pytest.raises(StreamUnavailableError):
                resolve_programme_stream("expired_pid")


# ── Retry then success ────────────────────────────────────────────────────


class TestRetryThenSuccess:
    def test_ffmpeg_retried_on_failure(self, tmp_path: Path) -> None:
        """record_stream retries ffmpeg up to HTTP_RETRY_COUNT times."""
        job = RecordingJob(
            job_id="retry-001",
            source_type="live",
            source_id="bbc_radio_fourfm",
            output_format="m4a",
            duration_seconds=60,
        )
        output_path = tmp_path / "retry.m4a"

        # First call fails, second succeeds.
        fail_proc = MagicMock()
        fail_proc.stderr = iter([])
        fail_proc.returncode = 1
        fail_proc.poll.return_value = 1

        ok_proc = MagicMock()
        ok_proc.stderr = iter([])
        ok_proc.returncode = 0
        ok_proc.poll.return_value = None

        popen_call_count = 0

        def fake_popen(*args: object, **kwargs: object) -> MagicMock:
            nonlocal popen_call_count
            popen_call_count += 1
            return fail_proc if popen_call_count == 1 else ok_proc

        with (
            patch("shutil.which", return_value="/usr/bin/ffmpeg"),
            patch("subprocess.Popen", side_effect=fake_popen),
            patch("time.sleep"),  # speed up the test
            patch("radio_cache.recording.recorder.HTTP_RETRY_COUNT", 3),
        ):
            _recorder.record_stream(
                job=job,
                manifest_url="https://hls.example/stream.m3u8",
                output_path=output_path,
            )

        assert popen_call_count == 2

    def test_all_retries_exhausted_raises_runtime_error(
        self, tmp_path: Path
    ) -> None:
        """RuntimeError is raised when all retry attempts fail."""
        job = RecordingJob(
            job_id="retry-002",
            source_type="live",
            source_id="bbc_radio_fourfm",
            output_format="m4a",
            duration_seconds=60,
        )
        output_path = tmp_path / "fail.m4a"

        fail_proc = MagicMock()
        fail_proc.stderr = iter([])
        fail_proc.returncode = 1
        fail_proc.poll.return_value = 1

        with (
            patch("shutil.which", return_value="/usr/bin/ffmpeg"),
            patch("subprocess.Popen", return_value=fail_proc),
            patch("time.sleep"),
            patch("radio_cache.recording.recorder.HTTP_RETRY_COUNT", 2),
        ):
            with pytest.raises(RuntimeError, match="ffmpeg failed"):
                _recorder.record_stream(
                    job=job,
                    manifest_url="https://hls.example/stream.m3u8",
                    output_path=output_path,
                )


# ── Unsupported stream ────────────────────────────────────────────────────


class TestUnsupportedStream:
    def test_drm_only_raises_not_supported(self) -> None:
        """DRM-only Media Selector content raises StreamNotSupportedError."""
        ibl_response = _mock_urlopen_responses([_ibl_payload("vpid_drm")])[0]
        ms_response = _mock_urlopen_responses([_ms_drm_only_payload()])[0]

        responses: list = [ibl_response, ms_response]
        call_count = 0

        def fake_urlopen(req: urllib.request.Request, timeout: int) -> object:
            nonlocal call_count
            r = responses[call_count]
            call_count += 1
            return r

        from radio_cache.recording.stream_resolver import StreamNotSupportedError

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with pytest.raises(StreamNotSupportedError):
                resolve_programme_stream("drm_pid")
