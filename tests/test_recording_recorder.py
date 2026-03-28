"""Unit tests for recorder helpers.

Covers:
- safe_filename sanitisation.
- build_output_path filename construction.
- _build_ffmpeg_command flag composition for m4a and mp3.
- Metadata flags appear when values are provided.
- Duration flag present/absent as expected.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from radio_cache.recording.recorder import (
    _build_ffmpeg_command,
    build_output_path,
    safe_filename,
)
from radio_cache.recording.models import RecordingJob


# ── safe_filename ─────────────────────────────────────────────────────────


class TestSafeFilename:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("normal title", "normal title"),
            ('path/to/"file"', "path_to__file_"),
            ("file<>name", "file__name"),
            ("  spaces  ", "spaces"),
            ("", "recording"),
            ("a" * 300, "a" * 200),
            ("tab\there", "tab_here"),
        ],
    )
    def test_sanitises_correctly(self, raw: str, expected: str) -> None:
        assert safe_filename(raw) == expected


# ── build_output_path ─────────────────────────────────────────────────────


class TestBuildOutputPath:
    def test_path_uses_output_dir(self, tmp_path: Path) -> None:
        job = _make_job("m4a")
        with patch("radio_cache.recording.recorder.RECORDINGS_OUTPUT_DIR", str(tmp_path)):
            path = build_output_path(job, title="My Programme")
        assert path.parent == tmp_path

    def test_path_uses_output_format_extension(self, tmp_path: Path) -> None:
        job = _make_job("mp3")
        with patch("radio_cache.recording.recorder.RECORDINGS_OUTPUT_DIR", str(tmp_path)):
            path = build_output_path(job, title="Radio Show")
        assert path.suffix == ".mp3"

    def test_filename_contains_sanitised_title(self, tmp_path: Path) -> None:
        job = _make_job("m4a")
        with patch("radio_cache.recording.recorder.RECORDINGS_OUTPUT_DIR", str(tmp_path)):
            path = build_output_path(job, title="drama: episode 1")
        assert "drama" in path.name

    def test_falls_back_to_source_id(self, tmp_path: Path) -> None:
        job = _make_job("m4a")
        with patch("radio_cache.recording.recorder.RECORDINGS_OUTPUT_DIR", str(tmp_path)):
            path = build_output_path(job, title="")
        assert "bbc_radio_fourfm" in path.name

    def test_output_dir_created_if_missing(self, tmp_path: Path) -> None:
        out = tmp_path / "new_subdir"
        job = _make_job("m4a")
        with patch("radio_cache.recording.recorder.RECORDINGS_OUTPUT_DIR", str(out)):
            build_output_path(job)
        assert out.is_dir()


# ── _build_ffmpeg_command ────────────────────────────────────────────────


class TestBuildFfmpegCommand:
    def test_m4a_uses_copy_codec(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=1800,
            output_format="m4a",
            title="",
            station="",
            programme="",
            date="",
        )
        assert "-c:a" in cmd
        idx = cmd.index("-c:a")
        assert cmd[idx + 1] == "copy"
        assert "libmp3lame" not in cmd

    def test_mp3_uses_lame_codec(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.mp3"),
            duration=None,
            output_format="mp3",
            title="",
            station="",
            programme="",
            date="",
        )
        assert "libmp3lame" in cmd

    def test_duration_flag_included_when_set(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=600,
            output_format="m4a",
            title="",
            station="",
            programme="",
            date="",
        )
        assert "-t" in cmd
        assert cmd[cmd.index("-t") + 1] == "600"

    def test_duration_flag_absent_when_none(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=None,
            output_format="m4a",
            title="",
            station="",
            programme="",
            date="",
        )
        assert "-t" not in cmd

    def test_metadata_title_included(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=None,
            output_format="m4a",
            title="My Drama",
            station="BBC Radio 4",
            programme="Afternoon Drama",
            date="2026-03-28",
        )
        assert "title=My Drama" in cmd
        assert "artist=BBC Radio 4" in cmd
        assert "album=Afternoon Drama" in cmd
        assert "date=2026-03-28" in cmd

    def test_empty_metadata_not_included(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=None,
            output_format="m4a",
            title="",
            station="",
            programme="",
            date="",
        )
        # No -metadata flags should be present
        assert "-metadata" not in cmd

    def test_reconnect_flags_present(self) -> None:
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=None,
            output_format="m4a",
            title="",
            station="",
            programme="",
            date="",
        )
        assert "-reconnect" in cmd
        assert "-reconnect_streamed" in cmd

    def test_output_path_is_last_positional_arg(self, tmp_path: Path) -> None:
        path = tmp_path / "out.m4a"
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=path,
            duration=None,
            output_format="m4a",
            title="",
            station="",
            programme="",
            date="",
        )
        assert cmd[-1] == str(path)


# ── Helpers ───────────────────────────────────────────────────────────────


def _make_job(fmt: str = "m4a") -> RecordingJob:
    return RecordingJob(
        job_id="test-job-id",
        source_type="live",
        source_id="bbc_radio_fourfm",
        output_format=fmt,  # type: ignore[arg-type]
        duration_seconds=1800,
    )
