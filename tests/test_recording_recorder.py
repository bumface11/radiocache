"""Unit tests for recorder helpers.

Covers:
- safe_filename sanitisation.
- build_output_path filename construction.
- _build_ffmpeg_command flag composition for m4a and mp3.
- Rich metadata flags (get-iplayer style) appear when values are provided.
- Cover art embedding when artwork_path is set.
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
            genre="Drama",
            synopsis="A gripping tale.",
            track=3,
            episode_id="b09xyz12",
            url="https://www.bbc.co.uk/sounds/play/b09xyz12",
        )
        assert "title=My Drama" in cmd
        assert "artist=BBC Radio 4" in cmd
        assert "album_artist=BBC Radio 4" in cmd
        assert "album=Afternoon Drama" in cmd
        assert "show=Afternoon Drama" in cmd
        assert "date=2026-03-28" in cmd
        assert "genre=Drama" in cmd
        assert "description=A gripping tale." in cmd
        assert "track=3" in cmd
        assert "episode_id=b09xyz12" in cmd
        assert "network=BBC Radio 4" in cmd
        assert "copyright=BBC" in cmd
        assert "encoder=RadioCache" in cmd
        # comment = synopsis + url joined by newline
        comment_args = [a for a in cmd if a.startswith("comment=")]
        assert len(comment_args) == 1
        assert "A gripping tale." in comment_args[0]
        assert "bbc.co.uk/sounds" in comment_args[0]

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
        # Dynamic tags should be absent
        metadata_values = [a for a in cmd if "=" in a and cmd[cmd.index(a) - 1] == "-metadata"]
        # Only copyright and encoder are always present
        assert "copyright=BBC" in cmd
        assert "encoder=RadioCache" in cmd
        # No title / artist / genre etc.
        assert not any(a.startswith("title=") for a in cmd)
        assert not any(a.startswith("artist=") for a in cmd)
        assert not any(a.startswith("genre=") for a in cmd)

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

    def test_artwork_embedded_m4a(self, tmp_path: Path) -> None:
        art = tmp_path / "cover.jpg"
        art.write_bytes(b"\xff\xd8fake-jpeg")
        cmd = _build_ffmpeg_command(
            ffmpeg_exe="ffmpeg",
            manifest_url="https://hls.example/stream.m3u8",
            output_path=Path("/out/file.m4a"),
            duration=None,
            output_format="m4a",
            title="Show",
            station="",
            programme="",
            date="",
            artwork_path=str(art),
        )
        # Artwork as second input
        inputs = [cmd[i + 1] for i, v in enumerate(cmd) if v == "-i"]
        assert len(inputs) == 2
        assert inputs[1] == str(art)
        # Mapped and set as attached pic
        assert "-disposition:v:0" in cmd
        assert "attached_pic" in cmd
        assert "-c:v" in cmd
        assert cmd[cmd.index("-c:v") + 1] == "mjpeg"

    def test_artwork_embedded_mp3(self, tmp_path: Path) -> None:
        art = tmp_path / "cover.jpg"
        art.write_bytes(b"\xff\xd8fake-jpeg")
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
            artwork_path=str(art),
        )
        assert "-id3v2_version" in cmd
        assert "3" in cmd
        assert "-disposition:v:0" in cmd

    def test_no_artwork_no_video_mapping(self) -> None:
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
        # Only one -i (the manifest), no disposition
        inputs = [cmd[i + 1] for i, v in enumerate(cmd) if v == "-i"]
        assert len(inputs) == 1
        assert "-disposition:v:0" not in cmd


# ── Helpers ───────────────────────────────────────────────────────────────


def _make_job(fmt: str = "m4a") -> RecordingJob:
    return RecordingJob(
        job_id="test-job-id",
        source_type="live",
        source_id="bbc_radio_fourfm",
        output_format=fmt,  # type: ignore[arg-type]
        duration_seconds=1800,
    )
