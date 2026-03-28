"""Unit tests for stream_resolver.

Covers:
- Live stream resolution for known / unknown stations.
- Variant selection from Media Selector JSON.
- Version-PID extraction from iBL API JSON.
- HTTP error mapping to the correct exception type.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from radio_cache.recording.stream_resolver import (
    KNOWN_STATIONS,
    StreamNotSupportedError,
    StreamUnavailableError,
    _fetch_version_pids,
    _select_hls_from_media_selector,
    resolve_live_stream,
    resolve_programme_stream,
)


# ── Live stream resolution ────────────────────────────────────────────────


class TestResolveLiveStream:
    def test_known_station_returns_hls_url(self) -> None:
        """Known station ID resolves to the expected Akamai HLS URL."""
        result = resolve_live_stream("bbc_radio_fourfm")
        assert result.transfer_format == "hls"
        assert result.is_live is True
        assert "bbc_radio_fourfm" in result.manifest_url
        assert result.manifest_url.endswith(".m3u8")

    def test_known_station_case_insensitive(self) -> None:
        """Station ID matching is case-insensitive."""
        result = resolve_live_stream("BBC_Radio_FourFM")
        assert "bbc_radio_fourfm" in result.manifest_url

    def test_unknown_station_raises_not_supported(self) -> None:
        """An unrecognised station ID raises StreamNotSupportedError."""
        with pytest.raises(StreamNotSupportedError, match="not in the supported"):
            resolve_live_stream("some_unknown_station")

    def test_all_known_stations_resolve(self) -> None:
        """Every station in KNOWN_STATIONS resolves without error."""
        for station in KNOWN_STATIONS:
            result = resolve_live_stream(station)
            assert result.manifest_url.startswith("https://")


# ── Version PID extraction ────────────────────────────────────────────────


def _mock_urlopen(payload: dict | None):
    """Return a context-manager mock that yields a fake HTTP response."""
    if payload is None:
        raise urllib.error.URLError("network error")
    body = json.dumps(payload).encode()
    mock_resp = MagicMock()
    mock_resp.read.return_value = body
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    return mock_resp


class TestFetchVersionPids:
    def test_ibl_episode_nested_versions(self) -> None:
        """Version PIDs extracted from episode.versions on iBL base endpoint."""
        payload = {"episode": {"versions": [{"id": "v001"}, {"id": "v002"}]}}
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            pids = _fetch_version_pids("e001")
        assert pids == ["v001", "v002"]

    def test_ibl_top_level_versions_key(self) -> None:
        """Version PIDs extracted from a top-level 'versions' list as fallback."""
        payload = {"versions": [{"id": "v003"}]}
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            pids = _fetch_version_pids("e001")
        assert pids == ["v003"]

    def test_404_falls_through_to_programmes_api(self) -> None:
        """HTTP 404 on the iBL endpoint causes fallback to BBC Programmes API."""
        http_404 = urllib.error.HTTPError(
            url="", code=404, msg="Not Found", hdrs=MagicMock(), fp=None
        )
        # Second call returns the BBC Programmes JSON format.
        programmes_payload = {
            "programme": {"versions": [{"pid": "vpid_fallback"}]}
        }
        call_n = 0

        def fake_urlopen(req: object, timeout: int) -> object:
            nonlocal call_n
            call_n += 1
            if call_n == 1:
                raise http_404
            return _mock_urlopen(programmes_payload)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            pids = _fetch_version_pids("e001")
        assert pids == ["vpid_fallback"]
        assert call_n == 2  # both endpoints were tried

    def test_bbc_programmes_json_format(self) -> None:
        """BBC Programmes JSON 'programme.versions[].pid' format is parsed."""
        # Simulate iBL returning empty, programmes API returning data.
        programmes_payload = {
            "programme": {"versions": [{"pid": "vpid_prog"}]}
        }
        call_n = 0

        def fake_urlopen(req: object, timeout: int) -> object:
            nonlocal call_n
            call_n += 1
            if call_n == 1:
                return _mock_urlopen({})  # iBL returns empty
            return _mock_urlopen(programmes_payload)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            pids = _fetch_version_pids("e001")
        assert pids == ["vpid_prog"]

    def test_401_raises_unavailable_immediately(self) -> None:
        """HTTP 401 from iBL endpoint raises StreamUnavailableError without fallback."""
        http_401 = urllib.error.HTTPError(
            url="", code=401, msg="Unauthorized", hdrs=MagicMock(), fp=None
        )
        with patch("urllib.request.urlopen", side_effect=http_401):
            with pytest.raises(StreamUnavailableError, match="401"):
                _fetch_version_pids("e001")

    def test_network_failure_returns_empty(self) -> None:
        """Network error on both endpoints returns empty list."""
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.URLError("timeout"),
        ):
            pids = _fetch_version_pids("e001")
        assert pids == []

    def test_items_without_id_are_skipped(self) -> None:
        """Version entries without an 'id' field are silently skipped."""
        payload = {"episode": {"versions": [{"id": "v001"}, {"kind": "audio"}, {"id": "v002"}]}}
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            pids = _fetch_version_pids("e001")
        assert pids == ["v001", "v002"]


# ── Media Selector variant selection ─────────────────────────────────────


def _ms_payload(connections: list[dict]) -> dict:
    """Build a minimal Media Selector response containing *connections*."""
    return {
        "result": "success",
        "media": [
            {
                "kind": "audio",
                "encoding": "aac",
                "connection": connections,
            }
        ],
    }


class TestSelectHlsFromMediaSelector:
    def test_returns_first_hls_href(self) -> None:
        """The first HLS connection href is returned."""
        payload = _ms_payload([
            {"transferFormat": "dash", "href": "https://dash.example/stream.mpd"},
            {"transferFormat": "hls", "href": "https://hls.example/stream.m3u8"},
        ])
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            url = _select_hls_from_media_selector("v001")
        assert url == "https://hls.example/stream.m3u8"

    def test_no_hls_raises_not_supported(self) -> None:
        """A response with only DASH connections raises StreamNotSupportedError."""
        payload = _ms_payload([
            {"transferFormat": "dash", "href": "https://dash.example/stream.mpd"},
        ])
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            with pytest.raises(StreamNotSupportedError):
                _select_hls_from_media_selector("v001")

    def test_error_result_raises_unavailable(self) -> None:
        """A non-success result field raises StreamUnavailableError."""
        payload = {"result": "geolocation"}
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            with pytest.raises(StreamUnavailableError, match="geolocation"):
                _select_hls_from_media_selector("v001")

    def test_http_401_raises_unavailable(self) -> None:
        """HTTP 401 from Media Selector raises StreamUnavailableError."""
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(
                url="", code=401, msg="Unauthorized", hdrs=MagicMock(), fp=None
            ),
        ):
            with pytest.raises(StreamUnavailableError, match="401"):
                _select_hls_from_media_selector("v001")

    def test_http_403_raises_unavailable(self) -> None:
        """HTTP 403 from Media Selector raises StreamUnavailableError."""
        with patch(
            "urllib.request.urlopen",
            side_effect=urllib.error.HTTPError(
                url="", code=403, msg="Forbidden", hdrs=MagicMock(), fp=None
            ),
        ):
            with pytest.raises(StreamUnavailableError, match="403"):
                _select_hls_from_media_selector("v001")

    def test_captions_kind_is_skipped(self) -> None:
        """Media entries with kind='captions' are ignored."""
        payload = {
            "result": "success",
            "media": [
                {
                    "kind": "captions",
                    "connection": [
                        {"transferFormat": "hls", "href": "https://captions.example/c.m3u8"}
                    ],
                },
                {
                    "kind": "audio",
                    "connection": [
                        {"transferFormat": "hls", "href": "https://audio.example/a.m3u8"}
                    ],
                },
            ],
        }
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(payload)):
            url = _select_hls_from_media_selector("v001")
        assert url == "https://audio.example/a.m3u8"


# ── resolve_programme_stream integration ─────────────────────────────────


class TestResolveProgrammeStream:
    def test_success_returns_resolved_stream(self) -> None:
        """A programme with a valid version and HLS connection resolves."""
        ibl_payload = {"versions": [{"id": "vpid001"}]}
        ms_payload = _ms_payload([
            {"transferFormat": "hls", "href": "https://hls.example/prog.m3u8"}
        ])

        responses = [ibl_payload, ms_payload]
        call_count = 0

        def fake_urlopen(req: urllib.request.Request, timeout: int) -> object:
            nonlocal call_count
            payload = responses[call_count]
            call_count += 1
            return _mock_urlopen(payload)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            result = resolve_programme_stream("e001")

        assert result.manifest_url == "https://hls.example/prog.m3u8"
        assert result.is_live is False
        assert result.transfer_format == "hls"

    def test_no_versions_raises_unavailable(self) -> None:
        """Empty versions list raises StreamUnavailableError."""
        ibl_payload: dict = {"versions": []}
        with patch("urllib.request.urlopen", return_value=_mock_urlopen(ibl_payload)):
            with pytest.raises(StreamUnavailableError):
                resolve_programme_stream("e_gone")

    def test_all_versions_drm_raises_not_supported(self) -> None:
        """All DRM versions cause StreamNotSupportedError."""
        ibl_payload = {"versions": [{"id": "vpid001"}]}
        ms_payload = _ms_payload([
            {"transferFormat": "dash", "href": "https://dash.example/drm.mpd"}
        ])

        responses = [ibl_payload, ms_payload]
        call_count = 0

        def fake_urlopen(req: urllib.request.Request, timeout: int) -> object:
            nonlocal call_count
            payload = responses[call_count]
            call_count += 1
            return _mock_urlopen(payload)

        with patch("urllib.request.urlopen", side_effect=fake_urlopen):
            with pytest.raises(StreamNotSupportedError):
                resolve_programme_stream("drm_programme")
