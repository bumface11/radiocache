"""Tests for the programme_link template filter."""

from __future__ import annotations

from radio_cache.models import Programme
from radio_cache_api import is_expired, programme_link


def _prog(available_until: str) -> Programme:
    return Programme(
        pid="m002ww40",
        title="Birds",
        synopsis="",
        available_until=available_until,
        url="https://www.bbc.co.uk/sounds/play/m002ww40",
    )


def test_expired_programme_links_to_programmes_page() -> None:
    assert is_expired("2020-01-01T00:00:00Z")
    assert (
        programme_link(_prog("2020-01-01T00:00:00Z"))
        == "https://www.bbc.co.uk/programmes/m002ww40"
    )


def test_available_or_unknown_programme_keeps_sounds_url() -> None:
    sounds = "https://www.bbc.co.uk/sounds/play/m002ww40"
    assert programme_link(_prog("2999-01-01T00:00:00Z")) == sounds
    assert programme_link(_prog("")) == sounds
    assert programme_link({"pid": "x", "url": sounds, "available_until": ""}) == sounds
